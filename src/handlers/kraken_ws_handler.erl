%%%-------------------------------------------------------------------
%% @doc WebSocket Handler
%% Handles NoLag WebSocket connections, authenticates with Titus,
%% and bridges to EMQX for pub/sub.
%% Supports room-level presence and lobby subscriptions.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_ws_handler).
-behaviour(cowboy_websocket).

-export([init/2]).
-export([websocket_init/1]).
-export([websocket_handle/2]).
-export([websocket_info/2]).
-export([terminate/3]).

%% Revalidation interval in seconds (~10 minutes)
-define(REVALIDATION_INTERVAL_SEC, 600).

%% Rate limiting defaults (messages per second)
-define(DEFAULT_RATE_LIMIT, 50).
-define(MAX_MESSAGE_SIZE, 921600).  %% 900KB flat platform ceiling (all plans)
-define(PROTOCOL_VERSION, 2).  %% v2: loud failures (42940), published acks, auto-provisioned rooms
-define(MAX_FRAME_SIZE, 1048576).  %% 1MB Cowboy hard cap; ~124KB envelope headroom over the payload ceiling

%% Connection state record
-record(state, {
    authenticated = false :: boolean(),
    actor_token_id :: binary() | undefined,
    connection_id :: binary() | undefined,  %% Unique per-connection ID for no-echo filtering
    organization_id :: binary() | undefined,
    project_id :: binary() | undefined,
    actor_type :: binary() | undefined,
    allowed_topics = [] :: list(),  %% Flattened topics from all apps (for ACL)
    allowed_lobbies = [] :: list(),  %% Lobbies this actor can subscribe to
    apps = [] :: list(),  %% Multi-app structure with per-app webhooks
    mqtt_client :: pid() | undefined,
    kraken_store :: enabled | undefined,  %% Firestore writer status (centralized gen_server)
    presence :: map() | undefined,
    current_room_id :: binary() | undefined,  %% Room actor has presence in
    subscribed_lobbies = [] :: list(),  %% Currently subscribed lobbies: [{Slug, [UUID]}]
    lobby_slug_map = #{} :: map(),  %% slug => [LobbyUUID] built from allowed_lobbies
    %% Timestamp-based revalidation tracking
    last_validation_at :: erlang:timestamp() | undefined,
    revalidation_in_progress = false :: boolean(),
    %% Message replay state
    replay_state = none :: none | buffering | replaying | done,
    replay_buffer = [] :: list(),  %% Messages buffered during replay
    replayed_msg_ids = [] :: list(),  %% Message IDs already replayed (for dedup)
    %% Rate limiting
    rate_limit = ?DEFAULT_RATE_LIMIT :: non_neg_integer(),
    msg_count = 0 :: non_neg_integer(),  %% Messages this second
    rate_limit_second = 0 :: non_neg_integer(),  %% Current second (erlang:system_time(second))
    %% Filter tracking: #{DisplayTopic => [FilterValue]}
    topic_filters = #{} :: map(),
    %% Negotiated protocol version (1 = legacy silent semantics,
    %% 2 = loud failures: 42940 unknown_topic, published acks)
    protocol_version = 1 :: pos_integer(),
    %% Connection limit for this organization
    max_connections = unlimited :: non_neg_integer() | unlimited,
    %% Per-plan message size limit in bytes
    max_message_size_bytes = ?MAX_MESSAGE_SIZE :: non_neg_integer(),
    %% Persistent session config (for agent/orchestrator actors)
    persistent_session = false :: boolean(),
    session_expiry_seconds = 3600 :: non_neg_integer(),
    %% Persistent Presence: true once this connection advertised persistent
    %% mode, so terminate/3 soft-offlines the durable record instead of dropping it
    persistent_presence = false :: boolean(),
    %% Access scope for tenant isolation (injected into topic patterns)
    scope_slug :: binary() | undefined,
    scope_id :: binary() | undefined,
    scope_name :: binary() | undefined
}).

%%====================================================================
%% Cowboy WebSocket callbacks
%%====================================================================

init(Req, _Opts) ->
    %% Upgrade to WebSocket
    %% Note: compress disabled as it can cause "Invalid frame header" errors with some clients
    {cowboy_websocket, Req, #state{}, #{
        idle_timeout => 60000,
        compress => false,
        max_frame_size => ?MAX_FRAME_SIZE  %% 1MB hard cap; plan-based check enforces actual limit
    }}.

websocket_init(State) ->
    kraken_log:info("[WS] Connection opened~n", []),
    %% Start per-connection Firestore writer (lazy connect)
    {ok, WriterPid} = kraken_store:start_writer(),
    {ok, State#state{kraken_store = WriterPid}}.

%% Handle empty binary (heartbeat) - respond with empty if authenticated
%% Also triggers periodic token revalidation based on elapsed time (~10 min)
websocket_handle({binary, <<>>}, #state{authenticated = true} = State) ->
    Now = erlang:timestamp(),

    %% Check if revalidation is needed (>= 10 minutes since last validation)
    NeedsRevalidation = case State#state.last_validation_at of
        undefined -> true;
        LastTime ->
            ElapsedSeconds = timer:now_diff(Now, LastTime) div 1000000,
            ElapsedSeconds >= ?REVALIDATION_INTERVAL_SEC
    end,

    State1 = case NeedsRevalidation andalso not State#state.revalidation_in_progress of
        true ->
            %% Time for revalidation - spawn async process
            WsPid = self(),
            ActorTokenId = State#state.actor_token_id,
            spawn(fun() ->
                async_revalidate(WsPid, ActorTokenId)
            end),
            %% Mark revalidation in progress
            State#state{revalidation_in_progress = true};
        false ->
            %% Not time yet or already in progress
            State
    end,
    {reply, {binary, <<>>}, State1};

%% Ignore heartbeat if not authenticated
websocket_handle({binary, <<>>}, State) ->
    {ok, State};

%% Handle binary messages (MessagePack)
websocket_handle({binary, Data}, State) ->
    case msgpack:unpack(Data, [{unpack_str, as_binary}]) of
        {ok, Message} ->
            handle_message(Message, State);
        {error, Reason} ->
            kraken_log:info("[WS] Failed to decode msgpack: ~p~n", [Reason]),
            {ok, State}
    end;

%% Handle text messages (JSON fallback)
websocket_handle({text, Data}, State) ->
    case jsx:decode(Data, [return_maps]) of
        Message when is_map(Message) ->
            handle_message(Message, State);
        _ ->
            kraken_log:info("[WS] Failed to decode JSON~n", []),
            {ok, State}
    end;

websocket_handle(_Frame, State) ->
    {ok, State}.

%% Handle internal messages

%% Store topic mapping from kraken_broker
%% We store both directions: MqttTopic -> DisplayTopic and DisplayTopic -> MqttTopic
%% This allows unsubscribe to find the actual MQTT topic (which may have $share/ prefix)
%% For shared subscriptions, we also store the base topic mapping since EMQX delivers
%% messages on the base topic, not the $share/... topic
websocket_info({store_topic_mapping, MqttTopic, DisplayTopic}, State) ->
    put({topic_mapping, MqttTopic}, DisplayTopic),
    put({mqtt_topic_for, DisplayTopic}, MqttTopic),
    %% For shared subscriptions, extract and store the base topic mapping
    %% $share/group/actual/topic -> actual/topic (group may contain slashes like proj/app/group)
    case MqttTopic of
        <<"$share/", Rest/binary>> ->
            %% The base topic starts after the group. Find it by looking for the room UUID pattern
            %% or just store based on the DisplayTopic's internal topic which we can derive
            %% Actually, easier: the base topic is everything after $share/group/ where group
            %% is the load balance group. We need to find where the actual topic starts.
            %% For now, let's extract from the end - the base topic pattern is UUID/topic-name
            BaseTopic = extract_base_topic_from_share(Rest),
            case BaseTopic of
                undefined -> ok;
                _ ->
                    put({topic_mapping, BaseTopic}, DisplayTopic)
            end;
        _ ->
            ok
    end,
    {ok, State};

%% Handle MQTT publish from emqtt msg_handler
websocket_info({mqtt_publish, #{topic := MqttTopic, payload := Payload}},
               #state{connection_id = ConnectionId, actor_token_id = ActorTokenId,
                      kraken_store = FirestoreWriter} = State) ->
    %% Look up the display topic — try exact match first, then parent topic lookup
    DisplayTopic = case get({topic_mapping, MqttTopic}) of
        undefined ->
            %% Try stripping last segment (filter value) to find parent topic mapping
            case find_display_topic_by_prefix(MqttTopic) of
                undefined -> MqttTopic;
                DT -> DT
            end;
        DT ->
            DT
    end,
    %% Extract filter value from MQTT topic by comparing with base topic
    FilterValue = extract_filter_from_mqtt_topic(MqttTopic, DisplayTopic),
    %% Decode the payload (it's msgpack encoded)
    case msgpack:unpack(Payload, [{unpack_str, as_binary}]) of
        {ok, Decoded} ->
            %% Extract msgId and actual data if present (for delivery tracking)
            {MsgId, ActualPayload} = extract_msg_id_and_data(Decoded),
            %% Check if this is a no-echo envelope with sender info
            case ActualPayload of
                #{<<"_sender">> := Sender, <<"data">> := _} when Sender =:= ConnectionId ->
                    %% Message is from this connection and echo=false, drop it
                    {ok, State};
                #{<<"_sender">> := Sender, <<"data">> := InnerData} ->
                    %% Message has sender info but it's from another connection, forward it
                    maybe_log_delivery(FirestoreWriter, MsgId, ActorTokenId, DisplayTopic),
                    forward_message_with_filter(DisplayTopic, InnerData, MsgId, FilterValue, State);
                _ ->
                    %% Regular message without sender info (echo=true), forward as-is
                    maybe_log_delivery(FirestoreWriter, MsgId, ActorTokenId, DisplayTopic),
                    forward_message_with_filter(DisplayTopic, ActualPayload, MsgId, FilterValue, State)
            end;
        {error, DecodeError} ->
            %% Decode failed, send raw
            kraken_log:info("[WS] Msgpack decode error: ~p~n", [DecodeError]),
            forward_message_with_filter(DisplayTopic, Payload, undefined, FilterValue, State)
    end;

%% Legacy format (in case it's still used somewhere)
websocket_info({mqtt_message, Topic, Payload}, State) ->
    %% Forward MQTT message to WebSocket client
    Response = #{
        <<"type">> => <<"message">>,
        <<"topic">> => Topic,
        <<"data">> => Payload
    },
    {reply, {binary, pack_msg(Response)}, State};

%% Handle room presence event (from kraken_presence)
websocket_info({presence_event, EventType, EventData}, State) ->
    %% Forward presence event to WebSocket client
    Response = #{
        <<"type">> => <<"presence">>,
        <<"event">> => atom_to_binary(EventType),
        <<"data">> => EventData
    },
    {reply, {binary, pack_msg(Response)}, State};

%% Handle lobby presence event (from kraken_presence - with room context)
websocket_info({lobby_presence_event, EventType, EventData},
               #state{allowed_topics = AllowedTopics} = State) ->
    %% Forward lobby presence event to WebSocket client (includes roomId and lobbyId)
    %% Resolve room UUID back to slug so the client SDK can match its room map
    RoomUUID = maps:get(room_id, EventData, null),
    RoomSlug = resolve_room_slug(RoomUUID, AllowedTopics),
    Response = #{
        <<"type">> => <<"lobbyPresence">>,
        <<"event">> => atom_to_binary(EventType),
        <<"lobbyId">> => maps:get(lobby_id, EventData, null),
        <<"roomId">> => RoomSlug,
        <<"actorId">> => maps:get(actor_token_id, EventData, null),
        <<"data">> => maps:get(presence, EventData, #{})
    },
    {reply, {binary, pack_msg(Response)}, State};

%% Handle revalidation success - update session with new data
websocket_info({revalidation_success, AuthData}, State) ->
    NewAllowedTopics = maps:get(allowed_topics, AuthData, State#state.allowed_topics),
    NewAllowedLobbies = maps:get(allowed_lobbies, AuthData, State#state.allowed_lobbies),
    NewLobbySlugMap = build_lobby_slug_map(NewAllowedLobbies),
    NewApps = maps:get(apps, AuthData, State#state.apps),
    NewMaxConn = maps:get(max_connections, AuthData, State#state.max_connections),
    NewMaxMsgSize = maps:get(max_message_size_bytes, AuthData, State#state.max_message_size_bytes),
    NewScopeId = maps:get(scope_id, AuthData, State#state.scope_id),
    NewScopeSlug = maps:get(scope_slug, AuthData, State#state.scope_slug),
    NewScopeName = maps:get(scope_name, AuthData, State#state.scope_name),
    OrgId = State#state.organization_id,
    NewState = State#state{
        allowed_topics = NewAllowedTopics,
        allowed_lobbies = NewAllowedLobbies,
        lobby_slug_map = NewLobbySlugMap,
        apps = NewApps,
        last_validation_at = erlang:timestamp(),
        revalidation_in_progress = false,
        max_connections = NewMaxConn,
        max_message_size_bytes = NewMaxMsgSize,
        scope_id = NewScopeId,
        scope_slug = NewScopeSlug,
        scope_name = NewScopeName
    },
    %% Check if org is now over limit after plan downgrade
    case check_connection_limit(OrgId, NewMaxConn) of
        ok ->
            {ok, NewState};
        {error, limit_reached} ->
            kraken_log:info("[WS] Org ~s now over connection limit (~p) after revalidation - disconnecting~n",
                [OrgId, NewMaxConn]),
            {reply, {close, 4002, <<"connection_limit_reached">>}, NewState}
    end;

%% Handle revalidation failure - disconnect client
websocket_info({revalidation_failed, Reason}, State) ->
    kraken_log:info("[WS] Revalidation failed for ~s: ~s - disconnecting~n",
        [State#state.actor_token_id, Reason]),
    %% Send disconnect message to client before closing
    Response = #{
        <<"type">> => <<"disconnect">>,
        <<"reason">> => Reason
    },
    {reply, {close, 4001, Reason}, State};

%% Handle revalidation retry (server/network error) - will try again next cycle
websocket_info({revalidation_retry, _Reason}, State) ->
    NewState = State#state{revalidation_in_progress = false},
    {ok, NewState};

%% Handle hydration data from webhook
websocket_info({hydration_data, TopicName, Data}, State) ->
    Response = #{
        <<"type">> => <<"hydration">>,
        <<"topic">> => TopicName,
        <<"data">> => Data
    },
    {reply, {binary, pack_msg(Response)}, State};

%% Handle hydration error from webhook (log but don't fail - hydration is best-effort)
websocket_info({hydration_error, TopicName, Error}, State) ->
    kraken_log:info("[WS] Hydration failed for ~s: ~s~n", [TopicName, Error]),
    {ok, State};

%% Handle replay complete - flush buffer and switch to live delivery
websocket_info({replay_complete, ActorId, ReplayedIds}, #state{actor_token_id = ActorId,
                                                                replay_buffer = Buffer} = State) ->
    %% Convert replayed IDs to a set for fast lookup
    ReplayedSet = sets:from_list(ReplayedIds),

    %% Filter buffer to remove already-replayed messages
    NewMessages = lists:filter(fun(Msg) ->
        MsgId = maps:get(<<"msgId">>, Msg, maps:get(<<"messageId">>, Msg, undefined)),
        not sets:is_element(MsgId, ReplayedSet)
    end, Buffer),

    %% Send buffered messages (not replay)
    lists:foreach(fun(Msg) ->
        MsgWithFlag = Msg#{<<"isReplay">> => false},
        self() ! {send_to_client, MsgWithFlag}
    end, NewMessages),

    %% Clear replay state
    NewState = State#state{
        replay_state = done,
        replay_buffer = [],
        replayed_msg_ids = ReplayedIds
    },
    {ok, NewState};

%% Handle send_to_client message (from replay service)
websocket_info({send_to_client, Message}, State) ->
    {reply, {binary, pack_msg(Message)}, State};

%% Replay progress (from kraken_replay worker): advance the live-buffering
%% gate from buffering -> replaying, and record the replayed ids for dedup.
websocket_info({update_replay_status, ActorId, Status},
               #state{actor_token_id = ActorId} = State) ->
    {ok, State#state{replay_state = Status}};
websocket_info({update_replayed_ids, ActorId, Ids},
               #state{actor_token_id = ActorId} = State) ->
    IdList = case is_list(Ids) of true -> Ids; false -> sets:to_list(Ids) end,
    {ok, State#state{replayed_msg_ids = IdList}};

websocket_info(_Info, State) ->
    {ok, State}.

terminate(Reason, _Req, #state{mqtt_client = MqttClient, current_room_id = RoomId,
                                actor_token_id = ActorTokenId,
                                organization_id = OrganizationId,
                                project_id = ProjectId,
                                authenticated = Authenticated,
                                subscribed_lobbies = SubscribedLobbies,
                                persistent_presence = PersistentPresence,
                                allowed_topics = AllowedTopics,
                                kraken_store = FirestoreWriter} = _State) ->
    kraken_log:info("[WS] Connection closed~n", []),

    %% Log disconnection to Firestore (only if authenticated)
    case Authenticated of
        true ->
            kraken_store:log_event(disconnection, #{
                actor_token_id => ActorTokenId,
                organization_id => OrganizationId,
                project_id => ProjectId,
                reason => format_disconnect_reason(Reason)
            });
        false ->
            ok
    end,

    %% Leave connection tracking group
    case OrganizationId of
        undefined -> ok;
        _ -> syn:leave(kraken_connections, {org, OrganizationId}, self())
    end,
    %% Leave room presence group if set
    case RoomId of
        undefined -> ok;
        _ ->
            kraken_presence:leave_room_presence(RoomId, ActorTokenId),
            %% Persistent Presence: soft-offline the durable record (kept discoverable + wakeable)
            pp_offline(PersistentPresence, RoomId, ActorTokenId, AllowedTopics)
    end,
    %% Durable delivery: record the offline boundary (cursor = now) for any
    %% load-balanced durable subscription, so a reconnecting group member
    %% replays ONLY messages dispatched while offline — not the window it
    %% already consumed live.
    dd_mark_offline_boundary(),
    %% Leave all subscribed lobbies (each entry is {Slug, [UUID]})
    lists:foreach(fun({_Slug, UUIDs}) ->
        lists:foreach(fun(UUID) ->
            kraken_presence:leave_lobby(UUID, ActorTokenId)
        end, UUIDs)
    end, SubscribedLobbies),
    %% Disconnect MQTT client
    case MqttClient of
        undefined -> ok;
        Pid -> catch kraken_broker:disconnect(Pid)
    end,
    %% Stop Firestore writer process
    kraken_store:stop_writer(FirestoreWriter),
    ok.

%%====================================================================
%% Message handling
%%====================================================================

%% Handle authentication message
handle_message(#{<<"type">> := <<"auth">>, <<"token">> := Token} = Message, State) ->
    %% Check if this is a reconnect (client wants subscriptions restored) or fresh connect
    %% Only restore subscriptions if reconnect is explicitly true
    %% Absence of reconnect flag = fresh connect (default behavior)
    IsReconnect = maps:get(<<"reconnect">>, Message, false) =:= true,

    %% Protocol version negotiation: absent => 1 (legacy). The negotiated
    %% version is min(client, server) and is echoed in the auth response.
    ClientProtocolVersion = case maps:get(<<"protocolVersion">>, Message, 1) of
        V when is_integer(V), V >= 1 -> V;
        _ -> 1
    end,
    NegotiatedVersion = min(ClientProtocolVersion, ?PROTOCOL_VERSION),

    %% Get client-provided projectId for debug logging (pre-auth events)
    %% This is only used for logging purposes, not for authorization
    ClientProjectId = maps:get(<<"projectId">>, Message, undefined),

    %% Log connection attempt to Firestore (uses client-provided projectId if available)
    kraken_store:log_event(connection_attempt, #{
        token_preview => token_preview(Token),
        is_reconnect => IsReconnect,
        project_id => ClientProjectId
    }),

    case kraken_auth:validate_token(Token) of
        {ok, AuthData} ->
            %% Debug: log full auth data structure
            kraken_log:info("[WS][Auth] validate_token OK - keys: ~p", [maps:keys(AuthData)]),
            kraken_log:info("[WS][Auth] apps count: ~p, allowed_topics count: ~p",
                [length(maps:get(apps, AuthData, [])), length(maps:get(allowed_topics, AuthData, []))]),
            %% Log first few allowed topics for debugging
            DebugTopics = lists:sublist(maps:get(allowed_topics, AuthData, []), 3),
            kraken_log:info("[WS][Auth] first allowed_topics: ~p", [DebugTopics]),
            %% Extract persistent session config for agent/orchestrator actors
            PersistentSession = maps:get(persistent_session, AuthData, false),
            SessionExpiry = maps:get(session_expiry_seconds, AuthData, 3600),
            %% Connect to EMQX (returns unique ConnectionId for no-echo filtering)
            case kraken_broker:connect(AuthData, PersistentSession, SessionExpiry) of
            {ok, MqttClient, ConnectionId} ->

            AllowedTopics = maps:get(allowed_topics, AuthData, []),
            Apps = maps:get(apps, AuthData, []),
            MaxConn = maps:get(max_connections, AuthData, unlimited),
            OrgId = maps:get(organization_id, AuthData, undefined),

            %% Check per-organization connection limit before proceeding
            case check_connection_limit(OrgId, MaxConn) of
            ok ->
            %% Under limit — join connection tracking group
            ActorTokenIdForSyn = maps:get(actor_token_id, AuthData),
            syn:join(kraken_connections, {org, OrgId}, self(), #{
                actor_token_id => ActorTokenIdForSyn
            }),

            AllowedLobbies = maps:get(allowed_lobbies, AuthData, []),
            LobbySlugMap = build_lobby_slug_map(AllowedLobbies),
            MaxMsgSize = maps:get(max_message_size_bytes, AuthData, ?MAX_MESSAGE_SIZE),
            ScopeSlug = maps:get(scope_slug, AuthData, undefined),
            ScopeId = maps:get(scope_id, AuthData, undefined),
            ScopeName = maps:get(scope_name, AuthData, undefined),
            NewState = State#state{
                authenticated = true,
                actor_token_id = ActorTokenIdForSyn,
                connection_id = ConnectionId,
                organization_id = OrgId,
                project_id = maps:get(project_id, AuthData),
                actor_type = maps:get(actor_type, AuthData),
                allowed_topics = AllowedTopics,
                allowed_lobbies = AllowedLobbies,
                apps = Apps,
                mqtt_client = MqttClient,
                last_validation_at = erlang:timestamp(),
                max_connections = MaxConn,
                lobby_slug_map = LobbySlugMap,
                max_message_size_bytes = MaxMsgSize,
                persistent_session = PersistentSession,
                session_expiry_seconds = SessionExpiry,
                scope_slug = ScopeSlug,
                scope_id = ScopeId,
                scope_name = ScopeName,
                protocol_version = NegotiatedVersion
            },

            %% Log connection success to Firestore
            kraken_store:log_event(connection_success, #{
                actor_token_id => ActorTokenIdForSyn,
                organization_id => OrgId,
                project_id => maps:get(project_id, AuthData),
                actor_type => maps:get(actor_type, AuthData),
                is_reconnect => IsReconnect,
                topics_count => length(AllowedTopics),
                apps_count => length(Apps)
            }),

            %% Only restore subscriptions if this is a reconnect, not a fresh connect
            %% Fresh connects should start with no subscriptions - client will subscribe as needed
            {RestoredSubscriptions, ResponseSubs} = case IsReconnect of
                true ->
                    ActiveSubscriptions = maps:get(active_subscriptions, AuthData, []),
                    restore_subscriptions(MqttClient, ActiveSubscriptions, NewState, self()),
                    {ActiveSubscriptions, ActiveSubscriptions};
                false ->
                    {[], []}
            end,

            %% Collect pending filter state from restore_subscriptions
            StateWithFilters = collect_pending_filters(NewState),

            Response = #{
                <<"type">> => <<"auth">>,
                <<"success">> => true,
                <<"actorTokenId">> => NewState#state.actor_token_id,
                <<"projectId">> => NewState#state.project_id,
                <<"actorType">> => NewState#state.actor_type,
                <<"protocolVersion">> => NegotiatedVersion,
                <<"restoredSubscriptions">> => ResponseSubs
            },
            {reply, {binary, pack_msg(Response)}, StateWithFilters};

            {error, limit_reached} ->
                %% Over connection limit — disconnect MQTT and reject
                catch kraken_broker:disconnect(MqttClient),
                kraken_log:info("[WS] Connection limit reached for org ~s (max: ~p)~n", [OrgId, MaxConn]),
                kraken_store:log_event(connection_rejected, #{
                    reason => <<"connection_limit_reached">>,
                    organization_id => OrgId,
                    project_id => maps:get(project_id, AuthData),
                    max_connections => MaxConn
                }),
                LimitResponse = #{<<"type">> => <<"auth">>, <<"success">> => false,
                                   <<"error">> => <<"connection_limit_reached">>},
                {reply, {binary, pack_msg(LimitResponse)}, State}
            end;

            {error, MqttReason} ->
                kraken_log:info("[WS] MQTT bridge connect failed: ~p~n", [MqttReason]),
                BrokerResponse = #{<<"type">> => <<"auth">>, <<"success">> => false,
                                   <<"error">> => <<"broker_unavailable">>},
                {reply, {binary, pack_msg(BrokerResponse)}, State}
            end;

        {error, Reason} ->
            %% Log connection rejection to Firestore (uses client-provided projectId if available)
            kraken_store:log_event(connection_rejected, #{
                reason => Reason,
                token_preview => token_preview(Token),
                project_id => ClientProjectId
            }),
            Response = #{
                <<"type">> => <<"auth">>,
                <<"success">> => false,
                <<"error">> => Reason
            },
            {reply, {binary, pack_msg(Response)}, State}
    end;

%% Handle subscribe message (with optional load balancing)
%% This is idempotent - if already subscribed, will switch modes if needed
handle_message(#{<<"type">> := <<"subscribe">>, <<"topic">> := Pattern} = Message,
               #state{authenticated = true, actor_token_id = ActorTokenId,
                      project_id = ProjectId, mqtt_client = MqttClient,
                      allowed_topics = AllowedTopics, apps = Apps,
                      scope_slug = ScopeSlug} = State) ->
    %% If actor has a scope, try rewriting the pattern to include scope slug
    EffectivePattern = maybe_inject_scope(Pattern, ScopeSlug, AllowedTopics),
    case kraken_acl:can_subscribe(EffectivePattern, AllowedTopics) of
        true ->
            %% Unified resolution (kraken_topics): exact internal topic, or a
            %% deterministic app-scoped fallback for wildcard-matched patterns.
            %% Never the shared `unknown/` namespace, and always keyed on the
            %% EFFECTIVE pattern so subscribe and publish agree.
            {MqttBaseTopic, ResRoomId, AppId, ResKind} =
                case kraken_topics:resolve(EffectivePattern, AllowedTopics) of
                    {exact, IT, RId, AId} -> {IT, RId, AId, exact};
                    {wildcard, FT, AId, _Rule} ->
                        kraken_log:info("[WS] Wildcard fallback subscribe: ~s -> ~s (actor ~s)",
                            [EffectivePattern, FT, ActorTokenId]),
                        {FT, undefined, AId, wildcard};
                    no_match ->
                        %% Unreachable when ACL passed, but stay safe
                        FT0 = kraken_topics:fallback_topic(<<"unscoped">>, EffectivePattern),
                        {FT0, undefined, <<"unscoped">>, wildcard}
                end,
            App = find_app_by_id(AppId, Apps),

            %% Check for load balancing option
            LoadBalance = maps:get(<<"loadBalance">>, Message, false) =:= true,
            ClientLoadBalanceGroup0 = maps:get(<<"loadBalanceGroup">>, Message, ActorTokenId),
            %% Handle null from JSON (jsx decodes null as atom 'null')
            ClientLoadBalanceGroup = case ClientLoadBalanceGroup0 of
                null -> ActorTokenId;
                undefined -> ActorTokenId;
                _ -> ClientLoadBalanceGroup0
            end,

            %% Scope load balance group by project AND app to prevent cross-app collision
            %% Two clients using the same group name in different apps won't interfere
            %% IMPORTANT: Use underscore separator, NOT slash! In EMQX $share/<group>/<topic>,
            %% the <group> is everything between $share/ and the next /, so slashes in group
            %% would be interpreted as part of the topic.
            LoadBalanceGroup = <<ProjectId/binary, "_", AppId/binary, "_", ClientLoadBalanceGroup/binary>>,

            %% Build the actual MQTT topic (with shared subscription prefix if load balanced)
            NewMqttTopic = case LoadBalance of
                true ->
                    %% EMQX shared subscription format: $share/group/topic
                    <<"$share/", LoadBalanceGroup/binary, "/", MqttBaseTopic/binary>>;
                false ->
                    MqttBaseTopic
            end,
            %% Check if already subscribed to this topic (possibly with different mode)
            %% If so, unsubscribe from old MQTT topic(s) first to switch modes
            ExistingMqttTopics = get({mqtt_topics_for, Pattern}),
            case ExistingMqttTopics of
                undefined ->
                    %% Not subscribed yet
                    ok;
                OldTopicList when is_list(OldTopicList) ->
                    %% Unsubscribe from all old MQTT topics
                    lists:foreach(fun(OldT) ->
                        kraken_broker:unsubscribe(MqttClient, OldT),
                        erase({topic_mapping, OldT})
                    end, OldTopicList)
            end,
            %% Also clean up legacy single-topic mapping if present
            case get({mqtt_topic_for, Pattern}) of
                undefined -> ok;
                LegacyOldTopic ->
                    case ExistingMqttTopics of
                        undefined ->
                            kraken_broker:unsubscribe(MqttClient, LegacyOldTopic),
                            erase({topic_mapping, LegacyOldTopic});
                        _ ->
                            ok  %% Already cleaned up above
                    end,
                    erase({mqtt_topic_for, Pattern})
            end,

            %% Extract QoS from client message (default 1), validate 0-2
            ReqQoS = maps:get(<<"qos">>, Message, 1),
            QoS = case ReqQoS of
                N when is_integer(N), N >= 0, N =< 2 -> N;
                _ -> 1
            end,

            %% Extract filters from subscribe message
            Filters = maps:get(<<"filters">>, Message, undefined),
            ValidatedFilters = validate_filters(Filters),

            %% Subscribe based on filters
            {NewMqttTopics, NewState} = case ValidatedFilters of
                {ok, FilterList} when is_list(FilterList), length(FilterList) > 0 ->
                    %% Normalize AND groups into composite strings
                    NormalizedFilters = normalize_filters(FilterList),
                    %% Subscribe to each filter as a sub-topic
                    FilterMqttTopics = lists:map(fun(Filter) ->
                        FilterMqttBase = <<MqttBaseTopic/binary, "/", Filter/binary>>,
                        case LoadBalance of
                            true -> <<"$share/", LoadBalanceGroup/binary, "/", FilterMqttBase/binary>>;
                            false -> FilterMqttBase
                        end
                    end, NormalizedFilters),
                    LegacyFilterTopics = legacy_compat_topics(ResKind, EffectivePattern, NormalizedFilters, LoadBalance, LoadBalanceGroup),
                    lists:foreach(fun(FMqttTopic) ->
                        ok = kraken_broker:subscribe(MqttClient, FMqttTopic, Pattern, self(), QoS)
                    end, FilterMqttTopics ++ LegacyFilterTopics),
                    NewTF = maps:put(Pattern, FilterList, State#state.topic_filters),
                    {FilterMqttTopics ++ LegacyFilterTopics, State#state{topic_filters = NewTF}};
                {ok, _} ->
                    %% No filters — subscribe to wildcard
                    WildcardTopic = <<MqttBaseTopic/binary, "/#">>,
                    WildcardMqttTopic = case LoadBalance of
                        true -> <<"$share/", LoadBalanceGroup/binary, "/", WildcardTopic/binary>>;
                        false -> WildcardTopic
                    end,
                    LegacyWildTopics = legacy_compat_topics(ResKind, EffectivePattern, wildcard, LoadBalance, LoadBalanceGroup),
                    lists:foreach(fun(LT) ->
                        ok = kraken_broker:subscribe(MqttClient, LT, Pattern, self(), QoS)
                    end, LegacyWildTopics),
                    ok = kraken_broker:subscribe(MqttClient, WildcardMqttTopic, Pattern, self(), QoS),
                    {[WildcardMqttTopic | LegacyWildTopics], State};
                {error, FilterError} ->
                    %% Invalid filters, return error
                    ErrorResp = #{
                        <<"type">> => <<"error">>,
                        <<"error">> => FilterError,
                        <<"topic">> => Pattern
                    },
                    {error, {reply, {binary, pack_msg(ErrorResp)}, State}}
            end,

            case NewMqttTopics of
                error ->
                    %% Filter validation error — NewState is the reply tuple
                    NewState;
                _ ->
                    %% Store topic mappings
                    put({mqtt_topics_for, Pattern}, NewMqttTopics),
                    put({base_topic_for, Pattern}, MqttBaseTopic),

                    %% Persist subscription to Titus (async) with load balance and filter info
                    TrackMetadata = #{
                        load_balance => LoadBalance,
                        load_balance_group => LoadBalanceGroup
                    },
                    TrackMetadata1 = case ValidatedFilters of
                        {ok, FL} when is_list(FL), length(FL) > 0 ->
                            maps:put(filters, FL, TrackMetadata);
                        _ ->
                            TrackMetadata
                    end,
                    kraken_subscriptions:track(ActorTokenId, Pattern, subscribe, TrackMetadata1),

                    %% Call hydration webhook if configured (async)
                    %% Per-topic webhook takes precedence, fallback to app-level
                    case App of
                        undefined -> ok;
                        _ ->
                            TopicName0 = extract_topic_name(Pattern),
                            TopicWebhooks = maps:get(<<"topic_webhooks">>, App, #{}),
                            TopicWebhookConfig = maps:get(TopicName0, TopicWebhooks, #{}),
                            HydrationWebhook = case maps:get(<<"on_subscribe">>, TopicWebhookConfig, null) of
                                null -> maps:get(<<"hydration_webhook">>, App, null);
                                Wh -> Wh
                            end,
                            case HydrationWebhook of
                                null -> ok;
                                undefined -> ok;
                                WebhookConfig when is_map(WebhookConfig) ->
                                    RoomName = extract_room_name(Pattern, AllowedTopics),
                                    ScopeInfo = build_scope_info(State),
                                    kraken_webhooks:call_hydration(self(), WebhookConfig, ActorTokenId, RoomName, TopicName0, ScopeInfo)
                            end
                    end,

                    %% Durable delivery: remember a load-balanced subscription so a
                    %% later persistent signal (persistent_session here, OR a
                    %% persistent-presence advertise) can replay the messages the
                    %% group missed while scaled to zero, claiming each one. We
                    %% can't replay at subscribe-only because the SDK advertises
                    %% persistence AFTER subscribing.
                    case LoadBalance of
                        true -> remember_lb_subscription(Pattern, ResRoomId, AppId, LoadBalanceGroup);
                        false -> ok
                    end,
                    NewState2 = case NewState#state.persistent_session of
                        true -> maybe_replay_lb_subs(NewState);
                        false -> NewState
                    end,

                    Response = #{
                        <<"type">> => <<"subscribed">>,
                        <<"topic">> => Pattern,
                        <<"loadBalance">> => LoadBalance
                    },
                    {reply, {binary, pack_msg(Response)}, NewState2}
            end;
        false ->
            %% Cache miss: the room isn't in this connection's cached
            %% allowed_topics. It may have been created (or granted) AFTER the
            %% actor connected. Ask the control plane once; on allow, merge and
            %% re-dispatch so the actor subscribes without reconnecting.
            case maybe_cache_miss_fallback(ActorTokenId, Pattern, ScopeSlug, AllowedTopics) of
                {ok, MergedTopics} ->
                    kraken_log:info("[WS] Cache-miss fallback granted subscribe to ~s (actor ~s)",
                        [Pattern, ActorTokenId]),
                    handle_message(Message, State#state{allowed_topics = MergedTopics});
                deny ->
                    %% Unknown/unauthorized room → LOUD. Rooms are never created
                    %% implicitly on the data path (that silently hid typo'd and
                    %% asymmetric slugs); they must be provisioned explicitly via
                    %% the control-plane rooms API before use.
                    kraken_log:info("[WS] ACL denied subscribe to ~s - allowed_topics: ~p~n",
                        [Pattern, [maps:get(<<"pattern">>, T, <<>>) || T <- AllowedTopics]]),
                    ErrFrame = unknown_topic_frame(State#state.protocol_version),
                    {reply, {binary, pack_msg(ErrFrame#{<<"topic">> => Pattern})}, State}
            end
    end;

%% Handle unsubscribe message
handle_message(#{<<"type">> := <<"unsubscribe">>, <<"topic">> := Pattern},
               #state{authenticated = true, actor_token_id = ActorTokenId,
                      mqtt_client = MqttClient} = State) ->
    %% Unsubscribe from all MQTT topics for this display topic (filters or wildcard)
    case get({mqtt_topics_for, Pattern}) of
        undefined ->
            %% Fallback: try legacy single-topic mapping
            MqttTopic = case get({mqtt_topic_for, Pattern}) of
                undefined -> Pattern;
                StoredMqttTopic -> StoredMqttTopic
            end,
            kraken_broker:unsubscribe(MqttClient, MqttTopic),
            erase({mqtt_topic_for, Pattern}),
            erase({topic_mapping, MqttTopic});
        MqttTopicList when is_list(MqttTopicList) ->
            lists:foreach(fun(MT) ->
                kraken_broker:unsubscribe(MqttClient, MT),
                erase({topic_mapping, MT})
            end, MqttTopicList),
            erase({mqtt_topics_for, Pattern}),
            erase({mqtt_topic_for, Pattern})
    end,
    erase({base_topic_for, Pattern}),
    %% Clean up filter state
    NewTopicFilters = maps:remove(Pattern, State#state.topic_filters),
    %% Persist unsubscription to Titus (async) - use pattern for tracking
    kraken_subscriptions:track(ActorTokenId, Pattern, unsubscribe),
    Response = #{
        <<"type">> => <<"unsubscribed">>,
        <<"topic">> => Pattern
    },
    {reply, {binary, pack_msg(Response)}, State#state{topic_filters = NewTopicFilters}};

%% Handle setFilters message — dynamically change filters for an existing subscription
handle_message(#{<<"type">> := <<"setFilters">>, <<"topic">> := Pattern, <<"filters">> := NewFilters},
               #state{authenticated = true, actor_token_id = ActorTokenId,
                      mqtt_client = MqttClient, project_id = ProjectId,
                      allowed_topics = AllowedTopics, apps = Apps} = State) ->
    MqttBaseTopic = get({base_topic_for, Pattern}),
    case MqttBaseTopic of
        undefined ->
            %% Not subscribed to this topic
            ErrorResp = #{
                <<"type">> => <<"error">>,
                <<"error">> => <<"not_subscribed">>,
                <<"topic">> => Pattern
            },
            {reply, {binary, pack_msg(ErrorResp)}, State};
        _ ->
            case validate_filters(NewFilters) of
                {error, FilterError} ->
                    ErrorResp = #{
                        <<"type">> => <<"error">>,
                        <<"error">> => FilterError,
                        <<"topic">> => Pattern
                    },
                    {reply, {binary, pack_msg(ErrorResp)}, State};
                {ok, ValidatedNewFilters} ->
                    %% Determine current subscription mode
                    CurrentMqttTopics = case get({mqtt_topics_for, Pattern}) of
                        undefined -> [];
                        L -> L
                    end,
                    CurrentFilters = maps:get(Pattern, State#state.topic_filters, []),

                    %% Determine load balance prefix from existing MQTT topics
                    SharePrefix = extract_share_prefix(CurrentMqttTopics),

                    %% Normalize AND groups into composite strings
                    NormalizedNewFilters = normalize_filters(ValidatedNewFilters),

                    %% Build new MQTT topics
                    {NewMqttTopics, NewFilterList} = case NormalizedNewFilters of
                        [] ->
                            %% Switching to wildcard
                            WildcardBase = <<MqttBaseTopic/binary, "/#">>,
                            WT = case SharePrefix of
                                undefined -> WildcardBase;
                                Prefix -> <<Prefix/binary, WildcardBase/binary>>
                            end,
                            {[WT], []};
                        _ ->
                            %% Switching to specific filters
                            FTs = lists:map(fun(F) ->
                                FilterBase = <<MqttBaseTopic/binary, "/", F/binary>>,
                                case SharePrefix of
                                    undefined -> FilterBase;
                                    Prefix -> <<Prefix/binary, FilterBase/binary>>
                                end
                            end, NormalizedNewFilters),
                            {FTs, ValidatedNewFilters}
                    end,

                    %% Subscribe to new topics FIRST (minimize message gap)
                    lists:foreach(fun(NT) ->
                        case lists:member(NT, CurrentMqttTopics) of
                            true -> ok;  %% Already subscribed
                            false -> ok = kraken_broker:subscribe(MqttClient, NT, Pattern, self(), 1)
                        end
                    end, NewMqttTopics),

                    %% Unsubscribe from old topics no longer needed
                    lists:foreach(fun(OT) ->
                        case lists:member(OT, NewMqttTopics) of
                            true -> ok;  %% Still needed
                            false ->
                                kraken_broker:unsubscribe(MqttClient, OT),
                                erase({topic_mapping, OT})
                        end
                    end, CurrentMqttTopics),

                    %% Update mappings
                    put({mqtt_topics_for, Pattern}, NewMqttTopics),

                    %% Update filter state
                    NewTopicFilters = case NewFilterList of
                        [] -> maps:remove(Pattern, State#state.topic_filters);
                        _ -> maps:put(Pattern, NewFilterList, State#state.topic_filters)
                    end,

                    %% Track with Titus
                    TrackMetadata = case NewFilterList of
                        [] -> #{};
                        _ -> #{filters => NewFilterList}
                    end,
                    kraken_subscriptions:track(ActorTokenId, Pattern, subscribe, TrackMetadata),

                    Response = #{
                        <<"type">> => <<"filtersUpdated">>,
                        <<"topic">> => Pattern,
                        <<"filters">> => ValidatedNewFilters
                    },
                    {reply, {binary, pack_msg(Response)}, State#state{topic_filters = NewTopicFilters}}
            end
    end;

%% Handle publish message
handle_message(#{<<"type">> := <<"publish">>, <<"topic">> := Pattern, <<"data">> := Data} = Message,
               #state{authenticated = true, mqtt_client = MqttClient,
                      actor_token_id = ActorTokenId, connection_id = ConnectionId,
                      allowed_topics = AllowedTopics, apps = Apps,
                      organization_id = OrganizationId, project_id = ProjectId,
                      kraken_store = FirestoreWriter,
                      scope_slug = ScopeSlug} = State) ->
    %% Check rate limit first
    case check_rate_limit(State) of
        {error, rate_limited, State1} ->
            Response = #{
                <<"type">> => <<"error">>,
                <<"code">> => 42910,
                <<"error">> => <<"rate_limit_exceeded">>,
                <<"topic">> => Pattern
            },
            {reply, {binary, pack_msg(with_msg_ref(Response, Message))}, State1};
        {ok, State1} ->
            %% Check message size against the flat 900KB platform ceiling
            PackedData = msgpack:pack(Data, [{pack_str, from_binary}]),
            DataSize = iolist_size(PackedData),
            case DataSize > ?MAX_MESSAGE_SIZE of
                true ->
                    SizeResponse = #{
                        <<"type">> => <<"error">>,
                        <<"code">> => 42930,
                        <<"error">> => <<"message_too_large">>,
                        <<"topic">> => Pattern,
                        <<"maxSizeBytes">> => ?MAX_MESSAGE_SIZE
                    },
                    {reply, {binary, pack_msg(with_msg_ref(SizeResponse, Message))}, State1};
                false ->
            %% Check monthly message quota
            case kraken_usage:is_project_blocked(ProjectId) of
                true ->
                    QuotaResponse = #{
                        <<"type">> => <<"error">>,
                        <<"code">> => 42920,
                        <<"error">> => <<"monthly_quota_exceeded">>,
                        <<"topic">> => Pattern
                    },
                    {reply, {binary, pack_msg(with_msg_ref(QuotaResponse, Message))}, State1};
                false ->
            EffPubPattern = maybe_inject_scope(Pattern, ScopeSlug, AllowedTopics),
            case kraken_acl:can_publish(EffPubPattern, AllowedTopics) of
                true ->
                    %% Unified resolution — identical to the subscribe path so
                    %% wildcard-matched publishers and subscribers can never
                    %% split-brain onto different MQTT topics.
                    {MqttBaseTopic0, InternalTopic, RoomId, AppId} =
                        case kraken_topics:resolve(EffPubPattern, AllowedTopics) of
                            {exact, IT, RId, AId} -> {IT, IT, RId, AId};
                            {wildcard, FT, AId, _Rule} ->
                                kraken_log:info("[WS] Wildcard fallback publish: ~s -> ~s (actor ~s)",
                                    [EffPubPattern, FT, ActorTokenId]),
                                {FT, undefined, undefined, AId};
                            no_match ->
                                FT0 = kraken_topics:fallback_topic(<<"unscoped">>, EffPubPattern),
                                {FT0, undefined, undefined, <<"unscoped">>}
                        end,
                    App = find_app_by_id(AppId, Apps),

                    %% Append filter to MQTT topic if specified
                    %% Supports both singular filter (string) and filters (array for AND composite)
                    Filter = maps:get(<<"filter">>, Message, undefined),
                    Filters = maps:get(<<"filters">>, Message, undefined),
                    CompositeFilter = case {Filter, Filters} of
                        {F, _} when is_binary(F), F =/= <<>> -> F;
                        {_, Fs} when is_list(Fs), length(Fs) > 0 ->
                            Lowered = [string:lowercase(F0) || F0 <- Fs, is_binary(F0)],
                            Sorted = lists:sort(Lowered),
                            iolist_to_binary(lists:join(<<"|">>, Sorted));
                        _ -> undefined
                    end,
                    MqttTopic = case CompositeFilter of
                        undefined -> MqttBaseTopic0;
                        null -> MqttBaseTopic0;
                        <<>> -> MqttBaseTopic0;
                        _ -> <<MqttBaseTopic0/binary, "/", CompositeFilter/binary>>
                    end,

                    %% Build context for Firestore logging
                    LogContext = #{
                        organization_id => OrganizationId,
                        project_id => ProjectId,
                        app_id => AppId,
                        room_id => RoomId
                    },

                    %% Record message if enabled and get MessageId
                    %% Use internal topic (room_uuid based) for storage to ensure uniqueness
                    {_, MessageId} = maybe_record_message(FirestoreWriter, Pattern, InternalTopic, Data, iolist_to_binary(PackedData), LogContext, ActorTokenId),
                    %% Check echo option (default true)
                    Echo = maps:get(<<"echo">>, Message, true),
                    %% Build payload with optional msgId for delivery tracking
                    PayloadWithMsgId = case MessageId of
                        undefined -> Data;
                        _ -> #{<<"_msgId">> => MessageId, <<"_data">> => Data}
                    end,
                    %% Extract QoS from client message (default 1), validate 0-2
                    ReqQoS = maps:get(<<"qos">>, Message, 1),
                    QoS = case ReqQoS of
                        N when is_integer(N), N >= 0, N =< 2 -> N;
                        _ -> 1
                    end,
                    %% Check retain option
                    Retain = maps:get(<<"retain">>, Message, false),
                    %% Publish to EMQX with connection ID if echo=false (for per-connection filtering)
                    %% Use internal topic (room_uuid/topic_name) for MQTT isolation
                    case Echo of
                        false ->
                            ok = kraken_broker:publish(MqttClient, MqttTopic, PayloadWithMsgId, ConnectionId, QoS, Retain);
                        _ ->
                            ok = kraken_broker:publish(MqttClient, MqttTopic, PayloadWithMsgId, undefined, QoS, Retain)
                    end,

                    %% Call trigger webhook if configured (async)
                    %% Per-topic webhook takes precedence, fallback to app-level
                    case App of
                        undefined -> ok;
                        _ ->
                            TopicName1 = extract_topic_name(Pattern),
                            TopicWebhooks1 = maps:get(<<"topic_webhooks">>, App, #{}),
                            TopicWebhookConfig1 = maps:get(TopicName1, TopicWebhooks1, #{}),
                            TriggerWebhook = case maps:get(<<"on_publish">>, TopicWebhookConfig1, null) of
                                null -> maps:get(<<"trigger_webhook">>, App, null);
                                Wh1 -> Wh1
                            end,
                            case TriggerWebhook of
                                null -> ok;
                                undefined -> ok;
                                WebhookConfig when is_map(WebhookConfig) ->
                                    RoomName = extract_room_name(Pattern, AllowedTopics),
                                    DlqContext = #{
                                        organization_id => OrganizationId,
                                        project_id => ProjectId,
                                        app_id => AppId
                                    },
                                    ScopeInfo1 = build_scope_info(State),
                                    kraken_webhooks:call_trigger(WebhookConfig, DlqContext, ActorTokenId, RoomName, TopicName1, Data, ScopeInfo1)
                            end
                    end,

                    %% Persistent Presence: wake offline persistent subscribers in this room
                    %% (the message is queued on their persistent session; wake brings them
                    %% back online to drain it)
                    pp_wake_offline(RoomId, AppId),

                    %% v2 publish ack: only when the client supplied a msgRef
                    case maps:get(<<"msgRef">>, Message, undefined) of
                        MsgRef when is_binary(MsgRef), MsgRef =/= <<>> ->
                            AckResp = #{
                                <<"type">> => <<"published">>,
                                <<"topic">> => Pattern,
                                <<"msgRef">> => MsgRef
                            },
                            {reply, {binary, pack_msg(AckResp)}, State1};
                        _ ->
                            {ok, State1}
                    end;
                false ->
                    %% Unknown/unauthorized room → LOUD (no implicit creation
                    %% on the data path; provision rooms via the control plane).
                    ErrFrame = with_msg_ref(
                        (unknown_topic_frame(State1#state.protocol_version))#{<<"topic">> => Pattern},
                        Message),
                    {reply, {binary, pack_msg(ErrFrame)}, State1}
            end
            end %% end is_project_blocked
            end %% end message_size check
    end;

%% Handle room presence update (NEW: requires roomId)
%% The client sends roomId as a room slug (e.g., "general").
%% We resolve it to a room UUID using allowed_topics so that
%% presence groups and lobby propagation use the correct ID.
handle_message(#{<<"type">> := <<"presence">>, <<"roomId">> := RoomSlug, <<"data">> := PresenceData},
               #state{authenticated = true, project_id = ProjectId, actor_token_id = ActorTokenId,
                      current_room_id = OldRoomId, allowed_topics = AllowedTopics} = State) ->
    %% Resolve room slug to UUID from allowed_topics
    RoomId = resolve_room_id(RoomSlug, AllowedTopics),
    case RoomId of
        undefined ->
            kraken_log:info("[WS] Presence update for unknown room slug: ~s~n", [RoomSlug]),
            {ok, State};
        _ ->
            %% If switching rooms, leave old room first
            case OldRoomId of
                undefined -> ok;
                RoomId -> ok;  %% Same room, no need to leave
                _ -> kraken_presence:leave_room_presence(OldRoomId, ActorTokenId)
            end,
            %% Update presence in new room (using UUID)
            kraken_presence:update_room_presence(RoomId, ActorTokenId, PresenceData, self(), ProjectId),
            %% Persistent Presence: write through a durable record when opted in
            Persistent = pp_write_through(RoomId, ActorTokenId, ProjectId,
                                          State#state.scope_id, PresenceData, AllowedTopics),
            NewState0 = State#state{presence = PresenceData, current_room_id = RoomId,
                                    persistent_presence = Persistent},
            %% A persistent-presence advertise is the actor declaring it's a
            %% durable, wakeable worker — start claim-based replay for any
            %% load-balanced subscription it made (covers actors whose auth
            %% doesn't set persistent_session).
            NewState = case Persistent of
                true -> maybe_replay_lb_subs(NewState0);
                false -> NewState0
            end,
            {ok, NewState}
    end;

%% Handle legacy presence update (backward compatibility - uses project-level)
%% TODO: Deprecate this in favor of room-scoped presence
handle_message(#{<<"type">> := <<"presence">>, <<"data">> := PresenceData},
               #state{authenticated = true} = State) ->
    %% Legacy: presence without roomId - just update state, no broadcast
    kraken_log:info("[WS] Warning: presence update without roomId is deprecated~n", []),
    NewState = State#state{presence = PresenceData},
    {ok, NewState};

%% Handle get room presence
handle_message(#{<<"type">> := <<"getPresence">>, <<"roomId">> := RoomSlug},
               #state{authenticated = true, allowed_topics = AllowedTopics} = State) ->
    %% Resolve room slug to UUID for presence lookup
    ResolvedRoomId = case resolve_room_id(RoomSlug, AllowedTopics) of
        undefined -> RoomSlug;  %% Fallback to slug if not found
        Uuid -> Uuid
    end,
    LivePresenceList = kraken_presence:get_room_presence(ResolvedRoomId),
    %% Persistent Presence: merge offline-but-registered actors into discovery
    PresenceList = pp_merge_room_presence(LivePresenceList, ResolvedRoomId, AllowedTopics),
    Response = #{
        <<"type">> => <<"presenceList">>,
        <<"roomId">> => RoomSlug,
        <<"data">> => PresenceList
    },
    {reply, {binary, pack_msg(Response)}, State};

%% Handle legacy get presence (backward compatibility - returns empty)
handle_message(#{<<"type">> := <<"getPresence">>},
               #state{authenticated = true} = State) ->
    kraken_log:info("[WS] Warning: getPresence without roomId is deprecated~n", []),
    Response = #{
        <<"type">> => <<"presenceList">>,
        <<"data">> => []
    },
    {reply, {binary, pack_msg(Response)}, State};

%% Handle lobby subscribe (NEW)
%% Client sends slug in lobbyId field; we resolve to UUIDs via lobby_slug_map
handle_message(#{<<"type">> := <<"lobbySubscribe">>, <<"lobbyId">> := LobbySlug},
               #state{authenticated = true, actor_token_id = ActorTokenId,
                      allowed_lobbies = AllowedLobbies, subscribed_lobbies = SubscribedLobbies,
                      allowed_topics = AllowedTopics,
                      lobby_slug_map = LobbySlugMap} = State) ->
    %% Check ACL for lobby access
    case can_subscribe_lobby(LobbySlug, AllowedLobbies) of
        true ->
            %% Resolve slug to UUIDs and join all syn groups
            LobbyUUIDs = maps:get(LobbySlug, LobbySlugMap, []),
            lists:foreach(fun(UUID) ->
                kraken_presence:join_lobby(UUID, ActorTokenId, self())
            end, LobbyUUIDs),
            %% Warm lobby cache from token's allowed_topics so that
            %% get_rooms_for_lobby and get_lobbies_for_room resolve correctly
            %% (avoids dependency on Titus API for room↔lobby mappings)
            RoomUUIDs = lists:usort(lists:filtermap(fun
                (Topic) when is_map(Topic) ->
                    case maps:get(<<"room_id">>, Topic, undefined) of
                        undefined -> false;
                        RoomId -> {true, RoomId}
                    end;
                (_) -> false
            end, AllowedTopics)),
            %% Only warm cache if we have room UUIDs — an empty warm would
            %% create a cache HIT with [] that blocks the Titus API fallback
            case RoomUUIDs of
                [] -> ok;
                _ ->
                    RoomLobbies = [{RId, LobbyUUIDs} || RId <- RoomUUIDs],
                    LobbyRooms = [{LUUID, RoomUUIDs} || LUUID <- LobbyUUIDs],
                    kraken_lobby_map:warm_cache(RoomLobbies, LobbyRooms)
            end,
            %% Aggregate presence from all UUIDs for snapshot
            %% get_lobby_presence returns a map #{RoomId => #{ActorId => Data}}
            %% Merge maps from multiple lobby UUIDs (same room can appear in multiple)
            RawPresence = lists:foldl(fun(UUID, Acc) ->
                maps:merge(Acc, kraken_presence:get_lobby_presence(UUID))
            end, #{}, LobbyUUIDs),
            %% Convert room UUIDs to slugs so the client SDK can match its room map
            LobbyPresence = convert_presence_room_ids_to_slugs(RawPresence, AllowedTopics),
            %% Send snapshot to client
            Response = #{
                <<"type">> => <<"lobbySubscribed">>,
                <<"lobbyId">> => LobbySlug,
                <<"presence">> => LobbyPresence
            },
            %% Store {Slug, [UUIDs]} in subscribed_lobbies, replacing any existing entry for this slug
            NewSubscribedLobbies = [{LobbySlug, LobbyUUIDs} |
                lists:keydelete(LobbySlug, 1, SubscribedLobbies)],
            NewState = State#state{subscribed_lobbies = NewSubscribedLobbies},
            {reply, {binary, pack_msg(Response)}, NewState};
        false ->
            Response = #{
                <<"type">> => <<"error">>,
                <<"error">> => <<"not_authorized">>,
                <<"lobbyId">> => LobbySlug
            },
            {reply, {binary, pack_msg(Response)}, State}
    end;

%% Handle lobby unsubscribe (NEW)
handle_message(#{<<"type">> := <<"lobbyUnsubscribe">>, <<"lobbyId">> := LobbySlug},
               #state{authenticated = true, actor_token_id = ActorTokenId,
                      subscribed_lobbies = SubscribedLobbies} = State) ->
    %% Find the {Slug, [UUIDs]} tuple and leave all UUID syn groups
    case lists:keyfind(LobbySlug, 1, SubscribedLobbies) of
        {LobbySlug, UUIDs} ->
            lists:foreach(fun(UUID) ->
                kraken_presence:leave_lobby(UUID, ActorTokenId)
            end, UUIDs);
        false ->
            ok
    end,
    Response = #{
        <<"type">> => <<"lobbyUnsubscribed">>,
        <<"lobbyId">> => LobbySlug
    },
    NewSubscribedLobbies = lists:keydelete(LobbySlug, 1, SubscribedLobbies),
    NewState = State#state{subscribed_lobbies = NewSubscribedLobbies},
    {reply, {binary, pack_msg(Response)}, NewState};

%% Handle get lobby presence (NEW)
handle_message(#{<<"type">> := <<"getLobbyPresence">>, <<"lobbyId">> := LobbySlug},
               #state{authenticated = true, allowed_lobbies = AllowedLobbies,
                      allowed_topics = AllowedTopics,
                      lobby_slug_map = LobbySlugMap} = State) ->
    case can_subscribe_lobby(LobbySlug, AllowedLobbies) of
        true ->
            %% Resolve slug to UUIDs and aggregate presence
            LobbyUUIDs = maps:get(LobbySlug, LobbySlugMap, []),
            RawPresence = lists:foldl(fun(UUID, Acc) ->
                maps:merge(Acc, kraken_presence:get_lobby_presence(UUID))
            end, #{}, LobbyUUIDs),
            LobbyPresence = convert_presence_room_ids_to_slugs(RawPresence, AllowedTopics),
            Response = #{
                <<"type">> => <<"lobbyPresenceList">>,
                <<"lobbyId">> => LobbySlug,
                <<"presence">> => LobbyPresence
            },
            {reply, {binary, pack_msg(Response)}, State};
        false ->
            Response = #{
                <<"type">> => <<"error">>,
                <<"error">> => <<"not_authorized">>,
                <<"lobbyId">> => LobbySlug
            },
            {reply, {binary, pack_msg(Response)}, State}
    end;

%% Handle single ACK (message delivery confirmation)
handle_message(#{<<"type">> := <<"ack">>, <<"msgId">> := MsgId},
               #state{authenticated = true, actor_token_id = ActorTokenId,
                      kraken_store = FirestoreWriter} = State) ->
    Timestamp = erlang:system_time(millisecond),
    kraken_store:mark_delivered(FirestoreWriter, MsgId, ActorTokenId, Timestamp),
    {ok, State};

%% Handle batch ACK (multiple message delivery confirmations)
handle_message(#{<<"type">> := <<"batchAck">>, <<"msgIds">> := MsgIds},
               #state{authenticated = true, actor_token_id = ActorTokenId,
                      kraken_store = FirestoreWriter} = State) ->
    Timestamp = erlang:system_time(millisecond),
    lists:foreach(fun(MsgId) ->
        kraken_store:mark_delivered(FirestoreWriter, MsgId, ActorTokenId, Timestamp)
    end, MsgIds),
    {ok, State};

%% Handle unauthenticated messages
handle_message(_Message, #state{authenticated = false} = State) ->
    Response = #{
        <<"type">> => <<"error">>,
        <<"error">> => <<"not_authenticated">>
    },
    {reply, {binary, pack_msg(Response)}, State};

%% Unknown message type
handle_message(Message, State) ->
    kraken_log:info("[WS] Unknown message: ~p~n", [Message]),
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% Restore subscriptions from Titus (on reconnect)
%% Subscriptions can be simple topic names (strings) or maps with load balance info
%% Uses internal topic (room_uuid/topic_name) for MQTT subscription
restore_subscriptions(_MqttClient, [], _State, _WsPid) ->
    ok;
restore_subscriptions(MqttClient, [Subscription | Rest], State, WsPid) ->
    AllowedTopics = State#state.allowed_topics,
    ActorTokenId = State#state.actor_token_id,
    ProjectId = State#state.project_id,
    Apps = State#state.apps,

    %% Extract subscription info (handle both string and map formats)
    %% Pattern = human-readable topic (e.g., app/room/topic)
    %% InternalTopic = UUID-based topic from subscription or looked up from allowed_topics
    {Pattern, InternalTopic, LoadBalance, ClientLoadBalanceGroup, Filters} = case Subscription of
        Sub when is_binary(Sub) ->
            %% Simple topic string (backward compatible) - look up internal topic
            {ITopic, _RoomId} = find_topic_info(Sub, AllowedTopics),
            {Sub, ITopic, false, ActorTokenId, []};
        Sub when is_map(Sub) ->
            %% Full subscription object with load balance info
            %% Support both old format (name, loadBalance) and new format (pattern, load_balance)
            Name = case maps:get(<<"pattern">>, Sub, undefined) of
                undefined -> maps:get(<<"name">>, Sub, <<>>);
                P -> P
            end,
            %% Check if subscription has internal topic already, otherwise look it up
            ITopic = case maps:get(<<"topic">>, Sub, undefined) of
                undefined ->
                    {LookedUp, _RoomId} = find_topic_info(Name, AllowedTopics),
                    LookedUp;
                T -> T
            end,
            LB = case maps:get(<<"load_balance">>, Sub, undefined) of
                undefined -> maps:get(<<"loadBalance">>, Sub, false);
                LoadBal -> LoadBal
            end,
            LBGroup0 = case maps:get(<<"load_balance_group">>, Sub, undefined) of
                undefined -> maps:get(<<"loadBalanceGroup">>, Sub, ActorTokenId);
                Group -> Group
            end,
            %% Handle null from JSON
            LBGroup = case LBGroup0 of
                null -> ActorTokenId;
                undefined -> ActorTokenId;
                _ -> LBGroup0
            end,
            %% Extract filters
            SubFilters = case maps:get(<<"filters">>, Sub, undefined) of
                undefined -> [];
                null -> [];
                FL when is_list(FL) -> FL;
                _ -> []
            end,
            {Name, ITopic, LB, LBGroup, SubFilters}
    end,

    %% Find the app for this topic (needed for load balance scoping) —
    %% wildcard-aware so restored wildcard-rule subs scope correctly
    AppId = case kraken_topics:find_rule(Pattern, AllowedTopics) of
        {exact, Rule0} -> maps:get(<<"app_id">>, Rule0, <<"unscoped">>);
        {wildcard, Rule0} -> maps:get(<<"app_id">>, Rule0, <<"unscoped">>);
        not_found ->
            case kraken_auth:find_app_for_topic(Pattern, Apps) of
                undefined -> <<"unscoped">>;
                App0 -> maps:get(<<"app_id">>, App0, <<"unscoped">>)
            end
    end,

    %% Scope load balance group by project AND app to prevent cross-app collision
    %% But only if it's not already scoped (restored subscriptions already have the full group)
    %% IMPORTANT: Use underscore separator, NOT slash! In EMQX $share/<group>/<topic>,
    %% the <group> is everything between $share/ and the next /, so slashes in group
    %% would be interpreted as part of the topic.
    ExpectedPrefix = <<ProjectId/binary, "_", AppId/binary, "_">>,
    LoadBalanceGroup = case binary:match(ClientLoadBalanceGroup, ExpectedPrefix) of
        {0, _} ->
            %% Already scoped (starts with proj_id_app_id_), use as-is
            ClientLoadBalanceGroup;
        _ ->
            %% Not scoped, add the prefix
            <<ProjectId/binary, "_", AppId/binary, "_", ClientLoadBalanceGroup/binary>>
    end,

    case kraken_acl:can_subscribe(Pattern, AllowedTopics) of
        true ->
            %% Prefer the server-restored internal topic; otherwise resolve
            %% the same way live subscribes do (never the unknown/ namespace)
            MqttBaseTopic = case InternalTopic of
                undefined ->
                    case kraken_topics:resolve(Pattern, AllowedTopics) of
                        {exact, IT2, _, _} -> IT2;
                        {wildcard, FT2, _, _} -> FT2;
                        no_match -> kraken_topics:fallback_topic(AppId, Pattern)
                    end;
                _ -> InternalTopic
            end,
            %% Store base topic for filter operations
            put({base_topic_for, Pattern}, MqttBaseTopic),
            %% Build MQTT topics based on filters
            MqttTopics = case Filters of
                [] ->
                    %% No filters — subscribe to wildcard
                    WildcardTopic = <<MqttBaseTopic/binary, "/#">>,
                    WT = case LoadBalance of
                        true -> <<"$share/", LoadBalanceGroup/binary, "/", WildcardTopic/binary>>;
                        false -> WildcardTopic
                    end,
                    [WT];
                _ ->
                    %% Subscribe to each filter
                    lists:map(fun(Filter) ->
                        FilterBase = <<MqttBaseTopic/binary, "/", Filter/binary>>,
                        case LoadBalance of
                            true -> <<"$share/", LoadBalanceGroup/binary, "/", FilterBase/binary>>;
                            false -> FilterBase
                        end
                    end, Filters)
            end,
            lists:foreach(fun(MT) ->
                ok = kraken_broker:subscribe(MqttClient, MT, Pattern, WsPid)
            end, MqttTopics),
            put({mqtt_topics_for, Pattern}, MqttTopics),
            %% Store filter state in WsPid process (restore_subscriptions runs in WsPid context)
            case Filters of
                [] -> ok;
                _ -> put({pending_filters, Pattern}, Filters)
            end;
        false ->
            ok
    end,
    restore_subscriptions(MqttClient, Rest, State, WsPid).

notify_usage(undefined, _Bytes) -> ok;
notify_usage(ProjectId, Bytes) ->
    catch kraken_usage:increment(ProjectId, 1, Bytes).

%% Record message and return {ok, MessageId} or {skip, undefined}
%% Pattern = human-readable topic (e.g., app/room/topic)
%% InternalTopic = UUID-based topic for Firestore (e.g., room_uuid/topic)
%% PackedPayload = msgpack-encoded payload binary (stored as Firestore bytesValue)
%% Context = #{organization_id, project_id, app_id, room_id}
maybe_record_message(FirestoreWriter, Pattern, InternalTopic, Data, PackedPayload, Context, ActorTokenId) ->
    %% Always track usage regardless of whether message recording is enabled
    ProjectId = maps:get(project_id, Context, undefined),
    PayloadBytes = byte_size(jsx:encode(Data)),
    notify_usage(ProjectId, PayloadBytes),

    RecordMessages = application:get_env(kraken, record_messages, false),
    %% Handle both atom and string values from config
    ShouldRecord = case RecordMessages of
        true -> true;
        "true" -> true;
        <<"true">> -> true;
        _ -> false
    end,
    case ShouldRecord of
        true ->
            MessageId = generate_uuid(),
            Timestamp = erlang:system_time(millisecond),
            %% Use internal topic for Firestore storage (ensures uniqueness across projects)
            %% Context includes org/project/app/room IDs for proper scoping
            kraken_store:log_message(FirestoreWriter, MessageId, Context, InternalTopic, Pattern, ActorTokenId, PackedPayload, Timestamp),
            {ok, MessageId};
        false ->
            {skip, undefined}
    end.

%% Generate a UUID v4
generate_uuid() ->
    <<A:32, B:16, C:16, D:16, E:48>> = crypto:strong_rand_bytes(16),
    %% Set version to 4 and variant to RFC 4122
    C2 = (C band 16#0fff) bor 16#4000,
    D2 = (D band 16#3fff) bor 16#8000,
    list_to_binary(io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
                                  [A, B, C2, D2, E])).

%% Extract msgId and data from payload (for delivery tracking)
%% Handle various payload structures:
%% 1. #{_msgId, _data} - direct message with tracking
%% 2. #{_sender, data => #{_msgId, _data}} - no-echo with tracking
%% 3. #{_sender, data} - no-echo without tracking
%% 4. Other - plain message without tracking
extract_msg_id_and_data(#{<<"_msgId">> := MsgId, <<"_data">> := Data}) ->
    %% Direct message with tracking
    {MsgId, Data};
extract_msg_id_and_data(#{<<"_sender">> := Sender, <<"data">> := InnerData}) ->
    %% No-echo envelope - check if inner data has msgId
    case InnerData of
        #{<<"_msgId">> := MsgId, <<"_data">> := ActualData} ->
            %% No-echo with tracking - rebuild envelope with actual data
            {MsgId, #{<<"_sender">> => Sender, <<"data">> => ActualData}};
        _ ->
            %% No-echo without tracking
            {undefined, #{<<"_sender">> => Sender, <<"data">> => InnerData}}
    end;
extract_msg_id_and_data(Data) ->
    %% Plain message without tracking
    {undefined, Data}.

%% Log delivery to Firestore if msgId is present
maybe_log_delivery(_FirestoreWriter, undefined, _ActorTokenId, _Topic) ->
    ok;
maybe_log_delivery(FirestoreWriter, MsgId, ActorTokenId, Topic) ->
    Timestamp = erlang:system_time(millisecond),
    kraken_store:log_delivery(FirestoreWriter, MsgId, ActorTokenId, Topic, Timestamp).

%% On disconnect, stamp each load-balanced durable subscription's group cursor
%% at "now" — the offline boundary. A reconnecting group member then replays
%% only messages dispatched after this point (what it missed while offline),
%% instead of re-surfacing the window it already consumed live. For a
%% scale-to-zero pool (one instance cycling) this disconnect is the group going
%% offline; multi-instance pools rely on the per-message claim + consumer
%% idempotency to dedup any overlap. Inert unless the delivery_store is enabled.
dd_mark_offline_boundary() ->
    case catch kraken_delivery_store:is_enabled() of
        true ->
            Now = erlang:system_time(millisecond),
            lists:foreach(
                fun({{lb_subscription, _Pattern}, Ctx}) when is_map(Ctx) ->
                        catch kraken_delivery_store:cursor_set(
                            #{app_id => maps:get(app_id, Ctx, <<>>),
                              group_id => maps:get(group_id, Ctx, <<>>)}, Now);
                   (_) -> ok
                end, get());
        _ ->
            ok
    end.

%% Remember a load-balanced subscription's replay context (group/room/app),
%% keyed by pattern, so a later persistent signal can replay it. Needs a
%% resolved room to scope the backlog query.
remember_lb_subscription(Pattern, RoomId, AppId, GroupId) when RoomId =/= undefined ->
    %% topic = the subscribed Pattern (the DISPLAY topic the SDK keys its handler
    %% on); replayed frames must carry it, not the message's internal topic.
    put({lb_subscription, Pattern},
        #{group_id => GroupId, room_id => RoomId, app_id => AppId, topic => Pattern}),
    ok;
remember_lb_subscription(_Pattern, _RoomId, _AppId, _GroupId) ->
    ok.

%% Start a claim-based durable replay for every load-balanced subscription on
%% this connection that hasn't replayed yet — a scale-to-zero worker draining
%% what its group missed. Called when the actor signals it's a durable worker:
%% persistent_session at subscribe, OR a persistent-presence advertise. Inert
%% unless the delivery_store is enabled. Sets replay_state=buffering so live
%% messages arriving during replay are buffered + deduped
%% (forward_message_with_filter). Dedup per group via {replay_started, Group}.
maybe_replay_lb_subs(State) ->
    case kraken_delivery_store:is_enabled() of
        false ->
            State;
        true ->
            ActorId = State#state.actor_token_id,
            Subs = [Ctx || {{lb_subscription, _Pattern}, Ctx} <- get()],
            lists:foldl(
                fun(#{group_id := GroupId, room_id := RoomId, app_id := AppId} = Sub, AccState) ->
                        case get({replay_started, GroupId}) of
                            true ->
                                AccState;
                            _ ->
                                put({replay_started, GroupId}, true),
                                Ctx = #{group_id => GroupId, room_id => RoomId,
                                        topic => maps:get(topic, Sub, undefined)},
                                catch kraken_replay:start_replay(ActorId, AppId, Ctx, self()),
                                AccState#state{replay_state = buffering}
                        end
                end, State, Subs)
    end.

%% Forward message to WebSocket client with optional msgId
forward_message_with_id(Topic, Data, MsgId, State) ->
    Response = case MsgId of
        undefined ->
            #{
                <<"type">> => <<"message">>,
                <<"topic">> => Topic,
                <<"data">> => Data
            };
        _ ->
            #{
                <<"type">> => <<"message">>,
                <<"topic">> => Topic,
                <<"data">> => Data,
                <<"msgId">> => MsgId,
                <<"requiresAck">> => true
            }
    end,
    try
        Packed = pack_msg(Response),
        {reply, {binary, Packed}, State}
    catch
        Error:Reason:Stacktrace ->
            kraken_log:error("[WS] ERROR packing message: ~p:~p~n~p~n", [Error, Reason, Stacktrace]),
            {ok, State}
    end.

%% Forward message to WebSocket client (legacy, without msgId)
forward_message(Topic, Data, State) ->
    forward_message_with_id(Topic, Data, undefined, State).

%% Convert binary to hex string for debugging
binary_to_hex(Bin) ->
    lists:flatten([[io_lib:format("~2.16.0B", [B]) || <<B:8>> <= Bin]]).

%% Check per-organization connection limit via syn
%% Returns ok if under limit, {error, limit_reached} if over
check_connection_limit(_OrgId, unlimited) -> ok;
check_connection_limit(undefined, _MaxConn) -> ok;
check_connection_limit(OrgId, MaxConn) ->
    Current = length(syn:members(kraken_connections, {org, OrgId})),
    case Current < MaxConn of
        true -> ok;
        false -> {error, limit_reached}
    end.

%% Check and update rate limit counter
%% Returns {ok, NewState} if under limit, {error, rate_limited, NewState} if over
check_rate_limit(State) ->
    CurrentSecond = erlang:system_time(second),
    RateLimitSecond = State#state.rate_limit_second,
    MsgCount = State#state.msg_count,
    RateLimit = State#state.rate_limit,

    case CurrentSecond of
        RateLimitSecond ->
            %% Same second, check limit
            case MsgCount >= RateLimit of
                true ->
                    {error, rate_limited, State};
                false ->
                    {ok, State#state{msg_count = MsgCount + 1}}
            end;
        _ ->
            %% New second, reset counter
            {ok, State#state{msg_count = 1, rate_limit_second = CurrentSecond}}
    end.

%% Pack message with binary keys as strings for JS compatibility
pack_msg(Map) ->
    iolist_to_binary(msgpack:pack(Map, [{pack_str, from_binary}])).

%% Async revalidation - called in spawned process
%% Sends result back to WebSocket handler process
async_revalidate(WsPid, ActorTokenId) ->
    case kraken_auth:revalidate_token(ActorTokenId) of
        {ok, AuthData} ->
            WsPid ! {revalidation_success, AuthData};
        {error, Reason} ->
            WsPid ! {revalidation_failed, Reason};
        {retry, Reason} ->
            WsPid ! {revalidation_retry, Reason}
    end.

%% Extract room name from full topic pattern (app-name/room-slug/topic)
%% Looks up roomName from allowed topics if available, otherwise parses the pattern
extract_room_name(FullTopic, AllowedTopics) ->
    %% First try to find roomName from allowed topics (set by Titus)
    case find_room_name_in_allowed(FullTopic, AllowedTopics) of
        undefined ->
            %% Fall back to parsing the topic pattern
            case binary:split(FullTopic, <<"/">>, [global]) of
                [_AppName, RoomSlug, _TopicName] -> RoomSlug;
                [_AppName, RoomSlug | _] -> RoomSlug;
                _ -> FullTopic
            end;
        RoomName ->
            RoomName
    end.

%% Find roomName from allowed topics list
find_room_name_in_allowed(_Topic, []) ->
    undefined;
find_room_name_in_allowed(Topic, [AllowedTopic | Rest]) when is_map(AllowedTopic) ->
    Pattern = maps:get(<<"pattern">>, AllowedTopic, <<>>),
    case Pattern =:= Topic of
        true ->
            maps:get(<<"roomName">>, AllowedTopic, undefined);
        false ->
            find_room_name_in_allowed(Topic, Rest)
    end;
find_room_name_in_allowed(Topic, [_ | Rest]) ->
    find_room_name_in_allowed(Topic, Rest).

%% Extract topic name (last segment) from full topic pattern
extract_topic_name(FullTopic) ->
    case binary:split(FullTopic, <<"/">>, [global]) of
        Parts when length(Parts) >= 3 ->
            lists:last(Parts);
        [_, TopicName] ->
            TopicName;
        [TopicName] ->
            TopicName;
        _ ->
            FullTopic
    end.

%% Build scope info map for webhook payloads
%% Returns null for unscoped actors, map with scope details for scoped actors
build_scope_info(#state{scope_slug = undefined}) -> null;
build_scope_info(#state{scope_id = ScopeId, scope_slug = ScopeSlug, scope_name = ScopeName}) ->
    #{
        <<"accessScopeId">> => ScopeId,
        <<"slug">> => ScopeSlug,
        <<"name">> => ScopeName
    }.

%% Extract base topic from $share subscription path
%% Input: proj_id_app_id_group_name/room_uuid/topic_name (the part after $share/)
%% Output: room_uuid/topic_name
%% The load balance group format is: proj_id_app_id_client_group (underscores, no slashes!)
%% So the base topic is everything after the first /
extract_base_topic_from_share(SharePath) ->
    %% binary:split without [global] splits only at the first occurrence
    %% So "group/room/topic" becomes ["group", "room/topic"]
    case binary:split(SharePath, <<"/">>) of
        [_Group, BaseTopic] ->
            %% BaseTopic is everything after the first slash (e.g., "room_uuid/topic_name")
            BaseTopic;
        [_OnlyGroup] ->
            %% No slash found - invalid format
            undefined
    end.

%% Validate filter values
%% Returns {ok, FilterList} or {error, ErrorBinary}
%% Accepts mixed lists: items can be binaries (OR) or lists-of-binaries (AND groups)
validate_filters(undefined) -> {ok, []};
validate_filters(null) -> {ok, []};
validate_filters(Filters) when is_list(Filters) ->
    case length(Filters) > 100 of
        true -> {error, <<"too_many_filters (max 100)">>};
        false ->
            %% Check each filter item for validity
            Invalid = lists:filter(fun(F) when is_binary(F) ->
                binary:match(F, <<"/">>) =/= nomatch orelse
                binary:match(F, <<"#">>) =/= nomatch orelse
                binary:match(F, <<"+">>) =/= nomatch orelse
                binary:match(F, <<"|">>) =/= nomatch orelse
                F =:= <<>>;
            (Group) when is_list(Group) ->
                %% AND group: must have ≥2 elements, each element validated same way
                length(Group) < 2 orelse
                lists:any(fun(G) when is_binary(G) ->
                    binary:match(G, <<"/">>) =/= nomatch orelse
                    binary:match(G, <<"#">>) =/= nomatch orelse
                    binary:match(G, <<"+">>) =/= nomatch orelse
                    binary:match(G, <<"|">>) =/= nomatch orelse
                    G =:= <<>>;
                (_) -> true
                end, Group);
            (_) -> true
            end, Filters),
            case Invalid of
                [] -> {ok, Filters};
                _ -> {error, <<"invalid_filter_chars (/, #, +, | not allowed)">>}
            end
    end;
validate_filters(_) -> {ok, []}.

%% Normalize filters: flatten AND groups into pipe-separated composite strings
%% Simple binaries pass through unchanged. AND groups are lowercased, sorted, joined with |.
normalize_filters(Filters) ->
    lists:map(fun
        (F) when is_binary(F) -> F;
        (Group) when is_list(Group) ->
            Lowered = [string:lowercase(G) || G <- Group],
            Sorted = lists:sort(Lowered),
            iolist_to_binary(lists:join(<<"|">>, Sorted))
    end, Filters).

%% Extract filter value from incoming MQTT topic by comparing with base topic
%% Returns the filter binary or undefined
extract_filter_from_mqtt_topic(MqttTopic, DisplayTopic) ->
    BaseTopic = get({base_topic_for, DisplayTopic}),
    case BaseTopic of
        undefined -> undefined;
        _ ->
            Prefix = <<BaseTopic/binary, "/">>,
            PrefixLen = byte_size(Prefix),
            case MqttTopic of
                <<Prefix:PrefixLen/binary, FilterVal/binary>> when byte_size(FilterVal) > 0 ->
                    FilterVal;
                _ -> undefined
            end
    end.

%% Find display topic by trying wildcard and parent MQTT topic mappings
%% For wildcard subscriptions (room_uuid/topic/#), EMQX delivers on:
%%   - room_uuid/topic (message without filter)
%%   - room_uuid/topic/filter1 (message with filter)
%% We have {topic_mapping, room_uuid/topic/#} stored via kraken_broker.
find_display_topic_by_prefix(MqttTopic) ->
    %% First try: this topic + /# (message published to base topic, matched by wildcard sub)
    case get({topic_mapping, <<MqttTopic/binary, "/#">>}) of
        undefined ->
            %% Second try: strip last segment and try parent/#
            %% (message published with filter, matched by wildcard sub)
            case binary:split(MqttTopic, <<"/">>, [global]) of
                Parts when length(Parts) >= 2 ->
                    ParentParts = lists:droplast(Parts),
                    ParentTopic = join_binary(ParentParts, <<"/">>),
                    case get({topic_mapping, <<ParentTopic/binary, "/#">>}) of
                        undefined ->
                            %% Also try exact parent topic mapping (backward compat)
                            get({topic_mapping, ParentTopic});
                        DT -> DT
                    end;
                _ -> undefined
            end;
        DT -> DT
    end.

%% Join a list of binaries with a separator
join_binary([], _Sep) -> <<>>;
join_binary([H], _Sep) -> H;
join_binary([H | T], Sep) ->
    lists:foldl(fun(Part, Acc) -> <<Acc/binary, Sep/binary, Part/binary>> end, H, T).

%% Extract $share/ prefix from existing MQTT topic list
%% Returns the prefix (e.g., <<"$share/group/">>) or undefined
extract_share_prefix([]) -> undefined;
extract_share_prefix([FirstTopic | _]) ->
    case FirstTopic of
        <<"$share/", Rest/binary>> ->
            %% Group is everything before the first / in Rest
            case binary:split(Rest, <<"/">>) of
                [Group, _] -> <<"$share/", Group/binary, "/">>;
                _ -> undefined
            end;
        _ -> undefined
    end.

%% Forward message to WebSocket client with optional filter value
forward_message_with_filter(Topic, Data, MsgId, FilterValue, State) ->
    BaseResponse = case MsgId of
        undefined ->
            #{
                <<"type">> => <<"message">>,
                <<"topic">> => Topic,
                <<"data">> => Data
            };
        _ ->
            #{
                <<"type">> => <<"message">>,
                <<"topic">> => Topic,
                <<"data">> => Data,
                <<"msgId">> => MsgId,
                <<"requiresAck">> => true
            }
    end,
    Response = case FilterValue of
        undefined -> BaseResponse;
        _ -> maps:put(<<"filter">>, FilterValue, BaseResponse)
    end,
    %% Durable replay seam: while a replay is in flight, buffer live messages
    %% instead of forwarding, so they are deduped against replayed messages and
    %% flushed (in order) after replayEnd ({replay_complete} handler).
    case State#state.replay_state of
        RS when RS =:= buffering; RS =:= replaying ->
            {ok, State#state{replay_buffer = State#state.replay_buffer ++ [Response]}};
        _ ->
            try
                Packed = pack_msg(Response),
                {reply, {binary, Packed}, State}
            catch
                Error:Reason:Stacktrace ->
                    kraken_log:error("[WS] ERROR packing message: ~p:~p~n~p~n", [Error, Reason, Stacktrace]),
                    {ok, State}
            end
    end.

%% Collect pending filter state from process dictionary after restore_subscriptions
collect_pending_filters(State) ->
    %% Scan process dictionary for {pending_filters, Pattern} keys
    PendingEntries = [{Pattern, Filters} ||
        {{pending_filters, Pattern}, Filters} <- get()],
    %% Clean up pending entries
    lists:foreach(fun({Pattern, _}) ->
        erase({pending_filters, Pattern})
    end, PendingEntries),
    %% Build topic_filters map
    NewTopicFilters = lists:foldl(fun({Pattern, Filters}, Acc) ->
        maps:put(Pattern, Filters, Acc)
    end, State#state.topic_filters, PendingEntries),
    State#state{topic_filters = NewTopicFilters}.

%% Convert room UUIDs to slugs in lobby presence snapshot.
%% get_lobby_presence returns #{RoomUUID => #{ActorId => Data}}
%% We need to convert the RoomUUID keys to slugs for the client SDK.
convert_presence_room_ids_to_slugs(PresenceMap, AllowedTopics) when is_map(PresenceMap) ->
    maps:fold(fun(RoomUUID, ActorMap, Acc) ->
        Slug = resolve_room_slug(RoomUUID, AllowedTopics),
        Key = case Slug of undefined -> RoomUUID; _ -> Slug end,
        Acc#{Key => ActorMap}
    end, #{}, PresenceMap);
convert_presence_room_ids_to_slugs(Other, _AllowedTopics) ->
    Other.

%% Resolve a room UUID back to a room slug using the allowed_topics list.
%% Used when forwarding lobby presence events to the client — the client
%% SDK maps rooms by slug, not UUID.
resolve_room_slug(_RoomUUID, []) ->
    undefined;
resolve_room_slug(RoomUUID, [AllowedTopic | Rest]) when is_map(AllowedTopic) ->
    case maps:get(<<"room_id">>, AllowedTopic, undefined) of
        RoomUUID ->
            maps:get(<<"room_slug">>, AllowedTopic, RoomUUID);
        _ ->
            resolve_room_slug(RoomUUID, Rest)
    end;
resolve_room_slug(RoomUUID, [_ | Rest]) ->
    resolve_room_slug(RoomUUID, Rest).

%% Resolve a room slug to a room UUID using the allowed_topics list.
%% The client sends room slugs (e.g., "general") but presence groups
%% and lobby lookups need room UUIDs.
resolve_room_id(_RoomSlug, []) ->
    undefined;
resolve_room_id(RoomSlug, [AllowedTopic | Rest]) when is_map(AllowedTopic) ->
    case maps:get(<<"room_slug">>, AllowedTopic, undefined) of
        RoomSlug ->
            maps:get(<<"room_id">>, AllowedTopic, undefined);
        _ ->
            resolve_room_id(RoomSlug, Rest)
    end;
resolve_room_id(RoomSlug, [_ | Rest]) ->
    resolve_room_id(RoomSlug, Rest).

%%====================================================================
%% Persistent Presence helpers
%%====================================================================

%% True when the client's presence payload opts into persistent mode.
pp_is_persistent(PresenceData) when is_map(PresenceData) ->
    maps:get(<<"persistent">>, PresenceData, false) =:= true;
pp_is_persistent(_) ->
    false.

%% Resolve the app_id that owns a room UUID, from allowed_topics.
pp_room_app_id(_RoomId, []) ->
    undefined;
pp_room_app_id(RoomId, [AllowedTopic | Rest]) when is_map(AllowedTopic) ->
    case maps:get(<<"room_id">>, AllowedTopic, undefined) of
        RoomId -> maps:get(<<"app_id">>, AllowedTopic, undefined);
        _ -> pp_room_app_id(RoomId, Rest)
    end;
pp_room_app_id(RoomId, [_ | Rest]) ->
    pp_room_app_id(RoomId, Rest).

%% Build the durable presence record written through to the presence store.
pp_record(AppId, RoomId, ActorTokenId, ProjectId, ScopeId, PresenceData) ->
    #{
        app_id => AppId,
        room_id => RoomId,
        actor_token_id => ActorTokenId,
        project_id => ProjectId,
        scope_id => ScopeId,
        node => atom_to_binary(node(), utf8),
        capabilities => maps:get(<<"capabilities">>, PresenceData, []),
        advertisement => PresenceData,
        wake => maps:get(<<"wake">>, PresenceData, undefined)
    }.

%% Write-through on advertise: persist the record (status online) when the
%% presence payload opts into persistent mode. Returns whether it was persistent.
pp_write_through(RoomId, ActorTokenId, ProjectId, ScopeId, PresenceData, AllowedTopics) ->
    case pp_is_persistent(PresenceData) of
        false ->
            false;
        true ->
            AppId = pp_room_app_id(RoomId, AllowedTopics),
            Record = pp_record(AppId, RoomId, ActorTokenId, ProjectId, ScopeId, PresenceData),
            catch kraken_presence_store:upsert(Record),
            true
    end.

%% Soft-offline on disconnect: keep the record (discoverable + wakeable),
%% just mark it offline. No-op for non-persistent connections.
pp_offline(false, _RoomId, _ActorTokenId, _AllowedTopics) ->
    ok;
pp_offline(true, RoomId, ActorTokenId, AllowedTopics) ->
    AppId = pp_room_app_id(RoomId, AllowedTopics),
    catch kraken_presence_store:offline(#{
        app_id => AppId,
        room_id => RoomId,
        actor_token_id => ActorTokenId,
        node => atom_to_binary(node(), utf8)
    }),
    ok.

%% Publish-path gate: wake offline persistent subscribers in a room so they
%% reconnect and drain the message queued on their persistent broker session.
%% No-op on the OSS/syn build (discover returns []). The proxy's discover impl
%% may cache to avoid a per-publish lookup.
pp_wake_offline(undefined, _AppId) ->
    ok;
pp_wake_offline(RoomId, AppId) ->
    case catch kraken_presence_store:discover(#{room_id => RoomId, app_id => AppId,
                                                status => <<"offline">>}) of
        {ok, Actors} when is_list(Actors) ->
            lists:foreach(
                fun(Actor) when is_map(Actor) ->
                        %% mark_waking debounces repeat wakes (status offline -> waking);
                        %% the proxy's wake backend extracts wake.url from the record and HMAC-POSTs.
                        catch kraken_presence_store:mark_waking(Actor),
                        catch kraken_wake:fire(Actor);
                   (_) ->
                        ok
                end, Actors);
        _ ->
            ok
    end.

%% Merge durable persistent records into a room's live presence list so that
%% discovery (getPresence / the agents-layer findAgents) returns offline-but-
%% registered actors. Live actors are tagged status=online; durable-only actors
%% are appended with their stored status. No-op extra on OSS (discover -> []).
pp_merge_room_presence(LiveList, RoomId, AllowedTopics) ->
    LiveWithStatus = [maps:put(<<"status">>, <<"online">>, A) || A <- LiveList, is_map(A)],
    LiveIds = [maps:get(<<"actorTokenId">>, A, undefined) || A <- LiveWithStatus],
    case catch kraken_presence_store:discover(#{room_id => RoomId,
                                                app_id => pp_room_app_id(RoomId, AllowedTopics)}) of
        {ok, Durable} when is_list(Durable) ->
            Extra = [#{<<"actorTokenId">> => maps:get(<<"actorTokenId">>, D, <<>>),
                       <<"presence">> => maps:get(<<"presence">>, D, #{}),
                       <<"status">> => maps:get(<<"status">>, D, <<"offline">>)}
                    || D <- Durable, is_map(D),
                       not lists:member(maps:get(<<"actorTokenId">>, D, undefined), LiveIds)],
            LiveWithStatus ++ Extra;
        _ ->
            LiveWithStatus
    end.

%% Find the internal topic (room_uuid/topic_name) and room_id for a given pattern
%% Returns {InternalTopic, RoomId} or {undefined, undefined} if not found
find_topic_info(_Pattern, []) ->
    {undefined, undefined};
find_topic_info(Pattern, [AllowedTopic | Rest]) when is_map(AllowedTopic) ->
    TopicPattern = maps:get(<<"pattern">>, AllowedTopic, <<>>),
    case TopicPattern =:= Pattern of
        true ->
            InternalTopic = maps:get(<<"topic">>, AllowedTopic, undefined),
            RoomId = maps:get(<<"room_id">>, AllowedTopic, undefined),
            {InternalTopic, RoomId};
        false ->
            find_topic_info(Pattern, Rest)
    end;
find_topic_info(Pattern, [_Other | Rest]) ->
    find_topic_info(Pattern, Rest).

%% Find an app map by its app_id (wildcard-resolution friendly — the old
%% exact-pattern lookup returned undefined for wildcard-matched topics).
find_app_by_id(_AppId, []) ->
    undefined;
find_app_by_id(AppId, [App | Rest]) when is_map(App) ->
    case maps:get(<<"app_id">>, App, undefined) of
        AppId -> App;
        _ -> find_app_by_id(AppId, Rest)
    end;
find_app_by_id(AppId, [_ | Rest]) ->
    find_app_by_id(AppId, Rest).

%% One-release dual-subscribe compat shim: wildcard-resolved subscriptions
%% also listen on the pre-fix fallback topic (`unknown/<pattern>...`) so
%% publishers on not-yet-upgraded broker nodes still reach upgraded
%% subscribers during a rolling deploy. Publishing always uses the new
%% deterministic base. Disable (and later remove) via fallback_compat=false.
legacy_compat_topics(exact, _EffectivePattern, _Filters, _LB, _LBGroup) ->
    [];
legacy_compat_topics(wildcard, EffectivePattern, FiltersOrWildcard, LoadBalance, LoadBalanceGroup) ->
    case application:get_env(kraken, fallback_compat, true) of
        false -> [];
        _ ->
            LegacyBase = kraken_topics:legacy_fallback_topic(undefined, EffectivePattern),
            Bases = case FiltersOrWildcard of
                wildcard -> [<<LegacyBase/binary, "/#">>];
                Filters when is_list(Filters) ->
                    [<<LegacyBase/binary, "/", F/binary>> || F <- Filters]
            end,
            case LoadBalance of
                true -> [<<"$share/", LoadBalanceGroup/binary, "/", B/binary>> || B <- Bases];
                false -> Bases
            end
    end.

%% Echo the client's publish msgRef on error frames (v2 publish acks).
with_msg_ref(Frame, Message) ->
    case maps:get(<<"msgRef">>, Message, undefined) of
        MsgRef when is_binary(MsgRef), MsgRef =/= <<>> -> Frame#{<<"msgRef">> => MsgRef};
        _ -> Frame
    end.

%% Loud unknown-topic error. v2 clients get a 42940 with an actionable
%% hint; v1 clients keep the legacy not_authorized frame. Rooms are never
%% created implicitly on the data path — provision them explicitly via the
%% control-plane rooms API before use.
unknown_topic_frame(PV) when PV >= 2 ->
    #{
        <<"type">> => <<"error">>,
        <<"code">> => 42940,
        <<"error">> => <<"unknown_topic">>,
        <<"hint">> => <<"room is not configured — provision it via the control-plane rooms API before use">>
    };
unknown_topic_frame(_PV) ->
    #{<<"type">> => <<"error">>, <<"error">> => <<"not_authorized">>}.

%% Find the internal topic (room_uuid/topic_name) for a given pattern
%% Returns the pattern itself if not found (fallback)
find_internal_topic(_Pattern, []) ->
    %% Fallback to pattern if not found in allowed topics
    undefined;
find_internal_topic(Pattern, [AllowedTopic | Rest]) when is_map(AllowedTopic) ->
    TopicPattern = maps:get(<<"pattern">>, AllowedTopic, <<>>),
    case TopicPattern =:= Pattern of
        true ->
            %% Found it - return the internal topic
            maps:get(<<"topic">>, AllowedTopic, Pattern);
        false ->
            find_internal_topic(Pattern, Rest)
    end;
find_internal_topic(Pattern, [_ | Rest]) ->
    find_internal_topic(Pattern, Rest).

%% Build a map of slug => [LobbyUUID] from {Slug, LobbyUUID} tuples
%% Used to resolve client-provided slugs to UUID-keyed syn groups
build_lobby_slug_map(AllowedLobbies) ->
    lists:foldl(fun
        ({Slug, LobbyId}, Acc) ->
            Existing = maps:get(Slug, Acc, []),
            Acc#{Slug => [LobbyId | Existing]};
        (_, Acc) ->
            Acc
    end, #{}, AllowedLobbies).

%% Check if actor can subscribe to a lobby (by slug)
%% AllowedLobbies is now a list of {Slug, LobbyUUID} tuples
can_subscribe_lobby(_LobbySlug, []) ->
    false;
can_subscribe_lobby(LobbySlug, AllowedLobbies) when is_list(AllowedLobbies) ->
    lists:any(fun
        ({Slug, _LobbyId}) -> Slug =:= LobbySlug;
        (Allowed) when is_binary(Allowed) -> Allowed =:= LobbySlug;
        (_) -> false
    end, AllowedLobbies);
can_subscribe_lobby(_, _) ->
    false.

%% Create a preview of a token for logging - shows only the key ID
%% Token format: at_live_<key_id>.<secret> or at_test_<key_id>.<secret>
%% Returns: at_live_<key_id> (without the secret part after the dot)
token_preview(Token) when is_binary(Token) ->
    %% First split on dot to remove the secret
    case binary:split(Token, <<".">>) of
        [KeyPart, _Secret] ->
            %% Return just the part before the dot (at_live_keyid)
            KeyPart;
        [_NoSecret] ->
            %% No dot found - show first 16 chars
            case byte_size(Token) > 16 of
                true ->
                    <<Preview:16/binary, _/binary>> = Token,
                    <<Preview/binary, "...">>;
                false ->
                    Token
            end
    end;
token_preview(_) ->
    <<"[invalid]">>.

%% Format disconnect reason for logging
format_disconnect_reason(normal) -> <<"normal">>;
format_disconnect_reason(remote) -> <<"remote">>;
format_disconnect_reason(stop) -> <<"stop">>;
format_disconnect_reason(timeout) -> <<"timeout">>;
format_disconnect_reason(crash) -> <<"crash">>;
format_disconnect_reason({crash, _Class, _Reason}) -> <<"crash">>;
format_disconnect_reason({error, closed}) -> <<"client_closed">>;
format_disconnect_reason({error, Reason}) when is_atom(Reason) -> atom_to_binary(Reason);
format_disconnect_reason(_Other) -> <<"actor_disconnect">>.

%% Subscribe cache-miss fallback. When an actor subscribes to a room that
%% isn't in its cached allowed_topics (typically created/granted after it
%% connected), ask the control plane ONCE whether access is live. On allow,
%% return the session topics merged with the freshly-resolved room topics so
%% the retry passes ACL. On deny/error/disabled, fail closed (deny) and
%% negative-cache the denial to stop a retry storm.
%%
%% Re-dispatch safety: we only return {ok, Merged} after re-deriving the
%% effective pattern (scope-aware) against the merged set and confirming it
%% now passes can_subscribe. The handle_message re-dispatch therefore always
%% takes the allow branch — it can never bounce back here and loop.
maybe_cache_miss_fallback(ActorTokenId, Pattern, ScopeSlug, AllowedTopics) ->
    case cache_miss_fallback_enabled() of
        false ->
            deny;
        true ->
            case kraken_acl:deny_cached(ActorTokenId, Pattern) of
                true -> deny;
                false -> cache_miss_fallback_check(ActorTokenId, Pattern, ScopeSlug, AllowedTopics)
            end
    end.

cache_miss_fallback_check(ActorTokenId, Pattern, ScopeSlug, AllowedTopics) ->
    case kraken_auth:check_room_access(ActorTokenId, Pattern) of
        {ok, NewTopics} when is_list(NewTopics), NewTopics =/= [] ->
            Merged = NewTopics ++ AllowedTopics,
            %% Titus injects the actor's scope into returned patterns, so the
            %% effective pattern must be recomputed against the merged set —
            %% exactly as the allow branch will on re-dispatch.
            EffectivePattern = maybe_inject_scope(Pattern, ScopeSlug, Merged),
            case kraken_acl:can_subscribe(EffectivePattern, Merged) of
                true ->
                    {ok, Merged};
                false ->
                    %% Control plane allowed the room but no returned topic
                    %% matched the requested pattern — treat as deny.
                    kraken_acl:deny_cache_insert(ActorTokenId, Pattern),
                    deny
            end;
        _ ->
            %% Deny, unsupported backend, or transport error → fail closed.
            kraken_acl:deny_cache_insert(ActorTokenId, Pattern),
            deny
    end.

cache_miss_fallback_enabled() ->
    application:get_env(kraken, cache_miss_fallback_enabled, true) =:= true.

%% Inject scope slug into topic pattern for scoped actors.
%% Rewrites "app/room/topic" -> "app/scope/room/topic"
%% Only rewrites if the unscoped pattern doesn't match but the scoped one does.
maybe_inject_scope(Pattern, undefined, _AllowedTopics) ->
    Pattern;
maybe_inject_scope(Pattern, ScopeSlug, AllowedTopics) ->
    case kraken_acl:can_subscribe(Pattern, AllowedTopics) of
        true ->
            %% Already matches without scope injection
            Pattern;
        false ->
            %% Try injecting scope: "app/room/topic" -> "app/scope/room/topic"
            case binary:split(Pattern, <<"/">>) of
                [AppSlug, Rest] ->
                    ScopedPattern = <<AppSlug/binary, "/", ScopeSlug/binary, "/", Rest/binary>>,
                    case kraken_acl:can_subscribe(ScopedPattern, AllowedTopics) of
                        true ->
                            kraken_log:info("[WS] Scope rewrite: ~s -> ~s", [Pattern, ScopedPattern]),
                            ScopedPattern;
                        false ->
                            Pattern  %% Scoped version doesn't match either
                    end;
                _ ->
                    Pattern  %% Can't split — no app prefix
            end
    end.
