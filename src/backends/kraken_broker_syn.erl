%%%-------------------------------------------------------------------
%% @doc Built-in syn-based broker backend (the zero-dependency default).
%%
%% Fan-out via syn process groups in the kraken_topics scope. Designed
%% as a STARTER broker: a single Erlang cluster, no external services.
%% For massive or multi-region deployments, configure an external MQTT
%% broker via kraken_broker_mqtt instead.
%%
%% Semantics matched to the MQTT backend:
%%  - exact-topic groups, plus wildcard subscriptions (+/#) matched at
%%    publish time
%%  - shared subscriptions ($share/Group/Topic) deliver to ONE member
%%  - retained messages stored in ETS with TTL, replayed on subscribe
%%  - subscribers receive {mqtt_publish, #{topic := T, payload := P}}
%% @end
%%%-------------------------------------------------------------------
-module(kraken_broker_syn).
-behaviour(kraken_broker).

-export([
    start/0,
    connect/0, connect/1, connect/3,
    subscribe/5,
    unsubscribe/2,
    publish/6,
    disconnect/1,
    format_shared_subscription/2,
    supports_load_balancing/0,
    capabilities/0
]).

-define(SCOPE, kraken_topics).
-define(RETAINED, kraken_retained_messages).
-define(RETAIN_TTL_MS, 3600_000).

start() ->
    case ets:info(?RETAINED, size) of
        undefined ->
            ets:new(?RETAINED, [named_table, public, set, {read_concurrency, true}]),
            ok;
        _ ->
            ok
    end.

%% Sessions are just the owning connection pid; syn cleans groups up
%% automatically when the process dies.
connect() ->
    {ok, #{pid => self()}}.

connect(_AuthData) ->
    {ok, #{pid => self()}}.

connect(AuthData, _PersistentSession, _SessionExpirySeconds) ->
    ActorTokenId = maps:get(actor_token_id, AuthData, <<"anon">>),
    UniqueId = integer_to_binary(erlang:unique_integer([positive])),
    ClientId = <<"kraken_syn_", ActorTokenId/binary, "_", UniqueId/binary>>,
    {ok, #{pid => self()}, ClientId}.

subscribe(_Session, MqttTopic, DisplayTopic, WsPid, _QoS) ->
    WsPid ! {store_topic_mapping, MqttTopic, DisplayTopic},
    case parse_share(MqttTopic) of
        {share, Group, BaseTopic} ->
            ok = syn:join(?SCOPE, {share, Group, BaseTopic}, WsPid),
            deliver_retained(BaseTopic, WsPid);
        {plain, Topic} ->
            ok = syn:join(?SCOPE, {topic, Topic}, WsPid),
            deliver_retained(Topic, WsPid)
    end,
    ok.

unsubscribe(_Session, MqttTopic) ->
    Self = self(),
    case parse_share(MqttTopic) of
        {share, Group, BaseTopic} ->
            catch syn:leave(?SCOPE, {share, Group, BaseTopic}, Self);
        {plain, Topic} ->
            catch syn:leave(?SCOPE, {topic, Topic}, Self)
    end,
    ok.

publish(_Session, Topic, Data, Sender, _QoS, Retain) ->
    Payload = encode(Data, Sender),
    case Retain of
        true ->
            ExpireAt = erlang:monotonic_time(millisecond) + ?RETAIN_TTL_MS,
            ets:insert(?RETAINED, {Topic, Payload, ExpireAt});
        false ->
            ok
    end,
    fanout(Topic, Payload),
    ok.

disconnect(_Session) ->
    %% syn removes dead processes from groups automatically; for live
    %% disconnects the connection handler unsubscribes per topic.
    ok.

format_shared_subscription(BaseTopic, Group) ->
    <<"$share/", Group/binary, "/", BaseTopic/binary>>.

supports_load_balancing() ->
    true.

capabilities() ->
    #{retained => true, shared_subscriptions => true, multi_region => false}.

%%====================================================================
%% Internal
%%====================================================================

encode(Data, undefined) ->
    msgpack:pack(Data, [{pack_str, from_binary}]);
encode(Data, Sender) ->
    Envelope = #{<<"data">> => Data, <<"_sender">> => Sender},
    msgpack:pack(Envelope, [{pack_str, from_binary}]).

fanout(Topic, Payload) ->
    Msg = {mqtt_publish, #{topic => Topic, payload => Payload}},
    %% Exact-topic subscribers (syn members are {Pid, Meta} tuples)
    [P ! Msg || {P, _} <- syn:members(?SCOPE, {topic, Topic})],
    %% Wildcard subscribers + shared groups: scan group names. Group
    %% count is bounded by active subscription patterns; fine for the
    %% starter-broker scale this backend targets.
    lists:foreach(
        fun({topic, Sub} = _G) when Sub =/= Topic ->
                case has_wildcard(Sub) andalso topic_matches(Topic, Sub) of
                    true ->
                        SubMsg = {mqtt_publish, #{topic => Sub, payload => Payload}},
                        [P ! SubMsg || {P, _} <- syn:members(?SCOPE, {topic, Sub})];
                    false ->
                        ok
                end;
           ({share, Group, Sub}) ->
                case Sub =:= Topic orelse (has_wildcard(Sub) andalso topic_matches(Topic, Sub)) of
                    true ->
                        deliver_one(syn:members(?SCOPE, {share, Group, Sub}), Topic, Payload);
                    false ->
                        ok
                end;
           (_) ->
                ok
        end,
        syn:group_names(?SCOPE)).

deliver_one([], _Topic, _Payload) ->
    ok;
deliver_one(Members, Topic, Payload) ->
    N = erlang:phash2(erlang:unique_integer(), length(Members)) + 1,
    {Pid, _Meta} = lists:nth(N, Members),
    Pid ! {mqtt_publish, #{topic => Topic, payload => Payload}},
    ok.

deliver_retained(Sub, WsPid) ->
    Now = erlang:monotonic_time(millisecond),
    case has_wildcard(Sub) of
        false ->
            case ets:lookup(?RETAINED, Sub) of
                [{_, Payload, ExpireAt}] when ExpireAt > Now ->
                    WsPid ! {mqtt_publish, #{topic => Sub, payload => Payload}};
                [{_, _, _}] ->
                    ets:delete(?RETAINED, Sub);
                [] ->
                    ok
            end;
        true ->
            ets:foldl(
                fun({Topic, Payload, ExpireAt}, _) when ExpireAt > Now ->
                        case topic_matches(Topic, Sub) of
                            true -> WsPid ! {mqtt_publish, #{topic => Topic, payload => Payload}};
                            false -> ok
                        end;
                   (_, _) ->
                        ok
                end, ok, ?RETAINED)
    end,
    ok.

parse_share(<<"$share/", Rest/binary>>) ->
    case binary:split(Rest, <<"/">>) of
        [Group, BaseTopic] -> {share, Group, BaseTopic};
        _ -> {plain, Rest}
    end;
parse_share(Topic) ->
    {plain, Topic}.

has_wildcard(Topic) ->
    binary:match(Topic, <<"+">>) =/= nomatch orelse
    binary:match(Topic, <<"#">>) =/= nomatch.

%% MQTT-style topic matching: Topic is concrete, Sub may contain +/#.
topic_matches(Topic, Sub) ->
    match_levels(binary:split(Topic, <<"/">>, [global]),
                 binary:split(Sub, <<"/">>, [global])).

match_levels(_, [<<"#">>]) -> true;
match_levels([], []) -> true;
match_levels([_ | Tr], [<<"+">> | Sr]) -> match_levels(Tr, Sr);
match_levels([L | Tr], [L | Sr]) -> match_levels(Tr, Sr);
match_levels(_, _) -> false.
