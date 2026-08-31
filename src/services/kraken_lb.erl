%%%-------------------------------------------------------------------
%% @doc Bookkeeping for load-balanced ($share) subscriptions.
%%
%% A load-balanced subscribe becomes an EMQX shared subscription,
%% `$share/<project>_<app>_<group>/<topic>', whose contract is that each
%% message reaches exactly one member of the group. Two things break that
%% contract once the subscriber holds a PERSISTENT session (agent and
%% orchestrator actors: `clean_start=false' plus a session expiry), and both
%% are handled here:
%%
%%   A disconnected member keeps its slot. EMQX retains the session, and the
%%   retained session keeps its shared subscription, so the broker goes on
%%   round-robining work to a member that is not there. One live member
%%   alongside one departed member receives half the traffic. The same
%%   retention means a session accumulates a subscription for every group
%%   name the actor has ever used, each taking its own copy.
%%   `release_shared_subscriptions/2' gives the slot back on the way out.
%%
%%   Replay cannot tell consumed work from missed work. The group cursor only
%%   moves at baseline, at the end of a replay, and on disconnect, so while
%%   members are live and consuming it stands still and the next member to
%%   join is handed everything since. `claim_live/3' writes the per-message
%%   claim that `dd_mark_offline_boundary' always assumed was there ("multi-
%%   instance pools rely on the per-message claim + consumer idempotency to
%%   dedup any overlap"), which makes replay skip what has already been
%%   taken.
%%
%% All of these read the connection's process dictionary, so they must be
%% called FROM the websocket handler process, never from a spawned helper.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_lb).

-export([
    remember_subscription/4,
    forget_subscription/1,
    mark_offline_boundary/0,
    release_shared_subscriptions/2,
    claim_live/3
]).

%%====================================================================
%% Subscription context
%%====================================================================

%% Remember a load-balanced subscription's replay context (group/room/app),
%% keyed by pattern, so a later persistent signal can replay it and so a live
%% delivery on this topic can be claimed. Needs a resolved room to scope the
%% backlog query; a wildcard subscription has none and is simply not tracked.
-spec remember_subscription(binary(), binary() | undefined, binary(), binary()) -> ok.
remember_subscription(Pattern, RoomId, AppId, GroupId) when RoomId =/= undefined ->
    %% topic = the subscribed Pattern (the DISPLAY topic the SDK keys its handler
    %% on); replayed frames must carry it, not the message's internal topic.
    put({lb_subscription, Pattern},
        #{group_id => GroupId, room_id => RoomId, app_id => AppId, topic => Pattern}),
    ok;
remember_subscription(_Pattern, _RoomId, _AppId, _GroupId) ->
    ok.

%% Drop the context when the actor unsubscribes. Without this the connection
%% keeps a replay context for a group it has left, which can arm a replay it
%% has no business draining.
-spec forget_subscription(binary()) -> ok.
forget_subscription(Pattern) ->
    erase({lb_subscription, Pattern}),
    ok.

%%====================================================================
%% Disconnect
%%====================================================================

%% On disconnect, stamp each load-balanced durable subscription's group cursor
%% at "now" — the offline boundary. A reconnecting group member then replays
%% only messages dispatched after this point (what it missed while offline),
%% instead of re-surfacing the window it already consumed live. Inert unless
%% the delivery_store is enabled.
-spec mark_offline_boundary() -> ok.
mark_offline_boundary() ->
    case durable_enabled() of
        true ->
            Now = erlang:system_time(millisecond),
            lists:foreach(
                fun({{lb_subscription, _Pattern}, Ctx}) when is_map(Ctx) ->
                        catch kraken_delivery_store:cursor_set(
                            #{app_id => maps:get(app_id, Ctx, <<>>),
                              group_id => maps:get(group_id, Ctx, <<>>)}, Now);
                   (_) -> ok
                end, get());
        false ->
            ok
    end.

%% Hand back this connection's share of every load-balanced group before the
%% session is retained.
%%
%% Only for a persistent session: a clean session drops its subscriptions on
%% disconnect anyway, so there is nothing to release and no reason to pay a
%% round trip per topic on every close.
%%
%% Only when the delivery store is enabled, and this one is load bearing.
%% Releasing the subscription also gives up the retained-session queue, which
%% is what holds a scaled-to-zero pool's backlog today. Durable delivery is
%% what replaces it: `kraken_replay' recovers that backlog from the message
%% store instead. Without a delivery store behind it this would convert a
%% distribution bug into lost work, so those deployments keep the old
%% behaviour.
-spec release_shared_subscriptions(term(), boolean()) -> ok.
release_shared_subscriptions(Session, true) ->
    case release_enabled() andalso durable_enabled() of
        true ->
            lists:foreach(
                fun({{mqtt_topics_for, _Pattern}, Topics}) when is_list(Topics) ->
                        release_each(Session, Topics);
                   (_) -> ok
                end, get());
        false ->
            ok
    end;
release_shared_subscriptions(_Session, _NotPersistent) ->
    ok.

release_each(Session, Topics) ->
    lists:foreach(
        fun(Topic) ->
            case is_shared(Topic) of
                true ->
                    %% Deliberately NOT kraken_subscriptions:track/4. That
                    %% reports to the control plane and drives the
                    %% active_subscriptions the next connect restores from;
                    %% this is a broker-level release, not the actor giving
                    %% the subscription up.
                    %%
                    %% catch: the MQTT backend hard-matches emqtt's reply and
                    %% the client is frequently already gone by terminate/3.
                    catch kraken_broker:unsubscribe(Session, Topic);
                false ->
                    ok
            end
        end, Topics).

is_shared(<<"$share/", _/binary>>) -> true;
is_shared(_) -> false.

%%====================================================================
%% Live delivery
%%====================================================================

%% Claim a message that was just delivered live through a load-balanced
%% subscription, so a later replay treats it as taken rather than offering it
%% to the next member that joins.
%%
%% Inert for anything that is not load balanced, and for messages with no id
%% (message recording off — nothing was stored, so there is nothing to replay
%% either). Detached, because the claim is a backend write and this is the
%% frame path; the result is discarded, since losing a claim race only means
%% replay may re-offer one message, which consumers already have to tolerate.
-spec claim_live(binary() | undefined, binary(), binary()) -> ok.
claim_live(undefined, _DisplayTopic, _ActorId) ->
    ok;
claim_live(MsgId, DisplayTopic, ActorId) ->
    case get({lb_subscription, DisplayTopic}) of
        Ctx when is_map(Ctx) ->
            case durable_enabled() of
                true -> detach_claim(Ctx, MsgId, ActorId);
                false -> ok
            end;
        _ ->
            ok
    end.

detach_claim(Ctx, MsgId, ActorId) ->
    Req = #{
        app_id => maps:get(app_id, Ctx, <<>>),
        group_id => maps:get(group_id, Ctx, <<>>),
        message_id => MsgId,
        actor_id => ActorId
    },
    kraken_detach:run(lb_claim_live, fun() ->
        catch kraken_delivery_store:claim(Req),
        ok
    end).

%%====================================================================
%% Config
%%====================================================================

durable_enabled() ->
    case catch kraken_delivery_store:is_enabled() of
        true -> true;
        _ -> false
    end.

%% Kill switch, mirroring cache_miss_fallback_enabled.
release_enabled() ->
    case application:get_env(kraken, release_shared_subs_on_close, true) of
        false -> false;
        "false" -> false;
        <<"false">> -> false;
        _ -> true
    end.
