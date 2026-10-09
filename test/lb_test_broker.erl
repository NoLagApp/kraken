%%%-------------------------------------------------------------------
%% @doc Test kraken_broker backend that records its unsubscribes, so the
%% shared-subscription release path can be asserted without EMQX.
%%
%% `fail/1' makes unsubscribe raise the way kraken_broker_mqtt does when the
%% emqtt client is already gone (it hard-matches `{ok, _, _}'), which at
%% terminate time is the common case.
%% @end
%%%-------------------------------------------------------------------
-module(lb_test_broker).
-behaviour(kraken_broker).

-export([
    start/0, connect/0, connect/1, connect/3,
    subscribe/5, unsubscribe/2, publish/6, disconnect/1,
    format_shared_subscription/2, supports_load_balancing/0, capabilities/0
]).
%% Test helpers
-export([reset/0, fail/1, unsubscribed/0, published/0]).

-define(TAB, lb_test_broker_tab).

ensure() ->
    case ets:info(?TAB) of
        undefined -> ets:new(?TAB, [named_table, public, ordered_set]);
        _ -> ?TAB
    end.

reset() ->
    ensure(),
    ets:delete_all_objects(?TAB),
    ets:insert(?TAB, {fail, false}),
    ok.

%% Make the next unsubscribes raise, like a dead emqtt client.
fail(Bool) ->
    ensure(),
    ets:insert(?TAB, {fail, Bool}),
    ok.

%% Topics released, in call order.
unsubscribed() ->
    ensure(),
    [T || {{seq, _N}, T} <- ets:tab2list(?TAB)].

%% Publishes, in call order, as {Topic, Payload, Sender, QoS, Retain}.
published() ->
    ensure(),
    [P || {{pub, _N}, P} <- ets:tab2list(?TAB)].

%%====================================================================
%% kraken_broker
%%====================================================================

start() -> ok.
connect() -> {ok, #{}}.
connect(_AuthData) -> {ok, #{}}.
connect(_AuthData, _Persistent, _Expiry) -> {ok, #{}, <<"lb_test_client">>}.

subscribe(_Session, _MqttTopic, _DisplayTopic, _WsPid, _QoS) -> ok.

unsubscribe(_Session, Topic) ->
    ensure(),
    case ets:lookup(?TAB, fail) of
        [{fail, true}] ->
            %% Same shape of failure as a badmatch on emqtt's reply.
            error({badmatch, {error, closed}});
        _ ->
            N = ets:update_counter(?TAB, seq_counter, {2, 1}, {seq_counter, 0}),
            ets:insert(?TAB, {{seq, N}, Topic}),
            ok
    end.

publish(_Session, Topic, Data, Sender, QoS, Retain) ->
    ensure(),
    N = ets:update_counter(?TAB, pub_counter, {2, 1}, {pub_counter, 0}),
    ets:insert(?TAB, {{pub, N}, {Topic, Data, Sender, QoS, Retain}}),
    ok.
disconnect(_Session) -> ok.

format_shared_subscription(BaseTopic, Group) ->
    <<"$share/", Group/binary, "/", BaseTopic/binary>>.

supports_load_balancing() -> true.

capabilities() ->
    #{retained => false, shared_subscriptions => true, multi_region => false}.
