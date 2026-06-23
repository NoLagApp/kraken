%%%-------------------------------------------------------------------
%% @doc Test kraken_presence_store backend (ETS-backed) for the
%% persistent-presence integration test. Mirrors firestore_presence_store
%% semantics without Firestore.
%% @end
%%%-------------------------------------------------------------------
-module(pp_test_store).
-behaviour(kraken_presence_store).

-export([upsert/1, offline/1, mark_waking/1, discover/1, reset/0]).

-define(TAB, pp_test_store_tab).

ensure() ->
    case ets:info(?TAB) of
        undefined -> ets:new(?TAB, [named_table, public, set]);
        _ -> ?TAB
    end.

reset() ->
    ensure(),
    ets:delete_all_objects(?TAB),
    ok.

pick(M, A, B) ->
    case maps:get(A, M, undefined) of
        undefined -> maps:get(B, M, <<>>);
        V -> V
    end.

key(R) ->
    {pick(R, app_id, <<"appId">>),
     pick(R, room_id, <<"roomId">>),
     pick(R, actor_token_id, <<"actorTokenId">>)}.

wake_url(R) ->
    case maps:get(wake, R, maps:get(<<"wake">>, R, undefined)) of
        #{<<"url">> := U} -> U;
        _ -> <<>>
    end.

upsert(R) ->
    ensure(),
    {AppId, RoomId, ActorId} = key(R),
    Rec = #{
        <<"appId">> => AppId,
        <<"roomId">> => RoomId,
        <<"actorTokenId">> => ActorId,
        <<"status">> => <<"online">>,
        <<"presence">> => maps:get(advertisement, R, #{}),
        <<"wakeUrl">> => wake_url(R)
    },
    ets:insert(?TAB, {{AppId, RoomId, ActorId}, Rec}),
    ok.

set_status(R, Status) ->
    ensure(),
    K = key(R),
    case ets:lookup(?TAB, K) of
        [{K, Rec}] -> ets:insert(?TAB, {K, Rec#{<<"status">> => Status}}), ok;
        [] -> ok
    end.

offline(R) -> set_status(R, <<"offline">>).
mark_waking(R) -> set_status(R, <<"waking">>).

discover(Q) ->
    ensure(),
    RoomId = pick(Q, room_id, <<"roomId">>),
    Status = case maps:get(status, Q, undefined) of
                 undefined -> maps:get(<<"status">>, Q, any);
                 S -> S
             end,
    Out = [Rec || {_K, Rec} <- ets:tab2list(?TAB),
                  maps:get(<<"roomId">>, Rec) =:= RoomId,
                  (Status =:= any orelse maps:get(<<"status">>, Rec) =:= Status)],
    {ok, Out}.
