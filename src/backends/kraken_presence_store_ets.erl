%%%-------------------------------------------------------------------
%% @doc In-memory (ETS) presence store — a runnable kraken_presence_store
%% backend for local/dev/E2E (no Firestore). A keeper process owns the
%% named public table so it survives individual connection processes
%% dying. NOT for production (single-node, non-durable).
%%
%% Enable with: presence_store_backend=kraken_presence_store_ets
%% @end
%%%-------------------------------------------------------------------
-module(kraken_presence_store_ets).
-behaviour(kraken_presence_store).

-export([upsert/1, offline/1, mark_waking/1, discover/1]).

-define(TAB, kraken_presence_ets).
-define(KEEPER, kraken_presence_ets_keeper).

%% Ensure a stable owner process + table exist.
ensure() ->
    case whereis(?KEEPER) of
        undefined ->
            Parent = self(),
            Pid = spawn(fun() ->
                catch ets:new(?TAB, [named_table, public, set]),
                Parent ! {keeper_ready, self()},
                keeper_loop()
            end),
            try register(?KEEPER, Pid) catch error:badarg -> ok end,
            receive {keeper_ready, Pid} -> ok after 1000 -> ok end,
            ok;
        _ ->
            ok
    end.

keeper_loop() ->
    receive _ -> keeper_loop() end.

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
        <<"appId">> => AppId, <<"roomId">> => RoomId, <<"actorTokenId">> => ActorId,
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
