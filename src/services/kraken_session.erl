%%%-------------------------------------------------------------------
%% @doc Which MQTT session a connection owns.
%%
%% An MQTT session belongs to a client INSTANCE, not to a credential. Kraken
%% keyed persistent sessions on the actor token alone, so every process holding
%% the same token asked the broker for the same session: the second connection
%% was a session takeover, the first was disconnected, and publishes in flight
%% on both stopped being acknowledged. Nothing about it looked wrong — both
%% authenticated, both appeared in presence — so it read as a slow broker.
%%
%% Two ways out, and this module implements both:
%%
%%   The client names its instance. `clientId' in the auth frame becomes part
%%   of the session key, so several workers under one token each keep their own
%%   resumable session and none of them collide. This is the one to reach for
%%   when concurrent workers need queue-and-flush; it has to come from the
%%   client, because only the client knows which reconnect is "the same worker
%%   coming back" rather than "a different worker".
%%
%%   Nobody names anything. Then only the FIRST live connection for an actor
%%   owns the shared session; any concurrent connection gets a clean, ephemeral
%%   one instead. It loses durability, which costs it nothing: before this, a
%%   second connection was kicked and had none either.
%%
%% Membership is joined before the count is read, so two connections racing
%% each other both see two members and both go ephemeral. That direction is
%% safe — nobody is disconnected — where the other one is a takeover.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_session).

-export([claim/3, release/1, sanitise_client_id/1]).

-define(SCOPE, kraken_actors).
-define(MAX_CLIENT_ID, 64).

%% Decide this connection's session identity.
%%
%% Returns `{Persistent, SessionKey}': whether the broker should resume a
%% session at all, and the key to build the MQTT ClientId from (`undefined'
%% meaning "the actor's own id", the historical behaviour).
-spec claim(ActorTokenId :: binary(), Persistent :: boolean(),
            ClientId :: binary() | undefined) ->
    {boolean(), binary() | undefined}.
claim(_ActorTokenId, false, _ClientId) ->
    %% A clean session has no identity to protect.
    {false, undefined};
claim(ActorTokenId, true, ClientId) when is_binary(ActorTokenId) ->
    case sanitise_client_id(ClientId) of
        undefined ->
            case join_and_count(ActorTokenId) of
                1 -> {true, undefined};
                _ -> {false, undefined}
            end;
        Instance ->
            _ = join_and_count(ActorTokenId),
            {true, <<ActorTokenId/binary, "_", Instance/binary>>}
    end;
claim(_ActorTokenId, _Persistent, _ClientId) ->
    {false, undefined}.

%% Stop counting against this actor. Called from terminate/3.
-spec release(ActorTokenId :: binary() | undefined) -> ok.
release(ActorTokenId) when is_binary(ActorTokenId) ->
    catch syn:leave(?SCOPE, {actor, ActorTokenId}, self()),
    ok;
release(_) ->
    ok.

%% The client id ends up inside an MQTT ClientId, so keep it to characters that
%% cannot be mistaken for topic structure or upset a broker, and bound its
%% length. Anything left empty is treated as absent rather than as "".
-spec sanitise_client_id(term()) -> binary() | undefined.
sanitise_client_id(Id) when is_binary(Id) ->
    Clean = << <<C>> || <<C>> <= Id, safe_char(C) >>,
    case byte_size(Clean) of
        0 -> undefined;
        N when N > ?MAX_CLIENT_ID -> binary:part(Clean, 0, ?MAX_CLIENT_ID);
        _ -> Clean
    end;
sanitise_client_id(_) ->
    undefined.

%%====================================================================
%% Internal
%%====================================================================

%% Join first, then count, so a race resolves to "both ephemeral" rather than
%% "both claim the same session and one kicks the other".
join_and_count(ActorTokenId) ->
    case catch begin
        syn:join(?SCOPE, {actor, ActorTokenId}, self()),
        length(syn:members(?SCOPE, {actor, ActorTokenId}))
    end of
        N when is_integer(N), N > 0 ->
            N;
        _ ->
            %% syn unavailable (or not this node's scope): behave as before,
            %% which is a single owning connection.
            1
    end.

safe_char(C) when C >= $a, C =< $z -> true;
safe_char(C) when C >= $A, C =< $Z -> true;
safe_char(C) when C >= $0, C =< $9 -> true;
safe_char($-) -> true;
safe_char($_) -> true;
safe_char(_) -> false.
