%%%-------------------------------------------------------------------
%% @doc E2E/dev auth backend: accepts ANY token and grants a single
%% pre-provisioned room ("echo-room" / room_id "echo-room-uuid" in app
%% "dev-app") with an exact topic mapping, so room-scoped presence +
%% publish resolve without Titus. The actor id = the presented token, so
%% multiple clients get distinct identities.
%%
%% Enable with: auth_backend=kraken_auth_e2e. NEVER for production.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_auth_e2e).
-behaviour(kraken_auth).

-export([validate_token/1, revalidate_token/1]).

attrs(Id) ->
    #{
        <<"actor_token_id">> => Id,
        <<"organization_id">> => <<"dev-org">>,
        <<"project_id">> => <<"dev-project">>,
        <<"actor_type">> => <<"agent">>,
        %% Persistent session so a load-balanced subscriber qualifies for
        %% claim-based durable replay (scale-to-zero) in the E2E.
        <<"persistent_session">> => true,
        <<"session_expiry_seconds">> => 3600,
        <<"apps">> => [#{
            <<"app_id">> => <<"dev-app">>,
            <<"app_name">> => <<"dev">>,
            <<"allowed_topics">> => [
                %% Exact rules -> resolve returns {exact, InternalTopic, RoomId, AppId}.
                %% Raw-client room.
                #{
                    <<"pattern">> => <<"dev/echo-room/tasks">>,
                    <<"topic">> => <<"echo-room-uuid/tasks">>,
                    <<"room_id">> => <<"echo-room-uuid">>,
                    <<"room_slug">> => <<"echo-room">>,
                    <<"permission">> => <<"pubSub">>
                },
                %% echo-runtime's room ("echo-workers"). room_slug lets presence
                %% (roomId="echo-workers") resolve; the exact "ew/tasks" pattern lets
                %% a dispatcher publish resolve to the same room_id (so the wake fires).
                #{
                    <<"pattern">> => <<"ew/tasks">>,
                    <<"topic">> => <<"echo-workers-uuid/tasks">>,
                    <<"room_id">> => <<"echo-workers-uuid">>,
                    <<"room_slug">> => <<"echo-workers">>,
                    <<"permission">> => <<"pubSub">>
                }
            ]
        }]
    }.

validate_token(Token) ->
    Id = case Token of
             T when is_binary(T), byte_size(T) > 0 -> T;
             _ -> <<"dev-actor">>
         end,
    {ok, kraken_auth:build_auth_data(attrs(Id))}.

revalidate_token(ActorTokenId) ->
    {ok, kraken_auth:build_auth_data(attrs(ActorTokenId))}.
