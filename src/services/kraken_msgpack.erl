%%%-------------------------------------------------------------------
%% @doc MessagePack packing that accepts arbitrary bytes.
%%
%% Kraken packs with `{pack_str, from_binary}' so Erlang binaries reach
%% JavaScript clients as strings. msgpack-erlang implements that by decoding
%% every binary as UTF-8, and raises `error:badarg' when a binary is not valid
%% UTF-8. MQTT payloads are opaque bytes (sensor frames, protobuf, CBOR...), so
%% a single binary payload used to crash the connection that published it.
%%
%% pack/1 returns exactly what `msgpack:pack(Term, [{pack_str, from_binary}])'
%% returns for every term that call already handled. Only when that call
%% raises does it re-encode the term, packing each binary that is not valid
%% UTF-8 as a MessagePack bin (raw bytes) and everything else unchanged.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_msgpack).

-export([pack/1]).

-define(OPTS, [{pack_str, from_binary}]).

-spec pack(term()) -> binary() | {error, term()}.
pack(Term) ->
    try msgpack:pack(Term, ?OPTS)
    catch
        error:badarg -> lenient(Term)
    end.

%%====================================================================
%% Internal
%%====================================================================

lenient(Term) ->
    try iolist_to_binary(encode(Term))
    catch
        throw:{kraken_msgpack, Error} -> {error, Error}
    end.

encode(Bin) when is_binary(Bin) ->
    case unicode:characters_to_binary(Bin) of
        Bin -> scalar(Bin);
        _ -> [bin_header(byte_size(Bin)), Bin]
    end;
encode(Map) when is_map(Map) ->
    [map_header(maps:size(Map))
     | [[encode(K), encode(V)] || {K, V} <- maps:to_list(Map)]];
encode(List) when is_list(List) ->
    [array_header(length(List)) | [encode(E) || E <- List]];
encode(Other) ->
    scalar(Other).

scalar(Term) ->
    case msgpack:pack(Term, ?OPTS) of
        Packed when is_binary(Packed) -> Packed;
        {error, Error} -> throw({kraken_msgpack, Error})
    end.

bin_header(N) when N < 16#100 -> <<16#C4, N:8>>;
bin_header(N) when N < 16#10000 -> <<16#C5, N:16>>;
bin_header(N) -> <<16#C6, N:32>>.

map_header(N) when N < 16 -> <<2#1000:4, N:4>>;
map_header(N) when N < 16#10000 -> <<16#DE, N:16>>;
map_header(N) -> <<16#DF, N:32>>.

array_header(N) when N < 16 -> <<2#1001:4, N:4>>;
array_header(N) when N < 16#10000 -> <<16#DC, N:16>>;
array_header(N) -> <<16#DD, N:32>>.
