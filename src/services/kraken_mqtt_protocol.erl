%%%-------------------------------------------------------------------
%% @doc MQTT Protocol Encoder/Decoder
%% Handles MQTT 3.1.1 packet parsing and serialization.
%% @end
%%%-------------------------------------------------------------------
-module(kraken_mqtt_protocol).

-export([decode/1, encode/1]).
-export([encode_connack/2, encode_suback/2, encode_unsuback/1,
         encode_puback/1, encode_pubrec/1, encode_pubrel/1, encode_pubcomp/1,
         encode_pingresp/0, encode_publish/4]).

%% MQTT Packet Types
-define(CONNECT,     1).
-define(CONNACK,     2).
-define(PUBLISH,     3).
-define(PUBACK,      4).
-define(PUBREC,      5).
-define(PUBREL,      6).
-define(PUBCOMP,     7).
-define(SUBSCRIBE,   8).
-define(SUBACK,      9).
-define(UNSUBSCRIBE, 10).
-define(UNSUBACK,    11).
-define(PINGREQ,     12).
-define(PINGRESP,    13).
-define(DISCONNECT,  14).

%% CONNACK Return Codes
-define(CONNACK_ACCEPTED,              0).
-define(CONNACK_UNACCEPTABLE_PROTOCOL, 1).
-define(CONNACK_IDENTIFIER_REJECTED,   2).
-define(CONNACK_SERVER_UNAVAILABLE,    3).
-define(CONNACK_BAD_CREDENTIALS,       4).
-define(CONNACK_NOT_AUTHORIZED,        5).

%%====================================================================
%% API - Decode
%%====================================================================

%% Decode an MQTT packet from binary
%% Returns {ok, PacketType, PacketData, Rest} | {error, Reason} | incomplete
decode(<<>>) ->
    incomplete;
decode(<<TypeFlags:8, Rest/binary>>) ->
    Type = TypeFlags bsr 4,
    Flags = TypeFlags band 16#0F,
    case decode_remaining_length(Rest) of
        {ok, Length, Rest2} ->
            case byte_size(Rest2) >= Length of
                true ->
                    <<Payload:Length/binary, Rest3/binary>> = Rest2,
                    case decode_packet(Type, Flags, Payload) of
                        {ok, Packet} -> {ok, Packet, Rest3};
                        {error, Reason} -> {error, Reason}
                    end;
                false ->
                    incomplete
            end;
        incomplete ->
            incomplete;
        {error, Reason} ->
            {error, Reason}
    end.

%% Decode variable length integer
decode_remaining_length(Binary) ->
    decode_remaining_length(Binary, 0, 1).

decode_remaining_length(<<>>, _Value, _Multiplier) ->
    incomplete;
decode_remaining_length(<<Byte:8, Rest/binary>>, Value, Multiplier) when Multiplier =< 2097152 ->
    NewValue = Value + (Byte band 127) * Multiplier,
    case Byte band 128 of
        0 -> {ok, NewValue, Rest};
        _ -> decode_remaining_length(Rest, NewValue, Multiplier * 128)
    end;
decode_remaining_length(_, _, _) ->
    {error, malformed_remaining_length}.

%% Decode specific packet types
decode_packet(?CONNECT, _Flags, Payload) ->
    decode_connect(Payload);
decode_packet(?PUBLISH, Flags, Payload) ->
    decode_publish(Flags, Payload);
decode_packet(?PUBACK, _Flags, <<PacketId:16>>) ->
    {ok, {puback, PacketId}};
decode_packet(?PUBREC, _Flags, <<PacketId:16>>) ->
    {ok, {pubrec, PacketId}};
decode_packet(?PUBREL, _Flags, <<PacketId:16>>) ->
    {ok, {pubrel, PacketId}};
decode_packet(?PUBCOMP, _Flags, <<PacketId:16>>) ->
    {ok, {pubcomp, PacketId}};
decode_packet(?SUBSCRIBE, _Flags, Payload) ->
    decode_subscribe(Payload);
decode_packet(?UNSUBSCRIBE, _Flags, Payload) ->
    decode_unsubscribe(Payload);
decode_packet(?PINGREQ, _Flags, <<>>) ->
    {ok, pingreq};
decode_packet(?DISCONNECT, _Flags, <<>>) ->
    {ok, disconnect};
decode_packet(Type, _Flags, _Payload) ->
    {error, {unsupported_packet_type, Type}}.

%% Decode CONNECT packet
decode_connect(Payload) ->
    case Payload of
        <<ProtoNameLen:16, ProtoName:ProtoNameLen/binary, ProtoLevel:8,
          ConnectFlags:8, KeepAlive:16, Rest/binary>> ->
            %% Parse connect flags
            CleanSession = (ConnectFlags band 2#00000010) bsr 1,
            WillFlag = (ConnectFlags band 2#00000100) bsr 2,
            WillQoS = (ConnectFlags band 2#00011000) bsr 3,
            WillRetain = (ConnectFlags band 2#00100000) bsr 5,
            PasswordFlag = (ConnectFlags band 2#01000000) bsr 6,
            UsernameFlag = (ConnectFlags band 2#10000000) bsr 7,

            %% Parse client ID
            case Rest of
                <<ClientIdLen:16, ClientId:ClientIdLen/binary, Rest2/binary>> ->
                    %% Skip will topic/message if present
                    Rest3 = case WillFlag of
                        1 ->
                            <<WillTopicLen:16, _WillTopic:WillTopicLen/binary,
                              WillMsgLen:16, _WillMsg:WillMsgLen/binary, R/binary>> = Rest2,
                            R;
                        0 ->
                            Rest2
                    end,
                    %% Parse username if present
                    {Username, Rest4} = case UsernameFlag of
                        1 ->
                            <<ULen:16, U:ULen/binary, R2/binary>> = Rest3,
                            {U, R2};
                        0 ->
                            {undefined, Rest3}
                    end,
                    %% Parse password if present
                    Password = case PasswordFlag of
                        1 ->
                            <<PLen:16, P:PLen/binary, _/binary>> = Rest4,
                            P;
                        0 ->
                            undefined
                    end,
                    {ok, {connect, #{
                        protocol_name => ProtoName,
                        protocol_level => ProtoLevel,
                        clean_session => CleanSession =:= 1,
                        keep_alive => KeepAlive,
                        client_id => ClientId,
                        username => Username,
                        password => Password,
                        will_qos => WillQoS,
                        will_retain => WillRetain =:= 1
                    }}};
                _ ->
                    {error, malformed_connect}
            end;
        _ ->
            {error, malformed_connect}
    end.

%% Decode PUBLISH packet
decode_publish(Flags, Payload) ->
    QoS = (Flags band 2#0110) bsr 1,
    Retain = Flags band 2#0001,
    Dup = (Flags band 2#1000) bsr 3,

    <<TopicLen:16, Topic:TopicLen/binary, Rest/binary>> = Payload,

    %% Packet ID only present for QoS > 0
    {PacketId, Data} = case QoS of
        0 -> {undefined, Rest};
        _ -> <<PId:16, D/binary>> = Rest, {PId, D}
    end,

    {ok, {publish, #{
        topic => Topic,
        qos => QoS,
        retain => Retain =:= 1,
        dup => Dup =:= 1,
        packet_id => PacketId,
        payload => Data
    }}}.

%% Decode SUBSCRIBE packet
decode_subscribe(<<PacketId:16, Rest/binary>>) ->
    Topics = decode_subscribe_topics(Rest, []),
    {ok, {subscribe, PacketId, Topics}}.

decode_subscribe_topics(<<>>, Acc) ->
    lists:reverse(Acc);
decode_subscribe_topics(<<Len:16, Topic:Len/binary, QoS:8, Rest/binary>>, Acc) ->
    decode_subscribe_topics(Rest, [{Topic, QoS} | Acc]).

%% Decode UNSUBSCRIBE packet
decode_unsubscribe(<<PacketId:16, Rest/binary>>) ->
    Topics = decode_unsubscribe_topics(Rest, []),
    {ok, {unsubscribe, PacketId, Topics}}.

decode_unsubscribe_topics(<<>>, Acc) ->
    lists:reverse(Acc);
decode_unsubscribe_topics(<<Len:16, Topic:Len/binary, Rest/binary>>, Acc) ->
    decode_unsubscribe_topics(Rest, [Topic | Acc]).

%%====================================================================
%% API - Encode
%%====================================================================

encode({connack, SessionPresent, ReturnCode}) ->
    encode_connack(SessionPresent, ReturnCode);
encode({suback, PacketId, ReturnCodes}) ->
    encode_suback(PacketId, ReturnCodes);
encode({unsuback, PacketId}) ->
    encode_unsuback(PacketId);
encode({puback, PacketId}) ->
    encode_puback(PacketId);
encode({pubrec, PacketId}) ->
    encode_pubrec(PacketId);
encode({pubrel, PacketId}) ->
    encode_pubrel(PacketId);
encode({pubcomp, PacketId}) ->
    encode_pubcomp(PacketId);
encode(pingresp) ->
    encode_pingresp();
encode({publish, Topic, Payload, QoS, PacketId}) ->
    encode_publish(Topic, Payload, QoS, PacketId).

%% Encode CONNACK
encode_connack(SessionPresent, ReturnCode) ->
    SP = case SessionPresent of true -> 1; false -> 0 end,
    RC = return_code_to_int(ReturnCode),
    <<?CONNACK:4, 0:4, 2, SP:8, RC:8>>.

return_code_to_int(accepted) -> ?CONNACK_ACCEPTED;
return_code_to_int(unacceptable_protocol) -> ?CONNACK_UNACCEPTABLE_PROTOCOL;
return_code_to_int(identifier_rejected) -> ?CONNACK_IDENTIFIER_REJECTED;
return_code_to_int(server_unavailable) -> ?CONNACK_SERVER_UNAVAILABLE;
return_code_to_int(bad_credentials) -> ?CONNACK_BAD_CREDENTIALS;
return_code_to_int(not_authorized) -> ?CONNACK_NOT_AUTHORIZED.

%% Encode SUBACK
encode_suback(PacketId, ReturnCodes) ->
    RCs = list_to_binary([suback_code(RC) || RC <- ReturnCodes]),
    Len = 2 + byte_size(RCs),
    <<?SUBACK:4, 0:4, Len:8, PacketId:16, RCs/binary>>.

suback_code(0) -> 0;      %% QoS 0 granted
suback_code(1) -> 1;      %% QoS 1 granted
suback_code(2) -> 2;      %% QoS 2 granted
suback_code(failure) -> 16#80.

%% Encode UNSUBACK
encode_unsuback(PacketId) ->
    <<?UNSUBACK:4, 0:4, 2, PacketId:16>>.

%% Encode PUBACK
encode_puback(PacketId) ->
    <<?PUBACK:4, 0:4, 2, PacketId:16>>.

%% Encode PUBREC (QoS 2 step 1 response)
encode_pubrec(PacketId) ->
    <<?PUBREC:4, 0:4, 2, PacketId:16>>.

%% Encode PUBREL (QoS 2 step 2 - fixed flags 0010)
encode_pubrel(PacketId) ->
    <<?PUBREL:4, 2:4, 2, PacketId:16>>.

%% Encode PUBCOMP (QoS 2 step 3 response)
encode_pubcomp(PacketId) ->
    <<?PUBCOMP:4, 0:4, 2, PacketId:16>>.

%% Encode PINGRESP
encode_pingresp() ->
    <<?PINGRESP:4, 0:4, 0>>.

%% Encode PUBLISH
encode_publish(Topic, Payload, QoS, PacketId) ->
    TopicBin = ensure_binary(Topic),
    PayloadBin = ensure_binary(Payload),
    TopicLen = byte_size(TopicBin),

    %% Build variable header
    VarHeader = case QoS of
        0 -> <<TopicLen:16, TopicBin/binary>>;
        _ -> <<TopicLen:16, TopicBin/binary, PacketId:16>>
    end,

    %% Full payload
    FullPayload = <<VarHeader/binary, PayloadBin/binary>>,
    RemainingLen = byte_size(FullPayload),

    %% Encode flags
    Retain = 0,
    Dup = 0,
    Flags = (Dup bsl 3) bor (QoS bsl 1) bor Retain,

    %% Build packet
    LenEncoded = encode_remaining_length(RemainingLen),
    <<?PUBLISH:4, Flags:4, LenEncoded/binary, FullPayload/binary>>.

%% Encode remaining length (variable length integer)
encode_remaining_length(N) when N < 128 ->
    <<N:8>>;
encode_remaining_length(N) ->
    encode_remaining_length(N, <<>>).

encode_remaining_length(0, Acc) ->
    Acc;
encode_remaining_length(N, Acc) ->
    Digit = N rem 128,
    N2 = N div 128,
    case N2 > 0 of
        true -> encode_remaining_length(N2, <<Acc/binary, (Digit bor 128):8>>);
        false -> <<Acc/binary, Digit:8>>
    end.

ensure_binary(B) when is_binary(B) -> B;
ensure_binary(L) when is_list(L) -> list_to_binary(L).
