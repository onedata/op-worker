%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @author Piotr Ociepka
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for translator module.
%%% @end
%%%--------------------------------------------------------------------

-module(translator_test).
-author("Jakub Kudzia").
-author("Piotr Ociepka").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
%% records generated from .proto
-include_lib("clproto/include/messages.hrl").
%% Erlang records
-include_lib("proto/oneclient/common_messages.hrl").
-include_lib("proto/oneclient/diagnostic_messages.hrl").
-include_lib("proto/oneclient/event_messages.hrl").
-include_lib("proto/common/handshake_messages.hrl").
-include_lib("proto/oneclient/stream_messages.hrl").
-include_lib("proto/oneclient/proxyio_messages.hrl").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%%%===================================================================
%%% Test functions
%%%===================================================================

%%--------------------------------------------------------------------
%% tests for translate_from_protobuf() function
%%--------------------------------------------------------------------

translate_status_from_protobuf_test() ->
    {Internal, Protobuf} = get_status(?OK, <<1, 2, 3>>),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)).

translate_file_block_from_protobuf_test() ->
    {Internal, Protobuf} = get_file_block(0, 0),
    {Internal2, Protobuf2} = get_file_block(32, 2048),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)),
    ?assertEqual(Internal2, clproto_translator:from_protobuf(Protobuf2)).

translate_events_from_protobuf_test() ->
    {Internal, Protobuf} = get_events(5),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)).

translate_event_from_protobuf_test() ->
    {Internal, Protobuf} = get_event(),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)).

translate_read_event_from_protobuf_test() ->
    {Internal, Protobuf} = get_read_event(2, 512, 100, 1024),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)).

translate_write_event_from_protobuf_test() ->
    {Internal, Protobuf} = get_write_event(2, 512, 2048, 100, 1024),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)).

translate_client_handshake_request_from_protobuf_test() ->
    Token = <<"DUMMY-TOKEN">>,
    {Internal, Protobuf} = get_client_handshake_request(Token, 1, [<<"18.01.01">>, <<"18.01.02">>]),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)).

translate_provider_handshake_request_from_protobuf_test() ->
    {Internal, Protobuf} = get_provider_handshake_request(<<"abcd">>, <<"token">>),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)).

translate_message_stream_from_protobuf_test() ->
    {Internal, Protobuf} = get_message_stream(1, 1),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)).

translate_end_of_message_stream_from_protobuf_test() ->
    {Internal, Protobuf} = get_end_of_message_stream(),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)).

translate_token_from_protobuf_test() ->
    Token = <<"DUMMY-TOKEN">>,
    {Internal, Protobuf} = get_token_auth(Token),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)).

translate_ping_from_protobuf_test() ->
    {Internal, Protobuf} = get_ping(1),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)).

translate_proxyio_request_remote_read_from_protobuf_test() ->
    ?assertEqual(#proxyio_request{
        storage_id = <<"StorageID">>,
        file_id = <<"FileID">>,
        proxyio_request = #remote_read{offset = 2, size = 40}},
        clproto_translator:from_protobuf(#'ProxyIORequest'{
            storage_id = <<"StorageID">>,
            file_id = <<"FileID">>,
            proxyio_request = {remote_read, #'RemoteRead'{offset = 2, size = 40}}
        })).

translate_proxyio_request_remote_write_from_protobuf_test() ->
    ?assertEqual(#proxyio_request{
        storage_id = <<"StorageID">>,
        file_id = <<"FileID">>,
        proxyio_request = #remote_write{byte_sequence =
            [#byte_sequence{offset = 2, data = <<"data">>}]}},

        clproto_translator:from_protobuf(#'ProxyIORequest'{
            storage_id = <<"StorageID">>,
            file_id = <<"FileID">>,
            proxyio_request = {remote_write, #'RemoteWrite'{byte_sequence =
                [#'ByteSequence'{offset = 2, data = <<"data">>}]}}
        })).

translate_get_protocol_version_from_protobuf_test() ->
    {Internal, Protobuf} = get_get_protocol_version(),
    ?assertEqual(Internal, clproto_translator:from_protobuf(Protobuf)).

%%--------------------------------------------------------------------
%% tests for translate_to_protobuf() function
%%--------------------------------------------------------------------

translate_status_to_protobuf_test() ->
    ?assertEqual({status, #'Status'{code = ?OK, description = <<"It's fine">>}},
        clproto_translator:to_protobuf(#status{code = ?OK, description = <<"It's fine">>})
    ),
    ?assertEqual({status, #'Status'{code = ?EIO}},
        clproto_translator:to_protobuf(#status{code = ?EIO})
    ).

translate_events_to_protobuf_test() ->
    ?assertEqual({events, #'Events'{events = [#'Event'{}]}},
        clproto_translator:to_protobuf(#events{events = [#event{}]})
    ).

translate_event_to_protobuf_test() ->
    ?assertEqual(#'Event'{}, clproto_translator:to_protobuf(#event{})).

translate_subscription_cancellation_to_protobuf_test() ->
    ?assertEqual({subscription_cancellation, #'SubscriptionCancellation'{id = 42}},
        clproto_translator:to_protobuf(#subscription_cancellation{id = <<"42">>})
    ).

translate_subscription_to_protobuf_test() ->
    ?assertEqual({subscription, #'Subscription'{id = 123}},
        clproto_translator:to_protobuf(#subscription{id = <<"123">>})
    ).

translate_read_subscription_to_protobuf_test() ->
    ?assertEqual({file_read, #'FileReadSubscription'{
        counter_threshold = 12,
        time_threshold = 500
    }}, clproto_translator:to_protobuf(#file_read_subscription{
        counter_threshold = 12,
        time_threshold = 500
    })).

translate_write_subscription_to_protobuf_test() ->
    ?assertEqual({file_written, #'FileWrittenSubscription'{
        counter_threshold = 12,
        time_threshold = 500
    }}, clproto_translator:to_protobuf(#file_written_subscription{
        counter_threshold = 12,
        time_threshold = 500
    })).

translate_handshake_response_to_protobuf_test() ->
    ?assertEqual({handshake_response, #'HandshakeResponse'{
        status = 'OK'
    }}, clproto_translator:to_protobuf(#handshake_response{
        status = 'OK'
    })),
    ?assertEqual({handshake_response, #'HandshakeResponse'{
        status = 'INVALID_MACAROON'
    }}, clproto_translator:to_protobuf(#handshake_response{
        status = 'INVALID_MACAROON'
    })).

translate_message_stream_to_protobuf_test() ->
    ?assertEqual(#'MessageStream'{stream_id = 27, sequence_number = 43412},
        clproto_translator:to_protobuf(#message_stream{
            stream_id = 27, sequence_number = 43412
        })
    ).

translate_message_stream_reset_to_protobuf_test() ->
    ?assertEqual({message_stream_reset, #'MessageStreamReset'{}},
        clproto_translator:to_protobuf(#message_stream_reset{})
    ).

translate_message_request_to_protobuf_test() ->
    ?assertEqual({message_request, #'MessageRequest'{
        stream_id = 94,
        lower_sequence_number = 821,
        upper_sequence_number = 196}
    }, clproto_translator:to_protobuf(#message_request{
        stream_id = 94,
        lower_sequence_number = 821,
        upper_sequence_number = 196
    })).

translate_message_acknowledgement_to_protobuf_test() ->
    ?assertEqual({message_acknowledgement, #'MessageAcknowledgement'{
        stream_id = 44,
        sequence_number = 202}
    }, clproto_translator:to_protobuf(#message_acknowledgement{
        stream_id = 44,
        sequence_number = 202
    })).

translate_pong_to_protobuf_test() ->
    ?assertEqual({pong, #'Pong'{}}, clproto_translator:to_protobuf(#pong{})),
    ?assertEqual({pong, #'Pong'{data = 123}},
        clproto_translator:to_protobuf(#pong{data = 123})
    ),
    ?assertEqual({pong, #'Pong'{data = "OneData"}},
        clproto_translator:to_protobuf(#pong{data = "OneData"})
    ),
    ?assertEqual({pong, #'Pong'{data = some_atom}},
        clproto_translator:to_protobuf(#pong{data = some_atom})
    ).

translate_protocol_version_to_protobuf_test() ->
    ?assertEqual({protocol_version, #'ProtocolVersion'{
        major = 3.0,
        minor = 1.1
    }}, clproto_translator:to_protobuf(#protocol_version{
        major = 3.0,
        minor = 1.1
    })),
    ?assertEqual({protocol_version, #'ProtocolVersion'{
        major = 4,
        minor = 2
    }}, clproto_translator:to_protobuf(#protocol_version{
        major = 4,
        minor = 2
    })),
    ?assertEqual({protocol_version, #'ProtocolVersion'{
        major = 3.4,
        minor = none
    }}, clproto_translator:to_protobuf(#protocol_version{
        major = 3.4,
        minor = none
    })).

translate_proxyio_response_remote_data_to_protobuf_test() ->
    ?assertEqual(
        {proxyio_response, #'ProxyIOResponse'{
            status = #'Status'{code = ?OK, description = <<"It's fine">>},
            proxyio_response = {remote_data, #'RemoteData'{data = <<"data">>}}
        }},
        clproto_translator:to_protobuf(#proxyio_response{
            status = #status{code = ?OK, description = <<"It's fine">>},
            proxyio_response = #remote_data{data = <<"data">>}
        })).

translate_proxyio_response_remote_write_result_to_protobuf_test() ->
    ?assertEqual(
        {proxyio_response, #'ProxyIOResponse'{
            status = #'Status'{code = ?OK, description = <<"It's fine">>},
            proxyio_response = {remote_write_result, #'RemoteWriteResult'{wrote = 42}}
        }},
        clproto_translator:to_protobuf(#proxyio_response{
            status = #status{code = ?OK, description = <<"It's fine">>},
            proxyio_response = #remote_write_result{wrote = 42}
        })).

%% tests 'undefined' atom translation
translate_undefined_to_protobuf_test() ->
    ?assertEqual(undefined, clproto_translator:to_protobuf(undefined)).


%%%===================================================================
%%% Internal functions
%%%===================================================================

get_status(Code, Description) ->
    {#status{code = Code, description = Description},
        #'Status'{code = Code, description = Description}}.

get_file_block(0, 0) ->
    {#file_block{}, #'FileBlock'{}};
get_file_block(Off, S) ->
    {#file_block{offset = Off, size = S}, #'FileBlock'{offset = Off, size = S}}.

get_file_block_list(Num, MaxS) ->
    lists:unzip([get_file_block(rand:uniform(MaxS), rand:uniform(MaxS)) || _ <- lists:seq(1, Num)]).

get_events(N) ->
    {InternalEvt, ProtobufEvt} = get_event(),
    {
        #events{events = lists:duplicate(N, InternalEvt)},
        #'Events'{events = lists:duplicate(N, ProtobufEvt)}
    }.

get_event() ->
    {#event{}, #'Event'{type = {type, undefined}}}.

get_read_event(FileGuid, Size, Num, MaxS) ->
    {InternalBlocks, ProtobufBlocks} = get_file_block_list(Num, MaxS),
    {
        #file_read_event{counter = 1, file_guid = FileGuid, size = Size,
            blocks = InternalBlocks},
        #'FileReadEvent'{counter = 1, file_uuid = FileGuid, size = Size,
            blocks = ProtobufBlocks}
    }.

get_write_event(FileGuid, Size, FileSize, Num, MaxS) ->
    {InternalBlocks, ProtobufBlocks} = get_file_block_list(Num, MaxS),
    {
        #file_written_event{counter = 1, file_guid = FileGuid, size = Size,
            file_size = FileSize, blocks = InternalBlocks},
        #'FileWrittenEvent'{counter = 1, file_uuid = FileGuid, size = Size,
            file_size = FileSize, blocks = ProtobufBlocks}
    }.

get_token_auth(Val) ->
    {
        #client_tokens{access_token = Val},
        #'Macaroon'{macaroon = Val}
    }.

get_client_handshake_request(Token, Nonce, CompOpVersions) ->
    {Internal, Protobuf} = get_token_auth(Token),
    {
        #client_handshake_request{client_tokens = Internal, nonce = Nonce, compatible_oneprovider_versions = CompOpVersions, session_mode = normal},
        #'ClientHandshakeRequest'{macaroon = Protobuf, session_id = Nonce, compatible_oneprovider_versions = CompOpVersions, session_mode = 'NORMAL'}
    }.

get_provider_handshake_request(ProviderId, Token) ->
    {
        #provider_handshake_request{provider_id = ProviderId, token = Token},
        #'ProviderHandshakeRequest'{provider_id = ProviderId, token = Token}
    }.

get_message_stream(StmId, SeqNum) ->
    {
        #message_stream{stream_id = StmId, sequence_number = SeqNum},
        #'MessageStream'{stream_id = StmId, sequence_number = SeqNum}
    }.

get_end_of_message_stream() ->
    {#end_of_message_stream{}, #'EndOfMessageStream'{}}.

get_ping(Data) ->
    {#ping{data = Data}, #'Ping'{data = Data}}.

get_get_protocol_version() ->
    {#get_protocol_version{}, #'GetProtocolVersion'{}}.

-endif.
