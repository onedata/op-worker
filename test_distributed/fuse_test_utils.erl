%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for tests using fuse client
%%% @end
%%%-------------------------------------------------------------------
-module(fuse_test_utils).
-author("Michal Stanisz").

-include("fuse_test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("global_definitions.hrl").
-include("proto/common/clproto_message_id.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("clproto/include/messages.hrl").

-export([
    reuse_or_create_fuse_session/4, reuse_or_create_fuse_session/5,

    connect_as_provider/3, connect_as_client/4,

    connect_via_token/1, connect_via_token/2,
    connect_via_token/3, connect_via_token/4,

    connect_as_user/4,

    generate_msg_id/0
]).
-export([connect_and_upgrade_proto/2]).
-export([receive_server_message/0, receive_server_message/1, receive_server_message/2]).

%% Fuse request messages
-export([generate_create_file_message/3, generate_create_dir_message/3, generate_delete_file_message/2,
    generate_open_file_message/2, generate_open_file_message/3, generate_release_message/3,
    generate_get_children_attrs_message/2, generate_get_children_message/2, generate_fsync_message/2]).

%% Subscription messages
-export([generate_file_renamed_subscription_message/4, generate_file_removed_subscription_message/4,
    generate_file_attr_changed_subscription_message/5, generate_file_location_changed_subscription_message/5]).
-export([generate_subscription_cancellation_message/3, generate_quota_exceeded_subscription_message/3]).

%% Misc messages
-export([generate_ping_message/0, generate_ping_message/1]).

%% ProxyIO messages
-export([generate_write_message/5, generate_read_message/5]).

-export([
    create_file/3, create_file/4,
    create_directory/3, create_directory/4,
    open/2, open/3, open/4,
    close/3, close/4,
    proxy_read/5, proxy_read/6,
    proxy_write/5, proxy_write/6,
    fsync/4, fsync/5,
    ls/2, ls/3,
    emit_file_read_event/5,
    emit_file_written_event/5,
    get_configuration/1, get_configuration/2,
    get_subscriptions/1, get_subscriptions/2, get_subscriptions/3,
    flush_events/3, flush_events/4,

    get_protocol_version/1, get_protocol_version/2,
    generate_rtransfer_conn_secret/1, generate_rtransfer_conn_secret/2,
    get_rtransfer_nodes_ips/1, get_rtransfer_nodes_ips/2,

    ping/1, ping/2
]).

-define(ID, generate_msg_id()).
-define(MSG_ID, integer_to_binary(?ID)).
-define(IRRELEVANT_FIELD_VALUE, <<"needless">>).

-define(ATTEMPTS, 8).

%% ====================================================================
%% API
%% ====================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates FUSE session or if session exists reuses it
%% and registers connection for it.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_fuse_session(Nonce :: binary(), session:auth(),
    session:credentials() | undefined, pid()) -> {ok, session:id()} | {error, term()}.
reuse_or_create_fuse_session(Nonce, Iden, Auth, Conn) ->
    {ok, SessId} = session_manager:reuse_or_create_fuse_session(Nonce, Iden, Auth),
    session_connections:register(SessId, Conn),
    {ok, SessId}.


%%--------------------------------------------------------------------
%% @doc
%% Calls reuse_or_create_fuse_session/4 on specified Worker.
%% @end
%%--------------------------------------------------------------------
-spec reuse_or_create_fuse_session(node(), Nonce :: binary(), session:auth(),
    session:credentials() | undefined, pid()) -> {ok, session:id()} | {error, term()}.
reuse_or_create_fuse_session(Worker, Nonce, Iden, Auth, Conn) ->
    ?assertMatch({ok, _}, rpc:call(Worker, ?MODULE,
        reuse_or_create_fuse_session, [Nonce, Iden, Auth, Conn]
    )).


generate_msg_id() ->
    ID = case get(msg_id_generator) of
        undefined -> 1;
        Value -> Value + 1
    end,
    put(msg_id_generator, ID),
    ID.

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using a providerId and token.
%% @end
%%--------------------------------------------------------------------
connect_as_provider(Node, ProviderId, Token) ->
    HandshakeReqMsg = #'ClientMessage'{
        message_body = {provider_handshake_request, #'ProviderHandshakeRequest'{
            provider_id = ProviderId,
            token = Token
        }
        }},
    RawMsg = messages:encode_msg(HandshakeReqMsg),

    % when
    {ok, Port} = test_utils:get_env(Node, ?APP_NAME, https_server_port),
    {ok, Sock} = connect_and_upgrade_proto(utils:get_host(Node), Port),
    ok = ssl:send(Sock, RawMsg),

    % then
    % then
    #'ServerMessage'{
        message_body = {handshake_response, #'HandshakeResponse'{
            status = Status
        }}
    } = ?assertMatch(#'ServerMessage'{
        message_body = {handshake_response, _}
    }, fuse_test_utils:receive_server_message()),

    case Status of
        'OK' ->
            {ok, Sock};
        _ ->
            ok = ssl:close(Sock),
            {error, Status}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using a token, nonce and version.
%% @end
%%--------------------------------------------------------------------
connect_as_client(Node, Nonce, Token, Version) ->
    HandshakeMessage = #'ClientMessage'{
        message_body = {client_handshake_request, #'ClientHandshakeRequest'{
            session_id = Nonce,
            macaroon = Token,
            version = Version
        }
        }},
    RawMsg = messages:encode_msg(HandshakeMessage),

    % when
    {ok, Port} = test_utils:get_env(Node, ?APP_NAME, https_server_port),
    {ok, Sock} = connect_and_upgrade_proto(utils:get_host(Node), Port),
    ok = ssl:send(Sock, RawMsg),

    % then
    #'ServerMessage'{
        message_body = {handshake_response, #'HandshakeResponse'{
            status = Status
        }}
    } = ?assertMatch(#'ServerMessage'{
        message_body = {handshake_response, _}
    }, fuse_test_utils:receive_server_message()),

    case Status of
        'OK' ->
            {ok, Sock};
        _ ->
            ok = ssl:close(Sock),
            {error, Status}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using a token, with default socket_opts
%% @equiv connect_via_token(Node, [{active, true}])
%% @end
%%--------------------------------------------------------------------
-spec connect_via_token(Node :: node()) ->
    {ok, {Sock :: ssl:sslsocket(), SessId :: session:id()}} | no_return().
connect_via_token(Node) ->
    connect_via_token(Node, [{active, true}]).

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using a token, with custom socket opts
%% @equiv connect_via_token(Node, SocketOpts, crypto:strong_rand_bytes(10))
%% @end
%%--------------------------------------------------------------------
connect_via_token(Node, SocketOpts) ->
    connect_via_token(Node, SocketOpts, crypto:strong_rand_bytes(10)).

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using a token, with custom socket opts
%% @equiv connect_via_token(Node, SocketOpts, crypto:strong_rand_bytes(10))
%% @end
%%--------------------------------------------------------------------
connect_via_token(Node, SocketOpts, Nonce) ->
    UserId = <<"default_user">>,
    AccessToken = initializer:create_access_token(UserId),
    connect_via_token(Node, SocketOpts, Nonce, AccessToken).

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using a token, with custom socket opts and nonce.
%% @end
%%--------------------------------------------------------------------
-spec connect_via_token(
    Node :: node(),
    SocketOpts :: list(),
    Nonce :: binary(),
    AccessToken :: tokens:serialized()
) ->
    {ok, {Sock :: term(), SessId :: session:id()}}.
connect_via_token(Node, SocketOpts, Nonce, AccessToken) ->
    % given
    OpVersion = rpc:call(Node, oneprovider, get_version, []),
    {ok, [Version | _]} = rpc:call(
        Node, compatibility, get_compatible_versions, [?ONEPROVIDER, OpVersion, ?ONECLIENT]
    ),

    HandshakeMessage = #'ClientMessage'{message_body = {client_handshake_request,
        #'ClientHandshakeRequest'{
            session_id = Nonce,
            macaroon = #'Macaroon'{macaroon = AccessToken},
            version = Version
        }
    }},
    HandshakeMessageRaw = messages:encode_msg(HandshakeMessage),
    ActiveOpt = case proplists:get_value(active, SocketOpts) of
        undefined -> [];
        Other -> [{active, Other}]
    end,
    {ok, Port} = test_utils:get_env(Node, ?APP_NAME, https_server_port),

    % when
    {ok, Sock} = connect_and_upgrade_proto(utils:get_host(Node), Port),
    ok = ssl:send(Sock, HandshakeMessageRaw),

    % then
    RM = receive_server_message(),
    ?assertMatch(#'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{
        status = 'OK'
    }}},
        RM
    ),

    SessId = datastore_key:new_from_digest([<<"fuse">>, Nonce]),
    ssl:setopts(Sock, ActiveOpt),
    {ok, {Sock, SessId}}.


connect_and_upgrade_proto(Hostname, Port) ->
    {ok, Sock} = (catch ssl:connect(Hostname, Port, [binary,
        {active, once}, {reuse_sessions, false}
    ], timer:minutes(1))),
    ssl:send(Sock, connection_utils:protocol_upgrade_request(list_to_binary(Hostname))),
    receive {ssl, Sock, Data} ->
        ?assert(connection_utils:verify_protocol_upgrade_response(Data)),
        ssl:setopts(Sock, [{active, once}, {packet, 4}]),
        {ok, Sock}
    after timer:minutes(1) ->
        exit(timeout)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Connect to given node with specified user authorization.
%% @end
%%--------------------------------------------------------------------
-spec connect_as_user(Config :: term(), node(), User :: binary(),  SocketOpts :: list()) ->
    {ok, {Sock :: term(), SessId :: session:id()}}.
connect_as_user(Config, Node, User, SocketOpts) ->
    SessId = ?config({session_id, {User, ?GET_DOMAIN(Node)}}, Config),
    Nonce = ?config({session_nonce, {User, ?GET_DOMAIN(Node)}}, Config),
    AccessToken = initializer:create_access_token(User),

    ?assertMatch(
        {ok, {_, SessId}},
        fuse_test_utils:connect_via_token(Node, SocketOpts, Nonce, AccessToken)
    ).


receive_server_message() ->
    receive_server_message([message_stream_reset, subscription, message_request,
        message_acknowledgement, processing_status, events]). % ignore events about parent changes

receive_server_message(IgnoredMsgList) ->
    receive_server_message(IgnoredMsgList, ?TIMEOUT).

receive_server_message(IgnoredMsgList, Timeout) ->
    Now = time_utils:system_time_millis(),
    receive
        {_, _, Data} ->
            % ignore listed messages
            Msg = messages:decode_msg(Data, 'ServerMessage'),
            MsgType = element(1, Msg#'ServerMessage'.message_body),
            case lists:member(MsgType, IgnoredMsgList) of
                true ->
                    NewTimeout = max(0, Timeout - (time_utils:system_time_millis() - Now)),
                    receive_server_message(IgnoredMsgList, NewTimeout);
                false ->
                    Msg
            end
    after Timeout ->
        {error, timeout}
    end.


%% Fuse request messages
generate_create_file_message(RootGuid, MsgId, File) ->
    FuseRequest = {file_request, #'FileRequest'{
        context_guid = RootGuid,
        file_request = {create_file, #'CreateFile'{
            name = File,
            mode = 8#644,
            flag = 'READ_WRITE'}
        }}
    },
    generate_fuse_request_message(MsgId, FuseRequest).

generate_create_dir_message(RootGuid, MsgId, Name) ->
    FuseRequest = {file_request, #'FileRequest'{
        context_guid = RootGuid,
        file_request = {create_dir, #'CreateDir'{name = Name, mode = 8#755}}
    }},
    generate_fuse_request_message(MsgId, FuseRequest).

generate_get_children_attrs_message(RootGuid, MsgId) ->
    FuseRequest = {file_request, #'FileRequest'{
        context_guid = RootGuid,
        file_request = {get_file_children_attrs,
            #'GetFileChildrenAttrs'{offset = 0, size = 100}}
    }},
    generate_fuse_request_message(MsgId, FuseRequest).

generate_get_children_message(RootGuid, MsgId) ->
    FuseRequest = {file_request, #'FileRequest'{
        context_guid = RootGuid,
        file_request = {get_file_children,
            #'GetFileChildren'{offset = 0, size = 100}}
    }},
    generate_fuse_request_message(MsgId, FuseRequest).

generate_fsync_message(RootGuid, MsgId) ->
    generate_fsync_message(RootGuid, undefined, false, MsgId).

generate_fsync_message(RootGuid, HandleId, DataOnly, MsgId) ->
    FuseRequest = {file_request, #'FileRequest'{
        context_guid = RootGuid,
        file_request = {fsync, #'FSync'{
            data_only = DataOnly,
            handle_id = HandleId
        }}
    }},
    generate_fuse_request_message(MsgId, FuseRequest).

generate_open_file_message(FileGuid, MsgId) ->
    generate_open_file_message(FileGuid, 'READ_WRITE', MsgId).

generate_open_file_message(FileGuid, Flag, MsgId) ->
    FuseRequest = {file_request, #'FileRequest'{
        context_guid = FileGuid,
        file_request = {open_file, #'OpenFile'{flag = Flag}}
    }},
    generate_fuse_request_message(MsgId, FuseRequest).

generate_release_message(HandleId, FileGuid, MsgId) ->
    FuseRequest = {file_request, #'FileRequest'{
        context_guid = FileGuid,
        file_request = {release, #'Release'{handle_id = HandleId}}
    }},
    generate_fuse_request_message(MsgId, FuseRequest).

generate_delete_file_message(FileGuid, MsgId) ->
    FuseRequest = {file_request, #'FileRequest'{
        context_guid = FileGuid,
        file_request = {delete_file, #'DeleteFile'{}}
    }},
    generate_fuse_request_message(MsgId, FuseRequest).

generate_fuse_request_message(MsgId, FuseRequest) ->
    Message = #'ClientMessage'{message_id = MsgId,
        message_body = {fuse_request, #'FuseRequest'{fuse_request = FuseRequest}}
    },
    messages:encode_msg(Message).


%% Subscription messages
generate_file_removed_subscription_message(StreamId, SequenceNumber, SubId, FileId) ->
    Type = {file_removed, #'FileRemovedSubscription'{file_uuid = FileId}},
    generate_subscription_message(StreamId, SequenceNumber, SubId, Type).

generate_file_attr_changed_subscription_message(StreamId, SequenceNumber, SubId, FileId, TimeThreshold) ->
    Type = {file_attr_changed, #'FileAttrChangedSubscription'{
        file_uuid = FileId, time_threshold = TimeThreshold}
    },
    generate_subscription_message(StreamId, SequenceNumber, SubId, Type).

generate_file_renamed_subscription_message(StreamId, SequenceNumber, SubId, FileId) ->
    Type = {file_renamed, #'FileRenamedSubscription'{file_uuid = FileId}},
    generate_subscription_message(StreamId, SequenceNumber, SubId, Type).

generate_file_location_changed_subscription_message(StreamId, SequenceNumber, SubId, FileId, TimeThreshold) ->
    Type = {file_location_changed, #'FileLocationChangedSubscription'{
        file_uuid = FileId, time_threshold = TimeThreshold}
    },
    generate_subscription_message(StreamId, SequenceNumber, SubId, Type).

generate_quota_exceeded_subscription_message(StreamId, SequenceNumber, SubId) ->
    Type = {quota_exceeded, #'QuotaExceededSubscription'{}},
    generate_subscription_message(StreamId, SequenceNumber, SubId, Type).

generate_subscription_message(StreamId, SequenceNumber, SubId, Type) ->
    Message = #'ClientMessage'{
        message_stream = #'MessageStream'{stream_id = StreamId, sequence_number = SequenceNumber},
        message_body = {subscription, #'Subscription'{id = SubId, type = Type}}
    },
    messages:encode_msg(Message).

generate_subscription_cancellation_message(StreamId, SequenceNumber, SubId) ->
    Message = #'ClientMessage'{
        message_stream = #'MessageStream'{stream_id = StreamId, sequence_number = SequenceNumber},
        message_body = {subscription_cancellation, #'SubscriptionCancellation'{id = SubId}}
    },
    messages:encode_msg(Message).


%% ProxyIO messages
generate_write_message(MsgId, HandleId, FileGuid, Offset, Data) ->
    Parameters = [
        #'Parameter'{key = ?PROXYIO_PARAMETER_HANDLE_ID, value = HandleId},
        #'Parameter'{key = ?PROXYIO_PARAMETER_FILE_GUID, value = FileGuid}
    ],
    ProxyIORequest = {remote_write, #'RemoteWrite'{
        byte_sequence = [#'ByteSequence'{offset = Offset, data = Data}]}
    },
    generate_proxyio_message(MsgId, Parameters, ProxyIORequest).

generate_read_message(MsgId, HandleId, FileGuid, Offset, Size) ->
    Parameters = [
        #'Parameter'{key = ?PROXYIO_PARAMETER_HANDLE_ID, value = HandleId},
        #'Parameter'{key = ?PROXYIO_PARAMETER_FILE_GUID, value = FileGuid}
    ],
    ProxyIORequest = {remote_read, #'RemoteRead'{offset = Offset, size = Size}},
    generate_proxyio_message(MsgId, Parameters, ProxyIORequest).

generate_proxyio_message(MsgId, Parameters, ProxyIORequest) ->
    Message = #'ClientMessage'{message_id = MsgId,
        message_body = {proxyio_request, #'ProxyIORequest'{
            storage_id = ?IRRELEVANT_FIELD_VALUE,
            file_id = ?IRRELEVANT_FIELD_VALUE,
            parameters = Parameters,
            proxyio_request = ProxyIORequest
        }}
    },
    messages:encode_msg(Message).


create_file(Sock, RootGuid, Filename) ->
    create_file(Sock, RootGuid, Filename, ?MSG_ID).

create_file(Sock, RootGuid, Filename, MsgId) ->
    ok = ssl:send(Sock, fuse_test_utils:generate_create_file_message(RootGuid, MsgId, Filename)),
    #'ServerMessage'{message_body = {fuse_response, #'FuseResponse'{
        fuse_response = {file_created, #'FileCreated'{
            handle_id = HandleId,
            file_attr = #'FileAttr'{uuid = FileGuid}}
        }}
    }} = ?assertMatch(#'ServerMessage'{
        message_body = {fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}},
        message_id = MsgId
    }, fuse_test_utils:receive_server_message()),
    {FileGuid, HandleId}.

create_directory(Sock, RootGuid, Dirname) ->
    create_directory(Sock, RootGuid, Dirname, ?MSG_ID).

create_directory(Sock, RootGuid, Dirname, MsgId) ->
    ok = ssl:send(Sock, fuse_test_utils:generate_create_dir_message(RootGuid, MsgId, Dirname)),
    #'ServerMessage'{message_body = {fuse_response, #'FuseResponse'{
        fuse_response = {dir, #'Dir'{uuid = DirId}}
    }}} = ?assertMatch(#'ServerMessage'{
        message_body = {fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}},
        message_id = MsgId
    }, fuse_test_utils:receive_server_message()),
    DirId.

open(Conn, FileGuid) ->
    open(Conn, FileGuid, 'READ_WRITE').

open(Conn, FileGuid, Mode) ->
    open(Conn, FileGuid, Mode, ?MSG_ID).

open(Conn, FileGuid, Mode, MsgId) ->
    RawMsg = generate_open_file_message(FileGuid, Mode, MsgId),
    ok = ssl:send(Conn, RawMsg),

    #'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{
            fuse_response = {file_opened, #'FileOpened'{handle_id = HandleId}}
        }
    }} = ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = MsgId}, receive_server_message()),

    HandleId.


close(Conn, FileGuid, HandleId) ->
    close(Conn, FileGuid, HandleId, ?MSG_ID).

close(Conn, FileGuid, HandleId, MsgId) ->
    RawMsg = generate_release_message(HandleId, FileGuid, MsgId),
    ok = ssl:send(Conn, RawMsg),

    ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = MsgId}, receive_server_message()).


proxy_read(Conn, FileGuid, HandleId, Offset, Size) ->
    proxy_read(Conn, FileGuid, HandleId, Offset, Size, ?MSG_ID).

proxy_read(Conn, FileGuid, HandleId, Offset, Size, MsgId) ->
    RawMsg = generate_read_message(MsgId, HandleId, FileGuid, Offset, Size),
    ok = ssl:send(Conn, RawMsg),

    #'ServerMessage'{message_body = {
        proxyio_response, #'ProxyIOResponse'{
            proxyio_response = {remote_data, #'RemoteData'{data = Data}}
        }
    }} = ?assertMatch(#'ServerMessage'{message_body = {
        proxyio_response, #'ProxyIOResponse'{status = #'Status'{code = ok}}
    }, message_id = MsgId}, receive_server_message()),

    Data.


proxy_write(Conn, FileGuid, HandleId, Offset, Data) ->
    proxy_write(Conn, FileGuid, HandleId, Offset, Data, ?MSG_ID).

proxy_write(Conn, FileGuid, HandleId, Offset, Data, MsgId) ->
    RawMsg = generate_write_message(MsgId, HandleId, FileGuid, Offset, Data),
    ok = ssl:send(Conn, RawMsg),

    #'ServerMessage'{message_body = {
        proxyio_response, #'ProxyIOResponse'{
            proxyio_response = {remote_write_result, #'RemoteWriteResult'{wrote = NBytes}}
        }
    }} = ?assertMatch(#'ServerMessage'{message_body = {
        proxyio_response, #'ProxyIOResponse'{status = #'Status'{code = ok}}
    }, message_id = MsgId}, receive_server_message()),

    NBytes.


fsync(Conn, FileGuid, HandleId, DataOnly) ->
    fsync(Conn, FileGuid, HandleId, DataOnly, ?MSG_ID).

fsync(Conn, FileGuid, HandleId, DataOnly, MsgId) ->
    RawMsg = generate_fsync_message(FileGuid, HandleId, DataOnly, MsgId),
    ok = ssl:send(Conn, RawMsg),

    ?assertMatch(#'ServerMessage'{message_body = {
        fuse_response, #'FuseResponse'{status = #'Status'{code = ok}}
    }, message_id = MsgId}, receive_server_message()).

ls(Conn, DirId) ->
    ls(Conn, DirId, ?MSG_ID).

ls(Conn, DirId, MsgId) ->
    ok = ssl:send(Conn, fuse_test_utils:generate_get_children_message(DirId, MsgId)),
    #'ServerMessage'{message_body = {fuse_response, #'FuseResponse'{
        fuse_response = {file_children, #'FileChildren'{
            child_links = ChildLinks
        }}
    }}} = ?assertMatch(#'ServerMessage'{
        message_body = {fuse_response, #'FuseResponse'{
            status = #'Status'{code = ok}}
        },
        message_id = MsgId
    }, fuse_test_utils:receive_server_message()),
    ChildLinks.


emit_file_read_event(Conn, StreamId, Seq, FileGuid, Blocks) ->
    {BlocksRead, BlocksSize} = lists:foldr(
        fun(#file_block{offset = O, size = S}, {AccBlocks, AccSize}) ->
            {[#'FileBlock'{offset = O, size = S} | AccBlocks], AccSize + S}
        end,
        {[], 0}, Blocks),

    Msg = #'ClientMessage'{
        message_stream = #'MessageStream'{
            stream_id = StreamId,
            sequence_number = Seq
        },
        message_body = {events, #'Events'{events = [#'Event'{
            type = {file_read, #'FileReadEvent'{
                counter = length(BlocksRead),
                file_uuid = FileGuid,
                size = BlocksSize,
                blocks = BlocksRead
            }}
        }]}}
    },
    RawMsg = messages:encode_msg(Msg),
    ok = ssl:send(Conn, RawMsg).


emit_file_written_event(Conn, StreamId, Seq, FileGuid, Blocks) ->
    {BlocksRead, BlocksSize} = lists:foldr(
        fun(#file_block{offset = O, size = S}, {AccBlocks, AccSize}) ->
            {[#'FileBlock'{offset = O, size = S} | AccBlocks], AccSize + S}
        end,
        {[], 0}, Blocks),

    Msg = #'ClientMessage'{
        message_stream = #'MessageStream'{
            stream_id = StreamId,
            sequence_number = Seq
        },
        message_body = {events, #'Events'{events = [#'Event'{
            type = {file_written, #'FileWrittenEvent'{
                counter = length(BlocksRead),
                file_uuid = FileGuid,
                size = BlocksSize,
                blocks = BlocksRead
            }}
        }]}}
    },
    RawMsg = messages:encode_msg(Msg),
    ok = ssl:send(Conn, RawMsg).


get_configuration(Conn) ->
    get_configuration(Conn, ?MSG_ID).

get_configuration(Conn, MsgId) ->
    Msg = #'ClientMessage'{
        message_id = MsgId,
        message_body = {get_configuration, #'GetConfiguration'{}}
    },

    RawMsg = messages:encode_msg(Msg),
    ok = ssl:send(Conn, RawMsg),

    #'ServerMessage'{message_body = {
        configuration, #'Configuration'{} = Configuration}
    } = ?assertMatch(#'ServerMessage'{message_id = MsgId}, receive_server_message()),

    Configuration.


get_subscriptions(Conn) ->
    get_subscriptions(Conn, all).

get_subscriptions(Conn, ChosenSubscriptions) ->
    get_subscriptions(Conn, ChosenSubscriptions, ?MSG_ID).

get_subscriptions(Conn, ChosenSubscriptions, MsgId) ->
    Configuration = get_configuration(Conn, MsgId),
    Subscriptions = Configuration#'Configuration'.subscriptions,

    case ChosenSubscriptions of
        all ->
            Subscriptions;
        _ ->
            lists:filter(fun(#'Subscription'{type = {Type, _}}) ->
                lists:member(Type, ChosenSubscriptions)
            end, Subscriptions)
    end.


flush_events(Conn, ProviderId, SubscriptionId) ->
    flush_events(Conn, ProviderId, SubscriptionId, ok).

flush_events(Conn, ProviderId, SubscriptionId, Code) ->
    flush_events(Conn, ProviderId, SubscriptionId, ?MSG_ID, Code).

flush_events(Conn, ProviderId, SubscriptionId, MsgId, Code) ->
    Msg = #'ClientMessage'{
        message_id = MsgId,
        message_body = {flush_events, #'FlushEvents'{
            provider_id = ProviderId,
            context = term_to_binary(?IRRELEVANT_FIELD_VALUE),
            subscription_id = SubscriptionId
        }}
    },
    RawMsg = messages:encode_msg(Msg),
    ok = ssl:send(Conn, RawMsg),

    ?assertMatch(#'ServerMessage'{message_body = {status, #'Status'{
        code = Code
    }}, message_id = MsgId}, receive_server_message([], 10), ?ATTEMPTS).


get_protocol_version(Conn) ->
    get_protocol_version(Conn, ?MSG_ID).

get_protocol_version(Conn, MsgId) ->
    Msg = #'ClientMessage'{
        message_id = MsgId,
        message_body = {get_protocol_version, #'GetProtocolVersion'{}}
    },
    RawMsg = messages:encode_msg(Msg),
    ok = ssl:send(Conn, RawMsg),

    #'ServerMessage'{message_body = {_, #'ProtocolVersion'{
        major = Major, minor = Minor
    }}} = ?assertMatch(#'ServerMessage'{
        message_id = MsgId,
        message_body = {protocol_version, #'ProtocolVersion'{}}
    }, fuse_test_utils:receive_server_message()),

    {Major, Minor}.


generate_rtransfer_conn_secret(Conn) ->
    generate_rtransfer_conn_secret(Conn, ?MSG_ID).

generate_rtransfer_conn_secret(Conn, MsgId) ->
    ClientMsg = #'ClientMessage'{
        message_id = MsgId,
        message_body = {generate_rtransfer_conn_secret, #'GenerateRTransferConnSecret'{
            secret = <<>>
        }}
    },
    RawMsg = messages:encode_msg(ClientMsg),
    ssl:send(Conn, RawMsg),

    #'ServerMessage'{
        message_body = {rtransfer_conn_secret, #'RTransferConnSecret'{
            secret = Secret
        }}
    } = ?assertMatch(#'ServerMessage'{
        message_id = MsgId,
        message_body = {rtransfer_conn_secret, #'RTransferConnSecret'{}}
    }, fuse_test_utils:receive_server_message()),

    Secret.


get_rtransfer_nodes_ips(Conn) ->
    get_rtransfer_nodes_ips(Conn, ?MSG_ID).

get_rtransfer_nodes_ips(Conn, MsgId) ->
    ClientMsg = #'ClientMessage'{
        message_id = MsgId,
        message_body = {get_rtransfer_nodes_ips, #'GetRTransferNodesIPs'{}}
    },
    RawMsg = messages:encode_msg(ClientMsg),
    ssl:send(Conn, RawMsg),

    #'ServerMessage'{
        message_body = {rtransfer_nodes_ips, #'RTransferNodesIPs'{
            nodes = RespNodes
        }}
    } = ?assertMatch(#'ServerMessage'{
        message_id = MsgId,
        message_body = {rtransfer_nodes_ips, #'RTransferNodesIPs'{}}
    }, fuse_test_utils:receive_server_message()),

    RespNodes.


generate_ping_message() ->
    generate_ping_message(?MSG_ID).

generate_ping_message(MsgId) ->
    Msg = #'ClientMessage'{
        message_id = MsgId,
        message_body = {ping, #'Ping'{}}
    },
    messages:encode_msg(Msg).


ping(Conn) ->
    ping(Conn, ?MSG_ID).

ping(Conn, MsgId) ->
    Ping = generate_ping_message(MsgId),

    ssl:send(Conn, Ping),

    ?assertMatch(#'ServerMessage'{
        message_id = MsgId,
        message_body = {pong, #'Pong'{}}
    }, fuse_test_utils:receive_server_message()),

    ok.
