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
-module(fuse_utils).
-author("Michal Stanisz").

-include("fuse_utils.hrl").
-include("global_definitions.hrl").
-include("proto/oneclient/message_id.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/common/handshake_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("clproto/include/messages.hrl").

-export([connect_via_macaroon/1, connect_via_macaroon/2, connect_via_macaroon/3]).
-export([receive_server_message/0, receive_server_message/1]).
-export([generate_delete_file_message/2, generate_open_file_message/2, generate_get_children_message/2, 
    generate_fsync_message/2, generate_create_message/3]).


%% ====================================================================
%% API
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using macaroon, with default socket_opts
%% @equiv connect_via_macaroon(Node, [{active, true}])
%% @end
%%--------------------------------------------------------------------
-spec connect_via_macaroon(Node :: node()) ->
    {ok, {Sock :: ssl:socket(), SessId :: session:id()}} | no_return().
connect_via_macaroon(Node) ->
    connect_via_macaroon(Node, [{active, true}]).

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using macaroon, with custom socket opts
%% @equiv connect_via_macaroon(Node, SocketOpts, crypto:strong_rand_bytes(10))
%% @end
%%--------------------------------------------------------------------
connect_via_macaroon(Node, SocketOpts) ->
    connect_via_macaroon(Node, SocketOpts, crypto:strong_rand_bytes(10)).

%%--------------------------------------------------------------------
%% @doc
%% Connect to given node using a macaroon, with custom socket opts and session id.
%% @end
%%--------------------------------------------------------------------
-spec connect_via_macaroon(Node :: node(), SocketOpts :: list(), session:id()) ->
    {ok, {Sock :: term(), SessId :: session:id()}}.
connect_via_macaroon(Node, SocketOpts, SessId) ->
    % given
    {ok, [Version | _]} = rpc:call(
        Node, application, get_env, [?APP_NAME, compatible_oc_versions]
    ),

    MacaroonAuthMessage = #'ClientMessage'{message_body = {client_handshake_request,
        #'ClientHandshakeRequest'{
            session_id = SessId,
            macaroon = #'Macaroon'{macaroon = ?MACAROON, disch_macaroons = ?DISCH_MACAROONS},
            version = list_to_binary(Version)
        }
    }},
    MacaroonAuthMessageRaw = messages:encode_msg(MacaroonAuthMessage),
    ActiveOpt = case proplists:get_value(active, SocketOpts) of
                    undefined -> [];
                    Other -> [{active, Other}]
                end,
    {ok, Port} = test_utils:get_env(Node, ?APP_NAME, https_server_port),

    % when
    {ok, Sock} = connect_and_upgrade_proto(utils:get_host(Node), Port),
    ok = ssl:send(Sock, MacaroonAuthMessageRaw),

    % then
    RM = receive_server_message(),
    #'ServerMessage'{message_body = {handshake_response, #'HandshakeResponse'{
        status = 'OK'
    }}} = ?assertMatch(#'ServerMessage'{message_body = {handshake_response, _}},
        RM
    ),
    ssl:setopts(Sock, ActiveOpt),
    {ok, {Sock, SessId}}.


connect_and_upgrade_proto(Hostname, Port) ->
    {ok, Sock} = (catch ssl:connect(Hostname, Port, [binary,
        {active, once}, {reuse_sessions, false}
    ], timer:minutes(1))),
    ssl:send(Sock, protocol_utils:protocol_upgrade_request(list_to_binary(Hostname))),
    receive {ssl, Sock, Data} ->
        ?assert(protocol_utils:verify_protocol_upgrade_response(Data)),
        ssl:setopts(Sock, [{active, once}, {packet, 4}]),
        {ok, Sock}
    after timer:minutes(1) ->
        exit(timeout)
    end.

receive_server_message() ->
    receive_server_message([message_stream_reset, subscription]).

receive_server_message(IgnoredMsgList) ->
    receive
        {_, _, Data} ->
            % ignore listed messages
            Msg = messages:decode_msg(Data, 'ServerMessage'),
            MsgType = element(1, Msg#'ServerMessage'.message_body),
            case lists:member(MsgType, IgnoredMsgList) of
                true -> receive_server_message(IgnoredMsgList);
                false -> Msg
            end
    after ?TIMEOUT ->
        {error, timeout}
    end.

generate_create_message(RootGuid, MsgId, File) ->
    Message = #'ClientMessage'{message_id = MsgId, message_body =
    {fuse_request, #'FuseRequest'{fuse_request = {file_request,
        #'FileRequest'{context_guid = RootGuid,
            file_request = {create_file, #'CreateFile'{name = File,
                mode = 8#644, flag = 'READ_WRITE'}}}
    }}}
    },
    messages:encode_msg(Message).

generate_get_children_message(RootGuid, MsgId) ->
    Message = #'ClientMessage'{message_id = MsgId, message_body =
    {fuse_request, #'FuseRequest'{fuse_request = {file_request,
        #'FileRequest'{context_guid = RootGuid,
            file_request = {get_file_children_attrs,
                #'GetFileChildrenAttrs'{offset = 0, size = 100}}}
    }}}
    },
    messages:encode_msg(Message).

generate_fsync_message(RootGuid, MsgId) ->
    Message = #'ClientMessage'{message_id = MsgId, message_body =
    {fuse_request, #'FuseRequest'{fuse_request = {file_request,
        #'FileRequest'{context_guid = RootGuid,
            file_request = {fsync,
                #'FSync'{data_only = false}}}
    }}}
    },
    messages:encode_msg(Message).

generate_open_file_message(FileGuid, MsgId) ->
    Message = #'ClientMessage'{message_id = MsgId, message_body =
    {fuse_request, #'FuseRequest'{fuse_request = {file_request,
        #'FileRequest'{context_guid = FileGuid,
            file_request = {open_file, #'OpenFile'{flag = 'READ_WRITE'}}}
    }}}
    },
    messages:encode_msg(Message).

generate_delete_file_message(FileGuid, MsgId) ->
    Message = #'ClientMessage'{message_id = MsgId, message_body =
    {fuse_request, #'FuseRequest'{fuse_request = {file_request,
        #'FileRequest'{context_guid = FileGuid,
            file_request = {delete_file, #'DeleteFile'{}}}
    }}}
    },
    messages:encode_msg(Message).
