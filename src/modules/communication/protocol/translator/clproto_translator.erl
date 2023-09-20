%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015-2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module handling translations between protobuf and internal protocol.
%%% @end
%%%-------------------------------------------------------------------
-module(clproto_translator).
-author("Tomasz Lichon").
-author("Rafal Slota").
-author("Michal Stanisz").

-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("proto/oneprovider/dbsync_messages.hrl").
-include("proto/oneprovider/dbsync_messages2.hrl").
-include("proto/oneprovider/provider_rpc_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("proto/oneprovider/remote_driver_messages.hrl").
-include("proto/oneprovider/rtransfer_messages.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([
    from_protobuf/1, session_mode_from_protobuf/1,
    to_protobuf/1, handshake_error_to_protobuf/1, session_mode_to_protobuf/1
]).

%%%===================================================================
%%% FROM PROTOBUF
%%%===================================================================

-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.

%% COMMON
from_protobuf(#'Status'{} = Msg) -> clproto_common_translator:from_protobuf(Msg);
from_protobuf(#'FileBlock'{} = Msg) -> clproto_common_translator:from_protobuf(Msg);
from_protobuf(#'FileRenamedEntry'{} = Msg) -> clproto_common_translator:from_protobuf(Msg);
from_protobuf(#'IpAndPort'{} = Msg) -> clproto_common_translator:from_protobuf(Msg);
from_protobuf(#'Dir'{} = Msg) -> clproto_common_translator:from_protobuf(Msg);


%% EVENT
from_protobuf(#'Event'{} = Msg) -> clproto_event_translator:from_protobuf(Msg);
from_protobuf(#'Events'{} = Msg) -> clproto_event_translator:from_protobuf(Msg);
from_protobuf(#'FlushEvents'{} = Msg) -> clproto_event_translator:from_protobuf(Msg);
from_protobuf(#'FileReadEvent'{} = Msg) -> clproto_event_translator:from_protobuf(Msg);
from_protobuf(#'FileWrittenEvent'{} = Msg) -> clproto_event_translator:from_protobuf(Msg);
from_protobuf(#'FileAttrChangedEvent'{} = Msg) -> clproto_event_translator:from_protobuf(Msg);
from_protobuf(#'FileLocationChangedEvent'{} = Msg) -> clproto_event_translator:from_protobuf(Msg);
from_protobuf(#'FilePermChangedEvent'{} = Msg) -> clproto_event_translator:from_protobuf(Msg);
from_protobuf(#'FileRemovedEvent'{} = Msg) -> clproto_event_translator:from_protobuf(Msg);
from_protobuf(#'FileRenamedEvent'{} = Msg) -> clproto_event_translator:from_protobuf(Msg);
from_protobuf(#'QuotaExceededEvent'{} = Msg) -> clproto_event_translator:from_protobuf(Msg);
from_protobuf(#'HelperParamsChangedEvent'{} = Msg) -> clproto_event_translator:from_protobuf(Msg);


%% SUBSCRIPTION
from_protobuf(#'Subscription'{} = Msg) -> clproto_subscription_translator:from_protobuf(Msg);
from_protobuf(#'FileReadSubscription'{} = Msg) -> clproto_subscription_translator:from_protobuf(Msg);
from_protobuf(#'FileWrittenSubscription'{} = Msg) -> clproto_subscription_translator:from_protobuf(Msg);
from_protobuf(#'FileAttrChangedSubscription'{} = Msg) -> clproto_subscription_translator:from_protobuf(Msg);
from_protobuf(#'ReplicaStatusChangedSubscription'{} = Msg) -> clproto_subscription_translator:from_protobuf(Msg);
from_protobuf(#'FileLocationChangedSubscription'{} = Msg) -> clproto_subscription_translator:from_protobuf(Msg);
from_protobuf(#'FilePermChangedSubscription'{} = Msg) -> clproto_subscription_translator:from_protobuf(Msg);
from_protobuf(#'FileRemovedSubscription'{} = Msg) -> clproto_subscription_translator:from_protobuf(Msg);
from_protobuf(#'FileRenamedSubscription'{} = Msg) -> clproto_subscription_translator:from_protobuf(Msg);
from_protobuf(#'QuotaExceededSubscription'{} = Msg) -> clproto_subscription_translator:from_protobuf(Msg);
from_protobuf(#'HelperParamsChangedSubscription'{} = Msg) -> clproto_subscription_translator:from_protobuf(Msg);
from_protobuf(#'SubscriptionCancellation'{} = Msg) -> clproto_subscription_translator:from_protobuf(Msg);


%% HANDSHAKE
from_protobuf(#'ClientHandshakeRequest'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);
from_protobuf(#'ProviderHandshakeRequest'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);
from_protobuf(#'Macaroon'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);
from_protobuf(#'HandshakeResponse'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);


% PROCESSING STATUS
from_protobuf(#'ProcessingStatus'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);


%% DIAGNOSTIC
from_protobuf(#'Ping'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);
from_protobuf(#'GetProtocolVersion'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);
from_protobuf(#'ProtocolVersion'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);
from_protobuf(#'GetConfiguration'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);


%% STREAM
from_protobuf(#'MessageStream'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);
from_protobuf(#'MessageRequest'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);
from_protobuf(#'MessageAcknowledgement'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);
from_protobuf(#'MessageStreamReset'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);
from_protobuf(#'EndOfMessageStream'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);


%% SESSION
from_protobuf(#'CloseSession'{} = Msg) -> clproto_connection_translator:from_protobuf(Msg);


%% FUSE
from_protobuf(#'FuseRequest'{} = Msg) -> clproto_fuse_translator:from_protobuf(Msg);
from_protobuf(#'FuseResponse'{} = Msg) -> clproto_fuse_translator:from_protobuf(Msg);
from_protobuf(#'ResolveGuid'{} = Msg) -> clproto_fuse_translator:from_protobuf(Msg);
from_protobuf(#'GetHelperParams'{} = Msg) -> clproto_fuse_translator:from_protobuf(Msg);
from_protobuf(#'GetFSStats'{} = Msg) -> clproto_fuse_translator:from_protobuf(Msg);
from_protobuf(#'CreateStorageTestFile'{} = Msg) -> clproto_fuse_translator:from_protobuf(Msg);
from_protobuf(#'VerifyStorageTestFile'{} = Msg) -> clproto_fuse_translator:from_protobuf(Msg);


%% PROXYIO
from_protobuf(#'ProxyIORequest'{} = Msg) -> clproto_proxyio_translator:from_protobuf(Msg);
from_protobuf(#'ByteSequence'{} = Msg) -> clproto_proxyio_translator:from_protobuf(Msg);
from_protobuf(#'ProxyIOResponse'{} = Msg) -> clproto_proxyio_translator:from_protobuf(Msg);


%% PROVIDER RPC
from_protobuf(#'ProviderRpcCall'{} = Msg) -> clproto_provider_rpc_translator:from_protobuf(Msg);
from_protobuf(#'ProviderRpcResponse'{} = Msg) -> clproto_provider_rpc_translator:from_protobuf(Msg);


%% PROVIDER
from_protobuf(#'ProviderRequest'{} = Msg) -> clproto_provider_req_translator:from_protobuf(Msg);
from_protobuf(#'ProviderResponse'{} = Msg) -> clproto_provider_req_translator:from_protobuf(Msg);


%% DBSYNC
from_protobuf(#'DBSyncRequest'{} = Msg) -> clproto_dbsync_translator:from_protobuf(Msg);
from_protobuf(#'DBSyncMessage'{} = Msg) -> clproto_dbsync_translator:from_protobuf(Msg);


%% REMOTE DRIVER
from_protobuf(#'GetRemoteDocument'{} = Msg) -> clproto_remote_driver_translator:from_protobuf(Msg);
from_protobuf(#'RemoteDocument'{} = Msg) -> clproto_remote_driver_translator:from_protobuf(Msg);


%% RTRANSFER
from_protobuf(#'GenerateRTransferConnSecret'{} = Msg) -> clproto_rtransfer_translator:from_protobuf(Msg);
from_protobuf(#'RTransferConnSecret'{} = Msg) -> clproto_rtransfer_translator:from_protobuf(Msg);
from_protobuf(#'GetRTransferNodesIPs'{} = Msg) -> clproto_rtransfer_translator:from_protobuf(Msg);
from_protobuf(#'RTransferNodesIPs'{} = Msg) -> clproto_rtransfer_translator:from_protobuf(Msg);


%% OTHER
from_protobuf(undefined) -> undefined;
from_protobuf(Message) when is_tuple(Message) -> throw({unrecognized_message, element(1, Message)}).


-spec session_mode_from_protobuf(undefined | 'NORMAL' | 'OPEN_HANDLE') ->
    session:mode().
session_mode_from_protobuf(Mode) ->
    clproto_connection_translator:session_mode_from_protobuf(Mode).


%%%===================================================================
%%% TO PROTOBUF
%%%===================================================================

-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.

%% COMMON
to_protobuf(#status{} = Msg) -> clproto_common_translator:to_protobuf(Msg);
to_protobuf(#file_block{} = Msg) -> clproto_common_translator:to_protobuf(Msg);
to_protobuf(#file_renamed_entry{} = Msg) -> clproto_common_translator:to_protobuf(Msg);
to_protobuf(#ip_and_port{} = Msg) -> clproto_common_translator:to_protobuf(Msg);
to_protobuf(#dir{} = Msg) -> clproto_common_translator:to_protobuf(Msg);


%% EVENT
to_protobuf(#event{} = Msg) -> clproto_event_translator:to_protobuf(Msg);
to_protobuf(#events{} = Msg) -> clproto_event_translator:to_protobuf(Msg);
to_protobuf(#flush_events{} = Msg) -> clproto_event_translator:to_protobuf(Msg);
to_protobuf(#file_read_event{} = Msg) -> clproto_event_translator:to_protobuf(Msg);
to_protobuf(#file_written_event{} = Msg) -> clproto_event_translator:to_protobuf(Msg);
to_protobuf(#file_attr_changed_event{} = Msg) -> clproto_event_translator:to_protobuf(Msg);
to_protobuf(#file_location_changed_event{} = Msg) -> clproto_event_translator:to_protobuf(Msg);
to_protobuf(#file_perm_changed_event{} = Msg) -> clproto_event_translator:to_protobuf(Msg);
to_protobuf(#file_removed_event{} = Msg) -> clproto_event_translator:to_protobuf(Msg);
to_protobuf(#file_renamed_event{} = Msg) -> clproto_event_translator:to_protobuf(Msg);
to_protobuf(#quota_exceeded_event{} = Msg) -> clproto_event_translator:to_protobuf(Msg);
to_protobuf(#helper_params_changed_event{} = Msg) -> clproto_event_translator:to_protobuf(Msg);


%% SUBSCRIPTION
to_protobuf(#subscription{} = Msg) -> clproto_subscription_translator:to_protobuf(Msg);
to_protobuf(#file_read_subscription{} = Msg) -> clproto_subscription_translator:to_protobuf(Msg);
to_protobuf(#file_written_subscription{} = Msg) -> clproto_subscription_translator:to_protobuf(Msg);
to_protobuf(#file_attr_changed_subscription{} = Msg) -> clproto_subscription_translator:to_protobuf(Msg);
to_protobuf(#replica_status_changed_subscription{} = Msg) -> clproto_subscription_translator:to_protobuf(Msg);
to_protobuf(#file_location_changed_subscription{} = Msg) -> clproto_subscription_translator:to_protobuf(Msg);
to_protobuf(#file_perm_changed_subscription{} = Msg) -> clproto_subscription_translator:to_protobuf(Msg);
to_protobuf(#file_removed_subscription{} = Msg) -> clproto_subscription_translator:to_protobuf(Msg);
to_protobuf(#file_renamed_subscription{} = Msg) -> clproto_subscription_translator:to_protobuf(Msg);
to_protobuf(#quota_exceeded_subscription{} = Msg) -> clproto_subscription_translator:to_protobuf(Msg);
to_protobuf(#helper_params_changed_subscription{} = Msg) -> clproto_subscription_translator:to_protobuf(Msg);
to_protobuf(#subscription_cancellation{} = Msg) -> clproto_subscription_translator:to_protobuf(Msg);


%% HANDSHAKE
to_protobuf(#provider_handshake_request{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);
to_protobuf(#handshake_response{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);
to_protobuf(#client_tokens{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);


% PROCESSING STATUS
to_protobuf(#processing_status{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);


%% DIAGNOSTIC
to_protobuf(#pong{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);
to_protobuf(#protocol_version{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);
to_protobuf(#configuration{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);


%% STREAM
to_protobuf(#message_stream{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);
to_protobuf(#message_request{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);
to_protobuf(#message_acknowledgement{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);
to_protobuf(#message_stream_reset{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);
to_protobuf(#end_of_message_stream{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);


%% SESSION
to_protobuf(#close_session{} = Msg) -> clproto_connection_translator:to_protobuf(Msg);

%% FUSE
to_protobuf(#fuse_request{} = Msg) -> clproto_fuse_translator:to_protobuf(Msg);
to_protobuf(#fuse_response{} = Msg) -> clproto_fuse_translator:to_protobuf(Msg);
to_protobuf(#resolve_guid{} = Msg) -> clproto_fuse_translator:to_protobuf(Msg);
to_protobuf(#get_helper_params{} = Msg) -> clproto_fuse_translator:to_protobuf(Msg);
to_protobuf(#get_fs_stats{} = Msg) -> clproto_fuse_translator:to_protobuf(Msg);


%% PROXYIO
to_protobuf(#proxyio_request{} = Msg) -> clproto_proxyio_translator:to_protobuf(Msg);
to_protobuf(#byte_sequence{} = Msg) -> clproto_proxyio_translator:to_protobuf(Msg);
to_protobuf(#proxyio_response{} = Msg) -> clproto_proxyio_translator:to_protobuf(Msg);


%% PROVIDER RPC
to_protobuf(#provider_rpc_call{} = Msg) -> clproto_provider_rpc_translator:to_protobuf(Msg);
to_protobuf(#provider_rpc_response{} = Msg) -> clproto_provider_rpc_translator:to_protobuf(Msg);


%% PROVIDER
to_protobuf(#provider_request{} = Msg) -> clproto_provider_req_translator:to_protobuf(Msg);
to_protobuf(#provider_response{} = Msg) -> clproto_provider_req_translator:to_protobuf(Msg);


%% DBSYNC
to_protobuf(#dbsync_request{} = Msg) -> clproto_dbsync_translator:to_protobuf(Msg);
to_protobuf(#dbsync_message{} = Msg) -> clproto_dbsync_translator:to_protobuf(Msg);


%% REMOTE DRIVER
to_protobuf(#get_remote_document{} = Msg) -> clproto_remote_driver_translator:to_protobuf(Msg);
to_protobuf(#remote_document{} = Msg) -> clproto_remote_driver_translator:to_protobuf(Msg);


%% RTRANSFER
to_protobuf(#generate_rtransfer_conn_secret{} = Msg) -> clproto_rtransfer_translator:to_protobuf(Msg);
to_protobuf(#rtransfer_conn_secret{} = Msg) -> clproto_rtransfer_translator:to_protobuf(Msg);
to_protobuf(#get_rtransfer_nodes_ips{} = Msg) -> clproto_rtransfer_translator:to_protobuf(Msg);
to_protobuf(#rtransfer_nodes_ips{} = Msg) -> clproto_rtransfer_translator:to_protobuf(Msg);


%% OTHER
to_protobuf(undefined) -> undefined.


-spec session_mode_to_protobuf(undefined | session:mode()) ->
    'NORMAL' | 'OPEN_HANDLE'.
session_mode_to_protobuf(Mode) ->
    clproto_connection_translator:session_mode_to_protobuf(Mode).


-spec handshake_error_to_protobuf(Type :: binary()) -> Type :: atom().
handshake_error_to_protobuf(Error) ->
    clproto_connection_translator:handshake_error_to_protobuf(Error).
