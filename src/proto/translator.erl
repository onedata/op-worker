%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Translations between protobuff and internal protocol
%%% @end
%%%-------------------------------------------------------------------
-module(translator).
-author("Tomasz Lichon").
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneprovider/dbsync_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([translate_from_protobuf/1, translate_to_protobuf/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% traslate protobuf record to internal record
%% @end
%%--------------------------------------------------------------------
-spec translate_from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
translate_from_protobuf(#'Status'{code = Code, description = Desc}) ->
    #status{code = Code, description = Desc};
translate_from_protobuf(#'Events'{events = Evts}) ->
    #events{events = [translate_from_protobuf(Evt) || Evt <- Evts]};
translate_from_protobuf(#'Event'{counter = Counter, object = {_, Record}}) ->
    #event{counter = Counter, object = translate_from_protobuf(Record)};
translate_from_protobuf(#'ReadEvent'{} = Record) ->
    #read_event{
        file_uuid = Record#'ReadEvent'.file_uuid,
        size = Record#'ReadEvent'.size,
        blocks = [translate_from_protobuf(B) || B <- Record#'ReadEvent'.blocks]
    };
translate_from_protobuf(#'WriteEvent'{} = Record) ->
    #write_event{
        file_uuid = Record#'WriteEvent'.file_uuid,
        size = Record#'WriteEvent'.size,
        file_size = Record#'WriteEvent'.file_size,
        blocks = [translate_from_protobuf(B) || B <- Record#'WriteEvent'.blocks]
    };
translate_from_protobuf(#'UpdateEvent'{object = {_, Obj}}) ->
    #update_event{
        object = translate_from_protobuf(Obj)
    };
translate_from_protobuf(#'Subscription'{id = Id, object = {_, Record}}) ->
    #subscription{
        id = Id,
        object = translate_from_protobuf(Record)
    };
translate_from_protobuf(#'FileAttrSubscription'{} = Record) ->
    #file_attr_subscription{
        file_uuid = Record#'FileAttrSubscription'.file_uuid,
        counter_threshold = Record#'FileAttrSubscription'.counter_threshold,
        time_threshold = Record#'FileAttrSubscription'.time_threshold
    };
translate_from_protobuf(#'FileLocationSubscription'{} = Record) ->
    #file_location_subscription{
        file_uuid = Record#'FileLocationSubscription'.file_uuid,
        counter_threshold = Record#'FileLocationSubscription'.counter_threshold,
        time_threshold = Record#'FileLocationSubscription'.time_threshold
    };
translate_from_protobuf(#'PermissionChangedSubscription'{} = Record) ->
    #permission_changed_subscription{
        file_uuid = Record#'PermissionChangedSubscription'.file_uuid
    };
translate_from_protobuf(#'FileRemovalSubscription'{} = Record) ->
    #file_removal_subscription{
        file_uuid = Record#'FileRemovalSubscription'.file_uuid
    };
translate_from_protobuf(#'ReadSubscription'{} = Record) ->
    #read_subscription{
        counter_threshold = Record#'ReadSubscription'.counter_threshold,
        size_threshold = Record#'ReadSubscription'.size_threshold,
        time_threshold = Record#'ReadSubscription'.time_threshold
    };
translate_from_protobuf(#'WriteSubscription'{} = Record) ->
    #write_subscription{
        counter_threshold = Record#'WriteSubscription'.counter_threshold,
        size_threshold = Record#'WriteSubscription'.size_threshold,
        time_threshold = Record#'WriteSubscription'.time_threshold
    };
translate_from_protobuf(#'SubscriptionCancellation'{id = Id}) ->
    #subscription_cancellation{id = Id};
translate_from_protobuf(#'FileBlock'{offset = Off, size = S, file_id = FID, storage_id = SID}) ->
    #file_block{offset = Off, size = S, file_id = FID, storage_id = SID};
translate_from_protobuf(#'HandshakeRequest'{token = Token, session_id = SessionId}) ->
    #handshake_request{auth = translate_from_protobuf(Token), session_id = SessionId};
translate_from_protobuf(#'GetConfiguration'{}) ->
    #get_configuration{};
translate_from_protobuf(#'MessageStream'{stream_id = StmId, sequence_number = SeqNum}) ->
    #message_stream{stream_id = StmId, sequence_number = SeqNum};
translate_from_protobuf(#'EndOfMessageStream'{}) ->
    #end_of_message_stream{};
translate_from_protobuf(#'MessageStreamReset'{stream_id = StmId}) ->
    #message_stream_reset{stream_id = StmId};
translate_from_protobuf(#'MessageRequest'{} = Record) ->
    #message_request{
        stream_id = Record#'MessageRequest'.stream_id,
        lower_sequence_number = Record#'MessageRequest'.lower_sequence_number,
        upper_sequence_number = Record#'MessageRequest'.upper_sequence_number
    };
translate_from_protobuf(#'MessageAcknowledgement'{} = Record) ->
    #message_acknowledgement{
        stream_id = Record#'MessageAcknowledgement'.stream_id,
        sequence_number = Record#'MessageAcknowledgement'.sequence_number
    };
translate_from_protobuf(#'Token'{value = Val, secondary_values = SecValues}) ->
    {ok, Macaroon} = macaroon:deserialize(Val),
    DischargeMacaroons = [R || {ok, R} <- [macaroon:deserialize(SecValue) || SecValue <- SecValues]],
    #auth{macaroon = Macaroon, disch_macaroons = DischargeMacaroons};
translate_from_protobuf(#'Ping'{data = Data}) ->
    #ping{data = Data};
translate_from_protobuf(#'GetProtocolVersion'{}) ->
    #get_protocol_version{};
translate_from_protobuf(#'FuseRequest'{fuse_request = {_, Record}}) ->
    #fuse_request{fuse_request = translate_from_protobuf(Record)};
translate_from_protobuf(#'GetFileAttr'{entry_type = 'PATH', entry = Path}) ->
    #get_file_attr{entry = {path, Path}};
translate_from_protobuf(#'GetFileAttr'{entry_type = 'UUID', entry = GUID}) ->
    #get_file_attr{entry = {guid, GUID}};
translate_from_protobuf(#'GetFileChildren'{uuid = UUID, offset = Offset, size = Size}) ->
    #get_file_children{uuid = UUID, offset = Offset, size = Size};
translate_from_protobuf(#'CreateDir'{parent_uuid = ParentUUID, name = Name, mode = Mode}) ->
    #create_dir{parent_uuid = ParentUUID, name = Name, mode = Mode};
translate_from_protobuf(#'DeleteFile'{uuid = UUID}) ->
    #delete_file{uuid = UUID};
translate_from_protobuf(#'UpdateTimes'{uuid = UUID, atime = ATime, mtime = MTime,
    ctime = CTime}) ->
    #update_times{uuid = UUID, atime = ATime, mtime = MTime, ctime = CTime};
translate_from_protobuf(#'ChangeMode'{uuid = UUID, mode = Mode}) ->
    #change_mode{uuid = UUID, mode = Mode};
translate_from_protobuf(#'Rename'{uuid = UUID, target_path = TargetPath}) ->
    #rename{uuid = UUID, target_path = TargetPath};
translate_from_protobuf(#'GetNewFileLocation'{name = Name, parent_uuid = ParentUUID, mode = Mode, flags = Flags}) ->
    #get_new_file_location{name = Name, parent_uuid = ParentUUID, mode = Mode, flags = open_flags_translate_from_protobuf(Flags)};
translate_from_protobuf(#'GetFileLocation'{uuid = UUID, flags = Flags}) ->
    #get_file_location{uuid = UUID, flags = open_flags_translate_from_protobuf(Flags)};
translate_from_protobuf(#'GetHelperParams'{storage_id = SID, force_proxy_io = ForceProxy}) ->
    #get_helper_params{storage_id = SID, force_proxy_io = ForceProxy};
translate_from_protobuf(#'Truncate'{uuid = UUID, size = Size}) ->
    #truncate{uuid = UUID, size = Size};
translate_from_protobuf(#'Release'{uuid = UUID, handle_id = HandleId}) ->
    #release{uuid = UUID, handle_id = HandleId};
translate_from_protobuf(#'ProxyIORequest'{parameters = Parameters, storage_id = SID, file_id = FID, proxyio_request = {_, Record}}) ->
    #proxyio_request{parameters = maps:from_list([translate_from_protobuf(P) || P <- Parameters]),
        storage_id = SID, file_id = FID, proxyio_request = translate_from_protobuf(Record)};
translate_from_protobuf(#'RemoteRead'{offset = Offset, size = Size}) ->
    #remote_read{offset = Offset, size = Size};
translate_from_protobuf(#'RemoteWrite'{byte_sequence = ByteSequences}) ->
    #remote_write{byte_sequence = [translate_from_protobuf(BS) || BS <- ByteSequences]};
translate_from_protobuf(#'ByteSequence'{offset = Offset, data = Data}) ->
    #byte_sequence{offset = Offset, data = Data};
translate_from_protobuf(#'GetXattr'{uuid = UUID, name = Name}) ->
    #get_xattr{uuid = UUID, name = Name};
translate_from_protobuf(#'SetXattr'{uuid = UUID, xattr = Xattr}) ->
    #set_xattr{uuid = UUID, xattr = translate_from_protobuf(Xattr)};
translate_from_protobuf(#'RemoveXattr'{uuid = UUID, name = Name}) ->
    #remove_xattr{uuid = UUID, name = Name};
translate_from_protobuf(#'ListXattr'{uuid = UUID}) ->
    #list_xattr{uuid = UUID};
translate_from_protobuf(#'Xattr'{name = Name, value = Value}) ->
    #xattr{name = Name, value = Value};
translate_from_protobuf(#'XattrList'{names = Names}) ->
    #xattr_list{names = Names};

translate_from_protobuf(#'ProtocolVersion'{major = Major, minor = Minor}) ->
    #protocol_version{major = Major, minor = Minor};
translate_from_protobuf(#'FuseResponse'{status = Status, fuse_response = {_, FuseResponse}}) ->
    #'fuse_response'{
        status = #'status'{
            code = Status#'Status'.code,
            description = Status#'Status'.description
        },
        fuse_response = translate_from_protobuf(FuseResponse)
    };
translate_from_protobuf(#'FuseResponse'{status = Status}) ->
    #'fuse_response'{
        status = #'status'{
            code = Status#'Status'.code,
            description = Status#'Status'.description
        },
        fuse_response = undefined
    };
translate_from_protobuf(#'ChildLink'{uuid = UUID, name = Name}) ->
    #child_link{uuid = UUID, name = Name};
translate_from_protobuf(#'FileAttr'{} = FileAttr) ->
    #'file_attr'{
        uuid = FileAttr#'FileAttr'.uuid,
        name = FileAttr#'FileAttr'.name,
        mode = FileAttr#'FileAttr'.mode,
        uid = FileAttr#'FileAttr'.uid,
        gid = FileAttr#'FileAttr'.gid,
        atime = FileAttr#'FileAttr'.atime,
        mtime = FileAttr#'FileAttr'.mtime,
        ctime = FileAttr#'FileAttr'.ctime,
        type = FileAttr#'FileAttr'.type,
        size = FileAttr#'FileAttr'.size
    };
translate_from_protobuf(#'FileChildren'{child_links = FileEntries}) ->
    #file_children{child_links = lists:map(
        fun(ChildLink) ->
            translate_from_protobuf(ChildLink)
        end, FileEntries)};
translate_from_protobuf(#'HelperParams'{helper_name = HelperName, helper_args = HelpersArgs}) ->
    #'helper_params'{helper_name = HelperName,
        helper_args = lists:map(
            fun(HelpersArg) ->
                translate_from_protobuf(HelpersArg)
            end, HelpersArgs)};
translate_from_protobuf(#'HelperArg'{key = Key, value = Value}) ->
    #'helper_arg'{key = Key, value = Value};
translate_from_protobuf(#'FileLocation'{} = Record) ->
    #file_location{
        uuid = Record#'FileLocation'.uuid,
        provider_id = Record#'FileLocation'.provider_id,
        space_uuid = Record#'FileLocation'.space_id,
        storage_id = Record#'FileLocation'.storage_id,
        file_id = Record#'FileLocation'.file_id,
        blocks = lists:map(
            fun(Block) ->
                translate_from_protobuf(Block)
            end, Record#'FileLocation'.blocks)
    };
translate_from_protobuf(#'ProxyIOResponse'{status = Status, proxyio_response = {_, ProxyIOResponse}}) ->
    #'proxyio_response'{
        status = translate_from_protobuf(Status),
        proxyio_response = translate_from_protobuf(ProxyIOResponse)
    };
translate_from_protobuf(#'ProxyIOResponse'{status = Status}) ->
    #'proxyio_response'{
        status = translate_from_protobuf(Status)
    };
translate_from_protobuf(#'RemoteData'{data = Data}) ->
    #'remote_data'{data = Data};
translate_from_protobuf(#'RemoteWriteResult'{wrote = Wrote}) ->
    #'remote_write_result'{wrote = Wrote};
translate_from_protobuf(#'ProxyIORequest'{parameters = Parameters, storage_id = SID, file_id = FID, proxyio_request = Record}) ->
    #'proxyio_request'{parameters = maps:from_list([translate_from_protobuf(P) || P <- Parameters]),
        storage_id = SID, file_id = FID, proxyio_request = translate_to_protobuf(Record)};

translate_from_protobuf(#'GetParent'{uuid = UUID}) ->
    #'get_parent'{uuid = UUID};
translate_from_protobuf(#'Dir'{uuid = UUID}) ->
    #'dir'{uuid = UUID};

translate_from_protobuf(#'CreateStorageTestFile'{storage_id = Id, file_uuid = FileUuid}) ->
    #create_storage_test_file{storage_id = Id, file_uuid = FileUuid};
translate_from_protobuf(#'VerifyStorageTestFile'{storage_id = SId, space_uuid = SpaceUuid,
    file_id = FId, file_content = FContent}) ->
    #verify_storage_test_file{storage_id = SId, space_uuid = SpaceUuid,
        file_id = FId, file_content = FContent};
translate_from_protobuf(#'Parameter'{key = Key, value = Value}) ->
    {Key, Value};

%% DBSync
translate_from_protobuf(#'DBSyncRequest'{message_body = {_, MessageBody}}) ->
    #dbsync_request{message_body = translate_from_protobuf(MessageBody)};
translate_from_protobuf(#'TreeBroadcast'{message_body = {_, MessageBody}, depth = Depth, excluded_providers = ExcludedProv,
    l_edge = LEdge, r_edge = REgde, request_id = ReqId, space_id = SpaceId}) ->
    #tree_broadcast{
        message_body = translate_from_protobuf(MessageBody),
        depth = Depth,
        l_edge = LEdge,
        r_edge = REgde,
        space_id = SpaceId,
        request_id = ReqId,
        excluded_providers = ExcludedProv
    };
translate_from_protobuf(#'ChangesRequest'{since_seq = Since, until_seq = Until}) ->
    #changes_request{since_seq = Since, until_seq = Until};
translate_from_protobuf(#'StatusRequest'{}) ->
    #status_request{};
translate_from_protobuf(#'StatusReport'{space_id = SpaceId, seq_num = SeqNum}) ->
    #status_report{space_id = SpaceId, seq = SeqNum};
translate_from_protobuf(#'BatchUpdate'{space_id = SpaceId, since_seq = Since, until_seq = Until, changes_encoded = Changes}) ->
    #batch_update{space_id = SpaceId, since_seq = Since, until_seq = Until, changes_encoded = Changes};

% Replication
translate_from_protobuf(#'SynchronizeBlock'{uuid = Uuid, block = #'FileBlock'{offset = O, size = S}}) ->
    #synchronize_block{uuid = Uuid, block = #file_block{offset = O, size = S}};
translate_from_protobuf(#'SynchronizeBlockAndComputeChecksum'{uuid = Uuid,
    block = #'FileBlock'{offset = O, size = S}}) ->
    #synchronize_block_and_compute_checksum{uuid = Uuid, block = #file_block{offset = O, size = S}};
translate_from_protobuf(#'Checksum'{value = Value}) ->
    #checksum{value = Value};

%% CDMI
translate_from_protobuf(#'Acl'{value = Value}) ->
    #acl{value = Value};
translate_from_protobuf(#'GetAcl'{uuid = UUID}) ->
    #get_acl{uuid = UUID};
translate_from_protobuf(#'SetAcl'{uuid = UUID, acl = Acl}) ->
    #set_acl{uuid = UUID, acl = translate_from_protobuf(Acl)};
translate_from_protobuf(#'GetTransferEncoding'{uuid = UUID}) ->
    #get_transfer_encoding{uuid = UUID};
translate_from_protobuf(#'SetTransferEncoding'{uuid = UUID, value = Value}) ->
    #set_transfer_encoding{uuid = UUID, value = Value};
translate_from_protobuf(#'GetCdmiCompletionStatus'{uuid = UUID}) ->
    #get_cdmi_completion_status{uuid = UUID};
translate_from_protobuf(#'SetCdmiCompletionStatus'{uuid = UUID, value = Value}) ->
    #set_cdmi_completion_status{uuid = UUID, value = Value};
translate_from_protobuf(#'GetMimetype'{uuid = UUID}) ->
    #get_mimetype{uuid = UUID};
translate_from_protobuf(#'SetMimetype'{uuid = UUID, value = Value}) ->
    #set_mimetype{uuid = UUID, value = Value};

translate_from_protobuf(#'TransferEncoding'{value = Value}) ->
    #transfer_encoding{value = Value};
translate_from_protobuf(#'CdmiCompletionStatus'{value = Value}) ->
    #cdmi_completion_status{value = Value};
translate_from_protobuf(#'Mimetype'{value = Value}) ->
    #mimetype{value = Value};
translate_from_protobuf(#'GetFilePath'{uuid = UUID}) ->
    #'get_file_path'{uuid = UUID};
translate_from_protobuf(#'FilePath'{value = Value}) ->
    #'file_path'{value = Value};
translate_from_protobuf(#'FSync'{uuid = UUID}) ->
    #'fsync'{uuid = UUID};

translate_from_protobuf(undefined) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% translate internal record to protobuf record
%% @end
%%--------------------------------------------------------------------
-spec translate_to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
translate_to_protobuf(#status{code = Code, description = Desc}) ->
    {status, #'Status'{code = Code, description = Desc}};
translate_to_protobuf(#events{events = Evts}) ->
    {events, #'Events'{events = [translate_to_protobuf(Evt) || Evt <- Evts]}};
translate_to_protobuf(#event{counter = Counter, object = Type}) ->
    #'Event'{counter = Counter, object = translate_to_protobuf(Type)};
translate_to_protobuf(#'read_event'{} = Record) ->
    {read_event, #'ReadEvent'{
        file_uuid = Record#'read_event'.file_uuid,
        size = Record#'read_event'.size,
        blocks = [translate_to_protobuf(B) || B <- Record#'read_event'.blocks]
    }};
translate_to_protobuf(#'write_event'{} = Record) ->
    {write_event, #'WriteEvent'{
        file_uuid = Record#'write_event'.file_uuid,
        size = Record#'write_event'.size,
        file_size = Record#'write_event'.file_size,
        blocks = [translate_to_protobuf(B) || B <- Record#'write_event'.blocks]
    }};
translate_to_protobuf(#update_event{object = Type}) ->
    {update_event, #'UpdateEvent'{object = translate_to_protobuf(Type)}};
translate_to_protobuf(#permission_changed_event{file_uuid = FileUuid}) ->
    {permission_changed_event, #'PermissionChangedEvent'{file_uuid = FileUuid}};
translate_to_protobuf(#file_removal_event{file_uuid = FileUuid}) ->
    {file_removal_event, #'FileRemovalEvent'{file_uuid = FileUuid}};
translate_to_protobuf(#subscription{id = Id, object = Type}) ->
    {subscription, #'Subscription'{id = Id, object = translate_to_protobuf(Type)}};
translate_to_protobuf(#read_subscription{} = Sub) ->
    {read_subscription, #'ReadSubscription'{
        counter_threshold = Sub#read_subscription.counter_threshold,
        time_threshold = Sub#read_subscription.time_threshold,
        size_threshold = Sub#read_subscription.size_threshold
    }};
translate_to_protobuf(#write_subscription{} = Sub) ->
    {write_subscription, #'WriteSubscription'{
        counter_threshold = Sub#write_subscription.counter_threshold,
        time_threshold = Sub#write_subscription.time_threshold,
        size_threshold = Sub#write_subscription.size_threshold
    }};
translate_to_protobuf(#file_attr_subscription{} = Record) ->
    {file_attr_subscription, #'FileAttrSubscription'{
        file_uuid = Record#'file_attr_subscription'.file_uuid,
        counter_threshold = Record#'file_attr_subscription'.counter_threshold,
        time_threshold = Record#'file_attr_subscription'.time_threshold
    }};
translate_to_protobuf(#'file_location_subscription'{} = Record) ->
    {file_location_subscription, #'FileLocationSubscription'{
        file_uuid = Record#'file_location_subscription'.file_uuid,
        counter_threshold = Record#'file_location_subscription'.counter_threshold,
        time_threshold = Record#'file_location_subscription'.time_threshold
    }};
translate_to_protobuf(#'permission_changed_subscription'{} = Record) ->
    {permission_changed_subscription, #'PermissionChangedSubscription'{
        file_uuid = Record#'permission_changed_subscription'.file_uuid
    }};
translate_to_protobuf(#subscription_cancellation{id = Id}) ->
    {subscription_cancellation, #'SubscriptionCancellation'{id = Id}};
translate_to_protobuf(#handshake_response{session_id = Id}) ->
    {handshake_response, #'HandshakeResponse'{
        session_id = Id
    }};
translate_to_protobuf(#configuration{subscriptions = Subs}) ->
    {configuration, #'Configuration'{
        subscriptions = lists:map(
            fun(Sub) ->
                {_, Record} = translate_to_protobuf(Sub),
                Record
            end, Subs)
    }};
translate_to_protobuf(#message_stream{stream_id = StmId, sequence_number = SeqNum}) ->
    #'MessageStream'{stream_id = StmId, sequence_number = SeqNum};
translate_to_protobuf(#end_of_message_stream{}) ->
    {end_of_stream, #'EndOfMessageStream'{}};
translate_to_protobuf(#message_stream_reset{stream_id = StmId}) ->
    {message_stream_reset, #'MessageStreamReset'{stream_id = StmId}};
translate_to_protobuf(#message_request{stream_id = StmId,
    lower_sequence_number = LowerSeqNum, upper_sequence_number = UpperSeqNum}) ->
    {message_request, #'MessageRequest'{stream_id = StmId,
        lower_sequence_number = LowerSeqNum, upper_sequence_number = UpperSeqNum}};
translate_to_protobuf(#message_acknowledgement{stream_id = StmId, sequence_number = SeqNum}) ->
    {message_acknowledgement,
        #'MessageAcknowledgement'{stream_id = StmId, sequence_number = SeqNum}};
translate_to_protobuf(#pong{data = Data}) ->
    {pong, #'Pong'{data = Data}};
translate_to_protobuf(#protocol_version{major = Major, minor = Minor}) ->
    {protocol_version, #'ProtocolVersion'{major = Major, minor = Minor}};
translate_to_protobuf(#fuse_response{status = Status, fuse_response = FuseResponse}) ->
    {fuse_response, #'FuseResponse'{
        status = #'Status'{
            code = Status#status.code,
            description = Status#status.description
        },
        fuse_response = translate_to_protobuf(FuseResponse)
    }};
translate_to_protobuf(#child_link{uuid = UUID, name = Name}) ->
    #'ChildLink'{uuid = UUID, name = Name};
translate_to_protobuf(#file_attr{} = FileAttr) ->
    {file_attr, #'FileAttr'{
        uuid = FileAttr#file_attr.uuid,
        name = FileAttr#file_attr.name,
        mode = FileAttr#file_attr.mode,
        uid = FileAttr#file_attr.uid,
        gid = FileAttr#file_attr.gid,
        atime = FileAttr#file_attr.atime,
        mtime = FileAttr#file_attr.mtime,
        ctime = FileAttr#file_attr.ctime,
        type = FileAttr#file_attr.type,
        size = FileAttr#file_attr.size
    }};
translate_to_protobuf(#file_children{child_links = FileEntries}) ->
    {file_children, #'FileChildren'{child_links = lists:map(fun(ChildLink) ->
        translate_to_protobuf(ChildLink)
    end, FileEntries)}};
translate_to_protobuf(#helper_params{helper_name = HelperName, helper_args = HelpersArgs}) ->
    {helper_params, #'HelperParams'{helper_name = HelperName,
        helper_args = lists:map(fun(HelpersArg) ->
            translate_to_protobuf(HelpersArg)
        end, HelpersArgs)}};
translate_to_protobuf(#helper_arg{key = Key, value = Value}) ->
    #'HelperArg'{key = Key, value = Value};
translate_to_protobuf(#file_location{} = Record) ->
    {file_location, #'FileLocation'{
        uuid = Record#file_location.uuid,
        provider_id = Record#file_location.provider_id,
        space_id = Record#file_location.space_uuid,
        storage_id = Record#file_location.storage_id,
        file_id = Record#file_location.file_id,
        blocks = lists:map(fun(Block) ->
            translate_to_protobuf(Block)
        end, Record#file_location.blocks),
        handle_id = Record#file_location.handle_id
    }};
translate_to_protobuf(#file_block{offset = Off, size = S, file_id = FID, storage_id = SID}) ->
    #'FileBlock'{offset = Off, size = S, file_id = FID, storage_id = SID};
translate_to_protobuf(#proxyio_response{status = Status, proxyio_response = ProxyIOResponse}) ->
    {status, StatProto} = translate_to_protobuf(Status),
    {proxyio_response, #'ProxyIOResponse'{
        status = StatProto,
        proxyio_response = translate_to_protobuf(ProxyIOResponse)
    }};
translate_to_protobuf(#remote_write{byte_sequence = ByteSequences}) ->
    {remote_write, #'RemoteWrite'{byte_sequence = [translate_to_protobuf(BS) || BS <- ByteSequences]}};
translate_to_protobuf(#'byte_sequence'{offset = Offset, data = Data}) ->
    #'ByteSequence'{offset = Offset, data = Data};
translate_to_protobuf(#remote_data{data = Data}) ->
    {remote_data, #'RemoteData'{data = Data}};
translate_to_protobuf(#get_xattr{uuid = Uuid, name = Name}) ->
    {get_xattr, #'GetXattr'{uuid = Uuid, name = Name}};
translate_to_protobuf(#set_xattr{uuid = Uuid, xattr = Xattr}) ->
    {_, XattrT} = translate_to_protobuf(Xattr),
    {set_xattr, #'SetXattr'{uuid = Uuid, xattr = XattrT}};
translate_to_protobuf(#remove_xattr{uuid = Uuid, name = Name}) ->
    {remove_xattr, #'RemoveXattr'{uuid = Uuid, name = Name}};
translate_to_protobuf(#list_xattr{uuid = Uuid}) ->
    {list_xattr, #'ListXattr'{uuid = Uuid}};
translate_to_protobuf(#xattr{name = Name, value = Value}) ->
    {xattr, #'Xattr'{name = Name, value = Value}};
translate_to_protobuf(#xattr_list{names = Names}) ->
    {xattr_list, #'XattrList'{names = Names}};
translate_to_protobuf(#auth{macaroon = Macaroon, disch_macaroons = DMacaroons}) ->
    {ok, Token} = macaroon:serialize(Macaroon),
    SecValues = [R || {ok, R} <- [macaroon:serialize(DMacaroon) || DMacaroon <- DMacaroons]],
    #'Token'{value = Token, secondary_values = SecValues};
translate_to_protobuf(#'remote_write_result'{wrote = Wrote}) ->
    {remote_write_result, #'RemoteWriteResult'{wrote = Wrote}};


translate_to_protobuf(#fuse_request{fuse_request = Record}) ->
    {fuse_request, #'FuseRequest'{fuse_request = translate_to_protobuf(Record)}};
translate_to_protobuf(#get_file_attr{entry = {path, Path}}) ->
    {get_file_attr, #'GetFileAttr'{entry = Path, entry_type = 'PATH'}};
translate_to_protobuf(#get_file_attr{entry = {guid, GUID}}) ->
    {get_file_attr, #'GetFileAttr'{entry_type = 'UUID', entry = GUID}};
translate_to_protobuf(#get_file_children{uuid = UUID, offset = Offset, size = Size}) ->
    {get_file_children, #'GetFileChildren'{uuid = UUID, offset = Offset, size = Size}};
translate_to_protobuf(#create_dir{parent_uuid = ParentUUID, name = Name, mode = Mode}) ->
    {create_dir, #'CreateDir'{parent_uuid = ParentUUID, name = Name, mode = Mode}};
translate_to_protobuf(#delete_file{uuid = UUID}) ->
    {delete_file, #'DeleteFile'{uuid = UUID}};
translate_to_protobuf(#update_times{uuid = UUID, atime = ATime, mtime = MTime,
    ctime = CTime}) ->
    {update_times, #'UpdateTimes'{uuid = UUID, atime = ATime, mtime = MTime, ctime = CTime}};
translate_to_protobuf(#change_mode{uuid = UUID, mode = Mode}) ->
    {change_mode, #'ChangeMode'{uuid = UUID, mode = Mode}};
translate_to_protobuf(#rename{uuid = UUID, target_path = TargetPath}) ->
    {rename, #'Rename'{uuid = UUID, target_path = TargetPath}};
translate_to_protobuf(#'get_new_file_location'{name = Name, parent_uuid = ParentUUID, mode = Mode, flags = Flags}) ->
    {get_new_file_location, #'GetNewFileLocation'{name = Name, parent_uuid = ParentUUID, mode = Mode, flags = open_flags_translate_to_protobuf(Flags)}};
translate_to_protobuf(#get_file_location{uuid = UUID, flags = Flags}) ->
    {get_file_location, #'GetFileLocation'{uuid = UUID, flags = open_flags_translate_to_protobuf(Flags)}};
translate_to_protobuf(#get_helper_params{storage_id = SID, force_proxy_io = ForceProxy}) ->
    {get_helper_params, #'GetHelperParams'{storage_id = SID, force_proxy_io = ForceProxy}};
translate_to_protobuf(#truncate{uuid = UUID, size = Size}) ->
    {truncate, #'Truncate'{uuid = UUID, size = Size}};
translate_to_protobuf(#release{uuid = UUID, handle_id = HandleId}) ->
    {release, #'Release'{uuid = UUID, handle_id = HandleId}};

translate_to_protobuf(#storage_test_file{helper_params = HelperParams,
    space_uuid = SpaceUuid, file_id = FileId, file_content = FileContent}) ->
    {_, Record} = translate_to_protobuf(HelperParams),
    {storage_test_file, #'StorageTestFile'{helper_params = Record,
        space_uuid = SpaceUuid, file_id = FileId, file_content = FileContent}};

%% DBSync
translate_to_protobuf(#dbsync_request{message_body = MessageBody}) ->
    {dbsync_request, #'DBSyncRequest'{message_body = translate_to_protobuf(MessageBody)}};
translate_to_protobuf(#tree_broadcast{message_body = MessageBody, depth = Depth, excluded_providers = ExcludedProv,
    l_edge = LEdge, r_edge = REgde, request_id = ReqId, space_id = SpaceId}) ->
    {tree_broadcast, #'TreeBroadcast'{
        message_body = translate_to_protobuf(MessageBody),
        depth = Depth,
        l_edge = LEdge,
        r_edge = REgde,
        space_id = SpaceId,
        request_id = ReqId,
        excluded_providers = ExcludedProv
    }};
translate_to_protobuf(#changes_request{since_seq = Since, until_seq = Until}) ->
    {changes_request, #'ChangesRequest'{since_seq = Since, until_seq = Until}};
translate_to_protobuf(#status_request{}) ->
    {status_request, #'StatusRequest'{}};
translate_to_protobuf(#status_report{space_id = SpaceId, seq = SeqNum}) ->
    {status_report, #'StatusReport'{space_id = SpaceId, seq_num = SeqNum}};
translate_to_protobuf(#batch_update{space_id = SpaceId, since_seq = Since, until_seq = Until, changes_encoded = Changes}) ->
    {batch_update, #'BatchUpdate'{space_id = SpaceId, since_seq = Since, until_seq = Until, changes_encoded = Changes}};



translate_to_protobuf(#'proxyio_request'{parameters = Parameters, storage_id = SID, file_id = FID, proxyio_request = Record}) ->
    ParametersProto = lists:map(
        fun({Key, Value}) ->
            #'Parameter'{key = Key, value = Value}
        end, maps:to_list(Parameters)),
    {proxyio_request, #'ProxyIORequest'{parameters = ParametersProto, storage_id = SID, file_id = FID, proxyio_request = translate_to_protobuf(Record)}};
translate_to_protobuf(#'remote_read'{offset = Offset, size = Size}) ->
    {remote_read, #'RemoteRead'{offset = Offset, size = Size}};
translate_to_protobuf(#'dir'{uuid = UUID}) ->
    {dir, #'Dir'{uuid = UUID}};
translate_to_protobuf(#'get_parent'{uuid = UUID}) ->
    {get_parent, #'GetParent'{uuid = UUID}};



% Replication
translate_to_protobuf(#synchronize_block{uuid = Uuid, block = Block}) ->
    {synchronize_block, #'SynchronizeBlock'{uuid = Uuid, block = translate_to_protobuf(Block)}};
translate_to_protobuf(#synchronize_block_and_compute_checksum{uuid = Uuid, block = Block}) ->
    {synchronize_block_and_compute_checksum,
        #'SynchronizeBlockAndComputeChecksum'{uuid = Uuid, block = translate_to_protobuf(Block)}};
translate_to_protobuf(#checksum{value = Value}) ->
    {checksum, #'Checksum'{value = Value}};

%% CDMI
translate_to_protobuf(#acl{value = Value}) ->
    {acl, #'Acl'{value = Value}};
translate_to_protobuf(#get_acl{uuid = UUID}) ->
    {get_acl, #'GetAcl'{uuid = UUID}};
translate_to_protobuf(#set_acl{uuid = UUID, acl = Acl}) ->
    {_, PAcl} = translate_to_protobuf(Acl),
    {set_acl, #'SetAcl'{uuid = UUID, acl = PAcl}};
translate_to_protobuf(#get_transfer_encoding{uuid = UUID}) ->
    {get_transfer_encoding, #'GetTransferEncoding'{uuid = UUID}};
translate_to_protobuf(#set_transfer_encoding{uuid = UUID, value = Value}) ->
    {set_transfer_encoding, #'SetTransferEncoding'{uuid = UUID, value = Value}};
translate_to_protobuf(#get_cdmi_completion_status{uuid = UUID}) ->
    {get_cdmi_completion_status, #'GetCdmiCompletionStatus'{uuid = UUID}};
translate_to_protobuf(#set_cdmi_completion_status{uuid = UUID, value = Value}) ->
    {set_cdmi_completion_status, #'SetCdmiCompletionStatus'{uuid = UUID, value = Value}};
translate_to_protobuf(#get_mimetype{uuid = UUID}) ->
    {get_mimetype, #'GetMimetype'{uuid = UUID}};
translate_to_protobuf(#set_mimetype{uuid = UUID, value = Value}) ->
    {set_mimetype, #'SetMimetype'{uuid = UUID, value = Value}};

translate_to_protobuf(#transfer_encoding{value = Value}) ->
    {transfer_encoding, #'TransferEncoding'{value = Value}};
translate_to_protobuf(#cdmi_completion_status{value = Value}) ->
    {cdmi_completion_status, #'CdmiCompletionStatus'{value = Value}};
translate_to_protobuf(#mimetype{value = Value}) ->
    {mimetype, #'Mimetype'{value = Value}};
translate_to_protobuf(#get_file_path{uuid = UUID}) ->
    {get_file_path, #'GetFilePath'{uuid = UUID}};
translate_to_protobuf(#file_path{value = Value}) ->
    {file_path, #'FilePath'{value = Value}};
translate_to_protobuf(#fsync{uuid = UUID}) ->
    {fsync, #'FSync'{uuid = UUID}};

translate_to_protobuf(undefined) ->
    undefined.

%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec open_flags_translate_to_protobuf(fslogic_worker:open_flags()) -> 'READ_WRITE' | 'READ' | 'WRITE'.
open_flags_translate_to_protobuf(undefined) ->
    'READ_WRITE';
open_flags_translate_to_protobuf(rdwr) ->
    'READ_WRITE';
open_flags_translate_to_protobuf(read) ->
    'READ';
open_flags_translate_to_protobuf(write) ->
    'WRITE'.


-spec open_flags_translate_from_protobuf('READ_WRITE' | 'READ' | 'WRITE') -> fslogic_worker:open_flags().
open_flags_translate_from_protobuf(undefined) ->
    rdwr;
open_flags_translate_from_protobuf('READ_WRITE') ->
    rdwr;
open_flags_translate_from_protobuf('READ') ->
    read;
open_flags_translate_from_protobuf('WRITE') ->
    write.