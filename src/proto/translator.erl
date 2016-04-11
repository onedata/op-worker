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
translate_from_protobuf(#'SubscriptionCancellation'{id = Id}) ->
    #subscription_cancellation{id = Id};
translate_from_protobuf(#'FileBlock'{offset = Off, size = S}) ->
    #file_block{offset = Off, size = S};
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
translate_from_protobuf(#'Token'{value = Val}) ->
    {ok, Macaroon} = macaroon:deserialize(Val),
    #auth{macaroon = Macaroon};
translate_from_protobuf(#'Ping'{data = Data}) ->
    #ping{data = Data};
translate_from_protobuf(#'GetProtocolVersion'{}) ->
    #get_protocol_version{};
translate_from_protobuf(#'FuseRequest'{fuse_request = {_, Record}}) ->
    #fuse_request{fuse_request = translate_from_protobuf(Record)};
translate_from_protobuf(#'GetFileAttr'{entry_type = 'PATH', entry = Path}) ->
    #get_file_attr{entry = {path, Path}};
translate_from_protobuf(#'GetFileAttr'{entry_type = 'UUID', entry = UUID}) ->
    #get_file_attr{entry = {uuid, UUID}};
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
    #get_new_file_location{name = Name, parent_uuid = ParentUUID, mode = Mode, flags = translate_open_flags(Flags)};
translate_from_protobuf(#'GetFileLocation'{uuid = UUID, flags = Flags}) ->
    #get_file_location{uuid = UUID, flags = translate_open_flags(Flags)};
translate_from_protobuf(#'GetHelperParams'{storage_id = SID, force_proxy_io = ForceProxy}) ->
    #get_helper_params{storage_id = SID, force_proxy_io = ForceProxy};
translate_from_protobuf(#'Truncate'{uuid = UUID, size = Size}) ->
    #truncate{uuid = UUID, size = Size};
translate_from_protobuf(#'Close'{uuid = UUID}) ->
    #close{uuid = UUID};
translate_from_protobuf(#'ProxyIORequest'{file_uuid = FileUUID, storage_id = SID, file_id = FID, proxyio_request = {_, Record}}) ->
    #proxyio_request{file_uuid = FileUUID, storage_id = SID, file_id = FID, proxyio_request = translate_from_protobuf(Record)};
translate_from_protobuf(#'RemoteRead'{offset = Offset, size = Size}) ->
    #remote_read{offset = Offset, size = Size};
translate_from_protobuf(#'RemoteWrite'{offset = Offset, data = Data}) ->
    #remote_write{offset = Offset, data = Data};
translate_from_protobuf(#'GetXattr'{uuid = UUID, name = Name}) ->
    #get_xattr{uuid = UUID, name = Name};
translate_from_protobuf(#'SetXattr'{uuid = UUID, xattr = {xattr, Xattr}}) ->
    #set_xattr{uuid = UUID, xattr = translate_from_protobuf(Xattr)};
translate_from_protobuf(#'RemoveXattr'{uuid = UUID, name = Name}) ->
    #remove_xattr{uuid = UUID, name = Name};
translate_from_protobuf(#'ListXattr'{uuid = UUID}) ->
    #list_xattr{uuid = UUID};
translate_from_protobuf(#'Xattr'{name = Name, value = Value}) ->
    #xattr{name = Name, value = Value};
translate_from_protobuf(#'XattrList'{names = Names}) ->
    #xattr_list{names = Names};
translate_from_protobuf(#'CreateStorageTestFile'{storage_id = Id, file_uuid = FileUuid}) ->
    #create_storage_test_file{storage_id = Id, file_uuid = FileUuid};
translate_from_protobuf(#'VerifyStorageTestFile'{storage_id = SId, space_uuid = SpaceUuid,
    file_id = FId, file_content = FContent}) ->
    #verify_storage_test_file{storage_id = SId, space_uuid = SpaceUuid,
        file_id = FId, file_content = FContent};

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
translate_from_protobuf(#'SynchronizeBlock'{uuid = Uuid, block = #'FileBlock'{offset = O, size = S}}) ->
    #synchronize_block{uuid = Uuid, block = #file_block{offset = O, size = S}};

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
translate_to_protobuf(#update_event{object = Type}) ->
    {update_event, #'UpdateEvent'{object = translate_to_protobuf(Type)}};
translate_to_protobuf(#permission_changed_event{file_uuid = FileUuid}) ->
    {permission_changed_event, #'PermissionChangedEvent'{file_uuid = FileUuid}};
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
translate_to_protobuf(#subscription_cancellation{id = Id}) ->
    {subscription_cancellation, #'SubscriptionCancellation'{id = Id}};
translate_to_protobuf(#handshake_response{session_id = Id}) ->
    {handshake_response, #'HandshakeResponse'{
        session_id = Id
    }};
translate_to_protobuf(#configuration{subscriptions = Subs}) ->
    {configuration, #'Configuration'{
        subscriptions = lists:map(fun(Sub) ->
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
translate_to_protobuf(#dir{}) ->
    undefined;
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
        space_id = Record#file_location.space_id,
        storage_id = Record#file_location.storage_id,
        file_id = Record#file_location.file_id,
        blocks = lists:map(fun(Block) ->
            translate_to_protobuf(Block)
        end, Record#file_location.blocks)
    }};
translate_to_protobuf(#file_block{offset = Off, size = S, file_id = FID, storage_id = SID}) ->
    #'FileBlock'{offset = Off, size = S, file_id = FID, storage_id = SID};
translate_to_protobuf(#proxyio_response{status = Status, proxyio_response = ProxyIOResponse}) ->
    {status, StatProto} = translate_to_protobuf(Status),
    {proxyio_response, #'ProxyIOResponse'{
        status = StatProto,
        proxyio_response = translate_to_protobuf(ProxyIOResponse)
    }};
translate_to_protobuf(#remote_data{data = Data}) ->
    {remote_data, #'RemoteData'{data = Data}};
translate_to_protobuf(#remote_write_result{wrote = Wrote}) ->
    {remote_write_result, #'RemoteWriteResult'{wrote = Wrote}};
translate_to_protobuf(#get_xattr{uuid = Uuid, name = Name}) ->
    {get_xattr, #'GetXattr'{uuid = Uuid, name = Name}};
translate_to_protobuf(#set_xattr{uuid = Uuid, xattr = Xattr}) ->
    {set_xattr, #'SetXattr'{uuid = Uuid, xattr = translate_to_protobuf(Xattr)}};
translate_to_protobuf(#remove_xattr{uuid = Uuid, name = Name}) ->
    {remove_xattr, #'RemoveXattr'{uuid = Uuid, name = Name}};
translate_to_protobuf(#list_xattr{uuid = Uuid}) ->
    {list_xattr, #'ListXattr'{uuid = Uuid}};
translate_to_protobuf(#xattr{name = Name, value = Value}) ->
    {xattr, #'Xattr'{name = Name, value = Value}};
translate_to_protobuf(#xattr_list{names = Names}) ->
    {xattr_list, #'XattrList'{names = Names}};
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
translate_to_protobuf(undefined) ->
    undefined.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Translates open flags for get[_new]_file_location requests.
%% @end
%%--------------------------------------------------------------------
-spec translate_open_flags('READ_WRITE' | 'READ' | 'WRITE') -> fslogic_worker:open_flags().
translate_open_flags('READ_WRITE') ->
    rdwr;
translate_open_flags('READ') ->
    read;
translate_open_flags('WRITE') ->
    write;
translate_open_flags(undefined) ->
    rdwr.
