%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
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

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include("proto/common/handshake_messages.hrl").
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
-include("modules/fslogic/data_distribution.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").

%% API
-export([
    translate_handshake_error/1,
    session_mode_translate_to_protobuf/1, session_mode_translate_from_protobuf/1,
    translate_from_protobuf/1, translate_to_protobuf/1
]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Translates handshake error type from REST to protobuf format.
%% @end
%%--------------------------------------------------------------------
-spec translate_handshake_error(Type :: binary()) -> Type :: atom().
translate_handshake_error(<<"token_expired">>) ->
    'MACAROON_EXPIRED';
translate_handshake_error(<<"token_not_found">>) ->
    'MACAROON_NOT_FOUND';
translate_handshake_error(<<"invalid_token">>) ->
    'INVALID_MACAROON';
translate_handshake_error(<<"invalid_method">>) ->
    'INVALID_METHOD';
translate_handshake_error(<<"root_resource_not_found">>) ->
    'ROOT_RESOURCE_NOT_FOUND';
translate_handshake_error(<<"invalid_provider">>) ->
    'INVALID_PROVIDER';
translate_handshake_error(<<"bad_signature_for_macaroon">>) ->
    'BAD_SIGNATURE_FOR_MACAROON';
translate_handshake_error(<<"failed_to_decrypt_caveat">>) ->
    'FAILED_TO_DESCRYPT_CAVEAT';
translate_handshake_error(<<"no_discharge_macaroon_for_caveat">>) ->
    'NO_DISCHARGE_MACAROON_FOR_CAVEAT';
translate_handshake_error(_) ->
    'INTERNAL_SERVER_ERROR'.


-spec session_mode_translate_to_protobuf(undefined | session:mode()) ->
    'NORMAL' | 'OPEN_HANDLE'.
session_mode_translate_to_protobuf(open_handle) -> 'OPEN_HANDLE';
session_mode_translate_to_protobuf(_) -> 'NORMAL'.


-spec session_mode_translate_from_protobuf(undefined | 'NORMAL' | 'OPEN_HANDLE') ->
    session:mode().
session_mode_translate_from_protobuf('OPEN_HANDLE') -> open_handle;
session_mode_translate_from_protobuf(_) -> normal.


%%--------------------------------------------------------------------
%% @doc
%% Translates protobuf record to internal record.
%% @end
%%--------------------------------------------------------------------
-spec translate_from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.

%% COMMON
translate_from_protobuf(#'Status'{
    code = Code,
    description = Desc
}) ->
    #status{
        code = Code,
        description = Desc
    };
translate_from_protobuf(#'FileBlock'{
    offset = Offset,
    size = Size
}) ->
    #file_block{
        offset = Offset,
        size = Size
    };
translate_from_protobuf(#'FileRenamedEntry'{
    old_uuid = OldGuid,
    new_uuid = NewGuid,
    new_parent_uuid = NewParentGuid,
    new_name = NewName
}) ->
    #file_renamed_entry{
        old_guid = OldGuid,
        new_guid = NewGuid,
        new_parent_guid = NewParentGuid,
        new_name = NewName
    };
translate_from_protobuf(#'IpAndPort'{
    ip = IpString,
    port = Port
}) ->
    {ok, IP} = inet:parse_ipv4strict_address(binary_to_list(IpString)),
    #ip_and_port{ip = IP, port = Port};
translate_from_protobuf(#'Dir'{uuid = UUID}) ->
    #dir{guid = UUID};


%% EVENT
translate_from_protobuf(#'Event'{type = {_, Record}}) ->
    #event{type = translate_from_protobuf(Record)};
translate_from_protobuf(#'Events'{events = Evts}) ->
    #events{events = [translate_from_protobuf(Evt) || Evt <- Evts]};
translate_from_protobuf(#'FlushEvents'{
    provider_id = ProviderId,
    subscription_id = SubscriptionId,
    context = Ctx
}) ->
    #flush_events{
        provider_id = ProviderId,
        subscription_id = integer_to_binary(SubscriptionId),
        context = Ctx
    };
translate_from_protobuf(#'FileReadEvent'{
    counter = Counter,
    file_uuid = FileGuid,
    size = Size,
    blocks = Blocks
}) ->
    #file_read_event{
        counter = Counter,
        file_guid = FileGuid,
        size = Size,
        blocks = [translate_from_protobuf(B) || B <- Blocks]
    };
translate_from_protobuf(#'FileWrittenEvent'{
    counter = Counter,
    file_uuid = FileGuid,
    size = Size,
    file_size = FileSize,
    blocks = Blocks
}) ->
    #file_written_event{
        counter = Counter,
        file_guid = FileGuid,
        size = Size,
        file_size = FileSize,
        blocks = [translate_from_protobuf(B) || B <- Blocks]
    };
translate_from_protobuf(#'FileAttrChangedEvent'{file_attr = FileAttr}) ->
    #file_attr_changed_event{
        file_attr = translate_from_protobuf(FileAttr)
    };
translate_from_protobuf(#'FileLocationChangedEvent'{
    file_location = FileLocation,
    change_beg_offset = O,
    change_end_offset = S
}) ->
    #file_location_changed_event{
        file_location = translate_from_protobuf(FileLocation),
        change_beg_offset = O,
        change_end_offset = S
    };
translate_from_protobuf(#'FilePermChangedEvent'{
    file_uuid = FileGuid
}) ->
    #file_perm_changed_event{
        file_guid = FileGuid
    };
translate_from_protobuf(#'FileRemovedEvent'{file_uuid = FileGuid}) ->
    #file_removed_event{file_guid = FileGuid};
translate_from_protobuf(#'FileRenamedEvent'{
    top_entry = TopEntry,
    child_entries = ChildEntries
}) ->
    #file_renamed_event{
        top_entry = translate_from_protobuf(TopEntry),
        child_entries = [translate_from_protobuf(E) || E <- ChildEntries]
    };
translate_from_protobuf(#'QuotaExceededEvent'{spaces = Spaces}) ->
    #quota_exceeded_event{spaces = Spaces};
translate_from_protobuf(#'HelperParamsChangedEvent'{storage_id = StorageId}) ->
    #helper_params_changed_event{storage_id = StorageId};


%% SUBSCRIPTION
translate_from_protobuf(#'Subscription'{
    id = Id,
    type = {_, Record}
}) ->
    #subscription{
        id = integer_to_binary(Id),
        type = translate_from_protobuf(Record)
    };
translate_from_protobuf(#'FileReadSubscription'{
    counter_threshold = CounterThreshold,
    time_threshold = TimeThreshold
}) ->
    #file_read_subscription{
        counter_threshold = CounterThreshold,
        time_threshold = TimeThreshold
    };
translate_from_protobuf(#'FileWrittenSubscription'{
    counter_threshold = CounterThreshold,
    time_threshold = TimeThreshold
}) ->
    #file_written_subscription{
        counter_threshold = CounterThreshold,
        time_threshold = TimeThreshold
    };
translate_from_protobuf(#'FileAttrChangedSubscription'{
    file_uuid = FileGuid,
    time_threshold = TimeThreshold
}) ->
    #file_attr_changed_subscription{
        file_guid = FileGuid,
        time_threshold = TimeThreshold
    };
translate_from_protobuf(#'ReplicaStatusChangedSubscription'{
    file_uuid = FileGuid,
    time_threshold = TimeThreshold
}) ->
    #replica_status_changed_subscription{
        file_guid = FileGuid,
        time_threshold = TimeThreshold
    };
translate_from_protobuf(#'FileLocationChangedSubscription'{
    file_uuid = FileGuid,
    time_threshold = TimeThreshold
}) ->
    #file_location_changed_subscription{
        file_guid = FileGuid,
        time_threshold = TimeThreshold
    };
translate_from_protobuf(#'FilePermChangedSubscription'{
    file_uuid = FileGuid
}) ->
    #file_perm_changed_subscription{
        file_guid = FileGuid
    };
translate_from_protobuf(#'FileRemovedSubscription'{
    file_uuid = FileGuid
}) ->
    #file_removed_subscription{
        file_guid = FileGuid
    };
translate_from_protobuf(#'FileRenamedSubscription'{
    file_uuid = FileGuid
}) ->
    #file_renamed_subscription{
        file_guid = FileGuid
    };
translate_from_protobuf(#'QuotaExceededSubscription'{}) ->
    #quota_exceeded_subscription{};
translate_from_protobuf(#'HelperParamsChangedSubscription'{
    storage_id = StorageId
}) ->
    #helper_params_changed_subscription{
        storage_id = StorageId
    };
translate_from_protobuf(#'SubscriptionCancellation'{id = Id}) ->
    #subscription_cancellation{id = integer_to_binary(Id)};


%% HANDSHAKE
translate_from_protobuf(#'ClientHandshakeRequest'{
    macaroon = Macaroon,
    session_id = Nonce,
    version = Version,
    compatible_oneprovider_versions = CompOpVersions,
    session_mode = SessionMode
}) ->
    #client_handshake_request{
        client_tokens = translate_from_protobuf(Macaroon),
        nonce = Nonce,
        version = Version,
        compatible_oneprovider_versions = CompOpVersions,
        session_mode = session_mode_translate_from_protobuf(SessionMode)
    };
translate_from_protobuf(#'ProviderHandshakeRequest'{
    provider_id = ProviderId,
    token = Token
}) ->
    #provider_handshake_request{
        provider_id = ProviderId,
        token = Token
    };
translate_from_protobuf(#'Macaroon'{
    macaroon = AccessToken
}) ->
    #client_tokens{access_token = AccessToken};
translate_from_protobuf(#'HandshakeResponse'{status = Status}) ->
    #handshake_response{status = Status};


% PROCESSING STATUS
translate_from_protobuf(#'ProcessingStatus'{code = Code}) ->
    #processing_status{code = Code};


%% DIAGNOSTIC
translate_from_protobuf(#'Ping'{data = Data}) ->
    #ping{data = Data};
translate_from_protobuf(#'GetProtocolVersion'{}) ->
    #get_protocol_version{};
translate_from_protobuf(#'ProtocolVersion'{
    major = Major,
    minor = Minor
}) ->
    #protocol_version{
        major = Major,
        minor = Minor
    };
translate_from_protobuf(#'GetConfiguration'{}) ->
    #get_configuration{};


%% STREAM
translate_from_protobuf(#'MessageStream'{
    stream_id = StmId,
    sequence_number = SeqNum
}) ->
    #message_stream{
        stream_id = StmId,
        sequence_number = SeqNum
    };
translate_from_protobuf(#'MessageRequest'{
    stream_id = StmId,
    lower_sequence_number = LowerSeqNum,
    upper_sequence_number = UpperSeqNum
}) ->
    #message_request{
        stream_id = StmId,
        lower_sequence_number = LowerSeqNum,
        upper_sequence_number = UpperSeqNum
    };
translate_from_protobuf(#'MessageAcknowledgement'{
    stream_id = StmId,
    sequence_number = SeqNum
}) ->
    #message_acknowledgement{
        stream_id = StmId,
        sequence_number = SeqNum
    };
translate_from_protobuf(#'MessageStreamReset'{stream_id = StmId}) ->
    #message_stream_reset{stream_id = StmId};
translate_from_protobuf(#'EndOfMessageStream'{}) ->
    #end_of_message_stream{};


%% FUSE
translate_from_protobuf(#'FuseRequest'{
    fuse_request = {_, Record}
}) ->
    #fuse_request{
        fuse_request = translate_from_protobuf(Record)
    };
translate_from_protobuf(#'ResolveGuid'{
    path = Path
}) ->
    #resolve_guid{
        path = Path
    };
translate_from_protobuf(#'GetHelperParams'{
    storage_id = StorageId,
    space_id = SpaceId,
    helper_mode = HelperMode
}) ->
    #get_helper_params{
        storage_id = StorageId,
        space_id = SpaceId,
        helper_mode = HelperMode
    };
translate_from_protobuf(#'GetFSStats'{
    file_id = FileGuid
}) ->
    #get_fs_stats{
        file_id = FileGuid
    };
translate_from_protobuf(#'ListFilesRecursively'{
    token = Token,
    start_after = StartAfter,
    prefix = Prefix,
    limit = Limit,
    include_dirs = IncludeDirs,
    xattrs = Xattrs
}) ->
    #get_recursive_file_list{
        optional_attrs = [size | xattrs_to_optional_attrs(Xattrs)],
        listing_options = maps_utils:remove_undefined(#{
            pagination_token => Token,
            start_after => StartAfter,
            prefix => Prefix,
            limit => Limit,
            include_directories => IncludeDirs
        })
    };
translate_from_protobuf(#'CreateStorageTestFile'{
    storage_id = Id,
    file_uuid = FileGuid
}) ->
    #create_storage_test_file{
        storage_id = Id,
        file_guid = FileGuid
    };
translate_from_protobuf(#'VerifyStorageTestFile'{
    storage_id = SId,
    space_id = SpaceId,
    file_id = FId,
    file_content = FContent
}) ->
    #verify_storage_test_file{
        storage_id = SId,
        space_id = SpaceId,
        file_id = FId,
        file_content = FContent
    };
translate_from_protobuf(#'FileRequest'{
    context_guid = ContextGuid,
    file_request = {_, Record}
}) ->
    #file_request{
        context_guid = ContextGuid,
        file_request = translate_from_protobuf(Record)
    };
translate_from_protobuf(#'GetFileAttr'{include_replication_status = IRS, include_link_count = ILC, xattrs = Xattrs}) ->
    #get_file_attr{optional_attrs = [size | attrs_flags_to_optional_attrs(IRS, ILC)] ++ xattrs_to_optional_attrs(Xattrs)};
translate_from_protobuf(#'GetFileAttrByPath'{path = Path, xattrs = Xattrs}) ->
    #get_file_attr_by_path{path = Path, optional_attrs = [size | xattrs_to_optional_attrs(Xattrs)]};
translate_from_protobuf(#'GetChildAttr'{
    name = Name,
    include_replication_status = IRS,
    include_link_count = ILC,
    xattrs = Xattrs
}) ->
    #get_child_attr{
        name = Name,
        optional_attrs = [size | attrs_flags_to_optional_attrs(IRS, ILC)] ++ xattrs_to_optional_attrs(Xattrs)
    };
translate_from_protobuf(#'GetFileChildren'{
    offset = Offset,
    size = Size,
    index_token = Token,
    index_startid = StartId
}) ->
    BaseListingOpts = case Token of
        undefined ->
            #{
                offset => Offset,
                index => StartId,
                tune_for_large_continuous_listing => true
            };
        _ ->
            #{
                pagination_token => file_listing:decode_pagination_token(Token)
            }
    end,
    #get_file_children{
        listing_options = maps_utils:remove_undefined(BaseListingOpts#{
            limit => Size
        })
    };
translate_from_protobuf(#'GetFileChildrenAttrs'{
    offset = Offset,
    size = Size,
    index_token = Token,
    include_replication_status = IRS,
    include_link_count = ILC,
    xattrs = Xattrs
}) ->
    BaseListingOpts = case Token of
        undefined ->
            #{
                offset => Offset,
                tune_for_large_continuous_listing => true
            };
        _ ->
            #{
                pagination_token => file_listing:decode_pagination_token(Token)
            }
    end,
    #get_file_children_attrs{
        listing_options = maps_utils:remove_undefined(BaseListingOpts#{
            limit => Size
        }),
        optional_attrs = [size | attrs_flags_to_optional_attrs(IRS, ILC)] ++ xattrs_to_optional_attrs(Xattrs)
    };
translate_from_protobuf(#'CreateDir'{
    name = Name,
    mode = Mode
}) ->
    #create_dir{
        name = Name,
        mode = Mode
    };
translate_from_protobuf(#'CreatePath'{
    path = Path
}) ->
    #create_path{
        path = Path
    };
translate_from_protobuf(#'DeleteFile'{
    silent = Silent
}) ->
    #delete_file{
        silent = Silent
    };
translate_from_protobuf(#'UpdateTimes'{
    atime = ATime,
    mtime = MTime,
    ctime = CTime
}) ->
    #update_times{
        atime = ATime,
        mtime = MTime,
        ctime = CTime
    };
translate_from_protobuf(#'ChangeMode'{mode = Mode}) ->
    #change_mode{mode = Mode};
translate_from_protobuf(#'Rename'{
    target_parent_uuid = TargetParentGuid,
    target_name = TargetName
}) ->
    #rename{
        target_parent_guid = TargetParentGuid,
        target_name = TargetName
    };
translate_from_protobuf(#'CreateFile'{
    name = Name,
    mode = Mode,
    flag = Flag
}) ->
    #create_file{
        name = Name,
        mode = Mode,
        flag = open_flag_translate_from_protobuf(Flag)
    };
translate_from_protobuf(#'StorageFileCreated'{}) ->
    #storage_file_created{};
translate_from_protobuf(#'MakeFile'{
    name = Name,
    mode = Mode
}) ->
    #make_file{
        name = Name,
        mode = Mode
    };
translate_from_protobuf(#'MakeLink'{
    target_parent_uuid = TargetParentGuid,
    target_name = Name
}) ->
    #make_link{
        target_parent_guid = TargetParentGuid,
        target_name = Name
    };
translate_from_protobuf(#'MakeSymlink'{
    target_name = TargetName,
    link = Link
}) ->
    #make_symlink{
        target_name = TargetName,
        link = Link
    };
translate_from_protobuf(#'OpenFile'{flag = Flag}) ->
    #open_file{flag = open_flag_translate_from_protobuf(Flag)};
translate_from_protobuf(#'OpenFileWithExtendedInfo'{flag = Flag}) ->
    #open_file_with_extended_info{
        flag = open_flag_translate_from_protobuf(Flag)
    };
translate_from_protobuf(#'GetFileLocation'{}) ->
    #get_file_location{};
translate_from_protobuf(#'ReadSymlink'{}) ->
    #read_symlink{};
translate_from_protobuf(#'Release'{handle_id = HandleId}) ->
    #release{handle_id = HandleId};
translate_from_protobuf(#'Truncate'{size = Size}) ->
    #truncate{size = Size};
translate_from_protobuf(#'SynchronizeBlock'{
    block = #'FileBlock'{
        offset = O,
        size = S
    },
    prefetch = Prefetch,
    priority = Priority
}) ->
    #synchronize_block{
        block = #file_block{offset = O, size = S},
        prefetch = Prefetch,
        priority = Priority
    };
translate_from_protobuf(#'SynchronizeBlockAndComputeChecksum'{
    block = #'FileBlock'{
        offset = O,
        size = S
    },
    prefetch = Prefetch,
    priority = Priority
}) ->
    #synchronize_block_and_compute_checksum{
        block = #file_block{offset = O, size = S},
        prefetch = Prefetch,
        priority = Priority
    };
translate_from_protobuf(#'BlockSynchronizationRequest'{
    block = #'FileBlock'{
        offset = O,
        size = S
    },
    prefetch = Prefetch,
    priority = Priority
}) ->
    #block_synchronization_request{
        block = #file_block{offset = O, size = S},
        prefetch = Prefetch,
        priority = Priority
    };
translate_from_protobuf(#'FuseResponse'{
    status = Status,
    fuse_response = {_, FuseResponse}
}) ->
    #fuse_response{
        status = translate_from_protobuf(Status),
        fuse_response = translate_from_protobuf(FuseResponse)
    };
translate_from_protobuf(#'FuseResponse'{status = Status}) ->
    #fuse_response{
        status = translate_from_protobuf(Status),
        fuse_response = undefined
    };
translate_from_protobuf(#'ChildLink'{
    uuid = FileGuid,
    name = Name
}) ->
    #child_link{
        guid = FileGuid,
        name = Name
    };
translate_from_protobuf(#'FileAttr'{} = FileAttr) ->
    Xattrs = lists:foldl(fun(Xattr, Acc) ->
        #xattr{name = Name, value = Value} = translate_from_protobuf(Xattr),
        Acc#{Name => Value}
    end, #{}, FileAttr#'FileAttr'.xattrs),
    #file_attr{
        guid = FileAttr#'FileAttr'.uuid,
        name = FileAttr#'FileAttr'.name,
        mode = FileAttr#'FileAttr'.mode,
        parent_guid = FileAttr#'FileAttr'.parent_uuid,
        uid = FileAttr#'FileAttr'.uid,
        gid = FileAttr#'FileAttr'.gid,
        atime = FileAttr#'FileAttr'.atime,
        mtime = FileAttr#'FileAttr'.mtime,
        ctime = FileAttr#'FileAttr'.ctime,
        type = FileAttr#'FileAttr'.type,
        size = FileAttr#'FileAttr'.size,
        provider_id = FileAttr#'FileAttr'.provider_id,
        shares = FileAttr#'FileAttr'.shares,
        owner_id = FileAttr#'FileAttr'.owner_id,
        fully_replicated = FileAttr#'FileAttr'.fully_replicated,
        nlink = FileAttr#'FileAttr'.nlink,
        index = file_listing:decode_index(FileAttr#'FileAttr'.index),
        xattrs = Xattrs
    };
translate_from_protobuf(#'FileChildren'{
    child_links = FileEntries,
    index_token = Token,
    is_last = _IsLast
}) ->
    #file_children{
        child_links = [translate_from_protobuf(E) || E <- FileEntries],
        pagination_token = file_listing:decode_pagination_token(Token)
    };
translate_from_protobuf(#'FileChildrenAttrs'{
    child_attrs = Children,
    index_token = Token,
    is_last = _IsLast
}) ->
    #file_children_attrs{
        child_attrs = [translate_from_protobuf(E) || E <- Children],
        pagination_token = file_listing:decode_pagination_token(Token)
    };
translate_from_protobuf(#'FileLocation'{} = Record) ->
    #file_location{
        uuid = file_id:guid_to_uuid(Record#'FileLocation'.uuid),
        provider_id = Record#'FileLocation'.provider_id,
        space_id = Record#'FileLocation'.space_id,
        storage_id = Record#'FileLocation'.storage_id,
        file_id = Record#'FileLocation'.file_id,
        blocks = lists:map(
            fun(#'FileBlock'{offset = Offset, size = Size}) ->
                #file_block{offset = Offset, size = Size}
            end, Record#'FileLocation'.blocks)
    };
translate_from_protobuf(#'FileLocationChanged'{
    file_location = FileLocation,
    change_beg_offset = O,
    change_end_offset = S
}) ->
    #file_location_changed{
        file_location = translate_from_protobuf(FileLocation),
        change_beg_offset = O,
        change_end_offset = S
    };
translate_from_protobuf(#'HelperParams'{
    helper_name = HelperName,
    helper_args = HelpersArgs
}) ->
    #helper_params{
        helper_name = HelperName,
        helper_args = [translate_from_protobuf(Arg) || Arg <- HelpersArgs]
    };
translate_from_protobuf(#'HelperArg'{
    key = Key,
    value = Value
}) ->
    #helper_arg{
        key = Key,
        value = Value
    };
translate_from_protobuf(#'FSStats'{
    space_id = SpaceId,
    storage_stats = StorageStats
}) ->
    #fs_stats{
        space_id = SpaceId,
        storage_stats = [translate_from_protobuf(Arg) || Arg <- StorageStats]
    };
translate_from_protobuf(#'StorageStats'{
    storage_id = StorageId,
    size = Size,
    occupied = Occupied
}) ->
    #storage_stats{
        storage_id = StorageId,
        size = Size,
        occupied = Occupied
    };
translate_from_protobuf(#'Parameter'{
    key = Key,
    value = Value
}) ->
    {Key, Value};
translate_from_protobuf(#'SyncResponse'{
    checksum = Checksum,
    file_location_changed = FileLocationChanged
}) ->
    #sync_response{
        checksum = Checksum,
        file_location_changed = translate_from_protobuf(FileLocationChanged)
    };
translate_from_protobuf(#'FileCreated'{
    handle_id = HandleId,
    file_attr = FileAttr,
    file_location = FileLocation
}) ->
    #file_created{
        handle_id = HandleId,
        file_attr = translate_from_protobuf(FileAttr),
        file_location = translate_from_protobuf(FileLocation)
    };
translate_from_protobuf(#'FileOpened'{
    handle_id = HandleId
}) ->
    #file_opened{
        handle_id = HandleId
    };
translate_from_protobuf(#'Symlink'{
    link = Link
}) ->
    #symlink{
        link = Link
    };
translate_from_protobuf(#'FileOpenedExtended'{
    handle_id = HandleId,
    provider_id = ProviderId,
    file_id = FileId,
    storage_id = StorageId
}) ->
    #file_opened_extended{
        handle_id = HandleId,
        provider_id = ProviderId,
        file_id = FileId,
        storage_id = StorageId
    };
translate_from_protobuf(#'FileRenamed'{
    new_uuid = NewGuid,
    child_entries = ChildEntries
}) ->
    #file_renamed{
        new_guid = NewGuid,
        child_entries = [translate_from_protobuf(E) || E <- ChildEntries]
    };
translate_from_protobuf(#'Uuid'{uuid = Guid}) ->
    #guid{
        guid = Guid
    };
translate_from_protobuf(#'ReportFileWritten'{offset = Offset, size = Size}) ->
    #report_file_written{
        offset = Offset,
        size = Size
    };
translate_from_protobuf(#'ReportFileRead'{offset = Offset, size = Size}) ->
    #report_file_read{
        offset = Offset,
        size = Size
    };


%% PROXYIO
translate_from_protobuf(#'ProxyIORequest'{
    parameters = ProtoParameters,
    storage_id = SID,
    file_id = FID,
    proxyio_request = {_, Record}
}) ->
    Parameters = [translate_from_protobuf(P) || P <- ProtoParameters],
    #proxyio_request{
        parameters = maps:from_list(Parameters),
        storage_id = SID,
        file_id = FID,
        proxyio_request = translate_from_protobuf(Record)
    };
translate_from_protobuf(#'RemoteRead'{
    offset = Offset,
    size = Size
}) ->
    #remote_read{
        offset = Offset,
        size = Size
    };
translate_from_protobuf(#'RemoteWrite'{byte_sequence = ByteSequences}) ->
    #remote_write{
        byte_sequence = [translate_from_protobuf(BS) || BS <- ByteSequences]
    };
translate_from_protobuf(#'ByteSequence'{offset = Offset, data = Data}) ->
    #byte_sequence{offset = Offset, data = Data};

translate_from_protobuf(#'ProxyIOResponse'{
    status = Status,
    proxyio_response = {_, ProxyIOResponse}
}) ->
    #proxyio_response{
        status = translate_from_protobuf(Status),
        proxyio_response = translate_from_protobuf(ProxyIOResponse)
    };
translate_from_protobuf(#'ProxyIOResponse'{status = Status}) ->
    #proxyio_response{
        status = translate_from_protobuf(Status),
        proxyio_response = undefined
    };
translate_from_protobuf(#'RemoteData'{data = Data}) ->
    #remote_data{data = Data};
translate_from_protobuf(#'RemoteWriteResult'{wrote = Wrote}) ->
    #remote_write_result{wrote = Wrote};

%% Provider RPC
translate_from_protobuf(#'ProviderRpcCall'{
    file_guid = FileGuid,
    operation = {_, Request}
}) ->
    #provider_rpc_call{
        file_guid = FileGuid,
        request = translate_from_protobuf(Request)
    };
translate_from_protobuf(#'ProviderHistoricalDirSizeStatsBrowseRequest'{
    request = {_, Request}
}) ->
    #provider_historical_dir_size_stats_browse_request{
        request = translate_from_protobuf(Request)
    };
translate_from_protobuf(#'TimeSeriesLayoutGetRequest'{}) ->
    #time_series_layout_get_request{};
translate_from_protobuf(#'TimeSeriesSliceGetRequest'{
    layout_as_json = EncodedLayout,
    start_timestamp = StartTimestamp,
    window_limit = WindowLimit,
    stop_timestamp = StopTimestamp
}) ->
    #time_series_slice_get_request{
        layout = json_utils:decode(EncodedLayout),
        start_timestamp = StartTimestamp,
        window_limit = WindowLimit,
        stop_timestamp = StopTimestamp
    };
translate_from_protobuf(#'ProviderCurrentDirSizeStatsBrowseRequest'{
    stat_names = StatNames
}) ->
    #provider_current_dir_size_stats_browse_request{
        stat_names = StatNames
    };
translate_from_protobuf(#'ProviderRegDistributionGetRequest'{}) ->
    #provider_reg_distribution_get_request{};
translate_from_protobuf(#'ProviderRegStorageLocationsGetRequest'{}) ->
    #provider_reg_storage_locations_get_request{};
translate_from_protobuf(#'ProviderQosStatusGetRequest'{qos_entry_id = QosEntryId}) ->
    #provider_qos_status_get_request{
        qos_entry_id = QosEntryId
    };

translate_from_protobuf(#'ProviderRpcResponse'{
    status = ok,
    result = {_, Result}
}) ->
    #provider_rpc_response{
        status = ok,
        result = translate_from_protobuf(Result)
    };
translate_from_protobuf(#'ProviderRpcResponse'{
    status = error,
    result = {error_json, ErrorAsJson}
}) ->
    #provider_rpc_response{
        status = error,
        result = errors:from_json(json_utils:decode(ErrorAsJson))
};
translate_from_protobuf(#'ProviderCurrentDirSizeStatsBrowseResult'{
    stats_as_json = StatsAsJson
}) ->
    #provider_current_dir_size_stats_browse_result{
        stats = json_utils:decode(StatsAsJson)
    };
translate_from_protobuf(#'TimeSeriesLayoutGetResult'{
    layout_as_json = LayoutAsJson
}) ->
    #time_series_layout_get_result{
        layout = json_utils:decode(LayoutAsJson)
    };
translate_from_protobuf(#'TimeSeriesSliceGetResult'{
    slice_as_json = EncodedSliceGetResult
}) ->
    ts_browse_result:from_json(json_utils:decode(EncodedSliceGetResult));
translate_from_protobuf(#'ProviderRegDistributionGetResult'{
    logical_size = LogicalSize,
    distribution_per_storage = DistributionPerStorage
}) ->
    {BlocksPerStorage, LocationsPerStorage} = lists:foldl(
        fun(#'StorageRegDistributionGetResult'{
            storage_id = StorageId,
            blocks = Blocks,
            location = Location
        }, {BlocksAcc, LocationsAcc}) ->
            {
                BlocksAcc#{StorageId => lists:map(fun(Block) -> translate_from_protobuf(Block) end, Blocks)},
                LocationsAcc#{StorageId => Location}
            }
        end,
    {#{}, #{}}, DistributionPerStorage),
    #provider_reg_distribution_get_result{
        logical_size = LogicalSize,
        blocks_per_storage = BlocksPerStorage,
        locations_per_storage = LocationsPerStorage
    };
translate_from_protobuf(#'ProviderRegStorageLocationsResult'{
    locations = Locations
}) ->
    #provider_reg_storage_locations_result{
        locations_per_storage = maps_utils:generate_from_list(
            fun(#'StorageLocation'{storage_id = StorageId, location = Location}) ->
                {StorageId, Location}
            end, Locations)
    };
translate_from_protobuf(#'ProviderQosStatusGetResult'{
    status = Status
}) ->
    #provider_qos_status_get_result{
        status = Status
    };


%% PROVIDER
translate_from_protobuf(#'ProviderRequest'{
    context_guid = ContextGuid,
    provider_request = {_, Record}
}) ->
    #provider_request{
        context_guid = ContextGuid,
        provider_request = translate_from_protobuf(Record)
    };
translate_from_protobuf(#'GetXattr'{
    name = Name,
    inherited = Inherited
}) ->
    #get_xattr{
        name = Name,
        inherited = Inherited
    };
translate_from_protobuf(#'SetXattr'{
    xattr = Xattr,
    create = Create,
    replace = Replace
}) ->
    #set_xattr{
        xattr = translate_from_protobuf(Xattr),
        create = Create,
        replace = Replace
    };
translate_from_protobuf(#'RemoveXattr'{name = Name}) ->
    #remove_xattr{name = Name};
translate_from_protobuf(#'ListXattr'{
    inherited = Inherited,
    show_internal = ShowInternal
}) ->
    #list_xattr{
        inherited = Inherited,
        show_internal = ShowInternal
    };
translate_from_protobuf(#'GetParent'{}) ->
    #get_parent{};
translate_from_protobuf(#'GetAcl'{}) ->
    #get_acl{};
translate_from_protobuf(#'SetAcl'{acl = Acl}) ->
    #set_acl{acl = translate_from_protobuf(Acl)};
translate_from_protobuf(#'GetFilePath'{}) ->
    #get_file_path{};
translate_from_protobuf(#'FSync'{
    data_only = DataOnly,
    handle_id = HandleId
}) ->
    #fsync{
        data_only = DataOnly,
        handle_id = HandleId
    };
translate_from_protobuf(#'ProviderResponse'{
    status = Status,
    provider_response = {_, ProviderResponse}
}) ->
    #provider_response{
        status = translate_from_protobuf(Status),
        provider_response = translate_from_protobuf(ProviderResponse)
    };
translate_from_protobuf(#'ProviderResponse'{status = Status}) ->
    #provider_response{
        status = translate_from_protobuf(Status)
    };
translate_from_protobuf(#'Xattr'{
    name = Name,
    value = Value
}) ->
    #xattr{
        name = Name,
        value = json_utils:decode(Value)
    };
translate_from_protobuf(#'XattrList'{names = Names}) ->
    #xattr_list{names = Names};
translate_from_protobuf(#'Acl'{value = Value}) ->
    #acl{value = acl:from_json(json_utils:decode(Value), cdmi)};
translate_from_protobuf(#'FilePath'{value = Value}) ->
    #file_path{value = Value};
translate_from_protobuf(#'CheckPerms'{flag = Flag}) ->
    #check_perms{flag = open_flag_translate_from_protobuf(Flag)};


%% DBSYNC
translate_from_protobuf(#'DBSyncRequest'{
    message_body = {_, MessageBody}
}) ->
    #dbsync_request{
        message_body = translate_from_protobuf(MessageBody)
    };
translate_from_protobuf(#'TreeBroadcast'{
    message_body = {_, MessageBody},
    depth = Depth,
    excluded_providers = ExcludedProv,
    l_edge = LEdge,
    r_edge = REgde,
    request_id = ReqId,
    space_id = SpaceId
}) ->
    #tree_broadcast{
        message_body = translate_from_protobuf(MessageBody),
        depth = Depth,
        l_edge = LEdge,
        r_edge = REgde,
        space_id = SpaceId,
        request_id = ReqId,
        excluded_providers = ExcludedProv
    };
translate_from_protobuf(#'BatchUpdate'{
    space_id = SpaceId,
    since_seq = Since,
    until_seq = Until,
    changes_encoded = Changes
}) ->
    #batch_update{
        space_id = SpaceId,
        since_seq = Since,
        until_seq = Until,
        changes_encoded = Changes
    };
translate_from_protobuf(#'StatusReport'{
    space_id = SpaceId,
    seq_num = SeqNum
}) ->
    #status_report{
        space_id = SpaceId,
        seq = SeqNum
    };
translate_from_protobuf(#'StatusRequest'{}) ->
    #status_request{};
translate_from_protobuf(#'ChangesRequest'{
    since_seq = Since,
    until_seq = Until
}) ->
    #changes_request{
        since_seq = Since,
        until_seq = Until
    };
translate_from_protobuf(#'DBSyncMessage'{
    message_body = {_, MB}
}) ->
    #dbsync_message{
        message_body = translate_from_protobuf(MB)
    };
translate_from_protobuf(#'TreeBroadcast2'{
    src_provider_id = SrcProviderId,
    low_provider_id = LowProviderId,
    high_provider_id = HighProviderId,
    message_id = MsgId,
    changes_batch = CB
}) ->
    #tree_broadcast2{
        src_provider_id = SrcProviderId,
        low_provider_id = LowProviderId,
        high_provider_id = HighProviderId,
        message_id = MsgId,
        message_body = translate_from_protobuf(CB)
    };
translate_from_protobuf(#'ChangesBatch'{
    space_id = SpaceId,
    since = Since,
    until = Until,
    timestamp = Timestamp,
    compressed = Compressed,
    docs = Docs
}) ->
    Timestamp2 = case Timestamp of
        0 -> undefined;
        _ -> Timestamp
    end,
    #changes_batch{
        space_id = SpaceId,
        since = Since,
        until = Until,
        timestamp = Timestamp2,
        compressed = Compressed,
        docs = Docs
    };
translate_from_protobuf(#'ChangesRequest2'{
    space_id = SpaceId,
    since = Since,
    until = Until,
    included_mutators = IncludedMutators
}) ->
    #changes_request2{
        space_id = SpaceId,
        since = Since,
        until = Until,
        included_mutators = IncludedMutators
    };


%% REMOTE DRIVER
translate_from_protobuf(#'GetRemoteDocument'{
    model = Model,
    key = Key,
    routing_key = RoutingKey
}) ->
    #get_remote_document{
        model = binary_to_atom(Model, utf8),
        key = Key,
        routing_key = RoutingKey
    };
translate_from_protobuf(#'RemoteDocument'{
    status = Status,
    compressed_data = Data
}) ->
    #remote_document{
        status = translate_from_protobuf(Status),
        compressed_data = Data
    };


%% RTRANSFER
translate_from_protobuf(#'GenerateRTransferConnSecret'{secret = Secret}) ->
    #generate_rtransfer_conn_secret{secret = Secret};
translate_from_protobuf(#'RTransferConnSecret'{secret = Secret}) ->
    #rtransfer_conn_secret{secret = Secret};
translate_from_protobuf(#'GetRTransferNodesIPs'{}) ->
    #get_rtransfer_nodes_ips{};
translate_from_protobuf(#'RTransferNodesIPs'{nodes = undefined}) ->
    #rtransfer_nodes_ips{nodes = []};
translate_from_protobuf(#'RTransferNodesIPs'{nodes = Nodes}) ->
    #rtransfer_nodes_ips{
        nodes = [translate_from_protobuf(N) || N <- Nodes]
    };


%% SESSION
translate_from_protobuf(#'CloseSession'{}) ->
    #close_session{};

%% MULTIPART UPLOAD
translate_from_protobuf(#'CreateMultipartUpload'{space_id = SpaceId, path = Path}) ->
    #create_multipart_upload{space_id = SpaceId, path = Path};
translate_from_protobuf(#'UploadMultipartPart'{multipart_upload_id = UploadId, part = Part}) ->
    #upload_multipart_part{multipart_upload_id = UploadId, part = translate_from_protobuf(Part)};
translate_from_protobuf(#'ListMultipartParts'{
    multipart_upload_id = UploadId,
    limit = Limit,
    part_marker = PartMarker
}) ->
    #list_multipart_parts{
        multipart_upload_id = UploadId,
        limit = Limit,
        part_marker = PartMarker
    };
translate_from_protobuf(#'AbortMultipartUpload'{multipart_upload_id = UploadId}) ->
    #abort_multipart_upload{multipart_upload_id = UploadId};
translate_from_protobuf(#'CompleteMultipartUpload'{multipart_upload_id = UploadId}) ->
    #complete_multipart_upload{multipart_upload_id = UploadId};
translate_from_protobuf(#'ListMultipartUploads'{
    space_id = SpaceId,
    limit = Limit,
    index_token = EncodedIndexToken
}) ->
    #list_multipart_uploads{
        space_id = SpaceId,
        limit = Limit,
        index_token = multipart_upload:decode_token(EncodedIndexToken)
    };
translate_from_protobuf(#'MultipartUploadRequest'{
    multipart_request = {_, Request}
}) ->
    #multipart_upload_request{
        multipart_request = translate_from_protobuf(Request)
    };
translate_from_protobuf(#'MultipartPart'{
    number = Number,
    size = Size,
    etag = Etag,
    last_modified = LastModified
}) ->
    #multipart_upload_part{
        number = Number,
        size = Size,
        etag = Etag,
        last_modified = LastModified
    };
translate_from_protobuf(#'MultipartParts'{
    parts = Parts,
    is_last = IsLast
}) ->
    #multipart_parts{
        parts = lists:map(fun(Part) -> translate_from_protobuf(Part) end, Parts),
        is_last = IsLast
    };
translate_from_protobuf(#'MultipartUpload'{
    multipart_upload_id = UploadId,
    path = Path,
    creation_time = CreationTime
}) ->
    #multipart_upload{
        multipart_upload_id = UploadId,
        path = Path,
        creation_time = CreationTime
    };
translate_from_protobuf(#'MultipartUploads'{
    uploads = Uploads,
    is_last = IsLast,
    next_page_token = EncodedNextPageToken
}) ->
    {multipart_uploads, #multipart_uploads{
        uploads = lists:map(fun(Upload) -> translate_from_protobuf(Upload) end, Uploads),
        is_last = IsLast,
        next_page_token = multipart_upload:decode_token(EncodedNextPageToken)
    }};

translate_from_protobuf(undefined) ->
    undefined;

translate_from_protobuf(Message) when is_tuple(Message) ->
    throw({unrecognized_message, element(1, Message)}).


%%--------------------------------------------------------------------
%% @doc
%% Translates internal record to protobuf record.
%% @end
%%--------------------------------------------------------------------
-spec translate_to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.

%% COMMON
translate_to_protobuf(#status{
    code = Code,
    description = Desc
}) ->
    {status, #'Status'{
        code = Code,
        description = Desc
    }};
translate_to_protobuf(#file_block{
    offset = Off,
    size = S
}) ->
    #'FileBlock'{
        offset = Off,
        size = S
    };
translate_to_protobuf(#file_renamed_entry{} = Record) ->
    #'FileRenamedEntry'{
        old_uuid = Record#'file_renamed_entry'.old_guid,
        new_uuid = Record#'file_renamed_entry'.new_guid,
        new_parent_uuid = Record#'file_renamed_entry'.new_parent_guid,
        new_name = Record#'file_renamed_entry'.new_name
    };
translate_to_protobuf(#ip_and_port{
    ip = IP,
    port = Port
}) ->
    #'IpAndPort'{
        ip = list_to_binary(inet:ntoa(IP)),
        port = Port
    };
translate_to_protobuf(#dir{guid = UUID}) ->
    {dir, #'Dir'{uuid = UUID}};


%% EVENT
translate_to_protobuf(#event{type = Type}) ->
    #'Event'{type = translate_to_protobuf(Type)};
translate_to_protobuf(#events{events = Evts}) ->
    {events, #'Events'{
        events = [translate_to_protobuf(Evt) || Evt <- Evts]}
    };
translate_to_protobuf(#flush_events{
    provider_id = ProviderId,
    subscription_id = SubId,
    context = Context
}) ->
    {flush_events, #'FlushEvents'{
        provider_id = ProviderId,
        subscription_id = binary_to_integer(SubId),
        context = Context}
    };
translate_to_protobuf(#file_read_event{
    counter = Counter,
    file_guid = FileGuid,
    size = Size,
    blocks = Blocks
}) ->
    {file_read, #'FileReadEvent'{
        counter = Counter,
        file_uuid = FileGuid,
        size = Size,
        blocks = [translate_to_protobuf(B) || B <- Blocks]
    }};
translate_to_protobuf(#file_written_event{
    counter = Counter,
    file_guid = FileGuid,
    size = Size,
    file_size = FileSize,
    blocks = Blocks
}) ->
    {file_written, #'FileWrittenEvent'{
        counter = Counter,
        file_uuid = FileGuid,
        size = Size,
        file_size = FileSize,
        blocks = [translate_to_protobuf(B) || B <- Blocks]
    }};
translate_to_protobuf(#file_attr_changed_event{file_attr = FileAttr}) ->
    {_, Record} = translate_to_protobuf(FileAttr),
    {file_attr_changed, #'FileAttrChangedEvent'{
        file_attr = Record
    }};
translate_to_protobuf(#file_location_changed_event{
    file_location = FileLocation,
    change_beg_offset = O,
    change_end_offset = S
}) ->
    {_, Record} = translate_to_protobuf(FileLocation),
    {file_location_changed, #'FileLocationChangedEvent'{
        file_location = Record,
        change_beg_offset = O,
        change_end_offset = S
    }};
translate_to_protobuf(#file_perm_changed_event{
    file_guid = FileGuid
}) ->
    {file_perm_changed, #'FilePermChangedEvent'{
        file_uuid = FileGuid
    }};
translate_to_protobuf(#file_removed_event{
    file_guid = FileGuid
}) ->
    {file_removed, #'FileRemovedEvent'{
        file_uuid = FileGuid
    }};
translate_to_protobuf(#file_renamed_event{
    top_entry = TopEntry,
    child_entries = ChildEntries
}) ->
    {file_renamed, #'FileRenamedEvent'{
        top_entry = translate_to_protobuf(TopEntry),
        child_entries = [translate_to_protobuf(Entry) || Entry <- ChildEntries]
    }};
translate_to_protobuf(#quota_exceeded_event{spaces = Spaces}) ->
    {quota_exceeded, #'QuotaExceededEvent'{spaces = Spaces}};
translate_to_protobuf(#helper_params_changed_event{storage_id = StorageId}) ->
    {helper_params_changed, #'HelperParamsChangedEvent'{storage_id = StorageId}};


%% SUBSCRIPTION
translate_to_protobuf(#subscription{
    id = Id,
    type = Type
}) ->
    {subscription, #'Subscription'{
        id = binary_to_integer(Id),
        type = translate_to_protobuf(Type)}
    };
translate_to_protobuf(#file_read_subscription{
    counter_threshold = CounterThreshold,
    time_threshold = TimeThreshold
}) ->
    {file_read, #'FileReadSubscription'{
        counter_threshold = CounterThreshold,
        time_threshold = TimeThreshold
    }};
translate_to_protobuf(#file_written_subscription{
    counter_threshold = CounterThreshold,
    time_threshold = TimeThreshold
}) ->
    {file_written, #'FileWrittenSubscription'{
        counter_threshold = CounterThreshold,
        time_threshold = TimeThreshold
    }};
translate_to_protobuf(#file_attr_changed_subscription{
    file_guid = FileGuid,
    time_threshold = TimeThreshold
}) ->
    {file_attr_changed, #'FileAttrChangedSubscription'{
        file_uuid = FileGuid,
        time_threshold = TimeThreshold
    }};
translate_to_protobuf(#replica_status_changed_subscription{
    file_guid = FileGuid,
    time_threshold = TimeThreshold
}) ->
    {replica_status_changed, #'ReplicaStatusChangedSubscription'{
        file_uuid = FileGuid,
        time_threshold = TimeThreshold
    }};
translate_to_protobuf(#file_location_changed_subscription{
    file_guid = FileGuid,
    time_threshold = TimeThreshold
}) ->
    {file_location_changed, #'FileLocationChangedSubscription'{
        file_uuid = FileGuid,
        time_threshold = TimeThreshold
    }};
translate_to_protobuf(#file_perm_changed_subscription{
    file_guid = FileGuid
}) ->
    {file_perm_changed, #'FilePermChangedSubscription'{
        file_uuid = FileGuid
    }};
translate_to_protobuf(#file_removed_subscription{
    file_guid = FileGuid
}) ->
    {file_removed, #'FileRemovedSubscription'{
        file_uuid = FileGuid
    }};
translate_to_protobuf(#file_renamed_subscription{
    file_guid = FileGuid
}) ->
    {file_renamed, #'FileRenamedSubscription'{
        file_uuid = FileGuid
    }};
translate_to_protobuf(#quota_exceeded_subscription{}) ->
    {quota_exceeded, #'QuotaExceededSubscription'{}};
translate_to_protobuf(#helper_params_changed_subscription{
    storage_id = StorageId
}) ->
    {helper_params_changed, #'HelperParamsChangedSubscription'{
        storage_id = StorageId
    }};
translate_to_protobuf(#subscription_cancellation{id = Id}) ->
    {subscription_cancellation, #'SubscriptionCancellation'{
        id = binary_to_integer(Id)}
    };


%% HANDSHAKE
translate_to_protobuf(#provider_handshake_request{
    provider_id = ProviderId,
    token = Token
}) ->
    {provider_handshake_request, #'ProviderHandshakeRequest'{
        provider_id = ProviderId,
        token = Token
    }};
translate_to_protobuf(#handshake_response{
    status = Status
}) ->
    {handshake_response, #'HandshakeResponse'{
        status = Status
    }};
translate_to_protobuf(#client_tokens{
    access_token = SerializedToken
}) ->
    #'Macaroon'{
        macaroon = SerializedToken
    };


% PROCESSING STATUS
translate_to_protobuf(#processing_status{code = Code}) ->
    {processing_status, #'ProcessingStatus'{code = Code}};


%% DIAGNOSTIC
translate_to_protobuf(#pong{data = Data}) ->
    {pong, #'Pong'{data = Data}};
translate_to_protobuf(#protocol_version{
    major = Major,
    minor = Minor
}) ->
    {protocol_version, #'ProtocolVersion'{
        major = Major,
        minor = Minor
    }};
translate_to_protobuf(#configuration{
    root_guid = RootGuid,
    subscriptions = Subs,
    disabled_spaces = Spaces
}) ->
    {configuration, #'Configuration'{
        root_uuid = RootGuid,
        subscriptions = lists:map(
            fun(Sub) ->
                {_, Record} = translate_to_protobuf(Sub),
                Record
            end, Subs),
        disabled_spaces = Spaces
    }};


%% STREAM
translate_to_protobuf(#message_stream{
    stream_id = StmId,
    sequence_number = SeqNum
}) ->
    #'MessageStream'{
        stream_id = StmId,
        sequence_number = SeqNum
    };
translate_to_protobuf(#message_request{
    stream_id = StmId,
    lower_sequence_number = LowerSeqNum,
    upper_sequence_number = UpperSeqNum
}) ->
    {message_request, #'MessageRequest'{
        stream_id = StmId,
        lower_sequence_number = LowerSeqNum,
        upper_sequence_number = UpperSeqNum
    }};
translate_to_protobuf(#message_acknowledgement{
    stream_id = StmId,
    sequence_number = SeqNum
}) ->
    {message_acknowledgement, #'MessageAcknowledgement'{
        stream_id = StmId,
        sequence_number = SeqNum
    }};
translate_to_protobuf(#message_stream_reset{stream_id = StmId}) ->
    {message_stream_reset, #'MessageStreamReset'{stream_id = StmId}};
translate_to_protobuf(#end_of_message_stream{}) ->
    {end_of_stream, #'EndOfMessageStream'{}};


%% FUSE
translate_to_protobuf(#fuse_request{
    fuse_request = Record
}) ->
    {fuse_request, #'FuseRequest'{
        fuse_request = translate_to_protobuf(Record)}
    };
translate_to_protobuf(#resolve_guid{path = Path}) ->
    {resolve_guid, #'ResolveGuid'{path = Path}};
translate_to_protobuf(#get_helper_params{
    storage_id = StorageId,
    space_id = SpaceId,
    helper_mode = HelperMode
}) ->
    {get_helper_params, #'GetHelperParams'{
        storage_id = StorageId,
        space_id = SpaceId,
        helper_mode = HelperMode
    }};
translate_to_protobuf(#get_fs_stats{
    file_id = FileGuid
}) ->
    {get_fs_stats, #'GetFSStats'{
        file_id = FileGuid
    }};
translate_to_protobuf(#get_recursive_file_list{
    listing_options = ListingOptions,
    optional_attrs = OptionalAttrs
}) ->
    Xattrs = case file_attr:should_fetch_xattrs(OptionalAttrs) of
        {true, XattrNames} -> XattrNames;
        false -> []
    end,
    {list_files_recursively, #'ListFilesRecursively'{
        token = maps:get(pagination_token, ListingOptions, undefined),
        start_after = maps:get(start_after, ListingOptions, undefined),
        prefix = maps:get(prefix, ListingOptions, undefined),
        limit = maps:get(limit, ListingOptions, undefined),
        include_dirs = maps:get(include_directories , ListingOptions, undefined),
        xattrs = Xattrs
    }};
translate_to_protobuf(#recursive_listing_result{
    % currently only recursive file listing can be requested with clproto therefore entry is always
    % of type recursive_file_listing_node:entry().
    entries = Entries,
    pagination_token = PaginationToken
}) ->
    {files_list, #'FileList'{
        files = lists:map(fun({Path, FileAttr}) ->
            % put file path as name as that is expected by oneclient
            {file_attr, TranslatedFileAttr} = translate_to_protobuf(FileAttr#file_attr{name = Path}),
            TranslatedFileAttr
        end, Entries),
        next_page_token = PaginationToken,
        is_last = PaginationToken == undefined
    }};
translate_to_protobuf(#file_request{
    context_guid = ContextGuid,
    file_request = Record
}) ->
    {file_request, #'FileRequest'{
        context_guid = ContextGuid,
        file_request = translate_to_protobuf(Record)}
    };
translate_to_protobuf(#get_file_attr{optional_attrs = OptionalAttrs}) ->
    {IRS, ILC} = optional_attrs_to_attrs_flags(OptionalAttrs),
    {get_file_attr, #'GetFileAttr'{include_replication_status = IRS, include_link_count = ILC}};
translate_to_protobuf(#get_file_attr_by_path{path = Path}) ->
    {get_file_attr_by_path, #'GetFileAttrByPath'{path = Path}};
translate_to_protobuf(#get_child_attr{name = Name, optional_attrs = OptionalAttrs}) ->
    {IRS, ILC} = optional_attrs_to_attrs_flags(OptionalAttrs),
    {get_child_attr, #'GetChildAttr'{name = Name, include_replication_status = IRS, include_link_count = ILC}};
translate_to_protobuf(#get_file_children{listing_options = ListingOpts}) ->
    {get_file_children, #'GetFileChildren'{
        offset = maps:get(offset, ListingOpts, undefined),
        size = maps:get(limit, ListingOpts, undefined),
        index_token = case maps:get(pagination_token, ListingOpts, undefined) of
            undefined -> undefined;
            PaginationToken -> file_listing:encode_pagination_token(PaginationToken)
        end,
        index_startid = maps:get(index, ListingOpts, undefined)
    }};
translate_to_protobuf(#get_file_children_attrs{
    listing_options = ListingOpts,
    optional_attrs = OptionalAttrs
}) ->
    {IRS, ILC} = optional_attrs_to_attrs_flags(OptionalAttrs),
    {get_file_children_attrs, #'GetFileChildrenAttrs'{
        offset = maps:get(offset, ListingOpts, undefined),
        size = maps:get(limit, ListingOpts, undefined),
        index_token = case maps:get(pagination_token, ListingOpts, undefined) of
            undefined -> undefined;
            PaginationToken -> file_listing:encode_pagination_token(PaginationToken)
        end,
        include_replication_status = IRS,
        include_link_count = ILC
    }};
translate_to_protobuf(#create_dir{
    name = Name,
    mode = Mode
}) ->
    {create_dir, #'CreateDir'{
        name = Name,
        mode = Mode
    }};
translate_to_protobuf(#create_path{
    path = Path
}) ->
    {create_path, #'CreatePath'{
        path = Path
    }};
translate_to_protobuf(#delete_file{silent = Silent}) ->
    {delete_file, #'DeleteFile'{silent = Silent}};
translate_to_protobuf(#update_times{
    atime = ATime,
    mtime = MTime,
    ctime = CTime
}) ->
    {update_times, #'UpdateTimes'{
        atime = ATime,
        mtime = MTime,
        ctime = CTime
    }};
translate_to_protobuf(#change_mode{mode = Mode}) ->
    {change_mode, #'ChangeMode'{mode = Mode}};
translate_to_protobuf(#rename{
    target_parent_guid = TargetParentGuid,
    target_name = TargetName
}) ->
    {rename, #'Rename'{
        target_parent_uuid = TargetParentGuid,
        target_name = TargetName
    }};
translate_to_protobuf(#create_file{
    name = Name,
    mode = Mode,
    flag = Flag
}) ->
    {create_file, #'CreateFile'{
        name = Name,
        mode = Mode,
        flag = open_flag_translate_to_protobuf(Flag)}
    };
translate_to_protobuf(#storage_file_created{}) ->
    {storage_file_created, #'StorageFileCreated'{}};
translate_to_protobuf(#make_file{
    name = Name,
    mode = Mode
}) ->
    {make_file, #'MakeFile'{
        name = Name,
        mode = Mode
    }};
translate_to_protobuf(#make_link{
    target_parent_guid = TargetParentGuid,
    target_name = Name
}) ->
    {make_link, #'MakeLink'{
        target_parent_uuid = TargetParentGuid,
        target_name = Name
    }};
translate_to_protobuf(#make_symlink{
    target_name = TargetName,
    link = Link
}) ->
    {make_symlink, #'MakeSymlink'{
        target_name = TargetName,
        link = Link
    }};
translate_to_protobuf(#open_file{flag = Flag}) ->
    {open_file, #'OpenFile'{
        flag = open_flag_translate_to_protobuf(Flag)}
    };
translate_to_protobuf(#open_file_with_extended_info{flag = Flag}) ->
    {open_file_with_extended_info, #'OpenFileWithExtendedInfo'{
        flag = open_flag_translate_to_protobuf(Flag)}
    };
translate_to_protobuf(#get_file_location{}) ->
    {get_file_location, #'GetFileLocation'{}};
translate_to_protobuf(#read_symlink{}) ->
    {read_symlink, #'ReadSymlink'{}};
translate_to_protobuf(#release{handle_id = HandleId}) ->
    {release, #'Release'{handle_id = HandleId}};
translate_to_protobuf(#truncate{size = Size}) ->
    {truncate, #'Truncate'{size = Size}};
translate_to_protobuf(#synchronize_block{
    block = Block,
    prefetch = Prefetch,
    priority = Priority}
) ->
    {synchronize_block, #'SynchronizeBlock'{
        block = translate_to_protobuf(Block),
        prefetch = Prefetch,
        priority = Priority
    }};
translate_to_protobuf(#synchronize_block_and_compute_checksum{
    block = Block,
    prefetch = Prefetch,
    priority = Priority
}) ->
    {synchronize_block_and_compute_checksum,
        #'SynchronizeBlockAndComputeChecksum'{
            block = translate_to_protobuf(Block),
            prefetch = Prefetch,
            priority = Priority
        }
    };
translate_to_protobuf(#block_synchronization_request{
    block = Block,
    prefetch = Prefetch,
    priority = Priority
}) ->
    {block_synchronization_request, #'BlockSynchronizationRequest'{
        block = translate_to_protobuf(Block),
        prefetch = Prefetch,
        priority = Priority
    }};
translate_to_protobuf(#fuse_response{
    status = Status,
    fuse_response = FuseResponse
}) ->
    {status, StatProto} = translate_to_protobuf(Status),
    {fuse_response, #'FuseResponse'{
        status = StatProto,
        fuse_response = translate_to_protobuf(FuseResponse)
    }};
translate_to_protobuf(#child_link{
    guid = FileGuid,
    name = Name
}) ->
    #'ChildLink'{
        uuid = FileGuid,
        name = Name
    };
translate_to_protobuf(#file_attr{} = FileAttr) ->
    Xattrs = maps:fold(fun(XattrName, XattrValue, Acc) ->
        {xattr, TranslatedXattr} = translate_to_protobuf(
            #xattr{name = XattrName, value = XattrValue}),
        [TranslatedXattr | Acc]
    end, [], FileAttr#file_attr.xattrs),
    {file_attr, #'FileAttr'{
        uuid = FileAttr#file_attr.guid,
        name = FileAttr#file_attr.name,
        mode = FileAttr#file_attr.mode,
        parent_uuid = FileAttr#file_attr.parent_guid,
        uid = FileAttr#file_attr.uid,
        gid = FileAttr#file_attr.gid,
        atime = FileAttr#file_attr.atime,
        mtime = FileAttr#file_attr.mtime,
        ctime = FileAttr#file_attr.ctime,
        type = FileAttr#file_attr.type,
        size = case FileAttr#file_attr.type of
            ?DIRECTORY_TYPE -> utils:ensure_defined(FileAttr#file_attr.size, 0);
            _ -> FileAttr#file_attr.size
        end,
        provider_id = FileAttr#file_attr.provider_id,
        shares = FileAttr#file_attr.shares,
        owner_id = FileAttr#file_attr.owner_id,
        fully_replicated = FileAttr#file_attr.fully_replicated,
        nlink = FileAttr#file_attr.nlink,
        index = file_listing:encode_index(FileAttr#file_attr.index),
        xattrs = Xattrs
    }};
translate_to_protobuf(#file_children{
    child_links = FileEntries,
    pagination_token = PaginationToken
}) ->
    {file_children, #'FileChildren'{
        child_links = [translate_to_protobuf(E) || E <- FileEntries],
        index_token = file_listing:encode_pagination_token(PaginationToken),
        is_last = file_listing:is_finished(PaginationToken)
    }};
translate_to_protobuf(#file_children_attrs{
    child_attrs = Children,
    pagination_token = ListingToken
}) ->
    {file_children_attrs, #'FileChildrenAttrs'{
        child_attrs = lists:map(fun(Child) ->
            {file_attr, Translated} = translate_to_protobuf(Child),
            Translated
        end, Children),
        index_token = file_listing:encode_pagination_token(ListingToken),
        is_last = file_listing:is_finished(ListingToken)
    }};
translate_to_protobuf(#file_location{
    uuid = Uuid,
    space_id = SpaceId,
    provider_id = ProviderId,
    storage_id = StorageId,
    file_id = FileId,
    blocks = Blocks
} = Record) ->
    {file_location, #'FileLocation'{
        uuid = file_id:pack_guid(Uuid, SpaceId),
        provider_id = ProviderId,
        space_id = SpaceId,
        storage_id = StorageId,
        file_id = FileId,
        version = version_vector:get_provider_version(Record),
        blocks = lists:map(fun(#file_block{offset = Offset, size = Size}) ->
            #'FileBlock'{
                offset = Offset,
                size = Size,
                file_id = Record#file_location.file_id,
                storage_id = Record#file_location.storage_id
            }
        end, Blocks)
    }};
translate_to_protobuf(#file_location_changed{
    file_location = FileLocation,
    change_beg_offset = O,
    change_end_offset = S
}) ->
    {_, Record} = translate_to_protobuf(FileLocation),
    {file_location_changed, #'FileLocationChanged'{
        file_location = Record,
        change_beg_offset = O,
        change_end_offset = S
    }};
translate_to_protobuf(#helper_params{
    helper_name = HelperName,
    helper_args = HelpersArgs
}) ->
    {helper_params, #'HelperParams'{
        helper_name = HelperName,
        helper_args = [translate_to_protobuf(Arg) || Arg <- HelpersArgs]
    }};
translate_to_protobuf(#helper_arg{
    key = Key,
    value = Value
}) ->
    #'HelperArg'{
        key = Key,
        value = Value
    };
translate_to_protobuf(#storage_test_file{
    helper_params = HelperParams,
    space_id = SpaceId,
    file_id = FileId,
    file_content = FileContent
}) ->
    {_, Record} = translate_to_protobuf(HelperParams),
    {storage_test_file, #'StorageTestFile'{
        helper_params = Record,
        space_id = SpaceId,
        file_id = FileId,
        file_content = FileContent
    }};
translate_to_protobuf(#sync_response{
    checksum = Value,
    file_location_changed = FileLocationChanged
}) ->
    {_, ProtoFileLocationChanged} = translate_to_protobuf(FileLocationChanged),
    {sync_response, #'SyncResponse'{
        checksum = Value,
        file_location_changed = ProtoFileLocationChanged
    }};
translate_to_protobuf(#file_created{
    handle_id = HandleId,
    file_attr = FileAttr,
    file_location = FileLocation
}) ->
    {_, ProtoFileAttr} = translate_to_protobuf(FileAttr),
    {_, ProtoFileLocation} = translate_to_protobuf(FileLocation),
    {file_created, #'FileCreated'{
        handle_id = HandleId,
        file_attr = ProtoFileAttr,
        file_location = ProtoFileLocation
    }};
translate_to_protobuf(#file_opened{
    handle_id = HandleId
}) ->
    {file_opened, #'FileOpened'{
        handle_id = HandleId
    }};
translate_to_protobuf(#symlink{
    link = Link
}) ->
    {symlink, #'Symlink'{
        link = Link
    }};
translate_to_protobuf(#file_opened_extended{} = Record) ->
    {file_opened_extended, #'FileOpenedExtended'{
        handle_id = Record#file_opened_extended.handle_id,
        provider_id = Record#file_opened_extended.provider_id,
        file_id = Record#file_opened_extended.file_id,
        storage_id = Record#file_opened_extended.storage_id
    }};
translate_to_protobuf(#fs_stats{
    space_id = SpaceId,
    storage_stats = StorageStats
}) ->
    {fs_stats, #'FSStats'{
        space_id = SpaceId,
        storage_stats = [translate_to_protobuf(Arg) || Arg <- StorageStats]
    }};
translate_to_protobuf(#storage_stats{
    storage_id = StorageId,
    size = Size,
    occupied = Occupied
}) ->
    #'StorageStats'{
        storage_id = StorageId,
        size = Size,
        occupied = Occupied
    };
translate_to_protobuf(#file_renamed{
    new_guid = NewGuid,
    child_entries = ChildEntries
}) ->
    {file_renamed, #'FileRenamed'{
        new_uuid = NewGuid,
        child_entries = [translate_to_protobuf(E) || E <- ChildEntries]
    }};
translate_to_protobuf(#guid{guid = Guid}) ->
    {uuid, #'Uuid'{
        uuid = Guid
    }};
translate_to_protobuf(#report_file_written{offset = Offset, size = Size}) ->
    {report_file_written, #'ReportFileWritten'{
        offset = Offset,
        size = Size
    }};
translate_to_protobuf(#report_file_read{offset = Offset, size = Size}) ->
    {report_file_read, #'ReportFileRead'{
        offset = Offset,
        size = Size
    }};


%% PROXYIO
translate_to_protobuf(#proxyio_request{
    parameters = Parameters,
    storage_id = SID,
    file_id = FID,
    proxyio_request = Record
}) ->
    ParametersProto = lists:map(fun({Key, Value}) ->
        #'Parameter'{key = Key, value = Value}
    end, maps:to_list(Parameters)),
    {proxyio_request, #'ProxyIORequest'{
        parameters = ParametersProto,
        storage_id = SID,
        file_id = FID,
        proxyio_request = translate_to_protobuf(Record)}
    };
translate_to_protobuf(#remote_read{
    offset = Offset,
    size = Size
}) ->
    {remote_read, #'RemoteRead'{
        offset = Offset,
        size = Size
    }};
translate_to_protobuf(#remote_write{
    byte_sequence = ByteSequences
}) ->
    {remote_write, #'RemoteWrite'{
        byte_sequence = [translate_to_protobuf(BS) || BS <- ByteSequences]}
    };
translate_to_protobuf(#byte_sequence{
    offset = Offset,
    data = Data
}) ->
    #'ByteSequence'{
        offset = Offset,
        data = Data
    };
translate_to_protobuf(#proxyio_response{
    status = Status,
    proxyio_response = ProxyIOResponse
}) ->
    {status, StatProto} = translate_to_protobuf(Status),
    {proxyio_response, #'ProxyIOResponse'{
        status = StatProto,
        proxyio_response = translate_to_protobuf(ProxyIOResponse)
    }};
translate_to_protobuf(#remote_data{data = Data}) ->
    {remote_data, #'RemoteData'{data = Data}};
translate_to_protobuf(#remote_write_result{wrote = Wrote}) ->
    {remote_write_result, #'RemoteWriteResult'{wrote = Wrote}};

%% Provider RPC
translate_to_protobuf(#provider_rpc_call{
    file_guid = FileGuid,
    request = Request
}) ->
    {provider_rpc_call, #'ProviderRpcCall'{
        file_guid = FileGuid,
        operation = translate_to_protobuf(Request)
    }};
translate_to_protobuf(#provider_current_dir_size_stats_browse_request{
    stat_names = StatNames
}) ->
    {provider_current_dir_size_stats_browse_request, #'ProviderCurrentDirSizeStatsBrowseRequest'{
        stat_names = StatNames
    }};
translate_to_protobuf(#provider_historical_dir_size_stats_browse_request{
    request = Request
}) ->
    {provider_historical_dir_size_stats_browse_request, #'ProviderHistoricalDirSizeStatsBrowseRequest'{
        request = translate_to_protobuf(Request)
    }};
translate_to_protobuf(#time_series_layout_get_request{}) ->
    {time_series_layout_get_request, #'TimeSeriesLayoutGetRequest'{}};
translate_to_protobuf(#time_series_slice_get_request{
    layout = Layout,
    start_timestamp = StartTimestamp,
    window_limit = WindowLimit,
    stop_timestamp = StopTimestamp
}) ->
    {time_series_slice_get_request, #'TimeSeriesSliceGetRequest'{
        layout_as_json = json_utils:encode(Layout),
        start_timestamp = StartTimestamp,
        window_limit = WindowLimit,
        stop_timestamp = StopTimestamp
    }};
translate_to_protobuf(#provider_reg_distribution_get_request{}) ->
    {provider_reg_distribution_get_request, #'ProviderRegDistributionGetRequest'{}};
translate_to_protobuf(#provider_reg_storage_locations_get_request{}) ->
    {provider_reg_storage_locations_get_request, #'ProviderRegStorageLocationsGetRequest'{}};
translate_to_protobuf(#provider_qos_status_get_request{
    qos_entry_id = QosEntryId
}) ->
    {provider_qos_status_get_request, #'ProviderQosStatusGetRequest'{
        qos_entry_id = QosEntryId
    }};

translate_to_protobuf(#provider_rpc_response{
    status = ok,
    result = Result
}) ->
    {provider_rpc_response, #'ProviderRpcResponse'{
        status = ok,
        result = translate_to_protobuf(Result)
    }};
translate_to_protobuf(#provider_rpc_response{
    status = error,
    result = Error
}) ->
    {provider_rpc_response, #'ProviderRpcResponse'{
        status = error,
        result = {error_json, json_utils:encode(errors:to_json(Error))}
    }};
translate_to_protobuf(#provider_current_dir_size_stats_browse_result{
    stats = Stats
}) ->
    {provider_current_dir_size_stats_browse_result, #'ProviderCurrentDirSizeStatsBrowseResult'{
        stats_as_json = json_utils:encode(Stats)
    }};
translate_to_protobuf(#time_series_layout_get_result{
    layout = Layout
}) ->
    {time_series_layout_get_result, #'TimeSeriesLayoutGetResult'{
        layout_as_json = json_utils:encode(Layout)
    }};
translate_to_protobuf(#time_series_slice_get_result{} = SliceGetResult) ->
    {time_series_slice_get_result, #'TimeSeriesSliceGetResult'{
        slice_as_json = json_utils:encode(ts_browse_result:to_json(SliceGetResult))
    }};
translate_to_protobuf(#provider_reg_distribution_get_result{
    logical_size = LogicalSize,
    blocks_per_storage = BlocksPerStorage,
    locations_per_storage = LocationsPerStorage
}) ->
    {provider_reg_distribution_get_result, #'ProviderRegDistributionGetResult'{
        logical_size = LogicalSize,
        distribution_per_storage = maps:fold(fun(StorageId, StorageBlocks, Acc) ->
            [#'StorageRegDistributionGetResult'{
                storage_id = StorageId,
                blocks = lists:map(fun(Block) -> translate_to_protobuf(Block) end, StorageBlocks),
                location = maps:get(StorageId, LocationsPerStorage)
            } | Acc]
        end, [], BlocksPerStorage)
    }};
translate_to_protobuf(#provider_reg_storage_locations_result{
    locations_per_storage = LocationsPerStorageMap
}) ->
    {provider_reg_storage_locations_result, #'ProviderRegStorageLocationsResult'{
        locations = maps:fold(fun(StorageId, Location, Acc) ->
            [#'StorageLocation'{storage_id = StorageId, location = Location} | Acc]
        end, [], LocationsPerStorageMap)
    }};
translate_to_protobuf(#provider_qos_status_get_result{
    status = Status
}) ->
    {provider_qos_status_get_result, #'ProviderQosStatusGetResult'{
        status = Status
    }};

%% PROVIDER
translate_to_protobuf(#provider_request{
    context_guid = ContextGuid,
    provider_request = Record
}) ->
    {provider_request, #'ProviderRequest'{
        context_guid = ContextGuid,
        provider_request = translate_to_protobuf(Record)
    }};
translate_to_protobuf(#get_xattr{
    name = Name,
    inherited = Inherited
}) ->
    {get_xattr, #'GetXattr'{
        name = Name,
        inherited = Inherited
    }};
translate_to_protobuf(#set_xattr{
    xattr = Xattr,
    create = Create,
    replace = Replace
}) ->
    {_, XattrT} = translate_to_protobuf(Xattr),
    {set_xattr, #'SetXattr'{
        xattr = XattrT,
        create = Create,
        replace = Replace
    }};
translate_to_protobuf(#remove_xattr{name = Name}) ->
    {remove_xattr, #'RemoveXattr'{name = Name}};
translate_to_protobuf(#list_xattr{
    inherited = Inherited,
    show_internal = ShowInternal
}) ->
    {list_xattr, #'ListXattr'{
        inherited = Inherited,
        show_internal = ShowInternal
    }};
translate_to_protobuf(#get_parent{}) ->
    {get_parent, #'GetParent'{}};
translate_to_protobuf(#get_acl{}) ->
    {get_acl, #'GetAcl'{}};
translate_to_protobuf(#set_acl{acl = Acl}) ->
    {_, PAcl} = translate_to_protobuf(Acl),
    {set_acl, #'SetAcl'{acl = PAcl}};
translate_to_protobuf(#get_file_path{}) ->
    {get_file_path, #'GetFilePath'{}};
translate_to_protobuf(#fsync{
    data_only = DataOnly,
    handle_id = HandleId
}) ->
    {fsync, #'FSync'{
        data_only = DataOnly,
        handle_id = HandleId
    }};


translate_to_protobuf(#provider_response{
    status = Status,
    provider_response = ProviderResponse
}) ->
    {status, StatProto} = translate_to_protobuf(Status),
    {provider_response, #'ProviderResponse'{
        status = StatProto,
        provider_response = translate_to_protobuf(ProviderResponse)
    }};
translate_to_protobuf(#xattr{
    name = Name,
    value = Value
}) ->
    {xattr, #'Xattr'{
        name = Name,
        value = json_utils:encode(Value)
    }};
translate_to_protobuf(#xattr_list{names = Names}) ->
    {xattr_list, #'XattrList'{names = Names}};
translate_to_protobuf(#acl{value = Value}) ->
    {acl, #'Acl'{
        value = json_utils:encode(acl:to_json(Value, cdmi))}
    };
translate_to_protobuf(#file_path{value = Value}) ->
    {file_path, #'FilePath'{value = Value}};
translate_to_protobuf(#check_perms{flag = Flag}) ->
    {check_perms, #'CheckPerms'{
        flag = open_flag_translate_to_protobuf(Flag)
    }};


%% DBSYNC
translate_to_protobuf(#dbsync_request{
    message_body = MessageBody
}) ->
    {dbsync_request, #'DBSyncRequest'{
        message_body = translate_to_protobuf(MessageBody)
    }};
translate_to_protobuf(#tree_broadcast{
    message_body = MessageBody,
    depth = Depth,
    excluded_providers = ExcludedProv,
    l_edge = LEdge,
    r_edge = REgde,
    request_id = ReqId,
    space_id = SpaceId
}) ->
    {tree_broadcast, #'TreeBroadcast'{
        message_body = translate_to_protobuf(MessageBody),
        depth = Depth,
        l_edge = LEdge,
        r_edge = REgde,
        space_id = SpaceId,
        request_id = ReqId,
        excluded_providers = ExcludedProv
    }};
translate_to_protobuf(#batch_update{
    space_id = SpaceId,
    since_seq = Since,
    until_seq = Until,
    changes_encoded = Changes
}) ->
    {batch_update, #'BatchUpdate'{
        space_id = SpaceId,
        since_seq = Since,
        until_seq = Until,
        changes_encoded = Changes
    }};
translate_to_protobuf(#status_report{
    space_id = SpaceId,
    seq = SeqNum
}) ->
    {status_report, #'StatusReport'{
        space_id = SpaceId,
        seq_num = SeqNum
    }};
translate_to_protobuf(#status_request{}) ->
    {status_request, #'StatusRequest'{}};
translate_to_protobuf(#changes_request{
    since_seq = Since,
    until_seq = Until
}) ->
    {changes_request, #'ChangesRequest'{
        since_seq = Since,
        until_seq = Until
    }};
translate_to_protobuf(#dbsync_message{
    message_body = MB
}) ->
    {dbsync_message, #'DBSyncMessage'{
        message_body = translate_to_protobuf(MB)
    }};
translate_to_protobuf(#tree_broadcast2{message_body = CB} = TB) ->
    {_, CB2} = translate_to_protobuf(CB),
    {tree_broadcast, #'TreeBroadcast2'{
        src_provider_id = TB#'tree_broadcast2'.src_provider_id,
        low_provider_id = TB#'tree_broadcast2'.low_provider_id,
        high_provider_id = TB#'tree_broadcast2'.high_provider_id,
        message_id = TB#'tree_broadcast2'.message_id,
        changes_batch = CB2
    }};
translate_to_protobuf(#changes_batch{} = CB) ->
    Timestamp = case CB#'changes_batch'.timestamp of
        undefined -> 0;
        Other -> Other
    end,
    {changes_batch, #'ChangesBatch'{
        space_id = CB#'changes_batch'.space_id,
        since = CB#'changes_batch'.since,
        until = CB#'changes_batch'.until,
        timestamp = Timestamp,
        compressed = CB#'changes_batch'.compressed,
        docs = CB#'changes_batch'.docs
    }};
translate_to_protobuf(#changes_request2{} = CR) ->
    {changes_request, #'ChangesRequest2'{
        space_id = CR#'changes_request2'.space_id,
        since = CR#'changes_request2'.since,
        until = CR#'changes_request2'.until,
        included_mutators = CR#'changes_request2'.included_mutators
    }};


%% PROVIDER
translate_to_protobuf(#get_remote_document{
    model = Model,
    key = Key,
    routing_key = RoutingKey
}) ->
    {get_remote_document, #'GetRemoteDocument'{
        model = atom_to_binary(Model, utf8),
        key = Key,
        routing_key = RoutingKey
    }};
translate_to_protobuf(#remote_document{
    compressed_data = Data,
    status = Status
}) ->
    {status, StatusProto} = translate_to_protobuf(Status),
    {remote_document, #'RemoteDocument'{
        status = StatusProto,
        compressed_data = Data
    }};


%% RTRANSFER
translate_to_protobuf(#generate_rtransfer_conn_secret{
    secret = Secret
}) ->
    {generate_rtransfer_conn_secret, #'GenerateRTransferConnSecret'{
        secret = Secret
    }};
translate_to_protobuf(#rtransfer_conn_secret{
    secret = Secret
}) ->
    {rtransfer_conn_secret, #'RTransferConnSecret'{
        secret = Secret
    }};
translate_to_protobuf(#get_rtransfer_nodes_ips{}) ->
    {get_rtransfer_nodes_ips, #'GetRTransferNodesIPs'{}};
translate_to_protobuf(#rtransfer_nodes_ips{nodes = Nodes}) ->
    {rtransfer_nodes_ips, #'RTransferNodesIPs'{
        nodes = [translate_to_protobuf(N) || N <- Nodes]
    }};


%% SESSION
translate_to_protobuf(#close_session{}) ->
    {close_session, #'CloseSession'{}};


%% MULTIPART UPLOAD

translate_to_protobuf(#create_multipart_upload{space_id = SpaceId, path = Path}) ->
    {create_multipart_upload, #'CreateMultipartUpload'{space_id = SpaceId, path = Path}};
translate_to_protobuf(#upload_multipart_part{multipart_upload_id = UploadId, part = Part}) ->
    {upload_multipart_part, #'UploadMultipartPart'{multipart_upload_id = UploadId, part = translate_to_protobuf(Part)}};
translate_to_protobuf(#list_multipart_parts{
    multipart_upload_id = UploadId, 
    limit = Limit, 
    part_marker = PartMarker
}) ->
    {list_multipart_parts, #'ListMultipartParts'{
        multipart_upload_id = UploadId, 
        limit = Limit, 
        part_marker = PartMarker
    }};
translate_to_protobuf(#abort_multipart_upload{multipart_upload_id = UploadId}) ->
    {abort_multipart_upload, #'AbortMultipartUpload'{multipart_upload_id = UploadId}};
translate_to_protobuf(#complete_multipart_upload{multipart_upload_id = UploadId}) ->
    {complete_multipart_upload, #'CompleteMultipartUpload'{multipart_upload_id = UploadId}};
translate_to_protobuf(#list_multipart_uploads{
    space_id = SpaceId,
    limit = Limit,
    index_token = IndexToken
}) ->
    {list_multipart_uploads, #'ListMultipartUploads'{
        space_id = SpaceId,
        limit = Limit,
        index_token = multipart_upload:encode_token(IndexToken)
    }};
translate_to_protobuf(#multipart_upload_request{
    multipart_request = Request
}) ->
    {multipart_upload_request, #'MultipartUploadRequest'{
        multipart_request = translate_to_protobuf(Request)
    }};
translate_to_protobuf(#multipart_upload_part{
    number = Number,
    size = Size,
    etag = Etag,
    last_modified = LastModified
}) ->
    #'MultipartPart'{
        number = Number,
        size = Size,
        etag = Etag,
        last_modified = LastModified
    };
translate_to_protobuf(#multipart_parts{
    parts = Parts,
    is_last = IsLast
}) ->
    {multipart_parts, #'MultipartParts'{
        parts = lists:map(fun(Part) -> translate_to_protobuf(Part) end, Parts),
        is_last = IsLast
    }};
translate_to_protobuf(#multipart_upload{
    multipart_upload_id = UploadId,
    path = Path,
    creation_time = CreationTime
}) ->
    {multipart_upload, #'MultipartUpload'{
        multipart_upload_id = UploadId,
        path = Path,
        creation_time = CreationTime
    }};
translate_to_protobuf(#multipart_uploads{
    uploads = Uploads,
    is_last = IsLast,
    next_page_token = NextPageToken
}) ->
    {multipart_uploads, #'MultipartUploads'{
        uploads = lists:map(fun(Upload) -> 
            {_, Record} = translate_to_protobuf(Upload), 
            Record
        end, Uploads),
        is_last = IsLast,
        next_page_token = multipart_upload:encode_token(NextPageToken)
    }};

translate_to_protobuf(undefined) ->
    undefined.


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec open_flag_translate_to_protobuf(fslogic_worker:open_flag()) ->
    'READ_WRITE' | 'READ' | 'WRITE'.
open_flag_translate_to_protobuf(read) -> 'READ';
open_flag_translate_to_protobuf(write) -> 'WRITE';
open_flag_translate_to_protobuf(_) -> 'READ_WRITE'.


-spec open_flag_translate_from_protobuf('READ_WRITE' | 'READ' | 'WRITE') ->
    fslogic_worker:open_flag().
open_flag_translate_from_protobuf('READ') -> read;
open_flag_translate_from_protobuf('WRITE') -> write;
open_flag_translate_from_protobuf(_) -> rdwr.


-spec optional_attrs_to_attrs_flags([attr_req:optional_attr()]) -> 
    {boolean(), boolean()}.
optional_attrs_to_attrs_flags(OptionalAttrsList) ->
    IRS = lists:member(replication_status, OptionalAttrsList),
    ILC = lists:member(link_count, OptionalAttrsList),
    {IRS, ILC}.


-spec attrs_flags_to_optional_attrs(boolean() | undefined, boolean() | undefined) -> 
    [attr_req:optional_attr()].
attrs_flags_to_optional_attrs(true = _IRS, true = _ILC) ->
    [replication_status, link_count];
attrs_flags_to_optional_attrs(true = _IRS, _ILC) ->
    [replication_status];
attrs_flags_to_optional_attrs(_IRS, true = _ILC) ->
    [link_count];
attrs_flags_to_optional_attrs(_IRS, _ILC) ->
    [].

-spec xattrs_to_optional_attrs([custom_metadata:name()]) -> [attr_req:optional_attr()].
xattrs_to_optional_attrs([]) -> [];
xattrs_to_optional_attrs(Xattrs) -> [{xattrs, Xattrs}].
