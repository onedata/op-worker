%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Translations between protobuf and internal protocol
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
-include("proto/oneprovider/dbsync_messages2.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([translate_handshake_error/1, translate_from_protobuf/1,
    translate_to_protobuf/1]).

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
    'TOKEN_EXPIRED';
translate_handshake_error(<<"token_not_found">>) ->
    'TOKEN_NOT_FOUND';
translate_handshake_error(<<"invalid_token">>) ->
    'INVALID_TOKEN';
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

%%--------------------------------------------------------------------
%% @doc
%% Traslates protobuf record to internal record.
%% @end
%%--------------------------------------------------------------------
-spec translate_from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.

%% COMMON
translate_from_protobuf(#'Status'{code = Code, description = Desc}) ->
    #status{code = Code, description = Desc};
translate_from_protobuf(#'FileBlock'{offset = Off, size = S}) ->
    #file_block{offset = Off, size = S};
translate_from_protobuf(#'FileRenamedEntry'{} = Record) ->
    #file_renamed_entry{
        old_guid = Record#'FileRenamedEntry'.old_uuid,
        new_guid = Record#'FileRenamedEntry'.new_uuid,
        new_parent_guid = Record#'FileRenamedEntry'.new_parent_uuid,
        new_name = Record#'FileRenamedEntry'.new_name
    };
translate_from_protobuf(#'Dir'{uuid = UUID}) ->
    #dir{guid = UUID};


%% EVENT
translate_from_protobuf(#'Event'{type = {_, Record}}) ->
    #event{type = translate_from_protobuf(Record)};
translate_from_protobuf(#'Events'{events = Evts}) ->
    #events{events = [translate_from_protobuf(Evt) || Evt <- Evts]};
translate_from_protobuf(#'FlushEvents'{} = Record) ->
    #flush_events{
        provider_id = Record#'FlushEvents'.provider_id,
        subscription_id = integer_to_binary(Record#'FlushEvents'.subscription_id),
        context = binary_to_term(Record#'FlushEvents'.context)
    };
translate_from_protobuf(#'FileReadEvent'{} = Record) ->
    #file_read_event{
        counter = Record#'FileReadEvent'.counter,
        file_guid = Record#'FileReadEvent'.file_uuid,
        size = Record#'FileReadEvent'.size,
        blocks = [translate_from_protobuf(B) || B <- Record#'FileReadEvent'.blocks]
    };
translate_from_protobuf(#'FileWrittenEvent'{} = Record) ->
    #file_written_event{
        counter = Record#'FileWrittenEvent'.counter,
        file_guid = Record#'FileWrittenEvent'.file_uuid,
        size = Record#'FileWrittenEvent'.size,
        file_size = Record#'FileWrittenEvent'.file_size,
        blocks = [translate_from_protobuf(B) || B <- Record#'FileWrittenEvent'.blocks]
    };
translate_from_protobuf(#'FileAttrChangedEvent'{file_attr = FileAttr}) ->
    #file_attr_changed_event{
        file_attr = translate_from_protobuf(FileAttr)
    };
translate_from_protobuf(#'FileLocationChangedEvent'{file_location = FileLocation}) ->
    #file_location_changed_event{
        file_location = translate_from_protobuf(FileLocation)
    };
translate_from_protobuf(#'FilePermChangedEvent'{file_uuid = FileGuid}) ->
    #file_perm_changed_event{file_guid = FileGuid};
translate_from_protobuf(#'FileRemovedEvent'{file_uuid = FileGuid}) ->
    #file_removed_event{file_guid = FileGuid};
translate_from_protobuf(#'FileRenamedEvent'{} = Record) ->
    #file_renamed_event{
        top_entry = translate_from_protobuf(Record#'FileRenamedEvent'.top_entry),
        child_entries = [translate_from_protobuf(Entry) ||
            Entry <- Record#'FileRenamedEvent'.child_entries]
    };
translate_from_protobuf(#'QuotaExceededEvent'{spaces = Spaces}) ->
    #quota_exceeded_event{spaces = Spaces};

%% SUBSCRIPTION
translate_from_protobuf(#'Subscription'{id = Id, type = {_, Record}}) ->
    #subscription{
        id = integer_to_binary(Id),
        type = translate_from_protobuf(Record)
    };
translate_from_protobuf(#'FileReadSubscription'{} = Record) ->
    #file_read_subscription{
        counter_threshold = Record#'FileReadSubscription'.counter_threshold,
        time_threshold = Record#'FileReadSubscription'.time_threshold
    };
translate_from_protobuf(#'FileWrittenSubscription'{} = Record) ->
    #file_written_subscription{
        counter_threshold = Record#'FileWrittenSubscription'.counter_threshold,
        time_threshold = Record#'FileWrittenSubscription'.time_threshold
    };
translate_from_protobuf(#'FileAttrChangedSubscription'{} = Record) ->
    #file_attr_changed_subscription{
        file_guid = Record#'FileAttrChangedSubscription'.file_uuid,
        time_threshold = Record#'FileAttrChangedSubscription'.time_threshold
    };
translate_from_protobuf(#'FileLocationChangedSubscription'{} = Record) ->
    #file_location_changed_subscription{
        file_guid = Record#'FileLocationChangedSubscription'.file_uuid,
        time_threshold = Record#'FileLocationChangedSubscription'.time_threshold
    };
translate_from_protobuf(#'FilePermChangedSubscription'{} = Record) ->
    #file_perm_changed_subscription{
        file_guid = Record#'FilePermChangedSubscription'.file_uuid
    };
translate_from_protobuf(#'FileRemovedSubscription'{} = Record) ->
    #file_removed_subscription{
        file_guid = Record#'FileRemovedSubscription'.file_uuid
    };
translate_from_protobuf(#'FileRenamedSubscription'{} = Record) ->
    #file_renamed_subscription{
        file_guid = Record#'FileRenamedSubscription'.file_uuid
    };
translate_from_protobuf(#'QuotaExceededSubscription'{}) ->
    #quota_exceeded_subscription{};
translate_from_protobuf(#'SubscriptionCancellation'{id = Id}) ->
    #subscription_cancellation{id = integer_to_binary(Id)};


%% HANDSHAKE
translate_from_protobuf(#'HandshakeRequest'{token = Token, session_id = SessionId}) ->
    #handshake_request{auth = translate_from_protobuf(Token), session_id = SessionId};
translate_from_protobuf(#'Token'{value = Token, secondary_values = []}) ->
    #token_auth{token = Token};
translate_from_protobuf(#'Token'{value = Macaroon, secondary_values = DischargeMacaroons}) ->
    #macaroon_auth{macaroon = Macaroon, disch_macaroons = DischargeMacaroons};


%% DIAGNOSTIC
translate_from_protobuf(#'Ping'{data = Data}) ->
    #ping{data = Data};
translate_from_protobuf(#'GetProtocolVersion'{}) ->
    #get_protocol_version{};
translate_from_protobuf(#'ProtocolVersion'{major = Major, minor = Minor}) ->
    #protocol_version{major = Major, minor = Minor};
translate_from_protobuf(#'GetConfiguration'{}) ->
    #get_configuration{};


%% STREAM
translate_from_protobuf(#'MessageStream'{stream_id = StmId, sequence_number = SeqNum}) ->
    #message_stream{stream_id = StmId, sequence_number = SeqNum};
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
translate_from_protobuf(#'MessageStreamReset'{stream_id = StmId}) ->
    #message_stream_reset{stream_id = StmId};
translate_from_protobuf(#'EndOfMessageStream'{}) ->
    #end_of_message_stream{};


%% FUSE
translate_from_protobuf(#'FuseRequest'{fuse_request = {_, Record}}) ->
    #fuse_request{fuse_request = translate_from_protobuf(Record)};
translate_from_protobuf(#'ResolveGuid'{path = Path}) ->
    #resolve_guid{path = Path};
translate_from_protobuf(#'GetHelperParams'{storage_id = SID, force_proxy_io = ForceProxy}) ->
    #get_helper_params{storage_id = SID, force_proxy_io = ForceProxy};
translate_from_protobuf(#'CreateStorageTestFile'{storage_id = Id, file_uuid = FileGuid}) ->
    #create_storage_test_file{storage_id = Id, file_guid = FileGuid};
translate_from_protobuf(#'VerifyStorageTestFile'{storage_id = SId, space_id = SpaceId,
    file_id = FId, file_content = FContent}) ->
    #verify_storage_test_file{storage_id = SId, space_id = SpaceId,
        file_id = FId, file_content = FContent};

translate_from_protobuf(#'FileRequest'{context_guid = ContextGuid, file_request = {_, Record}}) ->
    #file_request{context_guid = ContextGuid, file_request = translate_from_protobuf(Record)};
translate_from_protobuf(#'GetFileAttr'{}) ->
    #get_file_attr{};
translate_from_protobuf(#'GetChildAttr'{name = Name}) ->
    #get_child_attr{name = Name};
translate_from_protobuf(#'GetFileChildren'{offset = Offset, size = Size}) ->
    #get_file_children{offset = Offset, size = Size};
translate_from_protobuf(#'CreateDir'{name = Name, mode = Mode}) ->
    #create_dir{name = Name, mode = Mode};
translate_from_protobuf(#'DeleteFile'{silent = Silent}) ->
    #delete_file{silent = Silent};
translate_from_protobuf(#'UpdateTimes'{atime = ATime, mtime = MTime,
    ctime = CTime}) ->
    #update_times{atime = ATime, mtime = MTime, ctime = CTime};
translate_from_protobuf(#'ChangeMode'{mode = Mode}) ->
    #change_mode{mode = Mode};
translate_from_protobuf(#'Rename'{target_parent_uuid = TargetParentGuid, target_name = TargetName}) ->
    #rename{target_parent_guid = TargetParentGuid, target_name = TargetName};
translate_from_protobuf(#'CreateFile'{name = Name, mode = Mode, flag = Flag}) ->
    #create_file{name = Name, mode = Mode, flag = open_flag_translate_from_protobuf(Flag)};
translate_from_protobuf(#'MakeFile'{name = Name, mode = Mode}) ->
    #make_file{name = Name, mode = Mode};
translate_from_protobuf(#'OpenFile'{flag = Flag}) ->
    #open_file{flag = open_flag_translate_from_protobuf(Flag)};
translate_from_protobuf(#'GetFileLocation'{}) ->
    #get_file_location{};
translate_from_protobuf(#'Release'{handle_id = HandleId}) ->
    #release{handle_id = HandleId};
translate_from_protobuf(#'Truncate'{size = Size}) ->
    #truncate{size = Size};
translate_from_protobuf(#'SynchronizeBlock'{block = #'FileBlock'{offset = O, size = S}, prefetch = Prefetch}) ->
    #synchronize_block{block = #file_block{offset = O, size = S}, prefetch = Prefetch};
translate_from_protobuf(#'SynchronizeBlockAndComputeChecksum'{
    block = #'FileBlock'{offset = O, size = S}}) ->
    #synchronize_block_and_compute_checksum{block = #file_block{offset = O, size = S}};

translate_from_protobuf(#'FuseResponse'{status = Status, fuse_response = {_, FuseResponse}}) ->
    #fuse_response{
        status = translate_from_protobuf(Status),
        fuse_response = translate_from_protobuf(FuseResponse)
    };
translate_from_protobuf(#'FuseResponse'{status = Status}) ->
    #fuse_response{
        status = translate_from_protobuf(Status),
        fuse_response = undefined
    };
translate_from_protobuf(#'ChildLink'{uuid = FileGuid, name = Name}) ->
    #child_link{guid = FileGuid, name = Name};
translate_from_protobuf(#'FileAttr'{} = FileAttr) ->
    #file_attr{
        guid = FileAttr#'FileAttr'.uuid,
        name = FileAttr#'FileAttr'.name,
        mode = FileAttr#'FileAttr'.mode,
        parent_uuid = FileAttr#'FileAttr'.parent_uuid,
        uid = FileAttr#'FileAttr'.uid,
        gid = FileAttr#'FileAttr'.gid,
        atime = FileAttr#'FileAttr'.atime,
        mtime = FileAttr#'FileAttr'.mtime,
        ctime = FileAttr#'FileAttr'.ctime,
        type = FileAttr#'FileAttr'.type,
        size = FileAttr#'FileAttr'.size,
        provider_id = FileAttr#'FileAttr'.provider_id,
        shares = FileAttr#'FileAttr'.shares,
        owner_id = FileAttr#'FileAttr'.owner_id
    };
translate_from_protobuf(#'FileChildren'{child_links = FileEntries}) ->
    #file_children{child_links = lists:map(
        fun(ChildLink) ->
            translate_from_protobuf(ChildLink)
        end, FileEntries)};
translate_from_protobuf(#'FileLocation'{} = Record) ->
    #file_location{
        uuid = fslogic_uuid:guid_to_uuid(Record#'FileLocation'.uuid),
        provider_id = Record#'FileLocation'.provider_id,
        space_id = Record#'FileLocation'.space_id,
        storage_id = Record#'FileLocation'.storage_id,
        file_id = Record#'FileLocation'.file_id,
        blocks = lists:map(
            fun(#'FileBlock'{offset = Offset, size = Size}) ->
                #file_block{offset = Offset, size = Size}
            end, Record#'FileLocation'.blocks)
    };
translate_from_protobuf(#'HelperParams'{helper_name = HelperName, helper_args = HelpersArgs}) ->
    #helper_params{helper_name = HelperName,
        helper_args = lists:map(
            fun(HelpersArg) ->
                translate_from_protobuf(HelpersArg)
            end, HelpersArgs)};
translate_from_protobuf(#'HelperArg'{key = Key, value = Value}) ->
    #helper_arg{key = Key, value = Value};
translate_from_protobuf(#'Parameter'{key = Key, value = Value}) ->
    {Key, Value};
translate_from_protobuf(#'SyncResponse'{checksum = Checksum, file_location = FileLocation}) ->
    #sync_response{checksum = Checksum, file_location = translate_from_protobuf(FileLocation)};
translate_from_protobuf(#'FileCreated'{} = Record) ->
    #file_created{
        handle_id = Record#'FileCreated'.handle_id,
        file_attr = translate_from_protobuf(Record#'FileCreated'.file_attr),
        file_location = translate_from_protobuf(Record#'FileCreated'.file_location)
    };
translate_from_protobuf(#'FileOpened'{} = Record) ->
    #file_opened{
        handle_id = Record#'FileOpened'.handle_id
    };
translate_from_protobuf(#'FileRenamed'{new_uuid = NewGuid, child_entries = ChildEntries}) ->
    #file_renamed{
        new_guid = NewGuid,
        child_entries = [translate_from_protobuf(ChildEntry) || ChildEntry <- ChildEntries]
    };
translate_from_protobuf(#'Uuid'{uuid = Guid}) ->
    #guid{
        guid = Guid
    };


%% PROXYIO
translate_from_protobuf(#'ProxyIORequest'{parameters = Parameters,
    storage_id = SID, file_id = FID, proxyio_request = {_, Record}}) ->
    #proxyio_request{parameters = maps:from_list([translate_from_protobuf(P) || P <- Parameters]),
        storage_id = SID, file_id = FID, proxyio_request = translate_from_protobuf(Record)};
translate_from_protobuf(#'RemoteRead'{offset = Offset, size = Size}) ->
    #remote_read{offset = Offset, size = Size};
translate_from_protobuf(#'RemoteWrite'{byte_sequence = ByteSequences}) ->
    #remote_write{byte_sequence = [translate_from_protobuf(BS) || BS <- ByteSequences]};
translate_from_protobuf(#'ByteSequence'{offset = Offset, data = Data}) ->
    #byte_sequence{offset = Offset, data = Data};

translate_from_protobuf(#'ProxyIOResponse'{status = Status, proxyio_response = {_, ProxyIOResponse}}) ->
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


%% PROVIDER
translate_from_protobuf(#'ProviderRequest'{context_guid = ContextGuid,
    provider_request = {_, Record}}) ->
    #provider_request{context_guid = ContextGuid,
        provider_request = translate_from_protobuf(Record)};
translate_from_protobuf(#'GetXattr'{name = Name, inherited = Inherited}) ->
    #get_xattr{name = Name, inherited = Inherited};
translate_from_protobuf(#'SetXattr'{xattr = Xattr, create = Create, replace = Replace}) ->
    #set_xattr{xattr = translate_from_protobuf(Xattr), create = Create, replace = Replace};
translate_from_protobuf(#'RemoveXattr'{name = Name}) ->
    #remove_xattr{name = Name};
translate_from_protobuf(#'ListXattr'{inherited = Inherited, show_internal = ShowInternal}) ->
    #list_xattr{inherited = Inherited, show_internal = ShowInternal};
translate_from_protobuf(#'GetParent'{}) ->
    #get_parent{};
translate_from_protobuf(#'GetAcl'{}) ->
    #get_acl{};
translate_from_protobuf(#'SetAcl'{acl = Acl}) ->
    #set_acl{acl = translate_from_protobuf(Acl)};
translate_from_protobuf(#'GetTransferEncoding'{}) ->
    #get_transfer_encoding{};
translate_from_protobuf(#'SetTransferEncoding'{value = Value}) ->
    #set_transfer_encoding{value = Value};
translate_from_protobuf(#'GetCdmiCompletionStatus'{}) ->
    #get_cdmi_completion_status{};
translate_from_protobuf(#'SetCdmiCompletionStatus'{value = Value}) ->
    #set_cdmi_completion_status{value = Value};
translate_from_protobuf(#'GetMimetype'{}) ->
    #get_mimetype{};
translate_from_protobuf(#'SetMimetype'{value = Value}) ->
    #set_mimetype{value = Value};
translate_from_protobuf(#'GetFilePath'{}) ->
    #get_file_path{};
translate_from_protobuf(#'FSync'{data_only = DataOnly, handle_id = HandleId}) ->
    #fsync{data_only = DataOnly, handle_id = HandleId};
translate_from_protobuf(#'GetFileDistribution'{}) ->
    #get_file_distribution{};
translate_from_protobuf(#'ReplicateFile'{provider_id = ProviderId,
    block = Block}) ->
    #replicate_file{provider_id = ProviderId,
        block = translate_from_protobuf(Block)};
translate_from_protobuf(#'InvalidateFileReplica'{
    provider_id = ProviderId,
    migration_provider_id = MigrationProviderId
}) ->
    #invalidate_file_replica{
        provider_id = ProviderId,
        migration_provider_id = MigrationProviderId
    };
translate_from_protobuf(#'ReadMetadata'{type = Type, names = Names, inherited = Inherited}) ->
    #get_metadata{type = binary_to_existing_atom(Type, utf8), names = Names, inherited = Inherited};
translate_from_protobuf(#'WriteMetadata'{metadata = Metadata, names = Names}) ->
    #set_metadata{metadata = translate_from_protobuf(Metadata), names = Names};
translate_from_protobuf(#'RemoveMetadata'{type = Type}) ->
    #remove_metadata{type = binary_to_existing_atom(Type, utf8)};

translate_from_protobuf(#'ProviderResponse'{status = Status, provider_response = {_, ProviderResponse}}) ->
    #provider_response{
        status = translate_from_protobuf(Status),
        provider_response = translate_from_protobuf(ProviderResponse)
    };
translate_from_protobuf(#'ProviderResponse'{status = Status}) ->
    #provider_response{
        status = translate_from_protobuf(Status)
    };
translate_from_protobuf(#'Xattr'{name = Name, value = Value}) ->
    #xattr{name = Name, value = json_utils:decode_map(Value)};
translate_from_protobuf(#'XattrList'{names = Names}) ->
    #xattr_list{names = Names};
translate_from_protobuf(#'TransferEncoding'{value = Value}) ->
    #transfer_encoding{value = Value};
translate_from_protobuf(#'CdmiCompletionStatus'{value = Value}) ->
    #cdmi_completion_status{value = Value};
translate_from_protobuf(#'Mimetype'{value = Value}) ->
    #mimetype{value = Value};
translate_from_protobuf(#'Acl'{value = Value}) ->
    #acl{value = acl_logic:from_json_format_to_acl(json_utils:decode_map(Value))};
translate_from_protobuf(#'FilePath'{value = Value}) ->
    #file_path{value = Value};
translate_from_protobuf(#'ProviderFileDistribution'{provider_id = ProviderId, blocks = Blocks}) ->
    TranslatedBlocks = lists:map(fun translate_from_protobuf/1, Blocks),
    #provider_file_distribution{provider_id = ProviderId, blocks = TranslatedBlocks};
translate_from_protobuf(#'FileDistribution'{provider_file_distributions = Distributions}) ->
    TranslatedDistributions = lists:map(fun translate_from_protobuf/1, Distributions),
    #file_distribution{provider_file_distributions = TranslatedDistributions};
translate_from_protobuf(#'Metadata'{type = <<"json">>, value = Json}) ->
    #metadata{type = json, value = json_utils:decode_map(Json)};
translate_from_protobuf(#'Metadata'{type = <<"rdf">>, value = Rdf}) ->
    #metadata{type = rdf, value = Rdf};
translate_from_protobuf(#'CheckPerms'{flag = Flag}) ->
    #check_perms{flag = open_flag_translate_from_protobuf(Flag)};
translate_from_protobuf(#'CreateShare'{name = Name}) ->
    #create_share{name = Name};
translate_from_protobuf(#'RemoveShare'{}) ->
    #remove_share{};
translate_from_protobuf(#'Share'{share_id = ShareId, share_file_uuid = ShareGuid}) ->
    #share{share_id = ShareId, share_file_guid = ShareGuid};

%% DBSYNC
translate_from_protobuf(#'DBSyncRequest'{message_body = {_, MessageBody}}) ->
    #dbsync_request{message_body = translate_from_protobuf(MessageBody)};
translate_from_protobuf(#'TreeBroadcast'{message_body = {_, MessageBody},
    depth = Depth, excluded_providers = ExcludedProv, l_edge = LEdge,
    r_edge = REgde, request_id = ReqId, space_id = SpaceId}) ->
    #tree_broadcast{
        message_body = translate_from_protobuf(MessageBody),
        depth = Depth,
        l_edge = LEdge,
        r_edge = REgde,
        space_id = SpaceId,
        request_id = ReqId,
        excluded_providers = ExcludedProv
    };
translate_from_protobuf(#'BatchUpdate'{space_id = SpaceId, since_seq = Since,
    until_seq = Until, changes_encoded = Changes}) ->
    #batch_update{space_id = SpaceId, since_seq = Since, until_seq = Until,
        changes_encoded = Changes};
translate_from_protobuf(#'StatusReport'{space_id = SpaceId, seq_num = SeqNum}) ->
    #status_report{space_id = SpaceId, seq = SeqNum};
translate_from_protobuf(#'StatusRequest'{}) ->
    #status_request{};
translate_from_protobuf(#'ChangesRequest'{since_seq = Since, until_seq = Until}) ->
    #changes_request{since_seq = Since, until_seq = Until};
translate_from_protobuf(#'DBSyncMessage'{message_body = {_, MB}}) ->
    #dbsync_message{message_body = translate_from_protobuf(MB)};
translate_from_protobuf(#'TreeBroadcast2'{changes_batch = CB} = TB) ->
    #tree_broadcast2{
        src_provider_id = TB#'TreeBroadcast2'.src_provider_id,
        low_provider_id = TB#'TreeBroadcast2'.low_provider_id,
        high_provider_id = TB#'TreeBroadcast2'.high_provider_id,
        message_id = TB#'TreeBroadcast2'.message_id,
        message_body = translate_from_protobuf(CB)
    };
translate_from_protobuf(#'ChangesBatch'{} = CB) ->
    #changes_batch{
        space_id = CB#'ChangesBatch'.space_id,
        since = CB#'ChangesBatch'.since,
        until = CB#'ChangesBatch'.until,
        compressed_docs = CB#'ChangesBatch'.compressed_docs
    };
translate_from_protobuf(#'ChangesRequest2'{} = CR) ->
    #changes_request2{
        space_id = CR#'ChangesRequest2'.space_id,
        since = CR#'ChangesRequest2'.since,
        until = CR#'ChangesRequest2'.until
    };

translate_from_protobuf(undefined) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% Translates internal record to protobuf record.
%% @end
%%--------------------------------------------------------------------
-spec translate_to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.

%% COMMON
translate_to_protobuf(#status{code = Code, description = Desc}) ->
    {status, #'Status'{code = Code, description = Desc}};
translate_to_protobuf(#file_block{offset = Off, size = S}) ->
    #'FileBlock'{offset = Off, size = S};
translate_to_protobuf(#file_renamed_entry{} = Record) ->
    #'FileRenamedEntry'{
        old_uuid = Record#'file_renamed_entry'.old_guid,
        new_uuid = Record#'file_renamed_entry'.new_guid,
        new_parent_uuid = Record#'file_renamed_entry'.new_parent_guid,
        new_name = Record#'file_renamed_entry'.new_name
    };
translate_to_protobuf(#dir{guid = UUID}) ->
    {dir, #'Dir'{uuid = UUID}};


%% EVENT
translate_to_protobuf(#event{type = Type}) ->
    #'Event'{type = translate_to_protobuf(Type)};
translate_to_protobuf(#events{events = Evts}) ->
    {events, #'Events'{events = [translate_to_protobuf(Evt) || Evt <- Evts]}};
translate_to_protobuf(#flush_events{provider_id = ProviderId,
    subscription_id = SubId, context = Context}) ->
    {flush_events, #'FlushEvents'{provider_id = ProviderId,
        subscription_id = binary_to_integer(SubId), context = term_to_binary(Context)}};
translate_to_protobuf(#file_read_event{} = Record) ->
    {file_read, #'FileReadEvent'{
        counter = Record#file_read_event.counter,
        file_uuid = Record#file_read_event.file_guid,
        size = Record#file_read_event.size,
        blocks = [translate_to_protobuf(B) || B <- Record#file_read_event.blocks]
    }};
translate_to_protobuf(#file_written_event{} = Record) ->
    {file_written, #'FileWrittenEvent'{
        counter = Record#file_written_event.counter,
        file_uuid = Record#file_written_event.file_guid,
        size = Record#file_written_event.size,
        file_size = Record#file_written_event.file_size,
        blocks = [translate_to_protobuf(B) || B <- Record#file_written_event.blocks]
    }};
translate_to_protobuf(#file_attr_changed_event{file_attr = FileAttr}) ->
    {_, Record} = translate_to_protobuf(FileAttr),
    {file_attr_changed, #'FileAttrChangedEvent'{file_attr = Record}};
translate_to_protobuf(#file_location_changed_event{file_location = FileLocation}) ->
    {_, Record} = translate_to_protobuf(FileLocation),
    {file_location_changed, #'FileLocationChangedEvent'{file_location = Record}};
translate_to_protobuf(#file_perm_changed_event{file_guid = FileGuid}) ->
    {file_perm_changed, #'FilePermChangedEvent'{file_uuid = FileGuid}};
translate_to_protobuf(#file_removed_event{file_guid = FileGuid}) ->
    {file_removed, #'FileRemovedEvent'{file_uuid = FileGuid}};
translate_to_protobuf(#file_renamed_event{top_entry = TopEntry, child_entries = ChildEntries}) ->
    {file_renamed, #'FileRenamedEvent'{
        top_entry = translate_to_protobuf(TopEntry),
        child_entries = [translate_to_protobuf(Entry) || Entry <- ChildEntries]
    }};
translate_to_protobuf(#quota_exceeded_event{spaces = Spaces}) ->
    {quota_exceeded, #'QuotaExceededEvent'{spaces = Spaces}};

%% SUBSCRIPTION
translate_to_protobuf(#subscription{id = Id, type = Type}) ->
    {subscription, #'Subscription'{id = binary_to_integer(Id),
        type = translate_to_protobuf(Type)}};
translate_to_protobuf(#file_read_subscription{} = Sub) ->
    {file_read, #'FileReadSubscription'{
        counter_threshold = Sub#file_read_subscription.counter_threshold,
        time_threshold = Sub#file_read_subscription.time_threshold
    }};
translate_to_protobuf(#file_written_subscription{} = Sub) ->
    {file_written, #'FileWrittenSubscription'{
        counter_threshold = Sub#file_written_subscription.counter_threshold,
        time_threshold = Sub#file_written_subscription.time_threshold
    }};
translate_to_protobuf(#file_attr_changed_subscription{} = Record) ->
    {file_attr_changed, #'FileAttrChangedSubscription'{
        file_uuid = Record#file_attr_changed_subscription.file_guid,
        time_threshold = Record#file_attr_changed_subscription.time_threshold
    }};
translate_to_protobuf(#file_location_changed_subscription{} = Record) ->
    {file_location_changed, #'FileLocationChangedSubscription'{
        file_uuid = Record#file_location_changed_subscription.file_guid,
        time_threshold = Record#file_location_changed_subscription.time_threshold
    }};
translate_to_protobuf(#file_perm_changed_subscription{} = Record) ->
    {file_perm_changed, #'FilePermChangedSubscription'{
        file_uuid = Record#file_perm_changed_subscription.file_guid
    }};
translate_to_protobuf(#file_removed_subscription{} = Record) ->
    {file_removed, #'FileRemovedSubscription'{
        file_uuid = Record#file_removed_subscription.file_guid
    }};
translate_to_protobuf(#file_renamed_subscription{} = Record) ->
    {file_renamed, #'FileRenamedSubscription'{
        file_uuid = Record#file_renamed_subscription.file_guid
    }};
translate_to_protobuf(#quota_exceeded_subscription{}) ->
    {quota_exceeded, #'QuotaExceededSubscription'{}};
translate_to_protobuf(#subscription_cancellation{id = Id}) ->
    {subscription_cancellation, #'SubscriptionCancellation'{
        id = binary_to_integer(Id)}};


%% HANDSHAKE
translate_to_protobuf(#handshake_response{status = Status}) ->
    {handshake_response, #'HandshakeResponse'{status = Status}};
translate_to_protobuf(#macaroon_auth{macaroon = Macaroon, disch_macaroons = DMacaroons}) ->
    #'Token'{value = Macaroon, secondary_values = DMacaroons};
translate_to_protobuf(#token_auth{token = Token}) ->
    #'Token'{value = Token};


%% DIAGNOSTIC
translate_to_protobuf(#pong{data = Data}) ->
    {pong, #'Pong'{data = Data}};
translate_to_protobuf(#protocol_version{major = Major, minor = Minor}) ->
    {protocol_version, #'ProtocolVersion'{major = Major, minor = Minor}};
translate_to_protobuf(#configuration{root_guid = RootGuid, subscriptions = Subs, disabled_spaces = Spaces}) ->
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
translate_to_protobuf(#message_stream{stream_id = StmId, sequence_number = SeqNum}) ->
    #'MessageStream'{stream_id = StmId, sequence_number = SeqNum};
translate_to_protobuf(#message_request{stream_id = StmId,
    lower_sequence_number = LowerSeqNum, upper_sequence_number = UpperSeqNum}) ->
    {message_request, #'MessageRequest'{stream_id = StmId,
        lower_sequence_number = LowerSeqNum, upper_sequence_number = UpperSeqNum}};
translate_to_protobuf(#message_acknowledgement{stream_id = StmId, sequence_number = SeqNum}) ->
    {message_acknowledgement, #'MessageAcknowledgement'{stream_id = StmId,
        sequence_number = SeqNum}};
translate_to_protobuf(#message_stream_reset{stream_id = StmId}) ->
    {message_stream_reset, #'MessageStreamReset'{stream_id = StmId}};
translate_to_protobuf(#end_of_message_stream{}) ->
    {end_of_stream, #'EndOfMessageStream'{}};


%% FUSE
translate_to_protobuf(#fuse_request{fuse_request = Record}) ->
    {fuse_request, #'FuseRequest'{fuse_request = translate_to_protobuf(Record)}};
translate_to_protobuf(#resolve_guid{path = Path}) ->
    {resolve_guid, #'ResolveGuid'{path = Path}};
translate_to_protobuf(#get_helper_params{storage_id = SID, force_proxy_io = ForceProxy}) ->
    {get_helper_params, #'GetHelperParams'{storage_id = SID, force_proxy_io = ForceProxy}};

translate_to_protobuf(#file_request{context_guid = ContextGuid, file_request = Record}) ->
    {file_request, #'FileRequest'{context_guid = ContextGuid, file_request = translate_to_protobuf(Record)}};
translate_to_protobuf(#get_file_attr{}) ->
    {get_file_attr, #'GetFileAttr'{}};
translate_to_protobuf(#get_child_attr{name = Name}) ->
    {get_child_attr, #'GetChildAttr'{name = Name}};
translate_to_protobuf(#get_file_children{offset = Offset, size = Size}) ->
    {get_file_children, #'GetFileChildren'{offset = Offset, size = Size}};
translate_to_protobuf(#create_dir{name = Name, mode = Mode}) ->
    {create_dir, #'CreateDir'{name = Name, mode = Mode}};
translate_to_protobuf(#delete_file{silent = Silent}) ->
    {delete_file, #'DeleteFile'{silent = Silent}};
translate_to_protobuf(#update_times{atime = ATime, mtime = MTime, ctime = CTime}) ->
    {update_times, #'UpdateTimes'{atime = ATime, mtime = MTime, ctime = CTime}};
translate_to_protobuf(#change_mode{mode = Mode}) ->
    {change_mode, #'ChangeMode'{mode = Mode}};
translate_to_protobuf(#rename{target_parent_guid = TargetParentGuid, target_name = TargetName}) ->
    {rename, #'Rename'{target_parent_uuid = TargetParentGuid, target_name = TargetName}};
translate_to_protobuf(#create_file{name = Name, mode = Mode, flag = Flag}) ->
    {create_file, #'CreateFile'{name = Name, mode = Mode,
        flag = open_flag_translate_to_protobuf(Flag)}};
translate_to_protobuf(#make_file{name = Name, mode = Mode}) ->
    {make_file, #'MakeFile'{name = Name, mode = Mode}};
translate_to_protobuf(#open_file{flag = Flag}) ->
    {open_file, #'OpenFile'{flag = open_flag_translate_to_protobuf(Flag)}};
translate_to_protobuf(#get_file_location{}) ->
    {get_file_location, #'GetFileLocation'{}};
translate_to_protobuf(#release{handle_id = HandleId}) ->
    {release, #'Release'{handle_id = HandleId}};
translate_to_protobuf(#truncate{size = Size}) ->
    {truncate, #'Truncate'{size = Size}};
translate_to_protobuf(#synchronize_block{block = Block, prefetch = Prefetch}) ->
    {synchronize_block,
        #'SynchronizeBlock'{block = translate_to_protobuf(Block), prefetch = Prefetch}};
translate_to_protobuf(#synchronize_block_and_compute_checksum{block = Block}) ->
    {synchronize_block_and_compute_checksum,
        #'SynchronizeBlockAndComputeChecksum'{block = translate_to_protobuf(Block)}};

translate_to_protobuf(#fuse_response{status = Status, fuse_response = FuseResponse}) ->
    {status, StatProto} = translate_to_protobuf(Status),
    {fuse_response, #'FuseResponse'{
        status = StatProto,
        fuse_response = translate_to_protobuf(FuseResponse)
    }};
translate_to_protobuf(#child_link{guid = FileGuid, name = Name}) ->
    #'ChildLink'{uuid = FileGuid, name = Name};
translate_to_protobuf(#file_attr{} = FileAttr) ->
    {file_attr, #'FileAttr'{
        uuid = FileAttr#file_attr.guid,
        name = FileAttr#file_attr.name,
        mode = FileAttr#file_attr.mode,
        parent_uuid = FileAttr#file_attr.parent_uuid,
        uid = FileAttr#file_attr.uid,
        gid = FileAttr#file_attr.gid,
        atime = FileAttr#file_attr.atime,
        mtime = FileAttr#file_attr.mtime,
        ctime = FileAttr#file_attr.ctime,
        type = FileAttr#file_attr.type,
        size = FileAttr#file_attr.size,
        provider_id = FileAttr#file_attr.provider_id,
        shares = FileAttr#file_attr.shares,
        owner_id = FileAttr#file_attr.owner_id
    }};
translate_to_protobuf(#file_children{child_links = FileEntries}) ->
    {file_children, #'FileChildren'{child_links = lists:map(fun(ChildLink) ->
        translate_to_protobuf(ChildLink)
    end, FileEntries)}};
translate_to_protobuf(#file_location{} = Record) ->
    {file_location, #'FileLocation'{
        uuid = fslogic_uuid:uuid_to_guid(Record#file_location.uuid, Record#file_location.space_id),
        provider_id = Record#file_location.provider_id,
        space_id = Record#file_location.space_id,
        storage_id = Record#file_location.storage_id,
        file_id = Record#file_location.file_id,
        blocks = lists:map(fun(#file_block{offset = Offset, size = Size}) ->
            #'FileBlock'{
                offset = Offset,
                size = Size,
                file_id = Record#file_location.file_id,
                storage_id = Record#file_location.storage_id
            }
        end, Record#file_location.blocks)
    }};
translate_to_protobuf(#helper_params{helper_name = HelperName, helper_args = HelpersArgs}) ->
    {helper_params, #'HelperParams'{helper_name = HelperName,
        helper_args = lists:map(fun(HelpersArg) ->
            translate_to_protobuf(HelpersArg)
        end, HelpersArgs)}};
translate_to_protobuf(#helper_arg{key = Key, value = Value}) ->
    #'HelperArg'{key = Key, value = Value};
translate_to_protobuf(#storage_test_file{helper_params = HelperParams,
    space_id = SpaceId, file_id = FileId, file_content = FileContent}) ->
    {_, Record} = translate_to_protobuf(HelperParams),
    {storage_test_file, #'StorageTestFile'{helper_params = Record,
        space_id = SpaceId, file_id = FileId, file_content = FileContent}};
translate_to_protobuf(#sync_response{checksum = Value, file_location = FileLocation}) ->
    {_, ProtoFileLocation} = translate_to_protobuf(FileLocation),
    {sync_response, #'SyncResponse'{checksum = Value, file_location = ProtoFileLocation}};
translate_to_protobuf(#file_created{} = Record) ->
    {_, FileAttr} = translate_to_protobuf(Record#file_created.file_attr),
    {_, FileLocation} = translate_to_protobuf(Record#file_created.file_location),
    {file_created, #'FileCreated'{
        handle_id = Record#file_created.handle_id,
        file_attr = FileAttr,
        file_location = FileLocation
    }};
translate_to_protobuf(#file_opened{} = Record) ->
    {file_opened, #'FileOpened'{
        handle_id = Record#file_opened.handle_id
    }};
translate_to_protobuf(#file_renamed{new_guid = NewGuid, child_entries = ChildEntries}) ->
    {file_renamed, #'FileRenamed'{
        new_uuid = NewGuid,
        child_entries = [translate_to_protobuf(ChildEntry) || ChildEntry <- ChildEntries]
    }};
translate_to_protobuf(#guid{guid = Guid}) ->
    {uuid, #'Uuid'{
        uuid = Guid
    }};

%% PROXYIO
translate_to_protobuf(#proxyio_request{parameters = Parameters,
    storage_id = SID, file_id = FID, proxyio_request = Record}) ->
    ParametersProto = lists:map(
        fun({Key, Value}) ->
            #'Parameter'{key = Key, value = Value}
        end, maps:to_list(Parameters)),
    {proxyio_request, #'ProxyIORequest'{parameters = ParametersProto,
        storage_id = SID, file_id = FID, proxyio_request = translate_to_protobuf(Record)}};
translate_to_protobuf(#remote_read{offset = Offset, size = Size}) ->
    {remote_read, #'RemoteRead'{offset = Offset, size = Size}};
translate_to_protobuf(#remote_write{byte_sequence = ByteSequences}) ->
    {remote_write, #'RemoteWrite'{byte_sequence = [translate_to_protobuf(BS) || BS <- ByteSequences]}};
translate_to_protobuf(#byte_sequence{offset = Offset, data = Data}) ->
    #'ByteSequence'{offset = Offset, data = Data};

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


%% PROVIDER
translate_to_protobuf(#provider_request{context_guid = ContextGuid,
    provider_request = Record}) ->
    {provider_request, #'ProviderRequest'{context_guid = ContextGuid,
        provider_request = translate_to_protobuf(Record)}};
translate_to_protobuf(#get_xattr{name = Name, inherited = Inherited}) ->
    {get_xattr, #'GetXattr'{name = Name, inherited = Inherited}};
translate_to_protobuf(#set_xattr{xattr = Xattr, create = Create, replace = Replace}) ->
    {_, XattrT} = translate_to_protobuf(Xattr),
    {set_xattr, #'SetXattr'{xattr = XattrT, create = Create, replace = Replace}};
translate_to_protobuf(#remove_xattr{name = Name}) ->
    {remove_xattr, #'RemoveXattr'{name = Name}};
translate_to_protobuf(#list_xattr{inherited = Inherited, show_internal = ShowInternal}) ->
    {list_xattr, #'ListXattr'{inherited = Inherited, show_internal = ShowInternal}};
translate_to_protobuf(#get_parent{}) ->
    {get_parent, #'GetParent'{}};
translate_to_protobuf(#get_acl{}) ->
    {get_acl, #'GetAcl'{}};
translate_to_protobuf(#set_acl{acl = Acl}) ->
    {_, PAcl} = translate_to_protobuf(Acl),
    {set_acl, #'SetAcl'{acl = PAcl}};
translate_to_protobuf(#get_transfer_encoding{}) ->
    {get_transfer_encoding, #'GetTransferEncoding'{}};
translate_to_protobuf(#set_transfer_encoding{value = Value}) ->
    {set_transfer_encoding, #'SetTransferEncoding'{value = Value}};
translate_to_protobuf(#get_cdmi_completion_status{}) ->
    {get_cdmi_completion_status, #'GetCdmiCompletionStatus'{}};
translate_to_protobuf(#set_cdmi_completion_status{value = Value}) ->
    {set_cdmi_completion_status, #'SetCdmiCompletionStatus'{value = Value}};
translate_to_protobuf(#get_mimetype{}) ->
    {get_mimetype, #'GetMimetype'{}};
translate_to_protobuf(#set_mimetype{value = Value}) ->
    {set_mimetype, #'SetMimetype'{value = Value}};
translate_to_protobuf(#get_file_path{}) ->
    {get_file_path, #'GetFilePath'{}};
translate_to_protobuf(#fsync{data_only = DataOnly, handle_id = HandleId}) ->
    {fsync, #'FSync'{data_only = DataOnly, handle_id = HandleId}};
translate_to_protobuf(#get_file_distribution{}) ->
    {get_file_distribution, #'GetFileDistribution'{}};
translate_to_protobuf(#replicate_file{provider_id = ProviderId,
    block = Block}) ->
    {replicate_file, #'ReplicateFile'{provider_id = ProviderId,
        block = translate_to_protobuf(Block)}};
translate_to_protobuf(#invalidate_file_replica{
    provider_id = ProviderId,
    migration_provider_id = MigrationProviderId
}) ->
    {invalidate_file_replica, #'InvalidateFileReplica'{
        provider_id = ProviderId,
        migration_provider_id = MigrationProviderId
    }};
translate_to_protobuf(#get_metadata{type = Type, names = Names, inherited = Inherited}) ->
    {read_metadata, #'ReadMetadata'{type = atom_to_binary(Type, utf8), names = Names, inherited = Inherited}};
translate_to_protobuf(#set_metadata{metadata = Metadata, names = Names}) ->
    {_, MetadataProto} = translate_to_protobuf(Metadata),
    {write_metadata, #'WriteMetadata'{metadata = MetadataProto, names = Names}};
translate_to_protobuf(#remove_metadata{type = Type}) ->
    {remove_metadata, #'RemoveMetadata'{type = atom_to_binary(Type, utf8)}};

translate_to_protobuf(#provider_response{status = Status, provider_response = ProviderResponse}) ->
    {status, StatProto} = translate_to_protobuf(Status),
    {provider_response, #'ProviderResponse'{
        status = StatProto,
        provider_response = translate_to_protobuf(ProviderResponse)
    }};
translate_to_protobuf(#xattr{name = Name, value = Value}) ->
    {xattr, #'Xattr'{name = Name, value = json_utils:encode_map(Value)}};
translate_to_protobuf(#xattr_list{names = Names}) ->
    {xattr_list, #'XattrList'{names = Names}};
translate_to_protobuf(#transfer_encoding{value = Value}) ->
    {transfer_encoding, #'TransferEncoding'{value = Value}};
translate_to_protobuf(#cdmi_completion_status{value = Value}) ->
    {cdmi_completion_status, #'CdmiCompletionStatus'{value = Value}};
translate_to_protobuf(#mimetype{value = Value}) ->
    {mimetype, #'Mimetype'{value = Value}};
translate_to_protobuf(#acl{value = Value}) ->
    {acl, #'Acl'{value = json_utils:encode_map(acl_logic:from_acl_to_json_format(Value))}};
translate_to_protobuf(#file_path{value = Value}) ->
    {file_path, #'FilePath'{value = Value}};
translate_to_protobuf(#provider_file_distribution{provider_id = ProviderId, blocks = Blocks}) ->
    TranslatedBlocks = lists:map(fun translate_to_protobuf/1, Blocks),
    #'ProviderFileDistribution'{provider_id = ProviderId, blocks = TranslatedBlocks};
translate_to_protobuf(#file_distribution{provider_file_distributions = Distributions}) ->
    TranslatedDistributions = lists:map(fun translate_to_protobuf/1, Distributions),
    {file_distribution, #'FileDistribution'{provider_file_distributions = TranslatedDistributions}};
translate_to_protobuf(#metadata{type = json, value = Json}) ->
    {metadata, #'Metadata'{type = <<"json">>, value = json_utils:encode_map(Json)}};
translate_to_protobuf(#metadata{type = rdf, value = Rdf}) ->
    {metadata, #'Metadata'{type = <<"rdf">>, value = Rdf}};
translate_to_protobuf(#check_perms{flag = Flag}) ->
    {check_perms, #'CheckPerms'{flag = open_flag_translate_to_protobuf(Flag)}};
translate_to_protobuf(#create_share{name = Name}) ->
    {create_share, #'CreateShare'{name = Name}};
translate_to_protobuf(#remove_share{}) ->
    {remove_share, #'RemoveShare'{}};
translate_to_protobuf(#share{share_id = ShareId, share_file_guid = ShareGuid}) ->
    {share, #'Share'{share_id = ShareId, share_file_uuid = ShareGuid}};

%% DBSYNC
translate_to_protobuf(#dbsync_request{message_body = MessageBody}) ->
    {dbsync_request, #'DBSyncRequest'{message_body = translate_to_protobuf(MessageBody)}};
translate_to_protobuf(#tree_broadcast{message_body = MessageBody, depth = Depth,
    excluded_providers = ExcludedProv, l_edge = LEdge, r_edge = REgde,
    request_id = ReqId, space_id = SpaceId}) ->
    {tree_broadcast, #'TreeBroadcast'{
        message_body = translate_to_protobuf(MessageBody),
        depth = Depth,
        l_edge = LEdge,
        r_edge = REgde,
        space_id = SpaceId,
        request_id = ReqId,
        excluded_providers = ExcludedProv
    }};
translate_to_protobuf(#batch_update{space_id = SpaceId, since_seq = Since,
    until_seq = Until, changes_encoded = Changes}) ->
    {batch_update, #'BatchUpdate'{space_id = SpaceId, since_seq = Since,
        until_seq = Until, changes_encoded = Changes}};
translate_to_protobuf(#status_report{space_id = SpaceId, seq = SeqNum}) ->
    {status_report, #'StatusReport'{space_id = SpaceId, seq_num = SeqNum}};
translate_to_protobuf(#status_request{}) ->
    {status_request, #'StatusRequest'{}};
translate_to_protobuf(#changes_request{since_seq = Since, until_seq = Until}) ->
    {changes_request, #'ChangesRequest'{since_seq = Since, until_seq = Until}};
translate_to_protobuf(#dbsync_message{message_body = MB}) ->
    {dbsync_message, #'DBSyncMessage'{message_body = translate_to_protobuf(MB)}};
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
    {changes_batch, #'ChangesBatch'{
        space_id = CB#'changes_batch'.space_id,
        since = CB#'changes_batch'.since,
        until = CB#'changes_batch'.until,
        compressed_docs = CB#'changes_batch'.compressed_docs
    }};
translate_to_protobuf(#changes_request2{} = CR) ->
    {changes_request, #'ChangesRequest2'{
        space_id = CR#'changes_request2'.space_id,
        since = CR#'changes_request2'.since,
        until = CR#'changes_request2'.until
    }};

translate_to_protobuf(undefined) ->
    undefined.

%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec open_flag_translate_to_protobuf(fslogic_worker:open_flag()) ->
    'READ_WRITE' | 'READ' | 'WRITE'.
open_flag_translate_to_protobuf(read) ->
    'READ';
open_flag_translate_to_protobuf(write) ->
    'WRITE';
open_flag_translate_to_protobuf(_) ->
    'READ_WRITE'.


-spec open_flag_translate_from_protobuf('READ_WRITE' | 'READ' | 'WRITE') ->
    fslogic_worker:open_flag().
open_flag_translate_from_protobuf('READ') ->
    read;
open_flag_translate_from_protobuf('WRITE') ->
    write;
open_flag_translate_from_protobuf(_) ->
    rdwr.
