%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module handling translations between protobuf and internal protocol.
%%% op-worker is the server of these messages, so requests are translated
%%% only from protobuf and responses only to protobuf. FileAttr and
%%% FileLocation are also decoded, as parts of the events a client may send.
%%% @end
%%%-------------------------------------------------------------------
-module(clproto_fuse_translator).
-author("Tomasz Lichon").
-author("Rafal Slota").

-include("proto/oneclient/fuse_messages.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([
    from_protobuf/1, to_protobuf/1
]).

%%%===================================================================
%%% API
%%%===================================================================

-spec from_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
from_protobuf(#'FuseRequest'{
    fuse_request = {_, Record}
}) ->
    #fuse_request{
        fuse_request = from_protobuf(Record)
    };
from_protobuf(#'ResolveGuid'{
    path = Path
}) ->
    #resolve_guid{
        path = Path
    };
from_protobuf(#'GetHelperParams'{
    storage_id = StorageId,
    space_id = SpaceId,
    helper_mode = HelperMode
}) ->
    #get_helper_params{
        storage_id = StorageId,
        space_id = SpaceId,
        helper_mode = HelperMode
    };
from_protobuf(#'GetFSStats'{
    file_id = FileGuid
}) ->
    #get_fs_stats{
        file_id = FileGuid
    };
from_protobuf(#'CreateStorageTestFile'{
    storage_id = Id,
    file_uuid = FileGuid
}) ->
    #create_storage_test_file{
        storage_id = Id,
        file_guid = FileGuid
    };
from_protobuf(#'VerifyStorageTestFile'{
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
from_protobuf(#'FileAttr'{} = FileAttr) ->
    Xattrs = lists:foldl(fun(Xattr, Acc) ->
        #xattr{name = Name, value = Value} = from_protobuf(Xattr),
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
        %% @TODO VFS-11722 - send undefined to oneclient
        provider_id = utils:ensure_defined(FileAttr#'FileAttr'.provider_id, <<"unknown">>, undefined),
        shares = FileAttr#'FileAttr'.shares,
        owner_id = utils:ensure_defined(FileAttr#'FileAttr'.owner_id, <<"unknown">>, undefined),
        is_fully_replicated = FileAttr#'FileAttr'.fully_replicated,
        hardlink_count = FileAttr#'FileAttr'.nlink,
        index = file_listing:decode_index(FileAttr#'FileAttr'.index),
        xattrs = Xattrs
    };
from_protobuf(#'Xattr'{
    name = Name,
    value = Value
}) ->
    #xattr{
        name = Name,
        value = json_utils:decode(Value)
    };
from_protobuf(#'FileLocation'{} = Record) ->
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
from_protobuf(#'FSync'{
    data_only = DataOnly,
    handle_id = HandleId
}) ->
    #fsync{
        data_only = DataOnly,
        handle_id = HandleId
    };


%% FILE REQUEST
from_protobuf(#'FileRequest'{} = Msg) -> clproto_file_req_translator:from_protobuf(Msg);

%% MULTIPART UPLOAD
from_protobuf(#'MultipartUploadRequest'{} = Msg) -> clproto_multipart_upload_translator:from_protobuf(Msg);

%% OTHER
from_protobuf(undefined) -> undefined;
from_protobuf(Other) -> clproto_common_translator:from_protobuf(Other).


-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
to_protobuf(#fuse_response{
    status = Status,
    fuse_response = FuseResponse
}) ->
    {status, StatProto} = clproto_common_translator:to_protobuf(Status),
    {fuse_response, #'FuseResponse'{
        status = StatProto,
        fuse_response = to_protobuf(FuseResponse)
    }};
to_protobuf(#child_link{
    guid = FileGuid,
    name = Name
}) ->
    #'ChildLink'{
        uuid = FileGuid,
        name = Name
    };
to_protobuf(#file_attr{} = FileAttr) ->
    TranslatedXattrs =
        maps:fold(fun(XattrName, XattrValue, Acc) ->
            {xattr, TranslatedXattr} = to_protobuf(
                #xattr{name = XattrName, value = XattrValue}),
            [TranslatedXattr | Acc]
        end, [], utils:ensure_defined(FileAttr#file_attr.xattrs, #{})),
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
            %% @TODO VFS-11728 - remove the safeguard after stats are fixed and can not be negative
            ?DIRECTORY_TYPE -> utils:ensure_defined(max(FileAttr#file_attr.size, 0), 0);
            _ -> FileAttr#file_attr.size
        end,
        %% @TODO VFS-11722 - send undefined to oneclient
        provider_id = utils:ensure_defined(FileAttr#file_attr.provider_id, <<"unknown">>),
        shares = utils:ensure_defined(FileAttr#file_attr.shares, []),
        owner_id = utils:ensure_defined(FileAttr#file_attr.owner_id, <<"unknown">>),
        fully_replicated = FileAttr#file_attr.is_fully_replicated,
        nlink = FileAttr#file_attr.hardlink_count,
        index = file_listing:encode_index(FileAttr#file_attr.index),
        xattrs = TranslatedXattrs
    }};
to_protobuf(#xattr{
    name = Name,
    value = Value
}) ->
    {xattr, #'Xattr'{
        name = Name,
        value = json_utils:encode(Value)
    }};
to_protobuf(#xattr_list{names = Names}) ->
    {xattr_list, #'XattrList'{names = Names}};
to_protobuf(#file_children{
    child_links = FileEntries,
    pagination_token = PaginationToken
}) ->
    {file_children, #'FileChildren'{
        child_links = [to_protobuf(E) || E <- FileEntries],
        index_token = file_listing:encode_pagination_token(PaginationToken),
        is_last = file_listing:is_finished(PaginationToken)
    }};
to_protobuf(#file_children_attrs{
    child_attrs = Children,
    pagination_token = ListingToken
}) ->
    {file_children_attrs, #'FileChildrenAttrs'{
        child_attrs = lists:map(fun(Child) ->
            {file_attr, Translated} = to_protobuf(Child),
            Translated
        end, Children),
        index_token = file_listing:encode_pagination_token(ListingToken),
        is_last = file_listing:is_finished(ListingToken)
    }};
to_protobuf(#file_location{
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
to_protobuf(#file_location_changed{
    file_location = FileLocation,
    change_beg_offset = O,
    change_end_offset = S
}) ->
    {_, Record} = to_protobuf(FileLocation),
    {file_location_changed, #'FileLocationChanged'{
        file_location = Record,
        change_beg_offset = O,
        change_end_offset = S
    }};
to_protobuf(#helper_params{
    helper_name = HelperName,
    helper_args = HelpersArgs
}) ->
    {helper_params, #'HelperParams'{
        helper_name = HelperName,
        helper_args = [to_protobuf(Arg) || Arg <- HelpersArgs]
    }};
to_protobuf(#helper_arg{
    key = Key,
    value = Value
}) ->
    #'HelperArg'{
        key = Key,
        value = Value
    };
to_protobuf(#storage_test_file{
    helper_params = HelperParams,
    space_id = SpaceId,
    file_id = FileId,
    file_content = FileContent
}) ->
    {_, Record} = to_protobuf(HelperParams),
    {storage_test_file, #'StorageTestFile'{
        helper_params = Record,
        space_id = SpaceId,
        file_id = FileId,
        file_content = FileContent
    }};
to_protobuf(#sync_response{
    checksum = Value,
    file_location_changed = FileLocationChanged
}) ->
    {_, ProtoFileLocationChanged} = to_protobuf(FileLocationChanged),
    {sync_response, #'SyncResponse'{
        checksum = Value,
        file_location_changed = ProtoFileLocationChanged
    }};
to_protobuf(#file_created{
    handle_id = HandleId,
    file_attr = FileAttr,
    file_location = FileLocation
}) ->
    {_, ProtoFileAttr} = to_protobuf(FileAttr),
    {_, ProtoFileLocation} = to_protobuf(FileLocation),
    {file_created, #'FileCreated'{
        handle_id = HandleId,
        file_attr = ProtoFileAttr,
        file_location = ProtoFileLocation
    }};
to_protobuf(#file_opened{
    handle_id = HandleId
}) ->
    {file_opened, #'FileOpened'{
        handle_id = HandleId
    }};
to_protobuf(#symlink{
    link = Link
}) ->
    {symlink, #'Symlink'{
        link = Link
    }};
to_protobuf(#file_opened_extended{} = Record) ->
    {file_opened_extended, #'FileOpenedExtended'{
        handle_id = Record#file_opened_extended.handle_id,
        provider_id = Record#file_opened_extended.provider_id,
        file_id = Record#file_opened_extended.file_id,
        storage_id = Record#file_opened_extended.storage_id
    }};
to_protobuf(#fs_stats{
    space_id = SpaceId,
    storage_stats = StorageStats
}) ->
    {fs_stats, #'FSStats'{
        space_id = SpaceId,
        storage_stats = [to_protobuf(Arg) || Arg <- StorageStats]
    }};
to_protobuf(#storage_stats{
    storage_id = StorageId,
    size = Size,
    occupied = Occupied
}) ->
    #'StorageStats'{
        storage_id = StorageId,
        size = Size,
        occupied = Occupied
    };
to_protobuf(#file_renamed{
    new_guid = NewGuid,
    child_entries = ChildEntries
}) ->
    {file_renamed, #'FileRenamed'{
        new_uuid = NewGuid,
        child_entries = [to_protobuf(E) || E <- ChildEntries]
    }};
to_protobuf(#guid{guid = Guid}) ->
    {uuid, #'Uuid'{
        uuid = Guid
    }};
to_protobuf(#file_recursive_listing_result{
    entries = Entries,
    pagination_token = PaginationToken
}) ->
    {files_list, #'FileList'{
        files = lists:map(fun(#file_attr{path = Path} = FileAttr) ->
            % put file path as name as that is expected by oneclient
            {file_attr, TranslatedFileAttr} = to_protobuf(FileAttr#file_attr{name = Path}),
            TranslatedFileAttr
        end, Entries),
        next_page_token = PaginationToken,
        is_last = PaginationToken == undefined
    }};


%% MULTIPART UPLOAD
to_protobuf(#multipart_parts{} = Msg) -> clproto_multipart_upload_translator:to_protobuf(Msg);
to_protobuf(#multipart_upload{} = Msg) -> clproto_multipart_upload_translator:to_protobuf(Msg);
to_protobuf(#multipart_uploads{} = Msg) -> clproto_multipart_upload_translator:to_protobuf(Msg);


%% OTHER
to_protobuf(undefined) -> undefined;
to_protobuf(Other) -> clproto_common_translator:to_protobuf(Other).
