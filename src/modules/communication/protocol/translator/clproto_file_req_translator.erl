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
-module(clproto_file_req_translator).
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
from_protobuf(#'FileRequest'{
    context_guid = ContextGuid,
    file_request = {_, Record}
}) ->
    #file_request{
        context_guid = ContextGuid,
        file_request = from_protobuf(Record)
    };
from_protobuf(#'GetFileAttr'{include_replication_status = IRS, include_link_count = ILC, xattrs = Xattrs}) ->
    #get_file_attr{attributes = ?BASIC_ATTRS ++ [size | attrs_flags_to_attrs_list(IRS, ILC)] ++ xattrs_to_attrs_list(Xattrs)};
from_protobuf(#'GetFileAttrByPath'{path = Path, xattrs = Xattrs}) ->
    #get_file_attr_by_path{path = Path, attributes = ?BASIC_ATTRS ++ [size | xattrs_to_attrs_list(Xattrs)]};
from_protobuf(#'GetChildAttr'{
    name = Name,
    include_replication_status = IRS,
    include_link_count = ILC,
    xattrs = Xattrs
}) ->
    #get_child_attr{
        name = Name,
        attributes = ?BASIC_ATTRS ++ [size | attrs_flags_to_attrs_list(IRS, ILC)] ++ xattrs_to_attrs_list(Xattrs)
    };
from_protobuf(#'GetFileChildren'{
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
from_protobuf(#'GetFileChildrenAttrs'{
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
        attributes = ?BASIC_ATTRS ++ [size | attrs_flags_to_attrs_list(IRS, ILC)] ++ xattrs_to_attrs_list(Xattrs)
    };
from_protobuf(#'CreateDir'{
    name = Name,
    mode = Mode
}) ->
    #create_dir{
        name = Name,
        mode = Mode
    };
from_protobuf(#'CreatePath'{
    path = Path
}) ->
    #create_path{
        path = Path
    };
from_protobuf(#'DeleteFile'{
    silent = Silent
}) ->
    #delete_file{
        silent = Silent
    };
from_protobuf(#'UpdateTimes'{
    atime = ATime,
    mtime = MTime,
    ctime = CTime
}) ->
    #update_times{
        atime = ATime,
        mtime = MTime,
        ctime = CTime
    };
from_protobuf(#'ChangeMode'{mode = Mode}) ->
    #change_mode{mode = Mode};
from_protobuf(#'Rename'{
    target_parent_uuid = TargetParentGuid,
    target_name = TargetName
}) ->
    #rename{
        target_parent_guid = TargetParentGuid,
        target_name = TargetName
    };
from_protobuf(#'CreateFile'{
    name = Name,
    mode = Mode,
    flag = Flag
}) ->
    #create_file{
        name = Name,
        mode = Mode,
        flag = open_flag_translate_from_protobuf(Flag)
    };
from_protobuf(#'GetXattr'{
    name = Name,
    inherited = Inherited
}) ->
    #get_xattr{
        name = Name,
        inherited = Inherited
    };
from_protobuf(#'SetXattr'{
    xattr = Xattr,
    create = Create,
    replace = Replace
}) ->
    #set_xattr{
        xattr = clproto_fuse_translator:from_protobuf(Xattr),
        create = Create,
        replace = Replace
    };
from_protobuf(#'RemoveXattr'{name = Name}) ->
    #remove_xattr{name = Name};
from_protobuf(#'ListXattr'{
    inherited = Inherited,
    show_internal = ShowInternal
}) ->
    #list_xattr{
        inherited = Inherited,
        show_internal = ShowInternal
    };
from_protobuf(#'StorageFileCreated'{}) ->
    #storage_file_created{};
from_protobuf(#'MakeFile'{
    name = Name,
    mode = Mode
}) ->
    #make_file{
        name = Name,
        mode = Mode
    };
from_protobuf(#'MakeLink'{
    target_parent_uuid = TargetParentGuid,
    target_name = Name
}) ->
    #make_link{
        target_parent_guid = TargetParentGuid,
        target_name = Name
    };
from_protobuf(#'MakeSymlink'{
    target_name = TargetName,
    link = Link
}) ->
    #make_symlink{
        target_name = TargetName,
        link = Link
    };
from_protobuf(#'OpenFile'{flag = Flag}) ->
    #open_file{flag = open_flag_translate_from_protobuf(Flag)};
from_protobuf(#'OpenFileWithExtendedInfo'{flag = Flag}) ->
    #open_file_with_extended_info{
        flag = open_flag_translate_from_protobuf(Flag)
    };
from_protobuf(#'GetFileLocation'{}) ->
    #get_file_location{};
from_protobuf(#'ReadSymlink'{}) ->
    #read_symlink{};
from_protobuf(#'Release'{handle_id = HandleId}) ->
    #release{handle_id = HandleId};
from_protobuf(#'Truncate'{size = Size}) ->
    #truncate{size = Size};
from_protobuf(#'SynchronizeBlock'{
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
from_protobuf(#'SynchronizeBlockAndComputeChecksum'{
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
from_protobuf(#'BlockSynchronizationRequest'{
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
from_protobuf(#'FSync'{
    data_only = DataOnly,
    handle_id = HandleId
}) ->
    #fsync{
        data_only = DataOnly,
        handle_id = HandleId
    };

%% OTHER
from_protobuf(undefined) -> undefined.


-spec to_protobuf(tuple()) -> tuple(); (undefined) -> undefined.
to_protobuf(#file_request{
    context_guid = ContextGuid,
    file_request = Record
}) ->
    {file_request, #'FileRequest'{
        context_guid = ContextGuid,
        file_request = to_protobuf(Record)}
    };
to_protobuf(#get_file_attr{attributes = Attributes}) ->
    {IRS, ILC} = attributes_to_attrs_flags(Attributes),
    {get_file_attr, #'GetFileAttr'{include_replication_status = IRS, include_link_count = ILC}};
to_protobuf(#get_file_attr_by_path{path = Path}) ->
    {get_file_attr_by_path, #'GetFileAttrByPath'{path = Path}};
to_protobuf(#get_child_attr{name = Name, attributes = Attributes}) ->
    {IRS, ILC} = attributes_to_attrs_flags(Attributes),
    {get_child_attr, #'GetChildAttr'{name = Name, include_replication_status = IRS, include_link_count = ILC}};
to_protobuf(#get_file_children{listing_options = ListingOpts}) ->
    {get_file_children, #'GetFileChildren'{
        offset = maps:get(offset, ListingOpts, undefined),
        size = maps:get(limit, ListingOpts, undefined),
        index_token = case maps:get(pagination_token, ListingOpts, undefined) of
            undefined -> undefined;
            PaginationToken -> file_listing:encode_pagination_token(PaginationToken)
        end,
        index_startid = maps:get(index, ListingOpts, undefined)
    }};
to_protobuf(#get_file_children_attrs{
    listing_options = ListingOpts,
    attributes = Attributes
}) ->
    {IRS, ILC} = attributes_to_attrs_flags(Attributes),
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
to_protobuf(#create_dir{
    name = Name,
    mode = Mode
}) ->
    {create_dir, #'CreateDir'{
        name = Name,
        mode = Mode
    }};
to_protobuf(#create_path{
    path = Path
}) ->
    {create_path, #'CreatePath'{
        path = Path
    }};
to_protobuf(#delete_file{silent = Silent}) ->
    {delete_file, #'DeleteFile'{silent = Silent}};
to_protobuf(#update_times{
    atime = ATime,
    mtime = MTime,
    ctime = CTime
}) ->
    {update_times, #'UpdateTimes'{
        atime = ATime,
        mtime = MTime,
        ctime = CTime
    }};
to_protobuf(#change_mode{mode = Mode}) ->
    {change_mode, #'ChangeMode'{mode = Mode}};
to_protobuf(#rename{
    target_parent_guid = TargetParentGuid,
    target_name = TargetName
}) ->
    {rename, #'Rename'{
        target_parent_uuid = TargetParentGuid,
        target_name = TargetName
    }};
to_protobuf(#create_file{
    name = Name,
    mode = Mode,
    flag = Flag
}) ->
    {create_file, #'CreateFile'{
        name = Name,
        mode = Mode,
        flag = open_flag_translate_to_protobuf(Flag)}
    };
to_protobuf(#get_xattr{
    name = Name,
    inherited = Inherited
}) ->
    {get_xattr, #'GetXattr'{
        name = Name,
        inherited = Inherited
    }};
to_protobuf(#set_xattr{
    xattr = Xattr,
    create = Create,
    replace = Replace
}) ->
    {_, XattrT} = clproto_fuse_translator:to_protobuf(Xattr),
    {set_xattr, #'SetXattr'{
        xattr = XattrT,
        create = Create,
        replace = Replace
    }};
to_protobuf(#remove_xattr{name = Name}) ->
    {remove_xattr, #'RemoveXattr'{name = Name}};
to_protobuf(#list_xattr{
    inherited = Inherited,
    show_internal = ShowInternal
}) ->
    {list_xattr, #'ListXattr'{
        inherited = Inherited,
        show_internal = ShowInternal
    }};
to_protobuf(#storage_file_created{}) ->
    {storage_file_created, #'StorageFileCreated'{}};
to_protobuf(#make_file{
    name = Name,
    mode = Mode
}) ->
    {make_file, #'MakeFile'{
        name = Name,
        mode = Mode
    }};
to_protobuf(#make_link{
    target_parent_guid = TargetParentGuid,
    target_name = Name
}) ->
    {make_link, #'MakeLink'{
        target_parent_uuid = TargetParentGuid,
        target_name = Name
    }};
to_protobuf(#make_symlink{
    target_name = TargetName,
    link = Link
}) ->
    {make_symlink, #'MakeSymlink'{
        target_name = TargetName,
        link = Link
    }};
to_protobuf(#open_file{flag = Flag}) ->
    {open_file, #'OpenFile'{
        flag = open_flag_translate_to_protobuf(Flag)}
    };
to_protobuf(#open_file_with_extended_info{flag = Flag}) ->
    {open_file_with_extended_info, #'OpenFileWithExtendedInfo'{
        flag = open_flag_translate_to_protobuf(Flag)}
    };
to_protobuf(#get_file_location{}) ->
    {get_file_location, #'GetFileLocation'{}};
to_protobuf(#read_symlink{}) ->
    {read_symlink, #'ReadSymlink'{}};
to_protobuf(#release{handle_id = HandleId}) ->
    {release, #'Release'{handle_id = HandleId}};
to_protobuf(#truncate{size = Size}) ->
    {truncate, #'Truncate'{size = Size}};
to_protobuf(#synchronize_block{
    block = Block,
    prefetch = Prefetch,
    priority = Priority}
) ->
    {synchronize_block, #'SynchronizeBlock'{
        block = clproto_common_translator:to_protobuf(Block),
        prefetch = Prefetch,
        priority = Priority
    }};
to_protobuf(#synchronize_block_and_compute_checksum{
    block = Block,
    prefetch = Prefetch,
    priority = Priority
}) ->
    {synchronize_block_and_compute_checksum,
        #'SynchronizeBlockAndComputeChecksum'{
            block = clproto_common_translator:to_protobuf(Block),
            prefetch = Prefetch,
            priority = Priority
        }
    };
to_protobuf(#block_synchronization_request{
    block = Block,
    prefetch = Prefetch,
    priority = Priority
}) ->
    {block_synchronization_request, #'BlockSynchronizationRequest'{
        block = clproto_common_translator:to_protobuf(Block),
        prefetch = Prefetch,
        priority = Priority
    }};
to_protobuf(#fsync{
    data_only = DataOnly,
    handle_id = HandleId
}) ->
    {fsync, #'FSync'{
        data_only = DataOnly,
        handle_id = HandleId
    }};


%% OTHER
to_protobuf(undefined) -> undefined.


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


-spec attributes_to_attrs_flags([file_attr:attribute()]) ->
    {boolean(), boolean()}.
attributes_to_attrs_flags(AttributesList) ->
    IRS = lists:member(is_fully_replicated, AttributesList),
    ILC = lists:member(link_count, AttributesList),
    {IRS, ILC}.


-spec attrs_flags_to_attrs_list(boolean() | undefined, boolean() | undefined) ->
    [file_attr:attribute()].
attrs_flags_to_attrs_list(true = _IRS, true = _ILC) ->
    [is_fully_replicated, link_count];
attrs_flags_to_attrs_list(true = _IRS, _ILC) ->
    [is_fully_replicated];
attrs_flags_to_attrs_list(_IRS, true = _ILC) ->
    [link_count];
attrs_flags_to_attrs_list(_IRS, _ILC) ->
    [].

-spec xattrs_to_attrs_list([custom_metadata:name()]) -> [file_attr:attribute()].
xattrs_to_attrs_list([])     -> [];
xattrs_to_attrs_list(Xattrs) -> [{xattrs, Xattrs}].
