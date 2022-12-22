%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides basic functionality for navigating through Oneprovider's
%%% files tree. The structure of it is presented and described below.
%%%
%%%                       +----------+
%%%                       |  !ROOT   |_______________________
%%%                       +----------+                \      \
%%%                   ____/  / | \   \_____            \      \
%%%                  /      /  |  \        \            \      \
%%%  ==============================================================================================
%%%                /      /    |    \        \            \     |   |
%%%               /      /     |     \        \            \    |   |
%%%    +-----------+    /      |      \     +-----------+   |  ...  |
%%%    |  @user1   |   /       |       \    |  @user2   |   |       |
%%%    +-----------+  /        |        \   +-----------+   |       |
%%%         |        /         |         \     / |          |       |
%%%  ===============================================================|
%%%         |      /           |    _______\_/   |          |       |
%%%         |     /            |   /        \    |          |       |
%%%    +----------+      +----------+      +----------+    ...      |
%%%    | #space1  |      | #space2  |      | #*space3 | ____________|_______________________
%%%    +----------+      +----------+      +----------+             |        |        |     \
%%%         |                  |            |    |   \              |  +----------+   |      \
%%%  ===============================================================|  | ^share1  |   |       \
%%%         |                  |            |    |     \            |  +----------+   |        |
%%%        ...                ...           |    |      \___________|_______/         |        |
%%%                                         |    |                  |                 |        |
%%%            _____________________________/    |                  |                 |        |
%%%           /                                  |                  |           +----------+   |
%%%          /                                   |                  |           | ^share2  |   |
%%%    +----------+                        +----------+             |           +----------+   |
%%%    |   file1  |                        |  *dir1   |_____________|_________________/        |
%%%    +----------+                        +----------+             |                          |
%%%                                          /       \              |                    +----------+
%%%                                         /         \             |                    | ^share3  |
%%%                             +----------+          +----------+  |                    +----------+
%%%                             |   dir2   |          |  *file2  |__|__________________________/
%%%                             +----------+          +----------+  |
%%%                             /    |    \                         |
%%%                            ...  ...   ...                       |
%%%                                                                 |
%%%
%%% Description:
%%% 1) !ROOT - directory marked as parent for all user_root and space directories.
%%%            It is used internally by Oneprovider (it is local for each provider)
%%%            and as such cannot be modified nor listed.
%%% 2) @user - user root directory (mount root after mounting Oneclient).
%%%            Listing it returns all spaces that user belongs to.
%%%            Similarly to !ROOT it is local to each provider and cannot be modified.
%%% 3) #space - space directory. It is treated as normal directory and as such it's
%%%             modification is controlled by access rights.
%%%             All documents associated with it (and files/dirs inside of it)
%%%             are synchronized among providers supporting this space.
%%% 4) ^share - share root directory. It is virtual directory (there are no associated
%%%             documents in the db) that is being used in 'open_handle' mode. In that
%%%             mode listing space directory returns list of share root dirs instead
%%%             of regular files/dirs in the space so that only shared content can be
%%%             viewed (from this point down the tree the context is changed to shared one).
%%%             In the future it will be used as mount root when mounting Oneclient
%%%             for share with open handle (in such case it will be treated as root
%%%             dir with no parent).
%%% 5) *file - share root file. It is directly shared file or directory or space and
%%%            the only child of share root dir (in 'open_handle' mode)
%%% 6) file/dir - regular file/directory.
%%% @end
%%%--------------------------------------------------------------------
-module(file_tree).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    get_parent_guid_if_not_root_dir/2,
    get_original_parent/2,
    get_parent/2,

    get_child/3, list_children/3, list_children/4
]).

-type children_whitelist() :: undefined | [file_meta:name()].

-export_type([children_whitelist/0]).


-define(DEFAULT_LS_BATCH_LIMIT, op_worker:get_env(default_ls_batch_limit, 5000)).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns 'undefined' if file is root file (either userRootDir or share root)
%% or proper ParentGuid otherwise.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_guid_if_not_root_dir(file_ctx:ctx(), undefined | user_ctx:ctx()) ->
    {undefined | file_id:file_guid(), file_ctx:ctx()}.
get_parent_guid_if_not_root_dir(FileCtx, UserCtx) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    {ParentCtx, FileCtx2} = get_parent(FileCtx, UserCtx),

    case file_ctx:get_logical_guid_const(ParentCtx) of
        FileGuid -> {undefined, FileCtx2};
        ParentGuid -> {ParentGuid, FileCtx2}
    end.


%%--------------------------------------------------------------------
%% @doc
%% This function returns original parent of a file.
%% It means that it checks whether file is not a child of trash.
%% If it is, it returns ctx() of directory which was parent of the file
%% before it was moved to trash.
%% TODO VFS-7133 original parent uuid should be stored in file_meta doc
%% @end
%%--------------------------------------------------------------------
-spec get_original_parent(file_ctx:ctx(), undefined | file_ctx:ctx()) ->
    {file_ctx:ctx(), file_ctx:ctx()}.
get_original_parent(FileCtx, undefined) ->
    get_parent(FileCtx, undefined);
get_original_parent(FileCtx, OriginalParentCtx) ->
    {ParentCtx, FileCtx2} = get_parent(FileCtx, undefined),
    case file_ctx:is_trash_dir_const(ParentCtx) of
        true ->
            {OriginalParentCtx, FileCtx2};
        false ->
            {ParentCtx, FileCtx2}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns parent's file context. In case of user root dir and share root
%% dir/file returns the same file_ctx. Therefore, to check if given
%% file_ctx points to root dir (either user root dir or share root) it is
%% enough to call this function and compare returned parent ctx with its own
%% (by using e.g. 'file_ctx:equals').
%% @end
%%--------------------------------------------------------------------
-spec get_parent(file_ctx:ctx(), undefined | user_ctx:ctx()) ->
    {ParentFileCtx :: file_ctx:ctx(), NewFileCtx :: file_ctx:ctx()}.
get_parent(FileCtx, UserCtx) ->
    case file_ctx:get_cached_parent_const(FileCtx) of
        undefined ->
            get_parent_internal(FileCtx, UserCtx);
        ParentCtx ->
            {ParentCtx, FileCtx}
    end.


-spec get_child(file_ctx:ctx(), file_meta:name(), user_ctx:ctx()) ->
    {ChildCtx :: file_ctx:ctx(), file_ctx:ctx()} | no_return().
get_child(FileCtx, Name, UserCtx) ->
    {ChildCtx, NewFileCtx} = case file_ctx:is_root_dir_const(FileCtx) of
        true ->
            get_user_root_dir_child(UserCtx, FileCtx, Name);
        false ->
            case file_ctx:is_share_root_dir_const(FileCtx) of
                true ->
                    get_share_root_dir_child(UserCtx, FileCtx, Name);
                false ->
                    case is_space_dir_accessed_in_open_handle_mode(UserCtx, FileCtx) of
                        true ->
                            get_space_share_child(FileCtx, Name, UserCtx);
                        false ->
                            get_dir_child(FileCtx, Name)
                    end
            end
    end,
    {file_ctx:cache_parent(NewFileCtx, ChildCtx), NewFileCtx}.


-spec list_children(file_ctx:ctx(), user_ctx:ctx(), file_listing:options()) ->
    {[file_ctx:ctx()], file_listing:pagination_token(), file_ctx:ctx()}.
list_children(FileCtx, UserCtx, ListOpts) ->
    list_children(FileCtx, UserCtx, ListOpts, undefined).


-spec list_children(
    file_ctx:ctx(),
    user_ctx:ctx(),
    file_listing:options(),
    children_whitelist()
) ->
    {[file_ctx:ctx()], file_listing:pagination_token(), file_ctx:ctx()}.
list_children(FileCtx, UserCtx, ListOpts, ChildrenWhiteList) ->
    case file_ctx:is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            get_user_root_dir_children(UserCtx, FileCtx, ListOpts, ChildrenWhiteList);
        false ->
            case file_ctx:is_share_root_dir_const(FileCtx) of
                true ->
                    list_share_root_dir_children(UserCtx, FileCtx, ChildrenWhiteList);
                false ->
                    case is_space_dir_accessed_in_open_handle_mode(UserCtx, FileCtx) of
                        true ->
                            get_space_open_handle_shares(
                                UserCtx, FileCtx, ListOpts, ChildrenWhiteList
                            );
                        false ->
                            list_file_children(FileCtx, ListOpts, ChildrenWhiteList)
                    end
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_parent_internal(file_ctx:ctx(), undefined | user_ctx:ctx()) ->
    {ParentFileCtx :: file_ctx:ctx(), NewFileCtx :: file_ctx:ctx()}.
get_parent_internal(FileCtx, UserCtx) ->
    FileGuid = file_ctx:get_logical_guid_const(FileCtx),
    {FileUuid, SpaceId, ShareId} = file_id:unpack_share_guid(FileGuid),
    {Doc, FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
    {ok, ParentUuid} = file_meta:get_parent_uuid(Doc),

    IsShareRootFile = case ShareId of
        undefined ->
            false;
        _ ->
            % ShareId is added to file_meta.shares only for directly shared
            % files/directories and not their children
            lists:member(ShareId, Doc#document.value#file_meta.shares)
    end,

    Parent = case {
        fslogic_file_id:is_root_dir_uuid(ParentUuid),
        IsShareRootFile,
        (UserCtx =/= undefined andalso user_ctx:is_in_open_handle_mode(UserCtx))
    } of
        {_, true, true} ->
            % Share root file shall point to virtual share root dir in open handle mode
            ShareRootDirUuid = fslogic_file_id:shareid_to_share_root_dir_uuid(ShareId),
            file_ctx:new_by_uuid(ShareRootDirUuid, SpaceId, ShareId);
        {true, false, _} ->
            case ParentUuid =:= ?GLOBAL_ROOT_DIR_UUID
                andalso UserCtx =/= undefined
                andalso user_ctx:is_normal_user(UserCtx)
            of
                true ->
                    case file_ctx:is_user_root_dir_const(FileCtx2, UserCtx) of
                        true ->
                            FileCtx2;
                        false ->
                            UserId = user_ctx:get_user_id(UserCtx),
                            file_ctx:new_by_guid(fslogic_file_id:user_root_dir_guid(UserId))
                    end;
                _ ->
                    file_ctx:new_by_guid(fslogic_file_id:root_dir_guid())
            end;
        {true, true, _} ->
            case fslogic_file_id:is_space_dir_uuid(FileUuid) of
                true ->
                    FileCtx2;
                false ->
                    % userRootDir and globalRootDir can not be shared
                    throw(?EINVAL)
            end;
        {false, false, IsInOpenHandleMode} ->
            case file_ctx:is_share_root_dir_const(FileCtx2) of
                true ->
                    case IsInOpenHandleMode of
                        true ->
                            % Virtual share root dir should point to normal space dir
                            file_ctx:new_by_uuid(ParentUuid, SpaceId);
                        false ->
                            throw(?ENOENT)
                    end;
                false ->
                    file_ctx:new_by_uuid(ParentUuid, SpaceId, ShareId)
            end;
        {false, true, _} ->
            FileCtx2
    end,

    {Parent, file_ctx:cache_parent(Parent, FileCtx2)}.


%% @private
-spec get_user_root_dir_child(user_ctx:ctx(), file_ctx:ctx(), file_meta:name()) ->
    {ChildCtx :: file_ctx:ctx(), file_ctx:ctx()} | no_return().
get_user_root_dir_child(UserCtx, UserRootDirCtx, Name) ->
    UserDoc = user_ctx:get_user(UserCtx),
    SessId = user_ctx:get_session_id(UserCtx),

    ChildGuid = case user_logic:get_space_by_name(SessId, UserDoc, Name) of
        {true, SpaceId} ->
            fslogic_file_id:spaceid_to_space_dir_guid(SpaceId);
        false ->
            case user_ctx:is_root(UserCtx) of
                true -> fslogic_file_id:spaceid_to_space_dir_guid(Name);
                false -> throw(?ENOENT)
            end
    end,
    {file_ctx:new_by_guid(ChildGuid), UserRootDirCtx}.


%% @private
-spec get_user_root_dir_children(
    user_ctx:ctx(),
    file_ctx:ctx(),
    file_listing:options(),
    children_whitelist()
) ->
    {[file_ctx:ctx()], file_listing:pagination_token(), file_ctx:ctx()}.
get_user_root_dir_children(UserCtx, UserRootDirCtx, ListOpts, SpaceWhiteList) ->
    % offset can be negative if last_name is passed too
    Offset = max(maps:get(offset, ListOpts, 0), 0),
    Limit = maps:get(limit, ListOpts, ?DEFAULT_LS_BATCH_LIMIT),

    AllUserSpaces = user_ctx:get_eff_spaces(UserCtx),

    FilteredSpaces = case SpaceWhiteList of
        undefined ->
            AllUserSpaces;
        _ ->
            lists:filter(fun(Space) ->
                lists:member(Space, SpaceWhiteList)
            end, AllUserSpaces)
    end,
    SessId = user_ctx:get_session_id(UserCtx),

    Children = case Offset < length(FilteredSpaces) of
        true ->
            SessId = user_ctx:get_session_id(UserCtx),
            
            GroupedSpaces = lists:foldl(fun(SpaceId, Acc) ->
                {ok, SpaceName} = space_logic:get_name(SessId, SpaceId),
                Acc#{SpaceName => [SpaceId | maps:get(SpaceName, Acc, [])]}
            end, #{}, FilteredSpaces),

            SpacesChunk = lists:sublist(
                lists:sort(maps:fold(
                    fun (Name, [SpaceId], Acc) -> [{Name, SpaceId} | Acc];
                        (Name, Spaces, Acc) -> Acc ++ lists:map(fun(SpaceId) ->
                            {<<Name/binary, (?SPACE_NAME_ID_SEPARATOR)/binary, SpaceId/binary>>, SpaceId}
                        end, Spaces)
                end, [], GroupedSpaces)),
                Offset + 1,
                Limit
            ),
            lists:map(fun({SpaceName, SpaceId}) ->
                SpaceDirUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
                file_ctx:new_by_uuid(SpaceDirUuid, SpaceId, undefined, SpaceName)
            end, SpacesChunk);
        false ->
            []
    end,
    build_listing_result(UserCtx, Children, Limit, UserRootDirCtx).


%% @private
-spec get_space_share_child(file_ctx:ctx(), file_meta:name(), user_ctx:ctx()) ->
    {ChildCtx :: file_ctx:ctx(), file_ctx:ctx()} | no_return().
get_space_share_child(SpaceDirCtx, Name, UserCtx) ->
    SpaceId = file_ctx:get_space_id_const(SpaceDirCtx),
    SessId = user_ctx:get_session_id(UserCtx),
    {ok, Shares} = space_logic:get_shares(SessId, SpaceId),

    case lists:member(Name, Shares) of
        true ->
            ChildUuid = fslogic_file_id:shareid_to_share_root_dir_uuid(Name),
            {file_ctx:new_by_uuid(ChildUuid, SpaceId, Name), SpaceDirCtx};
        false ->
            throw(?ENOENT)
    end.


%% @private
-spec get_space_open_handle_shares(
    user_ctx:ctx(),
    file_ctx:ctx(),
    file_listing:options(),
    children_whitelist()
) ->
    {[file_ctx:ctx()], file_listing:pagination_token(), file_ctx:ctx()}.
get_space_open_handle_shares(UserCtx, SpaceDirCtx, ListOpts, ShareWhiteList) ->
    % offset can be negative if last_name is passed too
    Offset = max(maps:get(offset, ListOpts, 0), 0),
    Limit = maps:get(size, ListOpts, ?DEFAULT_LS_BATCH_LIMIT),

    SessId = user_ctx:get_session_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(SpaceDirCtx),
    {ok, AllSpaceShares} = space_logic:get_shares(SessId, SpaceId),

    IsOpenHandleShare = fun(ShareId) ->
        case share_logic:get(SessId, ShareId) of
            {ok, #document{value = #od_share{handle = <<_/binary>>}}} -> true;
            _ -> false
        end
    end,

    FilteredShares = case ShareWhiteList of
        undefined ->
            lists:filter(IsOpenHandleShare, AllSpaceShares);
        _ ->
            lists:filter(fun(ShareId) ->
                lists:member(ShareId, ShareWhiteList) andalso IsOpenHandleShare(ShareId)
            end, AllSpaceShares)
    end,

    Children = case Offset < length(FilteredShares) of
        true ->
            lists:map(fun(ShareId) ->
                ShareDirUuid = fslogic_file_id:shareid_to_share_root_dir_uuid(ShareId),
                file_ctx:new_by_uuid(ShareDirUuid, SpaceId, ShareId, ShareId)
            end, lists:sublist(lists:sort(FilteredShares), Offset + 1, Limit));
        false ->
            []
    end,
    build_listing_result(UserCtx, Children, Limit, SpaceDirCtx).


%% @private
-spec get_share_root_dir_child(user_ctx:ctx(), file_ctx:ctx(), file_meta:name()) ->
    {ChildCtx :: file_ctx:ctx(), file_ctx:ctx()} | no_return().
get_share_root_dir_child(UserCtx, ShareRootDirCtx, Name) ->
    ShareId = file_ctx:get_share_id_const(ShareRootDirCtx),
    ChildCtx = get_share_root_file(UserCtx, ShareId),

    case file_ctx:get_aliased_name(ChildCtx, UserCtx) of
        {Name, ChildCtx2} ->
            {ChildCtx2, ShareRootDirCtx};
        _ ->
            throw(?ENOENT)
    end.


%% @private
-spec list_share_root_dir_children(user_ctx:ctx(), file_ctx:ctx(), children_whitelist()) ->
    {[file_ctx:ctx()], file_listing:pagination_token(), file_ctx:ctx()}.
list_share_root_dir_children(UserCtx, ShareRootDirCtx, FileWhiteList) ->
    ShareId = file_ctx:get_share_id_const(ShareRootDirCtx),
    ChildCtx = get_share_root_file(UserCtx, ShareId),

    Children = case FileWhiteList of
        undefined ->
            [ChildCtx];
        _ ->
            {ChildName, ChildCtx2} = file_ctx:get_aliased_name(ChildCtx, UserCtx),
            case lists:member(ChildName, FileWhiteList) of
                true -> [ChildCtx2];
                false -> []
            end
    end,
    build_listing_result(UserCtx, Children, ?DEFAULT_LS_BATCH_LIMIT, ShareRootDirCtx).


%% @private
-spec get_share_root_file(user_ctx:ctx(), od_share:id()) -> file_ctx:ctx().
get_share_root_file(UserCtx, ShareId) ->
    SessId = user_ctx:get_session_id(UserCtx),
    {ok, #document{value = ShareRec}} = share_logic:get(SessId, ShareId),

    file_ctx:new_by_guid(ShareRec#od_share.root_file).


%% @private
-spec get_dir_child(file_ctx:ctx(), file_meta:name()) ->
    {ChildCtx :: file_ctx:ctx(), file_ctx:ctx()} | no_return().
get_dir_child(FileCtx, Name) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),

    case file_meta:get_type(FileDoc) of
        ?DIRECTORY_TYPE ->
            case canonical_path:resolve(FileDoc, <<"/", Name/binary>>) of
                {ok, ChildDoc} ->
                    SpaceId = file_ctx:get_space_id_const(FileCtx),
                    ShareId = file_ctx:get_share_id_const(FileCtx),
                    {file_ctx:new_by_doc(ChildDoc, SpaceId, ShareId), FileCtx2};
                {error, not_found} ->
                    throw(?ENOENT)
            end;
        _ ->
            throw(?ENOTDIR)
    end.


%% @private
-spec list_file_children(file_ctx:ctx(), file_listing:options(), children_whitelist()) ->
    {[file_ctx:ctx()], file_listing:pagination_token(), file_ctx:ctx()}.
list_file_children(FileCtx, ListOpts, ChildrenWhiteList) ->
    {#document{} = FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, FileUuid} = file_meta:get_uuid(FileDoc),

    case file_meta:get_type(FileDoc) of
        ?DIRECTORY_TYPE ->
            FileGuid = file_ctx:get_logical_guid_const(FileCtx),
            {_FileUuid, SpaceId, ShareId} = file_id:unpack_share_guid(FileGuid),

            {ok, ChildrenLinks, ListingToken} = 
                file_listing:list(FileUuid, ListOpts#{whitelist => ChildrenWhiteList}),
            Children = lists:map(fun({Name, Uuid}) ->
                file_ctx:new_by_uuid(Uuid, SpaceId, ShareId, Name)
            end, ChildrenLinks),

            {Children, ListingToken, FileCtx2};
        _ ->
            % In case of listing regular file - return it
            PaginationToken = file_listing:infer_pagination_token(
                [FileCtx2], undefined, ?DEFAULT_LS_BATCH_LIMIT),
            {[FileCtx2], PaginationToken, FileCtx2}
    end.


%% @private
-spec is_space_dir_accessed_in_open_handle_mode(user_ctx:ctx(), file_ctx:ctx()) ->
    boolean().
is_space_dir_accessed_in_open_handle_mode(UserCtx, FileCtx) ->
    ShareId = file_ctx:get_share_id_const(FileCtx),
    IsSpaceDir = file_ctx:is_space_dir_const(FileCtx),
    IsInOpenHandleMode = user_ctx:is_in_open_handle_mode(UserCtx),

    IsSpaceDir andalso IsInOpenHandleMode andalso ShareId == undefined.


%% @private
-spec build_listing_result(user_ctx:ctx(), [file_ctx:ctx()], file_listing:limit(), file_ctx:ctx()) -> 
    {[file_ctx:ctx()], file_listing:pagination_token(), file_ctx:ctx()}.
build_listing_result(_UserCtx, [], Limit, ListedCtx) ->
    {[], file_listing:infer_pagination_token([], undefined, Limit), ListedCtx};
build_listing_result(UserCtx, Children, Limit, ListedCtx) ->
    LastFileCtx = lists:last(Children),
    {Name, _} = file_ctx:get_aliased_name(LastFileCtx, UserCtx),
    {Children, file_listing:infer_pagination_token(Children, Name, Limit), ListedCtx}.
