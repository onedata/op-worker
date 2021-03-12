%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides basic functionality for navigating through files tree.
%%%
%%%
%%%
%%%
%%%
%%%                       +----------+
%%%                       |   ROOT   |-----------------------
%%%                       +----------+                \      \
%%%                   ----/  / | \   \-----            \      \
%%%                  /      /  |  \        \            \      \
%%%  ---------------/------/---|---\--------\------------\------|----------------------------------
%%%                /      /    |    \        \            \     |   |
%%%               /      /     |     \        \            \    |   |
%%%    +-----------+    /      |      \     +-----------+   |  ...  |
%%%    |   user1   |   /       |       \    |   user2   |   |       |
%%%    +-----------+  /        |        \   +-----------+   |       |
%%%         |        /         |         \     / |          |       |
%%%  -------|-------/----------|----------\---/--|----------|-------|
%%%         |      /           |    -------\--   |          |       |
%%%         |     /            |   /        \    |          |       |
%%%    +----------+      +----------+      +----------+    ...      |
%%%    |  space1  |      |  space2  |      |  space3  |             |
%%%    +----------+      +----------+      +----------+             |
%%%         |                  |                                    |
%%%  -------|------------------|------------------------------------|
%%%         |                  |                                    |
%%%        ...                ...                                   |
%%%                                                                 |
%%%                                                                 |
%%%                                                                 |
%%%                                                                 |
%%%                                                                 |
%%%                                                                 |
%%%                                                                 |
%%%                                                                 |
%%%                                                                 |
%%%  ----------------------------------------------------------------------------------------------
%%%
%%%  TODO ^ finish ascii art and describe onedata virtual file system someday
%%%
%%%
%%%
%%%
%%%
%%%
%%%
%%%
%%% @end
%%%--------------------------------------------------------------------
-module(files_tree).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_child/3, get_children/3, get_children/4]).

-type children_whitelist() :: undefined | [file_meta:name()].

-export_type([children_whitelist/0]).


-define(DEFAULT_LS_BATCH_SIZE, op_worker:get_env(ls_batch_size, 5000)).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_child(file_ctx:ctx(), file_meta:name(), user_ctx:ctx()) ->
    {ChildCtx :: file_ctx:ctx(), file_ctx:ctx()} | no_return().
get_child(FileCtx, Name, UserCtx) ->
    case file_ctx:is_root_dir_const(FileCtx) of
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
                            get_file_child(FileCtx, Name)
                    end
            end
    end.


-spec get_children(file_ctx:ctx(), user_ctx:ctx(), file_meta:list_opts()) ->
    {[file_ctx:ctx()], file_meta:list_extended_info(), file_ctx:ctx()}.
get_children(FileCtx, UserCtx, ListOpts) ->
    get_children(FileCtx, UserCtx, ListOpts, undefined).


-spec get_children(
    file_ctx:ctx(),
    user_ctx:ctx(),
    file_meta:list_opts(),
    children_whitelist()
) ->
    {[file_ctx:ctx()], file_meta:list_extended_info(), file_ctx:ctx()}.
get_children(FileCtx, UserCtx, ListOpts, ChildrenWhiteList) ->
    case file_ctx:is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            get_user_root_dir_children(UserCtx, FileCtx, ListOpts, ChildrenWhiteList);
        false ->
            case file_ctx:is_share_root_dir_const(FileCtx) of
                true ->
                    get_share_root_dir_children(UserCtx, FileCtx, ChildrenWhiteList);
                false ->
                    case is_space_dir_accessed_in_open_handle_mode(UserCtx, FileCtx) of
                        true ->
                            get_space_open_handle_shares(
                                UserCtx, FileCtx, ListOpts, ChildrenWhiteList
                            );
                        false ->
                            get_file_children(FileCtx, ListOpts, ChildrenWhiteList)
                    end
            end
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_user_root_dir_child(user_ctx:ctx(), file_ctx:ctx(), file_meta:name()) ->
    {ChildCtx :: file_ctx:ctx(), file_ctx:ctx()} | no_return().
get_user_root_dir_child(UserCtx, UserRootDirCtx, Name) ->
    UserDoc = user_ctx:get_user(UserCtx),
    SessId = user_ctx:get_session_id(UserCtx),

    ChildGuid = case user_logic:get_space_by_name(SessId, UserDoc, Name) of
        {true, SpaceId} ->
            fslogic_uuid:spaceid_to_space_dir_guid(SpaceId);
        false ->
            case user_ctx:is_root(UserCtx) of
                true -> fslogic_uuid:spaceid_to_space_dir_guid(Name);
                false -> throw(?ENOENT)
            end
    end,
    {file_ctx:new_by_guid(ChildGuid), UserRootDirCtx}.


%% @private
-spec get_user_root_dir_children(
    user_ctx:ctx(),
    file_ctx:ctx(),
    file_meta:list_opts(),
    children_whitelist()
) ->
    {[file_ctx:ctx()], file_meta:list_extended_info(), file_ctx:ctx()}.
get_user_root_dir_children(UserCtx, UserRootDirCtx, ListOpts, SpaceWhiteList) ->
    % offset can be negative if last_name is passed too
    Offset = max(maps:get(offset, ListOpts, 0), 0),
    Limit = maps:get(size, ListOpts, ?DEFAULT_LS_BATCH_SIZE),

    AllUserSpaces = user_ctx:get_eff_spaces(UserCtx),

    FilteredSpaces = case SpaceWhiteList of
        undefined ->
            AllUserSpaces;
        _ ->
            lists:filter(fun(Space) -> lists:member(Space, SpaceWhiteList) end, AllUserSpaces)
    end,

    Children = case Offset < length(FilteredSpaces) of
        true ->
            SessId = user_ctx:get_session_id(UserCtx),

            SpacesChunk = lists:sublist(
                lists:sort(lists:map(fun(SpaceId) ->
                    {ok, SpaceName} = space_logic:get_name(SessId, SpaceId),
                    {SpaceName, SpaceId}
                end, FilteredSpaces)),
                Offset + 1,
                Limit
            ),
            lists:map(fun({SpaceName, SpaceId}) ->
                SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
                file_ctx:new_by_uuid(SpaceDirUuid, SpaceId, undefined, SpaceName)
            end, SpacesChunk);
        false ->
            []
    end,
    {Children, #{is_last => length(Children) < Limit}, UserRootDirCtx}.


%% @private
-spec get_space_share_child(file_ctx:ctx(), file_meta:name(), user_ctx:ctx()) ->
    {ChildCtx :: file_ctx:ctx(), file_ctx:ctx()} | no_return().
get_space_share_child(SpaceDirCtx, Name, UserCtx) ->
    SpaceId = file_ctx:get_space_id_const(SpaceDirCtx),
    SessId = user_ctx:get_session_id(UserCtx),
    {ok, Shares} = space_logic:get_shares(SessId, SpaceId),

    case lists:member(Name, Shares) of
        true ->
            ChildUuid = fslogic_uuid:shareid_to_share_root_dir_uuid(Name),
            ChildShareGuid = file_id:pack_share_guid(ChildUuid, SpaceId, Name),
            {file_ctx:new_by_guid(ChildShareGuid), SpaceDirCtx};
        false ->
            throw(?ENOENT)
    end.


%% @private
-spec get_space_open_handle_shares(
    user_ctx:ctx(),
    file_ctx:ctx(),
    file_meta:list_opts(),
    children_whitelist()
) ->
    {[file_ctx:ctx()], file_meta:list_extended_info(), file_ctx:ctx()}.
get_space_open_handle_shares(UserCtx, SpaceDirCtx, ListOpts, ShareWhiteList) ->
    % offset can be negative if last_name is passed too
    Offset = max(maps:get(offset, ListOpts, 0), 0),
    Limit = maps:get(size, ListOpts, ?DEFAULT_LS_BATCH_SIZE),

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
                ShareDirUuid = fslogic_uuid:shareid_to_share_root_dir_uuid(ShareId),
                file_ctx:new_by_uuid(ShareDirUuid, SpaceId, ShareId, ShareId)
            end, lists:sublist(lists:sort(FilteredShares), Offset + 1, Limit));
        false ->
            []
    end,
    {Children, #{is_last => length(Children) < Limit}, SpaceDirCtx}.


%% @private
-spec get_share_root_dir_child(user_ctx:ctx(), file_ctx:ctx(), file_meta:name()) ->
    {ChildCtx :: file_ctx:ctx(), file_ctx:ctx()} | no_return().
get_share_root_dir_child(UserCtx, ShareRootDirCtx, Name) ->
    ShareId = file_ctx:get_share_id_const(ShareRootDirCtx),
    ChildCtx = get_share_root_file(UserCtx, ShareId),

    case file_ctx:get_aliased_name(ChildCtx, UserCtx) of
        {Name, ChildCtx} ->
            {ChildCtx, ShareRootDirCtx};
        _ ->
            throw(?ENOENT)
    end.


%% @private
-spec get_share_root_dir_children(user_ctx:ctx(), file_ctx:ctx(), children_whitelist()) ->
    {[file_ctx:ctx()], file_meta:list_extended_info(), file_ctx:ctx()}.
get_share_root_dir_children(UserCtx, ShareRootDirCtx, FileWhiteList) ->
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
    {Children, #{is_last => true}, ShareRootDirCtx}.


%% @private
-spec get_share_root_file(user_ctx:ctx(), od_share:id()) -> file_ctx:ctx().
get_share_root_file(UserCtx, ShareId) ->
    SessId = user_ctx:get_session_id(UserCtx),
    {ok, #document{value = ShareRec}} = share_logic:get(SessId, ShareId),

    file_ctx:new_by_guid(ShareRec#od_share.root_file).


%% @private
-spec get_file_child(file_ctx:ctx(), file_meta:name()) ->
    {ChildCtx :: file_ctx:ctx(), file_ctx:ctx()} | no_return().
get_file_child(FileCtx, Name) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),

    case canonical_path:resolve(FileDoc, <<"/", Name/binary>>) of
        {ok, ChildDoc} ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            ShareId = file_ctx:get_share_id_const(FileCtx),
            {file_ctx:new_by_doc(ChildDoc, SpaceId, ShareId), FileCtx2};
        {error, not_found} ->
            throw(?ENOENT)
    end.


%% @private
-spec get_file_children(file_ctx:ctx(), file_meta:list_opts(), children_whitelist()) ->
    {[file_ctx:ctx()], file_meta:list_extended_info(), file_ctx:ctx()}.
get_file_children(FileCtx, ListOpts, ChildrenWhiteList) ->
    {#document{} = FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),

    case file_meta:get_type(FileDoc) of
        ?DIRECTORY_TYPE ->
            FileGuid = file_ctx:get_guid_const(FileCtx),
            {_FileUuid, SpaceId, ShareId} = file_id:unpack_share_guid(FileGuid),

            {ok, ChildrenLinks, ListExtendedInfo} = case ChildrenWhiteList of
                undefined -> file_meta:list_children(FileDoc, ListOpts);
                _ -> file_meta:list_children_whitelisted(FileDoc, ListOpts, ChildrenWhiteList)
            end,
            Children = lists:map(fun({Name, Uuid}) ->
                file_ctx:new_by_uuid(Uuid, SpaceId, ShareId, Name)
            end, ChildrenLinks),

            {Children, ListExtendedInfo, FileCtx2};
        _ ->
            % In case of listing regular file - return it
            {[FileCtx2], #{is_last => true}, FileCtx2}
    end.


%% @private
-spec is_space_dir_accessed_in_open_handle_mode(user_ctx:ctx(), file_ctx:ctx()) ->
    boolean().
is_space_dir_accessed_in_open_handle_mode(UserCtx, FileCtx) ->
    ShareId = file_ctx:get_share_id_const(FileCtx),
    IsSpaceDir = file_ctx:is_space_dir_const(FileCtx),
    IsInOpenHandleMode = user_ctx:is_in_open_handle_mode(UserCtx),

    IsSpaceDir andalso IsInOpenHandleMode andalso ShareId == undefined.
