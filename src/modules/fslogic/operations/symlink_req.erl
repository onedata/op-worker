%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides set of symlink path processing methods.
%%% @end
%%%-------------------------------------------------------------------
-module(symlink_req).
-author("Bartosz Walkowicz").

-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([read/2, resolve/2]).


-record(resolution_ctx, {
    user_ctx :: user_ctx:ctx(),
    space_id :: od_space:id(),
    symlinks_encountered :: ordsets:ordset(file_id:file_guid())
}).
-type ctx() :: #resolution_ctx{}.


-define(SPACE_ID_PREFIX, "<__onedata_space_id:").
-define(SPACE_ID_SUFFIX, ">").

-define(SYMLINK_HOPS_LIMIT, 40).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec read(user_ctx:ctx(), file_ctx:ctx()) -> fslogic_worker:fuse_response().
read(UserCtx, FileCtx0) ->
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #symlink{link = read_symlink(UserCtx, FileCtx0)}
    }.


-spec resolve(user_ctx:ctx(), file_ctx:ctx()) -> fslogic_worker:fuse_response().
resolve(UserCtx, SymlinkFileCtx) ->
    ResolutionCtx = #resolution_ctx{
        user_ctx = UserCtx,
        space_id = file_ctx:get_space_id_const(SymlinkFileCtx),
        symlinks_encountered = ordsets:new()
    },

    {TargetFileCtx, _} = resolve_symlink(SymlinkFileCtx, ResolutionCtx),
    fslogic_authz:ensure_authorized(UserCtx, TargetFileCtx, [?TRAVERSE_ANCESTORS]),

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #guid{guid = file_ctx:get_logical_guid_const(TargetFileCtx)}
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec read_symlink(user_ctx:ctx(), file_ctx:ctx()) -> file_meta_symlinks:symlink().
read_symlink(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0, [?TRAVERSE_ANCESTORS]
    ),

    {Doc, FileCtx2} = file_ctx:get_file_doc(FileCtx1),
    {ok, SymlinkValue} = file_meta_symlinks:readlink(Doc),
    fslogic_times:update_atime(FileCtx2),

    SymlinkValue.


%% @private
-spec resolve_symlink(file_ctx:ctx(), ctx()) -> {file_ctx:ctx(), ctx()} | no_return().
resolve_symlink(SymlinkFileCtx, #resolution_ctx{
    user_ctx = UserCtx,
    space_id = SpaceId
} = ResolutionCtx) ->
    SpaceIdSize = byte_size(SpaceId),
    NewResolutionCtx = assert_hops_limit_not_reached(SymlinkFileCtx, ResolutionCtx),

    case filepath_utils:split(read_symlink(UserCtx, SymlinkFileCtx)) of
        [] ->
            throw(?ENOENT);
        [<<?DIRECTORY_SEPARATOR>> | _] ->
            % absolute path with no space id prefix - not supported
            throw(?ENOENT);
        [<<?SPACE_ID_PREFIX, SpaceId:SpaceIdSize/binary, ?SPACE_ID_SUFFIX>> | RestTokens] ->
            % absolute path with space id prefix (start at space dir)
            ShareId = file_ctx:get_share_id_const(SymlinkFileCtx),
            SpaceDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
            SpaceDirShareGuid = file_id:guid_to_share_guid(SpaceDirGuid, ShareId),
            SpaceDirCtx = file_ctx:new_by_guid(SpaceDirShareGuid),
            resolve_symlink_path(RestTokens, SpaceDirCtx, NewResolutionCtx);
        PathTokens ->
            % relative path
            {ParentCtx, _} = files_tree:get_parent(SymlinkFileCtx, UserCtx),
            resolve_symlink_path(PathTokens, ParentCtx, NewResolutionCtx)
    end.


%% @private
-spec assert_hops_limit_not_reached(file_ctx:ctx(), ctx()) -> ctx() | no_return().
assert_hops_limit_not_reached(SymlinkFileCtx, #resolution_ctx{
    symlinks_encountered = Symlinks
} = ResolutionCtx) ->
    SymlinkGuid = file_ctx:get_logical_guid_const(SymlinkFileCtx),

    AlreadyEncountered = ordsets:is_element(SymlinkGuid, Symlinks),
    HopsLimitReached = (1 + ordsets:size(Symlinks)) > ?SYMLINK_HOPS_LIMIT,

    case AlreadyEncountered orelse HopsLimitReached of
        true ->
            throw(?ELOOP);
        false ->
            ResolutionCtx#resolution_ctx{
                symlinks_encountered = ordsets:add_element(SymlinkGuid, Symlinks)
            }
    end.


%% @private
-spec resolve_symlink_path(filepath_utils:tokens(), file_ctx:ctx(), ctx()) ->
    {file_ctx:ctx(), ctx()} | no_return().
resolve_symlink_path([], FileCtx, ResolutionCtx) ->
    {FileCtx, ResolutionCtx};

resolve_symlink_path([<<".">> | RestTokens], FileCtx, ResolutionCtx) ->
    resolve_symlink_path(RestTokens, FileCtx, ResolutionCtx);

resolve_symlink_path([<<"..">> | RestTokens], FileCtx, #resolution_ctx{
    user_ctx = UserCtx
} = ResolutionCtx) ->
    NewFileCtx = case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            FileCtx;
        false ->
            {ParentCtx, _} = files_tree:get_parent(FileCtx, UserCtx),
            ParentCtx
    end,
    resolve_symlink_path(RestTokens, NewFileCtx, ResolutionCtx);

resolve_symlink_path([ChildName | RestTokens], FileCtx, #resolution_ctx{
    user_ctx = UserCtx
} = ResolutionCtx) ->
    {ChildCtx, _} = files_tree:get_child(FileCtx, ChildName, UserCtx),

    case file_ctx:is_symlink_const(ChildCtx) of
        true ->
            {TargetFileCtx, NewResolutionCtx} = resolve_symlink(ChildCtx, ResolutionCtx),
            resolve_symlink_path(RestTokens, TargetFileCtx, NewResolutionCtx);
        false ->
            resolve_symlink_path(RestTokens, ChildCtx, ResolutionCtx)
    end.
