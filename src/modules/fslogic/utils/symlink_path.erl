%% ===================================================================
%% @author Bartosz Walkowicz
%% @copyright (C) 2021 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc
%% This module provides set of symlink path processing methods.
%% @end
%% ===================================================================
-module(symlink_path).
-author("Bartosz Walkowicz").

-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([resolve/2]).


-record(resolution_ctx, {
    user_ctx :: user_ctx:ctx(),
    space_id :: od_space:id()
}).
-type ctx() :: #resolution_ctx{}.


-define(SPACE_ID_PREFIX, "<__onedata_space_id:").
-define(SPACE_ID_SUFFIX, ">").


%%%===================================================================
%%% API functions
%%%===================================================================


-spec resolve(user_ctx:ctx(), file_ctx:ctx()) -> file_ctx:ctx() | no_return().
resolve(UserCtx, SymlinkFileCtx) ->
    ResolutionCtx = #resolution_ctx{
        user_ctx = UserCtx,
        space_id = file_ctx:get_space_id_const(SymlinkFileCtx)
    },
    {TargetFileCtx, _} = resolve_symlink(SymlinkFileCtx, ResolutionCtx),

    fslogic_authz:ensure_authorized(UserCtx, TargetFileCtx, [?TRAVERSE_ANCESTORS]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec resolve_symlink(file_ctx:ctx(), ctx()) -> {file_ctx:ctx(), ctx()} | no_return().
resolve_symlink(SymlinkFileCtx, #resolution_ctx{
    user_ctx = UserCtx,
    space_id = SpaceId
} = ResolutionCtx) ->
    SpaceIdSize = byte_size(SpaceId),

    case filepath_utils:split(read_symlink(UserCtx, SymlinkFileCtx)) of
        [] ->
            throw(?ENOENT);
        [<<?DIRECTORY_SEPARATOR>> | _] ->
            % absolute path with no space id prefix - not supported
            throw(?ENOENT);
        [<<?SPACE_ID_PREFIX, SpaceId:SpaceIdSize/binary, ?SPACE_ID_SUFFIX>> | RestTokens] ->
            % absolute path starting from space dir
            SpaceDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
            SpaceDirCtx = file_ctx:new_by_guid(SpaceDirGuid),
            resolve_path(RestTokens, SpaceDirCtx, ResolutionCtx);
        PathTokens ->
            % relative path
            {ParentCtx, _} = files_tree:get_parent(SymlinkFileCtx, UserCtx),
            resolve_path(PathTokens, ParentCtx, ResolutionCtx)
    end.


%% @private
-spec read_symlink(user_ctx:ctx(), file_ctx:ctx()) -> file_meta_symlinks:symlink().
read_symlink(UserCtx, SymlinkFileCtx) ->
    #fuse_response{status = #status{code = ?OK}, fuse_response = #symlink{
        link = SymlinkValue
    }} = file_req:read_symlink(UserCtx, SymlinkFileCtx),

    SymlinkValue.


%% @private
-spec resolve_path(filepath_utils:tokens(), file_ctx:ctx(), ctx()) ->
    {file_ctx:ctx(), ctx()} | no_return().
resolve_path([], FileCtx, ResolutionCtx) ->
    {FileCtx, ResolutionCtx};

resolve_path([<<".">> | RestTokens], FileCtx, ResolutionCtx) ->
    resolve_path(RestTokens, FileCtx, ResolutionCtx);

resolve_path([<<"..">> | RestTokens], FileCtx, #resolution_ctx{
    user_ctx = UserCtx
} = ResolutionCtx) ->
    NewFileCtx = case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            FileCtx;
        false ->
            {ParentCtx, _} = files_tree:get_parent(FileCtx, UserCtx),
            ParentCtx
    end,
    resolve_path(RestTokens, NewFileCtx, ResolutionCtx);

resolve_path([ChildName | RestTokens], FileCtx, #resolution_ctx{
    user_ctx = UserCtx
} = ResolutionCtx) ->
    {ChildCtx, _} = files_tree:get_child(FileCtx, ChildName, UserCtx),

    case file_ctx:is_symlink_const(ChildCtx) of
        true ->
            {TargetFileCtx, NewResolutionCtx} = resolve_symlink(ChildCtx, ResolutionCtx),
            resolve_path(RestTokens, TargetFileCtx, NewResolutionCtx);
        false ->
            resolve_path(RestTokens, ChildCtx, ResolutionCtx)
    end.
