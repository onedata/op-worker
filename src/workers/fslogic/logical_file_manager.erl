%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module offers a high level API for operating on logical filesystem.
%%% When passing a file in arguments, one can use one of the following:
%%% {uuid, FileUUIDBin} - preferred and fast. UUIDs are returned from 'ls' function.
%%% {path, BinaryFilePath} - slower than by uuid (path has to be resolved). Discouraged, but there are cases when this is useful.
%%% {handle, BinaryHandle} - fastest, but not possible in all functions. The handle can be obtained from open function.
%%%
%%% This module is merely a convenient wrapper that calls functions from lfm_xxx modules.
%%% @end
%%%-------------------------------------------------------------------
-module(logical_file_manager).


% TODO Issues connected with logical_files_manager
% 1) User context lib - a simple library is needed to set and retrieve the user context. If not context is set, it should crash
% with an error. 'root' atom can be used to set root context.
% 2) Structure of file handle - what should it contain:
%   - file uuid
%   - expiration time
%   - connected session
%   - session type
%   - open mode
%   - ??
% 3) How to get rid of rubbish like not closed opens - check expired handles if a session connected with them has ended?
% 4) All errors should be listed in one hrl -> errors.hrl
% 4) Common types should be listed in one hrl -> types.hrl
% 5) chown is no longer featured in lfm as it is not needed by higher layers
% 6) Blocks related functions should go to another module (synchronize, mark_as_truncated etc).


-include("types.hrl").
-include("errors.hrl").

%% User context
-export([set_user_context/1]).
%% Functions operating on directories
-export([mkdir/1, ls/3, get_children_count/1]).
%% Functions operating on directories or files
-export([exists/1, mv/2, cp/2, rm/1]).
%% Functions operating on files
-export([create/1, open/2, write/3, read/3, truncate/2, get_block_map/1]).
%% Functions concerning file permissions
-export([set_perms/2, check_perms/2, set_acl/2, get_acl/1]).
%% Functions concerning file attributes
-export([stat/1, set_xattr/3, get_xattr/2, remove_xattr/2, list_xattr/1]).
%% Functions concerning symbolic links
-export([create_symlink/2, read_symlink/1, remove_symlink/1]).
%% Functions concerning file shares
-export([create_share/2, get_share/1, remove_share/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sets the user context for current process. All logical_files_manager functions
%% will be evaluated on behalf of the user.
%%
%% @end
%%--------------------------------------------------------------------
-spec set_user_context(UserID :: user_id() | root) -> ok | error_reply().
set_user_context(_UserID) ->
    % TODO this should call user context lib (or whatever it is called)
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Creates a directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec mkdir(Path :: file_path()) -> {ok, file_id()} | error_reply().
mkdir(Path) ->
    lfm_dirs:mkdir(Path).


%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory.
%% Returns up to Limit of entries, starting with Offset-th entry.
%%
%% @end
%%--------------------------------------------------------------------
-spec ls(FileKey :: file_id_or_path(), Limit :: integer(), Offset :: integer()) -> {ok, [{file_id(), file_name()}]} | error_reply().
ls(FileKey, Limit, Offset) ->
    lfm_dirs:ls(FileKey, Limit, Offset).


%%--------------------------------------------------------------------
%% @doc
%% Returns number of children of a directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_children_count(FileKey :: file_id_or_path()) -> {ok, integer()} | error_reply().
get_children_count(FileKey) ->
    lfm_dirs:get_children_count(FileKey).


%%--------------------------------------------------------------------
%% @doc
%% Checks if a file or directory exists.
%%
%% @end
%%--------------------------------------------------------------------
-spec exists(FileKey :: file_key()) -> {ok, boolean()} | error_reply().
exists(FileKey) ->
    lfm_files:exists(FileKey).


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%%
%% @end
%%--------------------------------------------------------------------
-spec mv(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
mv(FileKeyFrom, PathTo) ->
    lfm_files:mv(FileKeyFrom, PathTo).


%%--------------------------------------------------------------------
%% @doc
%% Copies a file or directory to given location.
%%
%% @end
%%--------------------------------------------------------------------
-spec cp(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
cp(PathFrom, PathTo) ->
    lfm_files:cp(PathFrom, PathTo).


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec rm(FileKey :: file_key()) -> ok | error_reply().
rm(FileKey) ->
    lfm_files:rm(FileKey).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file.
%%
%% @end
%%--------------------------------------------------------------------
-spec create(Path :: file_path()) -> {ok, file_id()} | error_reply().
create(Path) ->
    lfm_files:create(Path).


%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode and returns a file handle used to read or write.
%%
%% @end
%%--------------------------------------------------------------------
-spec open(FileKey :: file_id_or_path(), OpenType :: open_type()) -> {ok, file_handle()} | error_reply().
open(FileKey, OpenType) ->
    lfm_files:open(FileKey, OpenType).


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file. Returns number of written bytes.
%%
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: file_handle(), Offset :: integer(), Buffer :: binary()) -> {ok, integer()} | error_reply().
write(FileHandle, Offset, Buffer) ->
    lfm_files:write(FileHandle, Offset, Buffer).


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: file_handle(), Offset :: integer(), MaxSize :: integer()) -> {ok, binary()} | error_reply().
read(FileHandle, Offset, MaxSize) ->
    lfm_files:read(FileHandle, Offset, MaxSize).


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec truncate(FileKey :: file_key(), Size :: integer()) -> ok | error_reply().
truncate(FileKey, Size) ->
    lfm_files:truncate(FileKey, Size).


%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_block_map(FileKey :: file_key()) -> {ok, [block_range()]} | error_reply().
get_block_map(FileKey) ->
    lfm_files:get_block_map(FileKey).


%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec set_perms(FileKey :: file_key(), NewPerms :: perms_octal()) -> ok | error_reply().
set_perms(Path, NewPerms) ->
    lfm_perms:set_perms(Path, NewPerms).


%%--------------------------------------------------------------------
%% @doc
%% Checks if current user has given permissions for given file.
%%
%% @end
%%--------------------------------------------------------------------
-spec check_perms(FileKey :: file_key(), PermsType :: permission_type()) -> {ok, boolean()} | error_reply().
check_perms(Path, PermType) ->
    lfm_perms:check_perms(Path, PermType).


%%--------------------------------------------------------------------
%% @doc
%% Returns file's Access Control List.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_acl(FileKey :: file_key()) -> {ok, [access_control_entity()]} | error_reply().
get_acl(Path) ->
    lfm_perms:get_acl(Path).


%%--------------------------------------------------------------------
%% @doc
%% Updates file's Access Control List.
%%
%% @end
%%--------------------------------------------------------------------
-spec set_acl(FileKey :: file_key(), EntityList :: [access_control_entity()]) -> ok | error_reply().
set_acl(Path, EntityList) ->
    lfm_perms:set_acl(Path, EntityList).


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes.
%%
%% @end
%%--------------------------------------------------------------------
-spec stat(FileKey :: file_key()) -> {ok, file_attributes()} | error_reply().
stat(Path) ->
    lfm_attrs:stat(Path).


%%--------------------------------------------------------------------
%% @doc
%% Returns file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_xattr(FileKey :: file_key(), Key :: xattr_key()) -> {ok, xattr_value()} | error_reply().
get_xattr(Path, Key) ->
    lfm_attrs:get_xattr(Path, Key).


%%--------------------------------------------------------------------
%% @doc
%% Updates file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(FileKey :: file_key(), Key :: xattr_key(), Value :: xattr_value()) -> ok |  error_reply().
set_xattr(Path, Key, Value) ->
    lfm_attrs:set_xattr(Path, Key, Value).


%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(FileKey :: file_key(), Key :: xattr_key()) -> ok |  error_reply().
remove_xattr(Path, Key) ->
    lfm_attrs:remove_xattr(Path, Key).


%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(FileKey :: file_key()) -> {ok, [{Key :: xattr_key(), Value :: xattr_value()}]} | error_reply().
list_xattr(Path) ->
    lfm_attrs:list_xattr(Path).


%%--------------------------------------------------------------------
%% @doc
%% Creates a symbolic link.
%%
%% @end
%%--------------------------------------------------------------------
-spec create_symlink(Path :: binary(), TargetFileKey :: file_key()) -> {ok, file_id()} | error_reply().
create_symlink(Path, TargetFileKey) ->
    lfm_links:create_symlink(Path, TargetFileKey).


%%--------------------------------------------------------------------
%% @doc
%% Returns the symbolic link's target file.
%%
%% @end
%%--------------------------------------------------------------------
-spec read_symlink(FileKey :: file_key()) -> {ok, {file_id(), file_name()}} | error_reply().
read_symlink(FileKey) ->
    lfm_links:read_symlink(FileKey).


%%--------------------------------------------------------------------
%% @doc
%% Removes a symbolic link.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_symlink(FileKey :: file_key()) -> ok | error_reply().
remove_symlink(FileKey) ->
    lfm_links:remove_symlink(FileKey).


%%--------------------------------------------------------------------
%% @doc
%% Creates a share for given file. File can be shared with anyone or
%% only specified group of users.
%%
%% @end
%%--------------------------------------------------------------------
-spec create_share(FileKey :: file_key(), ShareWith :: all | [{user, user_id()} | {group, group_id()}]) ->
    {ok, ShareID :: share_id()} | error_reply().
create_share(Path, ShareWith) ->
    lfm_shares:create_share(Path, ShareWith).


%%--------------------------------------------------------------------
%% @doc
%% Returns shared file by share_id.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_share(ShareID :: share_id()) -> {ok, {file_id(), file_name()}} | error_reply().
get_share(ShareID) ->
    lfm_shares:get_share(ShareID).


%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareID.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_share(ShareID :: share_id()) -> ok | error_reply().
remove_share(ShareID) ->
    lfm_shares:remove_share(ShareID).


