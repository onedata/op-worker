%%%-------------------------------------------------------------------
%%% @author Tomasz Lichoń
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Public api for logical filesystem management, available in
%%% protocol plugins.
%%% @end
%%%-------------------------------------------------------------------
-module(onedata_file_api).

-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% Functions operating on directories
-export([mkdir/2, mkdir/3, ls/4, get_children_count/2]).
%% Functions operating on directories or files
-export([exists/1, mv/2, cp/2]).
%% Functions operating on files
-export([create/3, open/3, write/3, read/3, truncate/2, truncate/3,
    get_block_map/1, get_block_map/2, unlink/1, unlink/2]).
%% Functions concerning file permissions
-export([set_perms/2, check_perms/2, set_acl/2, get_acl/1]).
%% Functions concerning file attributes
-export([stat/1, stat/2, set_xattr/3, get_xattr/2, remove_xattr/2, list_xattr/1]).
%% Functions concerning symbolic links
-export([create_symlink/2, read_symlink/1, remove_symlink/1]).
%% Functions concerning file shares
-export([create_share/2, get_share/1, remove_share/1]).

%%--------------------------------------------------------------------
%% IDs of entities
-type file_uuid() :: binary().
-type group_id() :: binary().
-type user_id() :: binary().
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Types connected with files
-type file_path() :: binary().
-type file_handle() :: logical_file_manager:handle().
-type file_name() :: binary().
-type file_id_or_path() :: {uuid, file_uuid()} | {path, file_path()}.
-type file_key() :: {path, file_path()} | {uuid, file_uuid()} | {handle, file_handle()}.
-type open_mode() :: write | read | rdwr.
-type perms_octal() :: non_neg_integer().
-type permission_type() :: root | owner | delete | read | write | execute | rdwr.
-type file_attributes() :: #file_attr{}.
-type xattr_key() :: binary().
-type xattr_value() :: binary().
-type access_control_entity() :: term(). % TODO should be a proper record
-type block_range() :: term(). % TODO should be a proper record
-type share_id() :: binary().
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Misc
-type error_reply() :: {error, term()}.
%%--------------------------------------------------------------------

-export_type([file_handle/0]).
%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Creates a directory.
%%--------------------------------------------------------------------
-spec mkdir(Identity :: onedata_auth_api:auth(), Path :: file_path()) -> ok | error_reply().
mkdir(Auth, Path) ->
    logical_file_manager:mkdir(Auth, Path).
-spec mkdir(Auth :: onedata_auth_api:auth(), Path :: file_path(), Mode :: file_meta:posix_permissions()) -> ok | error_reply().
mkdir(Auth, Path, Mode) ->
    logical_file_manager:mkdir(Auth, Path, Mode).


%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory.
%% Returns up to Limit of entries, starting with Offset-th entry.
%% @end
%%--------------------------------------------------------------------
-spec ls(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path(), Limit :: integer(), Offset :: integer()) -> {ok, [{file_uuid(), file_name()}]} | error_reply().
ls(Auth, FileKey, Limit, Offset) ->
    logical_file_manager:ls(Auth, FileKey, Limit, Offset).


%%--------------------------------------------------------------------
%% @doc Returns number of children of a directory.
%%--------------------------------------------------------------------
-spec get_children_count(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path()) -> {ok, integer()} | error_reply().
get_children_count(Auth, FileKey) ->
    logical_file_manager:get_children_count(Auth, FileKey).


%%--------------------------------------------------------------------
%% @doc Checks if a file or directory exists.
%%--------------------------------------------------------------------
-spec exists(FileKey :: file_key()) -> {ok, boolean()} | error_reply().
exists(FileKey) ->
    logical_file_manager:exists(FileKey).

%%--------------------------------------------------------------------
%% @doc Moves a file or directory to a new location.
%%--------------------------------------------------------------------
-spec mv(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
mv(FileKeyFrom, PathTo) ->
    logical_file_manager:mv(FileKeyFrom, PathTo).

%%--------------------------------------------------------------------
%% @doc Copies a file or directory to given location.
%%--------------------------------------------------------------------
-spec cp(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
cp(PathFrom, PathTo) ->
    logical_file_manager:cp(PathFrom, PathTo).

%%--------------------------------------------------------------------
%% @doc Removes a file or an empty directory.
%%--------------------------------------------------------------------
-spec unlink(file_handle()) -> ok | error_reply().
unlink(Handle) ->
    logical_file_manager:unlink(Handle).
-spec unlink(onedata_auth_api:auth(), fslogic_worker:file()) -> ok | error_reply().
unlink(Auth, FileEntry) ->
    logical_file_manager:unlink(Auth, FileEntry).


%%--------------------------------------------------------------------
%% @doc Creates a new file.
%%--------------------------------------------------------------------
-spec create(Auth :: onedata_auth_api:auth(), Path :: file_path(), Mode :: file_meta:posix_permissions()) ->
    {ok, file_uuid()} | error_reply().
create(Auth, Path, Mode) ->
    logical_file_manager:create(Auth, Path, Mode).


%%--------------------------------------------------------------------
%% @doc Opens a file in selected mode and returns a file handle used to read or write.
%%--------------------------------------------------------------------
-spec open(onedata_auth_api:auth(), FileKey :: file_id_or_path(), OpenType :: open_mode()) -> {ok, file_handle()} | error_reply().
open(Auth, FileKey, OpenType) ->
    logical_file_manager:open(Auth, FileKey, OpenType).


%%--------------------------------------------------------------------
%% @doc Writes data to a file. Returns number of written bytes.
%%--------------------------------------------------------------------
-spec write(FileHandle :: file_handle(), Offset :: integer(), Buffer :: binary()) ->
    {ok, NewHandle :: file_handle(), integer()} | error_reply().
write(FileHandle, Offset, Buffer) ->
    logical_file_manager:write(FileHandle, Offset, Buffer).


%%--------------------------------------------------------------------
%% @doc Reads requested part of a file.
%%--------------------------------------------------------------------
-spec read(FileHandle :: file_handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: file_handle(), binary()} | error_reply().
read(FileHandle, Offset, MaxSize) ->
    logical_file_manager:read(FileHandle, Offset, MaxSize).


%%--------------------------------------------------------------------
%% @doc Truncates a file.
%%--------------------------------------------------------------------
-spec truncate(FileHandle :: file_handle(), Size :: non_neg_integer()) -> ok | error_reply().
truncate(Handle, Size) ->
    logical_file_manager:truncate(Handle, Size).
-spec truncate(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path(), Size :: non_neg_integer()) -> ok | error_reply().
truncate(Auth, FileKey, Size) ->
    logical_file_manager:truncate(Auth, FileKey, Size).


%%--------------------------------------------------------------------
%% @doc Returns block map for a file.
%%--------------------------------------------------------------------

-spec get_block_map(FileHandle :: file_handle()) -> {ok, [block_range()]} | error_reply().
get_block_map(Handle) ->
    logical_file_manager:get_block_map(Handle).
-spec get_block_map(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path()) -> {ok, [block_range()]} | error_reply().
get_block_map(Auth, FileKey) ->
    logical_file_manager:get_block_map(Auth, FileKey).


%%--------------------------------------------------------------------
%% @doc Changes the permissions of a file.
%%--------------------------------------------------------------------
-spec set_perms(FileKey :: file_key(), NewPerms :: perms_octal()) -> ok | error_reply().
set_perms(Path, NewPerms) ->
    logical_file_manager:set_perms(Path, NewPerms).

%%--------------------------------------------------------------------
%% @doc Checks if current user has given permissions for given file.
%%--------------------------------------------------------------------
-spec check_perms(FileKey :: file_key(), PermsType :: permission_type()) -> {ok, boolean()} | error_reply().
check_perms(Path, PermType) ->
    logical_file_manager:check_perms(Path, PermType).

%%--------------------------------------------------------------------
%% @doc Returns file's Access Control List.
%%--------------------------------------------------------------------
-spec get_acl(FileKey :: file_key()) -> {ok, [access_control_entity()]} | error_reply().
get_acl(Path) ->
    logical_file_manager:get_acl(Path).

%%--------------------------------------------------------------------
%% @doc Updates file's Access Control List.
%%--------------------------------------------------------------------
-spec set_acl(FileKey :: file_key(), EntityList :: [access_control_entity()]) -> ok | error_reply().
set_acl(Path, EntityList) ->
    logical_file_manager:set_acl(Path, EntityList).

%%--------------------------------------------------------------------
%% @doc Returns file attributes.
%%--------------------------------------------------------------------
-spec stat(file_handle()) -> {ok, file_attributes()} | error_reply().
stat(Handle) ->
    logical_file_manager:stat(Handle).
-spec stat(onedata_auth_api:auth(), file_key()) -> {ok, file_attributes()} | error_reply().
stat(Auth, FileKey) ->
    logical_file_manager:stat(Auth, FileKey).


%%--------------------------------------------------------------------
%% @doc Returns file's extended attribute by key.
%%--------------------------------------------------------------------
-spec get_xattr(FileKey :: file_key(), Key :: xattr_key()) -> {ok, xattr_value()} | error_reply().
get_xattr(Path, Key) ->
    logical_file_manager:get_xattr(Path, Key).

%%--------------------------------------------------------------------
%% @doc Updates file's extended attribute by key.
%%--------------------------------------------------------------------
-spec set_xattr(FileKey :: file_key(), Key :: xattr_key(), Value :: xattr_value()) -> ok |  error_reply().
set_xattr(Path, Key, Value) ->
    logical_file_manager:set_xattr(Path, Key, Value).

%%--------------------------------------------------------------------
%% @doc Removes file's extended attribute by key.
%%--------------------------------------------------------------------
-spec remove_xattr(FileKey :: file_key(), Key :: xattr_key()) -> ok |  error_reply().
remove_xattr(Path, Key) ->
    logical_file_manager:remove_xattr(Path, Key).

%%--------------------------------------------------------------------
%% @doc Returns complete list of extended attributes of a file.
%%--------------------------------------------------------------------
-spec list_xattr(FileKey :: file_key()) -> {ok, [{Key :: xattr_key(), Value :: xattr_value()}]} | error_reply().
list_xattr(Path) ->
    logical_file_manager:list_xattr(Path).

%%--------------------------------------------------------------------
%% @doc Creates a symbolic link.
%%--------------------------------------------------------------------
-spec create_symlink(Path :: binary(), TargetFileKey :: file_key()) -> {ok, file_uuid()} | error_reply().
create_symlink(Path, TargetFileKey) ->
    logical_file_manager:create_symlink(Path, TargetFileKey).

%%--------------------------------------------------------------------
%% @doc Returns the symbolic link's target file.
%%--------------------------------------------------------------------
-spec read_symlink(FileKey :: file_key()) -> {ok, {file_uuid(), file_name()}} | error_reply().
read_symlink(FileKey) ->
    logical_file_manager:read_symlink(FileKey).

%%--------------------------------------------------------------------
%% @doc Removes a symbolic link.
%%--------------------------------------------------------------------
-spec remove_symlink(FileKey :: file_key()) -> ok | error_reply().
remove_symlink(FileKey) ->
    logical_file_manager:remove_symlink(FileKey).

%%--------------------------------------------------------------------
%% @doc
%% Creates a share for given file. File can be shared with anyone or
%% only specified group of users.
%% @end
%%--------------------------------------------------------------------
-spec create_share(FileKey :: file_key(), ShareWith :: all | [{user, user_id()} | {group, group_id()}]) ->
    {ok, ShareID :: share_id()} | error_reply().
create_share(Path, ShareWith) ->
    logical_file_manager:create_share(Path, ShareWith).

%%--------------------------------------------------------------------
%% @doc Returns shared file by share_id.
%%--------------------------------------------------------------------
-spec get_share(ShareID :: share_id()) -> {ok, {file_uuid(), file_name()}} | error_reply().
get_share(ShareID) ->
    logical_file_manager:get_share(ShareID).

%%--------------------------------------------------------------------
%% @doc Removes file share by ShareID.
%%--------------------------------------------------------------------
-spec remove_share(ShareID :: share_id()) -> ok | error_reply().
remove_share(ShareID) ->
    logical_file_manager:remove_share(ShareID).