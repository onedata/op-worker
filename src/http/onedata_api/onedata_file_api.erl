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
-include_lib("ctool/include/posix/acl.hrl").

%% Functions operating on directories
-export([mkdir/2, mkdir/3, ls/4, get_children_count/2]).
%% Functions operating on directories or files
-export([exists/1, mv/3, cp/3, get_file_path/2, rm_recursive/2]).
%% Functions operating on files
-export([create/3, open/3, write/3, read/3, truncate/2, truncate/3, unlink/1,
    unlink/2, fsync/1, release/1, get_file_distribution/2, replicate_file/3]).
%% Functions concerning file permissions
-export([set_perms/3, check_perms/3, set_acl/3, get_acl/2, remove_acl/2]).
%% Functions concerning file attributes
-export([stat/1, stat/2, set_xattr/2, set_xattr/3, get_xattr/3, get_xattr/4,
    remove_xattr/2, remove_xattr/3, list_xattr/2, list_xattr/3]).
%% Functions concerning cdmi attributes
-export([get_transfer_encoding/2, set_transfer_encoding/3, get_cdmi_completion_status/2,
    set_cdmi_completion_status/3, get_mimetype/2, set_mimetype/3]).
%% Functions concerning symbolic links
-export([create_symlink/2, read_symlink/1, remove_symlink/1]).
%% Functions concerning file shares
-export([create_share/2, get_share/1, remove_share/1]).
%% Functions concerning metadata
-export([get_metadata/5, set_metadata/5]).

%%--------------------------------------------------------------------
%% IDs of entities
-type file_guid() :: binary().
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Types connected with files
-type file_path() :: binary().
-type file_handle() :: logical_file_manager:handle().
-type file_name() :: binary().
-type file_id_or_path() :: {guid, file_guid()} | {path, file_path()}.
-type file_key() :: {path, file_path()} | {guid, file_guid()} | {handle, file_handle()}.
-type open_mode() :: write | read | rdwr.
-type perms_octal() :: non_neg_integer().
-type permission_type() :: root | owner | delete | read | write | execute | rdwr.
-type file_attributes() :: #file_attr{}.
-type xattr_name() :: binary().
-type access_control_entity() :: #accesscontrolentity{}.
-type share_id() :: binary().
-type transfer_encoding() :: binary(). % <<"utf-8">> | <<"base64">>
-type cdmi_completion_status() :: binary(). % <<"Completed">> | <<"Processing">> | <<"Error">>
-type mimetype() :: binary().
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Misc
-type error_reply() :: {error, term()}.
%%--------------------------------------------------------------------

-export_type([file_handle/0, file_attributes/0, file_path/0, file_guid/0, file_key/0]).
%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc Creates a directory.
%%--------------------------------------------------------------------
-spec mkdir(Auth :: onedata_auth_api:auth(), Path :: file_path()) ->
    {ok, file_guid()} | error_reply().
mkdir(Auth, Path) ->
    logical_file_manager:mkdir(Auth, Path).
-spec mkdir(Auth :: onedata_auth_api:auth(), Path :: file_path(), Mode :: file_meta:posix_permissions()) ->
    {ok, file_guid()} | error_reply().
mkdir(Auth, Path, Mode) ->
    logical_file_manager:mkdir(Auth, Path, Mode).


%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory.
%% Returns up to Limit of entries, starting with Offset-th entry.
%% @end
%%--------------------------------------------------------------------
-spec ls(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path(), Offset :: integer(), Limit :: integer()) ->
    {ok, [{file_guid(), file_name()}]} | error_reply().
ls(Auth, FileKey, Offset, Limit) ->
    logical_file_manager:ls(Auth, FileKey, Offset, Limit).


%%--------------------------------------------------------------------
%% @doc Returns number of children of a directory.
%%--------------------------------------------------------------------
-spec get_children_count(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path()) ->
    {ok, integer()} | error_reply().
get_children_count(Auth, FileKey) ->
    logical_file_manager:get_children_count(Auth, FileKey).


%%--------------------------------------------------------------------
%% @doc Deletes a file or directory recursively.
%%--------------------------------------------------------------------
-spec rm_recursive(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path())
        -> ok | error_reply().
rm_recursive(Auth, FileKey) ->
    logical_file_manager:rm_recursive(Auth, FileKey).


%%--------------------------------------------------------------------
%% @doc Checks if a file or directory exists.
%%--------------------------------------------------------------------
-spec exists(FileKey :: file_key()) -> {ok, boolean()} | error_reply().
exists(FileKey) ->
    logical_file_manager:exists(FileKey).

%%--------------------------------------------------------------------
%% @doc Moves a file or directory to a new location.
%%--------------------------------------------------------------------
-spec mv(onedata_auth_api:auth(), file_id_or_path(), file_path()) ->
    ok | error_reply().
mv(Auth, FileEntry, TargetPath) ->
    {ok, _} = logical_file_manager:mv(Auth, FileEntry, TargetPath),
    ok.

%%--------------------------------------------------------------------
%% @doc Copies a file or directory to given location.
%%--------------------------------------------------------------------
-spec cp(onedata_auth_api:auth(), file_id_or_path(), file_path()) -> ok | error_reply().
cp(Auth, FileEntry, TargetPath) ->
    logical_file_manager:cp(Auth, FileEntry, TargetPath).

%%--------------------------------------------------------------------
%% @doc Returns full path of file
%%--------------------------------------------------------------------
-spec get_file_path(Auth :: onedata_auth_api:auth(), Uuid :: file_guid()) ->
    {ok, file_path()}.
get_file_path(Auth, Uuid) ->
    logical_file_manager:get_file_path(Auth, Uuid).

%%--------------------------------------------------------------------
%% @doc Removes a file or an empty directory.
%%--------------------------------------------------------------------
-spec unlink(file_handle()) -> ok | error_reply().
unlink(Handle) ->
    logical_file_manager:unlink(Handle, false).
-spec unlink(onedata_auth_api:auth(), file_id_or_path()) -> ok | error_reply().
unlink(Auth, FileEntry) ->
    logical_file_manager:unlink(Auth, FileEntry, false).

%%--------------------------------------------------------------------
%% @doc Flushes waiting events for session connected with handler.
%%--------------------------------------------------------------------
-spec fsync(file_handle()) -> ok | error_reply().
fsync(Handle) ->
    logical_file_manager:fsync(Handle).

%%--------------------------------------------------------------------
%% @doc Creates a new file.
%%--------------------------------------------------------------------
-spec create(Auth :: onedata_auth_api:auth(), Path :: file_path(), Mode :: file_meta:posix_permissions()) ->
    {ok, file_guid()} | error_reply().
create(Auth, Path, Mode) ->
    logical_file_manager:create(Auth, Path, Mode).


%%--------------------------------------------------------------------
%% @doc Opens a file in selected mode and returns a file handle used to read or write.
%%--------------------------------------------------------------------
-spec open(onedata_auth_api:auth(), FileKey :: file_id_or_path(), OpenType :: open_mode()) ->
    {ok, file_handle()} | error_reply().
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
-spec truncate(FileHandle :: file_handle(), Size :: non_neg_integer()) ->
    ok | error_reply().
truncate(Handle, Size) ->
    logical_file_manager:truncate(Handle, Size).
-spec truncate(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path(), Size :: non_neg_integer()) ->
    ok | error_reply().
truncate(Auth, FileKey, Size) ->
    logical_file_manager:truncate(Auth, FileKey, Size).

%%--------------------------------------------------------------------
%% @doc Releases previously opened file.
%%--------------------------------------------------------------------
-spec release(FileHandle :: file_handle()) ->
    ok | error_reply().
release(FileHandle) ->
    logical_file_manager:release(FileHandle).


%%--------------------------------------------------------------------
%% @doc Returns block map for a file.
%%--------------------------------------------------------------------
-spec get_file_distribution(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path()) ->
    {ok, list()} | error_reply().
get_file_distribution(Auth, FileKey) ->
    logical_file_manager:get_file_distribution(Auth, FileKey).


%%--------------------------------------------------------------------
%% @doc Replicates file on given provider.
%%--------------------------------------------------------------------
-spec replicate_file(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path(), ProviderId :: binary()) ->
    ok | error_reply().
replicate_file(Auth, FileKey, ProviderId) ->
    logical_file_manager:replicate_file(Auth, FileKey, ProviderId).


%%--------------------------------------------------------------------
%% @doc Changes the permissions of a file.
%%--------------------------------------------------------------------
-spec set_perms(onedata_auth_api:auth(), file_key(), perms_octal()) -> ok | error_reply().
set_perms(Auth, FileKey, NewPerms) ->
    logical_file_manager:set_perms(Auth, FileKey, NewPerms).

%%--------------------------------------------------------------------
%% @doc Checks if current user has given permissions for given file.
%%--------------------------------------------------------------------
-spec check_perms(onedata_auth_api:auth(), FileKey :: file_key(), PermsType :: permission_type()) ->
    {ok, boolean()} | error_reply().
check_perms(Auth, FileKey, PermType) ->
    logical_file_manager:check_perms(Auth, FileKey, PermType).

%%--------------------------------------------------------------------
%% @doc Returns file's Access Control List.
%%--------------------------------------------------------------------
-spec get_acl(onedata_auth_api:auth(), file_key()) ->
    {ok, [access_control_entity()]} | error_reply().
get_acl(Auth, FileKey) ->
    logical_file_manager:get_acl(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc Updates file's Access Control List.
%%--------------------------------------------------------------------
-spec set_acl(onedata_auth_api:auth(), file_key(), EntityList :: [access_control_entity()]) ->
    ok | error_reply().
set_acl(Auth, FileKey, EntityList) ->
    logical_file_manager:set_acl(Auth, FileKey, EntityList).

%%--------------------------------------------------------------------
%% @doc Removes file's Access Control List.
%%--------------------------------------------------------------------
-spec remove_acl(onedata_auth_api:auth(), file_key()) -> ok | error_reply().
remove_acl(Auth, FileKey) ->
    logical_file_manager:remove_acl(Auth, FileKey).

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
-spec get_xattr(Handle :: file_handle(), XattrName :: xattr_name(), boolean()) ->
    {ok, #xattr{}} | error_reply().
get_xattr(Handle, XattrName, Inherited) ->
    logical_file_manager:get_xattr(Handle, XattrName, Inherited).
-spec get_xattr(onedata_auth_api:auth(), file_key(), xattr_name(), boolean()) ->
    {ok, #xattr{}} | error_reply().
get_xattr(Auth, FileKey, XattrName, Inherited) ->
    logical_file_manager:get_xattr(Auth, FileKey, XattrName, Inherited).


%%--------------------------------------------------------------------
%% @doc Updates file's extended attribute by key.
%%--------------------------------------------------------------------
-spec set_xattr(file_handle(), #xattr{}) -> ok | error_reply().
set_xattr(Handle, Xattr) ->
    logical_file_manager:set_xattr(Handle, Xattr).
-spec set_xattr(onedata_auth_api:auth(), file_key(), #xattr{}) -> ok | error_reply().
set_xattr(Auth, FileKey, Xattr) ->
    logical_file_manager:set_xattr(Auth, FileKey, Xattr).

%%--------------------------------------------------------------------
%% @doc Removes file's extended attribute by key.
%%--------------------------------------------------------------------
-spec remove_xattr(file_handle(), xattr_name()) -> ok | error_reply().
remove_xattr(Handle, XattrName) ->
    logical_file_manager:remove_xattr(Handle, XattrName).

-spec remove_xattr(onedata_auth_api:auth(), file_key(), xattr_name()) ->
    ok | error_reply().
remove_xattr(Auth, FileKey, XattrName) ->
    logical_file_manager:remove_xattr(Auth, FileKey, XattrName).

%%--------------------------------------------------------------------
%% @doc Returns complete list of extended attribute names of a file.
%%--------------------------------------------------------------------
-spec list_xattr(file_handle(), boolean()) -> {ok, [xattr_name()]} | error_reply().
list_xattr(Handle, Inherited) ->
    logical_file_manager:list_xattr(Handle, Inherited).
-spec list_xattr(onedata_auth_api:auth(), file_key(), boolean()) -> {ok, [xattr_name()]} | error_reply().
list_xattr(Auth, FileKey, Inherited) ->
    logical_file_manager:list_xattr(Auth, FileKey, Inherited).

%%--------------------------------------------------------------------
%% @doc Returns encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec get_transfer_encoding(onedata_auth_api:auth(), file_key()) ->
    {ok, transfer_encoding()} | error_reply().
get_transfer_encoding(Auth, FileKey) ->
    logical_file_manager:get_transfer_encoding(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc Sets encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec set_transfer_encoding(onedata_auth_api:auth(), file_key(), transfer_encoding()) ->
    ok | error_reply().
set_transfer_encoding(Auth, FileKey, Encoding) ->
    logical_file_manager:set_transfer_encoding(Auth, FileKey, Encoding).

%%--------------------------------------------------------------------
%% @doc
%% Returns completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status(onedata_auth_api:auth(), file_key()) ->
    {ok, cdmi_completion_status()} | error_reply().
get_cdmi_completion_status(Auth, FileKey) ->
    logical_file_manager:get_cdmi_completion_status(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc
%% Sets completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status(onedata_auth_api:auth(), file_key(), cdmi_completion_status()) ->
    ok | error_reply().
set_cdmi_completion_status(Auth, FileKey, CompletionStatus) ->
    logical_file_manager:set_cdmi_completion_status(Auth, FileKey, CompletionStatus).

%%--------------------------------------------------------------------
%% @doc Returns mimetype of file.
%%--------------------------------------------------------------------
-spec get_mimetype(onedata_auth_api:auth(), file_key()) ->
    {ok, mimetype()} | error_reply().
get_mimetype(Auth, FileKey) ->
    logical_file_manager:get_mimetype(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc Sets mimetype of file.
%%--------------------------------------------------------------------
-spec set_mimetype(onedata_auth_api:auth(), file_key(), mimetype()) ->
    ok | error_reply().
set_mimetype(Auth, FileKey, Mimetype) ->
    logical_file_manager:set_mimetype(Auth, FileKey, Mimetype).

%%--------------------------------------------------------------------
%% @doc Creates a symbolic link.
%%--------------------------------------------------------------------
-spec create_symlink(Path :: binary(), TargetFileKey :: file_key()) ->
    {ok, file_guid()} | error_reply().
create_symlink(Path, TargetFileKey) ->
    logical_file_manager:create_symlink(Path, TargetFileKey).

%%--------------------------------------------------------------------
%% @doc Returns the symbolic link's target file.
%%--------------------------------------------------------------------
-spec read_symlink(FileKey :: file_key()) ->
    {ok, {file_guid(), file_name()}} | error_reply().
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
-spec get_share(ShareID :: share_id()) ->
    {ok, {file_guid(), file_name()}} | error_reply().
get_share(ShareID) ->
    logical_file_manager:get_share(ShareID).

%%--------------------------------------------------------------------
%% @doc Removes file share by ShareID.
%%--------------------------------------------------------------------
-spec remove_share(ShareID :: share_id()) -> ok | error_reply().
remove_share(ShareID) ->
    logical_file_manager:remove_share(ShareID).

%%--------------------------------------------------------------------
%% @doc
%% Get json metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec get_metadata(onedata_auth_api:auth(), file_key(), binary(), [binary()], boolean()) ->
    {ok, #{}} | error_reply().
get_metadata(Auth, FileKey, Type, Names, Inherited) ->
    logical_file_manager:get_metadata(Auth, FileKey, Type, Names, Inherited).

%%--------------------------------------------------------------------
%% @doc
%% Set json metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec set_metadata(onedata_auth_api:auth(), file_key(), binary(), term(), [binary()]) ->
    ok | error_reply().
set_metadata(Auth, FileKey, Type, Value, Names) ->
    logical_file_manager:set_metadata(Auth, FileKey, Type, Value, Names).