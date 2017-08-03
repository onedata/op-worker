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
-export([mkdir/2, mkdir/3, mkdir/4, ls/4, get_children_count/2]).
%% Functions operating on directories or files
-export([mv/3, cp/3, get_file_path/2, rm_recursive/2]).
%% Functions operating on files
-export([create/3, create/4, open/3, write/3, read/3, truncate/3, unlink/2, fsync/1,
    release/1, get_file_distribution/2, replicate_file/3, invalidate_file_replica/4]).
%% Functions concerning file permissions
-export([set_perms/3, check_perms/3, set_acl/3, get_acl/2, remove_acl/2]).
%% Functions concerning file attributes
-export([stat/2, set_xattr/3, set_xattr/5, get_xattr/4, remove_xattr/3, list_xattr/4]).
%% Functions concerning cdmi attributes
-export([get_transfer_encoding/2, set_transfer_encoding/3,
    get_cdmi_completion_status/2, set_cdmi_completion_status/3,
    get_mimetype/2, set_mimetype/3]).
%% Functions concerning file shares
-export([create_share/3, remove_share/2, remove_share_by_guid/2]).
%% Functions concerning metadata
-export([get_metadata/5, set_metadata/5, has_custom_metadata/2, remove_metadata/3]).

%%--------------------------------------------------------------------
%% IDs of entities
-type file_guid() :: binary().
%%--------------------------------------------------------------------

% todo TL do something with those types.
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
-type access_control_entity() :: #access_control_entity{}.
-type transfer_encoding() :: binary(). % <<"utf-8">> | <<"base64">>
-type cdmi_completion_status() :: binary(). % <<"Completed">> | <<"Processing">> | <<"Error">>
-type mimetype() :: binary().
-type share_id() :: binary().
-type share_file_guid() :: binary().
-type share_name() :: binary().
-type metadata_type() :: custom_metadata:type().
-type metadata_filter() :: custom_metadata:filter().
-type metadata_value() :: custom_metadata:value().
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
%% @end
%%--------------------------------------------------------------------
-spec mkdir(Auth :: onedata_auth_api:auth(), Path :: file_path()) ->
    {ok, file_guid()} | error_reply().
mkdir(Auth, Path) ->
    logical_file_manager:mkdir(Auth, Path).

-spec mkdir(Auth :: onedata_auth_api:auth(), Path :: file_path(),
    Mode :: file_meta:posix_permissions() | undefined) ->
    {ok, file_guid()} | error_reply().
mkdir(Auth, Path, Mode) ->
    logical_file_manager:mkdir(Auth, Path, Mode).

-spec mkdir(SessId :: onedata_auth_api:auth(), ParentGuid :: file_guid(),
    Name :: file_name(), Mode :: file_meta:posix_permissions() | undefined) ->
    {ok, DirUUID :: file_meta:uuid()} | error_reply().
mkdir(SessId, ParentGuid, Name, Mode) ->
    logical_file_manager:mkdir(SessId, ParentGuid, Name, Mode).

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
%% @end
%%--------------------------------------------------------------------
-spec get_children_count(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path()) ->
    {ok, integer()} | error_reply().
get_children_count(Auth, FileKey) ->
    logical_file_manager:get_children_count(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc Deletes a file or directory recursively.
%% @end
%%--------------------------------------------------------------------
-spec rm_recursive(Auth :: onedata_auth_api:auth(),
    FileKey :: file_id_or_path()) -> ok | error_reply().
rm_recursive(Auth, FileKey) ->
    logical_file_manager:rm_recursive(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc Moves a file or directory to a new location.
%% @end
%%--------------------------------------------------------------------
-spec mv(onedata_auth_api:auth(), file_id_or_path(), file_path()) ->
    {ok, file_guid()} | error_reply().
mv(Auth, FileEntry, TargetPath) ->
    logical_file_manager:mv(Auth, FileEntry, TargetPath).

%%--------------------------------------------------------------------
%% @doc Copies a file or directory to given location.
%% @end
%%--------------------------------------------------------------------
-spec cp(onedata_auth_api:auth(), file_id_or_path(), file_path()) ->
    {ok, file_guid()} | error_reply().
cp(Auth, FileEntry, TargetPath) ->
    logical_file_manager:cp(Auth, FileEntry, TargetPath).

%%--------------------------------------------------------------------
%% @doc Returns full path of file
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(Auth :: onedata_auth_api:auth(), Uuid :: file_guid()) ->
    {ok, file_path()}.
get_file_path(Auth, Uuid) ->
    logical_file_manager:get_file_path(Auth, Uuid).

%%--------------------------------------------------------------------
%% @doc Removes a file or an empty directory.
%% @end
%%--------------------------------------------------------------------
-spec unlink(onedata_auth_api:auth(), file_id_or_path()) -> ok | error_reply().
unlink(Auth, FileEntry) ->
    logical_file_manager:unlink(Auth, FileEntry, false).

%%--------------------------------------------------------------------
%% @doc Flushes waiting events for session connected with handler.
%% @end
%%--------------------------------------------------------------------
-spec fsync(file_handle()) -> ok | error_reply().
fsync(Handle) ->
    logical_file_manager:fsync(Handle).

%%--------------------------------------------------------------------
%% @doc Creates a new file.
%% @end
%%--------------------------------------------------------------------
-spec create(Auth :: onedata_auth_api:auth(), Path :: file_path(), Mode :: file_meta:posix_permissions()) ->
    {ok, file_guid()} | error_reply().
create(Auth, Path, Mode) ->
    logical_file_manager:create(Auth, Path, Mode).

-spec create(Auth :: onedata_auth_api:auth(), ParentGuid :: file_guid(),
    Name :: file_name(), Mode :: file_meta:posix_permissions()) ->
    {ok, file_guid()} | error_reply().
create(Auth, ParentGuid, Name, Mode) ->
    logical_file_manager:create(Auth, ParentGuid, Name, Mode).

%%--------------------------------------------------------------------
%% @doc Opens a file in selected mode and returns a file handle used to read or write.
%% @end
%%--------------------------------------------------------------------
-spec open(onedata_auth_api:auth(), FileKey :: file_id_or_path(), OpenType :: open_mode()) ->
    {ok, file_handle()} | error_reply().
open(Auth, FileKey, OpenType) ->
    logical_file_manager:open(Auth, FileKey, OpenType).

%%--------------------------------------------------------------------
%% @doc Writes data to a file. Returns number of written bytes.
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: file_handle(), Offset :: integer(), Buffer :: binary()) ->
    {ok, NewHandle :: file_handle(), integer()} | error_reply().
write(FileHandle, Offset, Buffer) ->
    logical_file_manager:write(FileHandle, Offset, Buffer).

%%--------------------------------------------------------------------
%% @doc Reads requested part of a file.
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: file_handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: file_handle(), binary()} | error_reply().
read(FileHandle, Offset, MaxSize) ->
    logical_file_manager:read(FileHandle, Offset, MaxSize).

%%--------------------------------------------------------------------
%% @doc Truncates a file.
%% @end
%%--------------------------------------------------------------------
-spec truncate(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path(), Size :: non_neg_integer()) ->
    ok | error_reply().
truncate(Auth, FileKey, Size) ->
    logical_file_manager:truncate(Auth, FileKey, Size).

%%--------------------------------------------------------------------
%% @doc Releases previously opened file.
%% @end
%%--------------------------------------------------------------------
-spec release(FileHandle :: file_handle()) ->
    ok | error_reply().
release(FileHandle) ->
    logical_file_manager:release(FileHandle).

%%--------------------------------------------------------------------
%% @doc Returns block map for a file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path()) ->
    {ok, list()} | error_reply().
get_file_distribution(Auth, FileKey) ->
    logical_file_manager:get_file_distribution(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc Replicates file on given provider.
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path(), ProviderId :: binary()) ->
    ok | error_reply().
replicate_file(Auth, FileKey, ProviderId) ->
    logical_file_manager:replicate_file(Auth, FileKey, ProviderId).

%%--------------------------------------------------------------------
%% @doc
%% Invalidates file replica on given provider, migrates unique data to provider
%% given as MigrateProviderId
%% @end
%%--------------------------------------------------------------------
-spec invalidate_file_replica(Auth :: onedata_auth_api:auth(), FileKey :: file_id_or_path(),
    ProviderId :: oneprovider:id(), MigrationProviderId :: undefined | oneprovider:id()) ->
    ok | error_reply().
invalidate_file_replica(Auth, FileKey, ProviderId, MigrationProviderId) ->
    logical_file_manager:invalidate_file_replica(Auth, FileKey, ProviderId, MigrationProviderId).

%%--------------------------------------------------------------------
%% @doc Changes the permissions of a file.
%% @end
%%--------------------------------------------------------------------
-spec set_perms(onedata_auth_api:auth(), file_key(), perms_octal()) -> ok | error_reply().
set_perms(Auth, FileKey, NewPerms) ->
    logical_file_manager:set_perms(Auth, FileKey, NewPerms).

%%--------------------------------------------------------------------
%% @doc Checks if current user has given permissions for given file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms(onedata_auth_api:auth(), FileKey :: file_key(), PermsType :: permission_type()) ->
    {ok, boolean()} | error_reply().
check_perms(Auth, FileKey, PermType) ->
    logical_file_manager:check_perms(Auth, FileKey, PermType).

%%--------------------------------------------------------------------
%% @doc Returns file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec get_acl(onedata_auth_api:auth(), file_key()) ->
    {ok, [access_control_entity()]} | error_reply().
get_acl(Auth, FileKey) ->
    logical_file_manager:get_acl(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc Updates file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec set_acl(onedata_auth_api:auth(), file_key(), EntityList :: [access_control_entity()]) ->
    ok | error_reply().
set_acl(Auth, FileKey, EntityList) ->
    logical_file_manager:set_acl(Auth, FileKey, EntityList).

%%--------------------------------------------------------------------
%% @doc Removes file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec remove_acl(onedata_auth_api:auth(), file_key()) -> ok | error_reply().
remove_acl(Auth, FileKey) ->
    logical_file_manager:remove_acl(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc Returns file attributes.
%% @end
%%--------------------------------------------------------------------
-spec stat(onedata_auth_api:auth(), file_key()) -> {ok, file_attributes()} | error_reply().
stat(Auth, FileKey) ->
    logical_file_manager:stat(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc Returns file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec get_xattr(onedata_auth_api:auth(), file_key(), xattr_name(), boolean()) ->
    {ok, #xattr{}} | error_reply().
get_xattr(Auth, FileKey, XattrName, Inherited) ->
    logical_file_manager:get_xattr(Auth, FileKey, XattrName, Inherited).

%%--------------------------------------------------------------------
%% @equiv set_xattr(Auth, FileKey, Xattr, false, false).
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(onedata_auth_api:auth(), file_key(), #xattr{}) ->
    ok | error_reply().
set_xattr(Auth, FileKey, Xattr) ->
    set_xattr(Auth, FileKey, Xattr, false, false).

%%--------------------------------------------------------------------
%% @doc Updates file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(onedata_auth_api:auth(), file_key(), #xattr{},
    Create :: boolean(), Replace :: boolean()) -> ok | error_reply().
set_xattr(Auth, FileKey, Xattr, Create, Replace) ->
    logical_file_manager:set_xattr(Auth, FileKey, Xattr, Create, Replace).

%%--------------------------------------------------------------------
%% @doc Removes file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(onedata_auth_api:auth(), file_key(), xattr_name()) ->
    ok | error_reply().
remove_xattr(Auth, FileKey, XattrName) ->
    logical_file_manager:remove_xattr(Auth, FileKey, XattrName).

%%--------------------------------------------------------------------
%% @doc Returns complete list of extended attribute names of a file.
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(onedata_auth_api:auth(), file_key(), boolean(), boolean()) -> {ok, [xattr_name()]} | error_reply().
list_xattr(Auth, FileKey, Inherited, ShowInternal) ->
    logical_file_manager:list_xattr(Auth, FileKey, Inherited, ShowInternal).

%%--------------------------------------------------------------------
%% @doc Returns encoding suitable for rest transfer.
%% @end
%%--------------------------------------------------------------------
-spec get_transfer_encoding(onedata_auth_api:auth(), file_key()) ->
    {ok, transfer_encoding()} | error_reply().
get_transfer_encoding(Auth, FileKey) ->
    logical_file_manager:get_transfer_encoding(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc Sets encoding suitable for rest transfer.
%% @end
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
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype(onedata_auth_api:auth(), file_key()) ->
    {ok, mimetype()} | error_reply().
get_mimetype(Auth, FileKey) ->
    logical_file_manager:get_mimetype(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc Sets mimetype of file.
%% @end
%%--------------------------------------------------------------------
-spec set_mimetype(onedata_auth_api:auth(), file_key(), mimetype()) ->
    ok | error_reply().
set_mimetype(Auth, FileKey, Mimetype) ->
    logical_file_manager:set_mimetype(Auth, FileKey, Mimetype).

%%--------------------------------------------------------------------
%% @doc
%% Creates a share for given file. File can be shared with anyone or
%% only specified group of users.
%% @end
%%--------------------------------------------------------------------
-spec create_share(onedata_auth_api:auth(), file_key(), share_name()) ->
    {ok, {share_id(), share_file_guid()}} | error_reply().
create_share(Auth, FileKey, Name) ->
    logical_file_manager:create_share(Auth, FileKey, Name).

%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareID.
%% @end
%%--------------------------------------------------------------------
-spec remove_share(onedata_auth_api:auth(), share_id()) -> ok | error_reply().
remove_share(Auth, ShareID) ->
    logical_file_manager:remove_share(Auth, ShareID).

%%--------------------------------------------------------------------
%% @doc Removes file share by ShareGuid.
%% @end
%%--------------------------------------------------------------------
-spec remove_share_by_guid(onedata_auth_api:auth(), share_file_guid()) -> ok | error_reply().
remove_share_by_guid(Auth, ShareGuid) ->
    logical_file_manager:remove_share_by_guid(Auth, ShareGuid).

%%--------------------------------------------------------------------
%% @doc Get json metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec get_metadata(onedata_auth_api:auth(), file_key(), metadata_type(), metadata_filter(), boolean()) ->
    {ok, custom_metadata:value()} | error_reply().
get_metadata(Auth, FileKey, Type, Names, Inherited) ->
    logical_file_manager:get_metadata(Auth, FileKey, Type, Names, Inherited).

%%--------------------------------------------------------------------
%% @doc Set json metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec set_metadata(onedata_auth_api:auth(), file_key(), metadata_type(), metadata_value(), metadata_filter()) ->
    ok | error_reply().
set_metadata(Auth, FileKey, Type, Value, Names) ->
    logical_file_manager:set_metadata(Auth, FileKey, Type, Value, Names).

%%--------------------------------------------------------------------
%% @doc Check if file has custom metadata defined
%% @end
%%--------------------------------------------------------------------
-spec has_custom_metadata(onedata_auth_api:auth(), file_key()) ->
    {ok, boolean()} | error_reply().
has_custom_metadata(Auth, FileKey) ->
    logical_file_manager:has_custom_metadata(Auth, FileKey).

%%--------------------------------------------------------------------
%% @doc Remove metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec remove_metadata(onedata_auth_api:auth(), file_key(), metadata_type()) ->
    ok | error_reply().
remove_metadata(Auth, FileKey, Type) ->
    logical_file_manager:remove_metadata(Auth, FileKey, Type).