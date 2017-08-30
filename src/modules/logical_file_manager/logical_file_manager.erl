%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module offers a high level API for operating on logical filesystem.
%%% When passing a file in arguments, one can use one of the following:
%%% {guid, FileGuid} - preferred and fast. guids are returned from 'ls' function.
%%% {path, BinaryFilePath} - slower than by guid (path has to be resolved).
%%%    Discouraged, but there are cases when this is useful.
%%% Some functions accepts also Handle obtained from open operation.
%%%
%%% This module is merely a convenient wrapper that calls functions from lfm_xxx modules.
%%% @end
%%%-------------------------------------------------------------------
-module(logical_file_manager).

-define(run(F),
    try
        F()
    catch
        _:{badmatch, {error, {not_found, file_meta}}} ->
            {error, ?ENOENT};
        _:{badmatch, Error} ->
            Error;
        _:___Reason ->
            ?error_stacktrace("logical_file_manager generic error: ~p", [___Reason]),
            {error, ___Reason}
    end).

-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").

-type handle() :: lfm_context:ctx().
-type file_key() :: fslogic_worker:file_guid_or_path() | {handle, handle()}.
-type error_reply() :: {error, term()}.

-export_type([handle/0, file_key/0, error_reply/0]).

%% Functions operating on directories
-export([mkdir/2, mkdir/3, mkdir/4, ls/4, read_dir_plus/4,
    get_child_attr/3, get_children_count/2, get_parent/2]).
%% Functions operating on directories or files
-export([mv/3, cp/3, get_file_path/2, rm_recursive/2, unlink/3, replicate_file/3,
    invalidate_file_replica/4]).
%% Functions operating on files
-export([create/2, create/3, create/4, open/3, fsync/1, fsync/3, write/3, read/3,
    truncate/3, release/1, get_file_distribution/2, create_and_open/4, create_and_open/5]).
%% Functions concerning file permissions
-export([set_perms/3, check_perms/3, set_acl/3, get_acl/2, remove_acl/2]).
%% Functions concerning file attributes
-export([stat/2, get_xattr/4, set_xattr/3, set_xattr/5, remove_xattr/3, list_xattr/4,
    update_times/5]).
%% Functions concerning cdmi attributes
-export([get_transfer_encoding/2, set_transfer_encoding/3, get_cdmi_completion_status/2,
    set_cdmi_completion_status/3, get_mimetype/2, set_mimetype/3]).
%% Functions concerning file shares
-export([create_share/3, remove_share/2, remove_share_by_guid/2]).
%% Functions concerning metadata
-export([get_metadata/5, set_metadata/5, has_custom_metadata/2, remove_metadata/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(session:id(), Path :: file_meta:path()) ->
    {ok, DirGuid :: file_meta:uuid()} | error_reply().
mkdir(SessId, Path) ->
    ?run(fun() -> lfm_dirs:mkdir(SessId, Path) end).

-spec mkdir(session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions() | undefined) ->
    {ok, DirGUID :: fslogic_worker:file_guid()} | error_reply().
mkdir(SessId, Path, Mode) ->
    ?run(fun() -> lfm_dirs:mkdir(SessId, Path, Mode) end).

-spec mkdir(session:id(), ParentGuid :: fslogic_worker:file_guid(),
    Name :: file_meta:name(), Mode :: file_meta:posix_permissions() | undefined) ->
    {ok, DirGuid :: fslogic_worker:file_guid()} | error_reply().
mkdir(SessId, ParentGuid, Name, Mode) ->
    ?run(fun() -> lfm_dirs:mkdir(SessId, ParentGuid, Name, Mode) end).

%%--------------------------------------------------------------------
%% @doc
%% Deletes a directory with all its children.
%% @end
%%--------------------------------------------------------------------
-spec rm_recursive(session:id(), fslogic_worker:file_guid_or_path()) ->
    ok | error_reply().
rm_recursive(SessId, FileKey) ->
    ?run(fun() -> lfm_files:rm(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory.
%% Returns up to Limit of entries, starting with Offset-th entry.
%% @end
%%--------------------------------------------------------------------
-spec ls(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(), Limit :: integer()) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}]} | error_reply().
ls(SessId, FileKey, Offset, Limit) ->
    ?run(fun() -> lfm_dirs:ls(SessId, FileKey, Offset, Limit) end).

%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory. Returns attributes of files.
%% Returns up to Limit of entries, starting with Offset-th entry.
%% @end
%%--------------------------------------------------------------------
-spec read_dir_plus(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(), Limit :: integer()) ->
    {ok, [#file_attr{}]} | logical_file_manager:error_reply().
read_dir_plus(SessId, FileKey, Offset, Limit) ->
    ?run(fun() -> lfm_dirs:read_dir_plus(SessId, FileKey, Offset, Limit) end).

%%--------------------------------------------------------------------
%% @doc
%% Gets attribute of a child with given name.
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(session:id(), ParentGuid :: fslogic_worker:file_guid(),
    ChildName :: file_meta:name()) ->
    {ok, #file_attr{}} | error_reply().
get_child_attr(SessId, ParentGuid, ChildName)  ->
    ?run(fun() -> lfm_dirs:get_child_attr(SessId, ParentGuid, ChildName) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns number of children of a directory.
%% @end
%%--------------------------------------------------------------------
-spec get_children_count(session:id(),
    FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, integer()} | error_reply().
get_children_count(SessId, FileKey) ->
    ?run(fun() -> lfm_dirs:get_children_count(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns uuid of parent for given file.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
get_parent(SessId, FileKey) ->
    ?run(fun() -> lfm_files:get_parent(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%% @end
%%--------------------------------------------------------------------
-spec mv(session:id(), fslogic_worker:file_guid_or_path(), file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
mv(SessId, FileEntry, TargetPath) ->
    ?run(fun() -> lfm_files:mv(SessId, FileEntry, TargetPath) end).

%%--------------------------------------------------------------------
%% @doc
%% Copies a file or directory to given location.
%% @end
%%--------------------------------------------------------------------
-spec cp(session:id(), fslogic_worker:file_guid_or_path(), file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
cp(SessId, FileEntry, TargetPath) ->
    ?run(fun() -> lfm_files:cp(SessId, FileEntry, TargetPath) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns full path of file
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(session:id(), FileGUID :: fslogic_worker:file_guid()) ->
    {ok, file_meta:path()}.
get_file_path(SessId, FileGUID) ->
    ?run(fun() -> lfm_files:get_file_path(SessId, FileGUID) end).

%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%% @end
%%--------------------------------------------------------------------
-spec unlink(session:id(), fslogic_worker:file_guid_or_path(), boolean()) ->
    ok | error_reply().
unlink(SessId, FileEntry, Silent) ->
    ?run(fun() -> lfm_files:unlink(SessId, FileEntry, Silent) end).

%%--------------------------------------------------------------------
%% @doc
%% Replicates file on given provider.
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    ProviderId :: oneprovider:id()) ->
    ok | error_reply().
replicate_file(SessId, FileKey, ProviderId) ->
    ?run(fun() -> lfm_files:replicate_file(SessId, FileKey, ProviderId) end).

%%--------------------------------------------------------------------
%% @doc
%% Invalidates file replica on given provider, migrates unique data to provider
%% given as MigrateProviderId
%% @end
%%--------------------------------------------------------------------
-spec invalidate_file_replica(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    ProviderId :: oneprovider:id(), MigrationProviderId :: undefined | oneprovider:id()) ->
    ok | error_reply().
invalidate_file_replica(SessId, FileKey, ProviderId, MigrationProviderId) ->
    ?run(fun() -> lfm_files:invalidate_file_replica(SessId, FileKey, ProviderId, MigrationProviderId) end).

%%--------------------------------------------------------------------
%% @doc
%% Creates a new file
%% @end
%%--------------------------------------------------------------------
-spec create(session:id(), Path :: file_meta:path()) ->
    {ok, file_meta:file_guid()} | error_reply().
create(SessId, Path) ->
    ?run(fun() -> lfm_files:create(SessId, Path) end).

-spec create(session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
create(SessId, Path, Mode) ->
    ?run(fun() -> lfm_files:create(SessId, Path, Mode) end).

-spec create(session:id(), ParentGuid :: fslogic_worker:file_guid(),
    Name :: file_meta:name(), Mode :: undefined | file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
create(SessId, ParentGuid, Name, Mode) ->
    ?run(fun() -> lfm_files:create(SessId, ParentGuid, Name, Mode) end).

%%--------------------------------------------------------------------
%% @doc
%% Creates and opens a new file
%% @end
%%--------------------------------------------------------------------
-spec create_and_open(session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions(), fslogic_worker:open_flag()) ->
    {ok, {fslogic_worker:file_guid(), logical_file_manager:handle()}}
    | error_reply().
create_and_open(SessId, Path, Mode, OpenFlag) ->
    ?run(fun() -> lfm_files:create_and_open(SessId, Path, Mode, OpenFlag) end).

-spec create_and_open(session:id(), ParentGuid :: fslogic_worker:file_guid(),
    Name :: file_meta:name(), Mode :: undefined | file_meta:posix_permissions(),
    fslogic_worker:open_flag()) ->
    {ok, {fslogic_worker:file_guid(), logical_file_manager:handle()}}
    | error_reply().
create_and_open(SessId, ParentGuid, Name, Mode, OpenFlag) ->
    ?run(fun() -> lfm_files:create_and_open(SessId, ParentGuid, Name, Mode, OpenFlag) end).

%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode and returns a file handle used to read or write.
%% @end
%%--------------------------------------------------------------------
-spec open(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    OpenType :: helpers:open_flag()) ->
    {ok, handle()} | error_reply().
open(SessId, FileKey, OpenType) ->
    ?run(fun() -> lfm_files:open(SessId, FileKey, OpenType) end).

%%--------------------------------------------------------------------
%% @doc
%% Gets necessary data from handle and executes fsync/3
%% @end
%%--------------------------------------------------------------------
-spec fsync(FileHandle :: handle()) -> ok | {error, Reason :: term()}.
fsync(FileHandle) ->
    ?run(fun() -> lfm_files:fsync(FileHandle) end).

%%--------------------------------------------------------------------
%% @doc
%% Flushes waiting events for session connected with handler.
%% @end
%%--------------------------------------------------------------------
-spec fsync(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    oneprovider:id()) -> ok | {error, Reason :: term()}.
fsync(SessId, FileKey, ProviderId) ->
    ?run(fun() -> lfm_files:fsync(SessId, FileKey, ProviderId) end).

%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file. Returns number of written bytes.
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: handle(), Offset :: integer(), Buffer :: binary()) ->
    {ok, NewHandle :: handle(), integer()} | error_reply().
write(FileHandle, Offset, Buffer) ->
    ?run(fun() -> lfm_files:write(FileHandle, Offset, Buffer) end).

%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file.
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: handle(), binary()} | error_reply().
read(FileHandle, Offset, MaxSize) ->
    ?run(fun() -> lfm_files:read(FileHandle, Offset, MaxSize) end).

%%--------------------------------------------------------------------
%% @doc
%% Truncates a file.
%% @end
%%--------------------------------------------------------------------
-spec truncate(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Size :: non_neg_integer()) -> ok | error_reply().
truncate(SessId, FileKey, Size) ->
    ?run(fun() -> lfm_files:truncate(SessId, FileKey, Size) end).

%%--------------------------------------------------------------------
%% @doc
%% Releases previously opened  file.
%% @end
%%--------------------------------------------------------------------
-spec release(handle()) -> ok | error_reply().
release(FileHandle) ->
    ?run(fun() -> lfm_files:release(FileHandle) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, list()} | error_reply().
get_file_distribution(SessId, FileKey) ->
    ?run(fun() -> lfm_files:get_file_distribution(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file.
%% @end
%%--------------------------------------------------------------------
-spec set_perms(session:id(), FileKey :: file_key(),
    NewPerms :: file_meta:posix_permissions()) ->
    ok | error_reply().
set_perms(SessId, FileKey, NewPerms) ->
    ?run(fun() -> lfm_perms:set_perms(SessId, FileKey, NewPerms) end).

%%--------------------------------------------------------------------
%% @doc
%% Checks if current user has given permissions for given file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms(session:id(), file_key(), helpers:open_flag()) ->
    {ok, boolean()} | error_reply().
check_perms(SessId, FileKey, PermType) ->
    ?run(fun() -> lfm_perms:check_perms(SessId, FileKey, PermType) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec get_acl(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, [lfm_perms:access_control_entity()]} | error_reply().
get_acl(SessId, FileKey) ->
    ?run(fun() -> lfm_perms:get_acl(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Updates file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec set_acl(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    EntityList :: [lfm_perms:access_control_entity()]) -> ok | error_reply().
set_acl(SessId, FileKey, EntityList) ->
    ?run(fun() -> lfm_perms:set_acl(SessId, FileKey, EntityList) end).

%%--------------------------------------------------------------------
%% @doc
%% Removes file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec remove_acl(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    ok | error_reply().
remove_acl(SessId, FileKey) ->
    ?run(fun() -> lfm_perms:remove_acl(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes.
%% @end
%%--------------------------------------------------------------------
-spec stat(session:id(), file_key()) ->
    {ok, lfm_attrs:file_attributes()} | error_reply().
stat(SessId, FileKey) ->
    ?run(fun() -> lfm_attrs:stat(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Changes file timestamps.
%% @end
%%--------------------------------------------------------------------
-spec update_times(session:id(), file_key(), ATime :: file_meta:time() | undefined,
    MTime :: file_meta:time() | undefined, CTime :: file_meta:time() | undefined) ->
    ok | error_reply().
update_times(SessId, FileKey, ATime, MTime, CTime) ->
    ?run(fun() -> lfm_attrs:update_times(SessId, FileKey, ATime, MTime, CTime) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec get_xattr(session:id(), file_key(), xattr:name(), boolean()) ->
    {ok, #xattr{}} | error_reply().
get_xattr(SessId, FileKey, XattrName, Inherited) ->
    ?run(fun() -> lfm_attrs:get_xattr(SessId, FileKey, XattrName, Inherited) end).

%%--------------------------------------------------------------------
%% @equiv set_xattr(SessId, FileKey, Xattr, false, false).
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(session:id(), file_key(), #xattr{}) ->
    ok | error_reply().
set_xattr(SessId, FileKey, Xattr) ->
    set_xattr(SessId, FileKey, Xattr, false, false).

%%--------------------------------------------------------------------
%% @doc
%% Updates file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(session:id(), file_key(), #xattr{}, Create :: boolean(), Replace :: boolean()) ->
    ok | error_reply().
set_xattr(SessId, FileKey, Xattr, Create, Replace) ->
    ?run(fun() -> lfm_attrs:set_xattr(SessId, FileKey, Xattr, Create, Replace) end).

%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(session:id(), file_key(), xattr:name()) -> ok | error_reply().
remove_xattr(SessId, FileKey, XattrName) ->
    ?run(fun() -> lfm_attrs:remove_xattr(SessId, FileKey, XattrName) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes of a file.
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(session:id(), file_key(), boolean(), boolean()) ->
    {ok, [xattr:name()]} | error_reply().
list_xattr(SessId, FileKey, Inherited, ShowInternal) ->
    ?run(fun() -> lfm_attrs:list_xattr(SessId, FileKey, Inherited, ShowInternal) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns encoding suitable for rest transfer.
%% @end
%%--------------------------------------------------------------------
-spec get_transfer_encoding(session:id(), file_key()) ->
    {ok, xattr:transfer_encoding()} | error_reply().
get_transfer_encoding(SessId, FileKey) ->
    ?run(fun() -> lfm_attrs:get_transfer_encoding(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Sets encoding suitable for rest transfer.
%% @end
%%--------------------------------------------------------------------
-spec set_transfer_encoding(session:id(), file_key(), xattr:transfer_encoding()) ->
    ok | error_reply().
set_transfer_encoding(SessId, FileKey, Encoding) ->
    ?run(fun() ->
        lfm_attrs:set_transfer_encoding(SessId, FileKey, Encoding) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status(session:id(), file_key()) ->
    {ok, xattr:cdmi_completion_status()} | error_reply().
get_cdmi_completion_status(SessId, FileKey) ->
    ?run(fun() -> lfm_attrs:get_cdmi_completion_status(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Sets completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status(session:id(), file_key(), xattr:cdmi_completion_status()) ->
    ok | error_reply().
set_cdmi_completion_status(SessId, FileKey, CompletionStatus) ->
    ?run(fun() ->
        lfm_attrs:set_cdmi_completion_status(SessId, FileKey, CompletionStatus) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns mimetype of file.
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype(session:id(), file_key()) ->
    {ok, xattr:mimetype()} | error_reply().
get_mimetype(SessId, FileKey) ->
    ?run(fun() -> lfm_attrs:get_mimetype(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Sets mimetype of file.
%% @end
%%--------------------------------------------------------------------
-spec set_mimetype(session:id(), file_key(), xattr:mimetype()) ->
    ok | error_reply().
set_mimetype(SessId, FileKey, Mimetype) ->
    ?run(fun() -> lfm_attrs:set_mimetype(SessId, FileKey, Mimetype) end).

%%--------------------------------------------------------------------
%% @doc
%% Creates a share for given file. File can be shared with anyone or
%% only specified group of users.
%% @end
%%--------------------------------------------------------------------
-spec create_share(session:id(), file_key(), od_share:name()) ->
    {ok, {od_share:id(), od_share:share_guid()}} | error_reply().
create_share(SessId, FileKey, Name) ->
    ?run(fun() -> lfm_shares:create_share(SessId, FileKey, Name) end).

%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareID.
%% @end
%%--------------------------------------------------------------------
-spec remove_share(session:id(), od_share:id()) -> ok | error_reply().
remove_share(SessId, ShareID) ->
    ?run(fun() -> lfm_shares:remove_share(SessId, ShareID) end).

%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareGuid.
%% @end
%%--------------------------------------------------------------------
-spec remove_share_by_guid(session:id(), od_share:share_guid()) -> ok | error_reply().
remove_share_by_guid(SessId, ShareGuid) ->
    ?run(fun() -> lfm_shares:remove_share_by_guid(SessId, ShareGuid) end).

%%--------------------------------------------------------------------
%% @doc
%% Get metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec get_metadata(session:id(), file_key(), custom_metadata:type(),
    custom_metadata:filter(), boolean()) ->
    {ok, custom_metadata:value()} | error_reply().
get_metadata(SessId, FileKey, Type, Names, Inherited) ->
    ?run(fun() -> lfm_attrs:get_metadata(SessId, FileKey, Type, Names, Inherited) end).

%%--------------------------------------------------------------------
%% @doc
%% Set metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec set_metadata(session:id(), file_key(), custom_metadata:type(),
    custom_metadata:value(), custom_metadata:filter()) -> ok | error_reply().
set_metadata(SessId, FileKey, Type, Value, Names) ->
    ?run(fun() -> lfm_attrs:set_metadata(SessId, FileKey, Type, Value, Names) end).

%%--------------------------------------------------------------------
%% @doc
%% Check if file has custom metadata defined
%% @end
%%--------------------------------------------------------------------
-spec has_custom_metadata(session:id(), file_key()) -> {ok, boolean()} | error_reply().
has_custom_metadata(SessId, FileKey) ->
    ?run(fun() -> lfm_attrs:has_custom_metadata(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Remove metadata linked with file
%% @end
%%--------------------------------------------------------------------
-spec remove_metadata(session:id(), file_key(), custom_metadata:type()) ->
    ok | error_reply().
remove_metadata(SessId, FileKey, Type) ->
    ?run(fun() -> lfm_attrs:remove_metadata(SessId, FileKey, Type) end).
