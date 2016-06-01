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
% 4) All errors should be listed in one hrl -> ctool/include/posix/errors.hrl
% 4) Common types should be listed in one hrl -> types.hrl
% 5) chown is no longer featured in lfm as it is not needed by higher layers
% 6) Blocks related functions should go to another module (synchronize, mark_as_truncated etc).


-define(run(F),
    try
        F()
    catch
        _:{badmatch, {error, {not_found, file_meta}}} ->
            {error, ?ENOENT};
        _:{badmatch, {error, enoent}} ->
            {error, ?ENOENT};
        _:___Reason ->
            ?error_stacktrace("logical_file_manager generic error: ~p", [___Reason]),
            {error, ___Reason}
    end).


-include("global_definitions.hrl").
-include("modules/fslogic/lfm_internal.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-type handle() :: #lfm_handle{}.
-type file_key() :: fslogic_worker:file_guid_or_path() | {handle, handle()}.
-type error_reply() :: {error, term()}.

-export_type([handle/0, file_key/0, error_reply/0]).

%% Functions operating on directories
-export([mkdir/2, mkdir/3, ls/4, get_children_count/2, get_parent/2]).
%% Functions operating on directories or files
-export([exists/1, mv/3, cp/3, get_file_path/2, rm_recursive/2, unlink/1, unlink/2]).
%% Functions operating on files
-export([create/2, create/3, open/3, fsync/1, write/3, read/3, truncate/2,
    truncate/3, release/1, get_file_distribution/2, replicate_file/3]).
%% Functions concerning file permissions
-export([set_perms/3, check_perms/2, set_acl/2, set_acl/3, get_acl/1, get_acl/2,
    remove_acl/1, remove_acl/2]).
%% Functions concerning file attributes
-export([stat/1, stat/2, get_xattr/2, get_xattr/3, set_xattr/2, set_xattr/3,
    remove_xattr/2, remove_xattr/3, list_xattr/1, list_xattr/2, update_times/4,
    update_times/5]).
%% Functions concerning cdmi attributes
-export([get_transfer_encoding/2, set_transfer_encoding/3, get_cdmi_completion_status/2,
    set_cdmi_completion_status/3, get_mimetype/2, set_mimetype/3]).
%% Functions concerning symbolic links
-export([create_symlink/2, read_symlink/1, remove_symlink/1]).
%% Functions concerning file shares
-export([create_share/2, get_share/1, remove_share/1]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates a directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(SessId :: session:id(), Path :: file_meta:path()) ->
    {ok, DirUUID :: file_meta:uuid()} | error_reply().
mkdir(SessId, Path) ->
    ?run(fun() -> lfm_dirs:mkdir(SessId, Path) end).

-spec mkdir(SessId :: session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions()) ->
    {ok, DirGUID :: fslogic_worker:file_guid()} | error_reply().
mkdir(SessId, Path, Mode) ->
    ?run(fun() -> lfm_dirs:mkdir(SessId, Path, Mode) end).

%%--------------------------------------------------------------------
%% @doc
%% Deletes a directory with all its children.
%% @end
%%--------------------------------------------------------------------
-spec rm_recursive(session:id(), fslogic_worker:file_guid_or_path()) -> ok | error_reply().
rm_recursive(SessId, FileKey) ->
    ?run(fun() -> lfm_utils:rm(SessId, FileKey) end).

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
%% Returns number of children of a directory.
%% @end
%%--------------------------------------------------------------------
-spec get_children_count(SessId :: session:id(),
    FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, integer()} | error_reply().
get_children_count(SessId, FileKey) ->
    ?run(fun() -> lfm_dirs:get_children_count(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns uuid of parent for given file.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
get_parent(SessId, FileKey) ->
    ?run(fun() -> lfm_files:get_parent(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Checks if a file or directory exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(FileKey :: file_key()) ->
    {ok, boolean()} | error_reply().
exists(FileKey) ->
    ?run(fun() -> lfm_files:exists(FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%% @end
%%--------------------------------------------------------------------
-spec mv(session:id(), fslogic_worker:file(), file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
mv(SessId, FileEntry, TargetPath) ->
    ?run(fun() -> lfm_files:mv(SessId, FileEntry, TargetPath) end).

%%--------------------------------------------------------------------
%% @doc
%% Copies a file or directory to given location.
%% @end
%%--------------------------------------------------------------------
-spec cp(session:id(), fslogic_worker:file_guid_or_path(), file_meta:path()) ->
    ok | error_reply().
cp(Auth, FileEntry, TargetPath) ->
    ?run(fun() -> lfm_files:cp(Auth, FileEntry, TargetPath) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns full path of file
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(SessId :: session:id(), FileGUID :: fslogic_worker:file_guid()) ->
    {ok, file_meta:path()}.
get_file_path(SessId, FileGUID) ->
    ?run(fun() -> lfm_files:get_file_path(SessId, FileGUID) end).

%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%% @end
%%--------------------------------------------------------------------
-spec unlink(handle()) -> ok | error_reply().
unlink(Handle) ->
    ?run(fun() -> lfm_files:unlink(Handle) end).

-spec unlink(session:id(), fslogic_worker:file_guid_or_path()) -> ok | error_reply().
unlink(SessId, FileEntry) ->
    ?run(fun() -> lfm_files:unlink(SessId, FileEntry) end).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file with default mode.
%% @end
%%--------------------------------------------------------------------
-spec create(SessId :: session:id(), Path :: file_meta:path()) ->
    {ok, file_meta:uuid()} | error_reply().
create(SessId, Path) ->
    ?run(fun() -> lfm_files:create(SessId, Path) end).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file.
%% @end
%%--------------------------------------------------------------------
-spec create(SessId :: session:id(), Path :: file_meta:path(),
    Mode :: file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
create(SessId, Path, Mode) ->
    ?run(fun() -> lfm_files:create(SessId, Path, Mode) end).

%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode and returns a file handle used to read or write.
%% @end
%%--------------------------------------------------------------------
-spec open(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    OpenType :: helpers:open_mode()) ->
    {ok, handle()} | error_reply().
open(SessId, FileKey, OpenType) ->
    ?run(fun() -> lfm_files:open(SessId, FileKey, OpenType) end).

%%--------------------------------------------------------------------
%% @doc
%% Flushes waiting events for session connected with handler.
%% @end
%%--------------------------------------------------------------------
-spec fsync(FileHandle :: handle()) -> ok | {error, Reason :: term()}.
fsync(FileHandle) ->
    ?run(fun() -> lfm_files:fsync(FileHandle) end).

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
-spec truncate(Handle :: handle(), Size :: non_neg_integer()) ->
    ok | error_reply().
truncate(Handle, Size) ->
    ?run(fun() -> lfm_files:truncate(Handle, Size) end).

-spec truncate(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Size :: non_neg_integer()) ->
    ok | error_reply().
truncate(SessId, FileKey, Size) ->
    ?run(fun() -> lfm_files:truncate(SessId, FileKey, Size) end).

%%--------------------------------------------------------------------
%% @doc
%% Releases previously opened  file.
%% @end
%%--------------------------------------------------------------------
-spec release(handle()) ->
    ok | error_reply().
release(FileHandle) ->
    ?run(fun() -> lfm_files:release(FileHandle) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, list()} | error_reply().
get_file_distribution(SessId, FileKey) ->
    ?run(fun() -> lfm_files:get_file_distribution(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Replicates file on given provider.
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    ProviderId :: oneprovider:id()) ->
    ok | error_reply().
replicate_file(SessId, FileKey, ProviderId) ->
    ?run(fun() -> lfm_files:replicate_file(SessId, FileKey, ProviderId) end).


%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file.
%% @end
%%--------------------------------------------------------------------
-spec set_perms(SessId :: session:id(), FileKey :: file_key(),
    NewPerms :: file_meta:posix_permissions()) ->
    ok | error_reply().
set_perms(SessId, FileKey, NewPerms) ->
    ?run(fun() -> lfm_perms:set_perms(SessId, FileKey, NewPerms) end).


%%--------------------------------------------------------------------
%% @doc
%% Checks if current user has given permissions for given file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms(FileKey :: file_key(), PermsType :: check_permissions:check_type()) ->
    {ok, boolean()} | error_reply().
check_perms(Path, PermType) ->
    ?run(fun() -> lfm_perms:check_perms(Path, PermType) end).


%%--------------------------------------------------------------------
%% @doc
%% Returns file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec get_acl(Handle :: handle()) ->
    {ok, [lfm_perms:access_control_entity()]} | error_reply().
get_acl(Handle) ->
    ?run(fun() -> lfm_perms:get_acl(Handle) end).

-spec get_acl(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, [lfm_perms:access_control_entity()]} | error_reply().
get_acl(SessId, FileKey) ->
    ?run(fun() -> lfm_perms:get_acl(SessId, FileKey) end).


%%--------------------------------------------------------------------
%% @doc
%% Updates file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec set_acl(Handle :: handle(), EntityList :: [lfm_perms:access_control_entity()]) ->
    ok | error_reply().
set_acl(Handle, EntityList) ->
    ?run(fun() -> lfm_perms:set_acl(Handle, EntityList) end).

-spec set_acl(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(), EntityList :: [lfm_perms:access_control_entity()]) ->
    ok | error_reply().
set_acl(SessId, FileKey, EntityList) ->
    ?run(fun() -> lfm_perms:set_acl(SessId, FileKey, EntityList) end).


%%--------------------------------------------------------------------
%% @doc
%% Remove file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec remove_acl(Handle :: handle()) -> ok | error_reply().
remove_acl(Handle) ->
    ?run(fun() -> lfm_perms:remove_acl(Handle) end).

-spec remove_acl(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    ok | error_reply().
remove_acl(SessId, FileKey) ->
    ?run(fun() -> lfm_perms:remove_acl(SessId, FileKey) end).


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes.
%% @end
%%--------------------------------------------------------------------
-spec stat(Handle :: handle()) ->
    {ok, lfm_attrs:file_attributes()} | error_reply().
stat(Handle) ->
    ?run(fun() -> lfm_attrs:stat(Handle) end).

-spec stat(session:id(), file_key()) -> {ok, lfm_attrs:file_attributes()} | error_reply().
stat(SessId, FileKey) ->
    ?run(fun() -> lfm_attrs:stat(SessId, FileKey) end).


%%--------------------------------------------------------------------
%% @doc
%% Changes file timestamps.
%% @end
%%--------------------------------------------------------------------
-spec update_times(Handle :: handle(), ATime :: file_meta:time() | undefined,
    MTime :: file_meta:time() | undefined, CTime :: file_meta:time() | undefined) ->
    ok | error_reply().
update_times(Handle, ATime, MTime, CTime) ->
    ?run(fun() -> lfm_attrs:update_times(Handle, ATime, MTime, CTime) end).

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
-spec get_xattr(Handle :: handle(), XattrName :: xattr:name()) ->
    {ok, #xattr{}} | error_reply().
get_xattr(Handle, XattrName) ->
    ?run(fun() -> lfm_attrs:get_xattr(Handle, XattrName) end).

-spec get_xattr(session:id(), file_key(), xattr:name()) ->
    {ok, #xattr{}} | error_reply().
get_xattr(SessId, FileKey, XattrName) ->
    ?run(fun() -> lfm_attrs:get_xattr(SessId, FileKey, XattrName) end).


%%--------------------------------------------------------------------
%% @doc
%% Updates file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(Handle :: handle(), Xattr :: #xattr{}) -> ok | error_reply().
set_xattr(Handle, Xattr) ->
    ?run(fun() -> lfm_attrs:set_xattr(Handle, Xattr) end).

-spec set_xattr(session:id(), file_key(), #xattr{}) -> ok | error_reply().
set_xattr(SessId, FileKey, Xattr) ->
    ?run(fun() -> lfm_attrs:set_xattr(SessId, FileKey, Xattr) end).


%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(handle(), xattr:name()) -> ok | error_reply().
remove_xattr(Handle, XattrName) ->
    ?run(fun() -> lfm_attrs:remove_xattr(Handle, XattrName) end).

-spec remove_xattr(session:id(), file_key(), xattr:name()) -> ok | error_reply().
remove_xattr(SessId, FileKey, XattrName) ->
    ?run(fun() -> lfm_attrs:remove_xattr(SessId, FileKey, XattrName) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes of a file.
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(handle()) -> {ok, [xattr:name()]} | error_reply().
list_xattr(Handle) ->
    ?run(fun() -> lfm_attrs:list_xattr(Handle) end).

-spec list_xattr(session:id(), file_key()) ->
    {ok, [xattr:name()]} | error_reply().
list_xattr(SessId, FileKey) ->
    ?run(fun() -> lfm_attrs:list_xattr(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc Returns encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec get_transfer_encoding(session:id(), file_key()) ->
    {ok, xattr:transfer_encoding()} | error_reply().
get_transfer_encoding(SessId, FileKey) ->
    ?run(fun() -> lfm_attrs:get_transfer_encoding(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc Sets encoding suitable for rest transfer.
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
%% @doc Returns mimetype of file.
%%--------------------------------------------------------------------
-spec get_mimetype(session:id(), file_key()) ->
    {ok, xattr:mimetype()} | error_reply().
get_mimetype(SessId, FileKey) ->
    ?run(fun() -> lfm_attrs:get_mimetype(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc Sets mimetype of file.
%%--------------------------------------------------------------------
-spec set_mimetype(session:id(), file_key(), xattr:mimetype()) ->
    ok | error_reply().
set_mimetype(SessId, FileKey, Mimetype) ->
    ?run(fun() -> lfm_attrs:set_mimetype(SessId, FileKey, Mimetype) end).

%%--------------------------------------------------------------------
%% @doc
%% Creates a symbolic link.
%% @end
%%--------------------------------------------------------------------
-spec create_symlink(Path :: binary(), TargetFileKey :: file_key()) ->
    {ok, file_meta:uuid()} | error_reply().
create_symlink(Path, TargetFileKey) ->
    ?run(fun() -> lfm_links:create_symlink(Path, TargetFileKey) end).


%%--------------------------------------------------------------------
%% @doc
%% Returns the symbolic link's target file.
%% @end
%%--------------------------------------------------------------------
-spec read_symlink(FileKey :: file_key()) ->
    {ok, {file_meta:uuid(), file_meta:name()}} | error_reply().
read_symlink(FileKey) ->
    ?run(fun() -> lfm_links:read_symlink(FileKey) end).


%%--------------------------------------------------------------------
%% @doc
%% Removes a symbolic link.
%% @end
%%--------------------------------------------------------------------
-spec remove_symlink(FileKey :: file_key()) -> ok | error_reply().
remove_symlink(FileKey) ->
    ?run(fun() -> lfm_links:remove_symlink(FileKey) end).


%%--------------------------------------------------------------------
%% @doc
%% Creates a share for given file. File can be shared with anyone or
%% only specified group of users.
%% @end
%%--------------------------------------------------------------------
-spec create_share(FileKey :: file_key(),
    ShareWith :: all | [{user, onedata_user:id()} | {group, onedata_group:id()}]) ->
    {ok, lfm_shares:share_id()} | error_reply().
create_share(Path, ShareWith) ->
    ?run(fun() -> lfm_shares:create_share(Path, ShareWith) end).


%%--------------------------------------------------------------------
%% @doc
%% Returns shared file by share_id.
%% @end
%%--------------------------------------------------------------------
-spec get_share(lfm_shares:share_id()) ->
    {ok, {file_meta:uuid(), file_meta:name()}} | error_reply().
get_share(ShareID) ->
    ?run(fun() -> lfm_shares:get_share(ShareID) end).


%%--------------------------------------------------------------------
%% @doc
%% Removes file share by ShareID.
%% @end
%%--------------------------------------------------------------------
-spec remove_share(lfm_shares:share_id()) -> ok | error_reply().
remove_share(ShareID) ->
    ?run(fun() -> lfm_shares:remove_share(ShareID) end).
