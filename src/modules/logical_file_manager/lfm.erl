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
-module(lfm).

-define(run(F),
    try
        F()
    catch
        _:{badmatch, {error, not_found}} ->
            {error, ?ENOENT};
        _:{badmatch, Error} ->
            Error;
        _:___Reason ->
            ?error_stacktrace("logical_file_manager generic error: ~p", [___Reason]),
            {error, ___Reason}
    end).

-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").

-type handle() :: lfm_context:ctx().
-type file_key() :: fslogic_worker:file_guid_or_path() | {handle, handle()}.
-type error_reply() :: {error, term()}.

-export_type([handle/0, file_key/0, error_reply/0]).

%% Functions operating on directories
-export([
    mkdir/2, mkdir/3, mkdir/4,
    get_children/4, get_children/5, get_children/6, get_children_attrs/4, get_children_attrs/5, get_children_details/5,
    get_child_attr/3, get_children_count/2, get_parent/2
]).
%% Functions operating on directories or files
-export([mv/3, mv/4, cp/3, cp/4, get_file_path/2, get_file_guid/2, rm_recursive/2, unlink/3]).
-export([
    schedule_file_replication/4, schedule_replica_eviction/4,
    schedule_replication_by_view/6, schedule_replica_eviction_by_view/6
]).
%% Functions operating on files
-export([create/2, create/3, create/4, open/3, get_file_location/2, fsync/1, fsync/3,
    write/3, read/3, check_size_and_read/3,
    silent_read/3, truncate/3, release/1, get_file_distribution/2,
    create_and_open/4, create_and_open/5]).
%% Functions concerning file permissions
-export([set_perms/3, check_perms/3, set_acl/3, get_acl/2, remove_acl/2]).
%% Functions concerning file attributes
-export([
    stat/2, get_details/2,
    get_xattr/4, set_xattr/3, set_xattr/5, remove_xattr/3, list_xattr/4,
    update_times/5
]).
%% Functions concerning cdmi attributes
-export([get_transfer_encoding/2, set_transfer_encoding/3, get_cdmi_completion_status/2,
    set_cdmi_completion_status/3, get_mimetype/2, set_mimetype/3]).
%% Functions concerning file shares
-export([create_share/3, remove_share/2]).
%% Functions concerning metadata
-export([get_metadata/5, set_metadata/5, has_custom_metadata/2, remove_metadata/3]).
%% Utility functions
-export([check_result/1]).
%% Functions concerning qos
-export([add_qos_entry/4, get_qos_entry/2, remove_qos_entry/2, get_effective_file_qos/2,
    check_qos_fulfilled/2, check_qos_fulfilled/3]).

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
-spec get_children(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(), Limit :: integer()) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}]} | error_reply().
get_children(SessId, FileKey, Offset, Limit) ->
    ?run(fun() -> lfm_dirs:get_children(SessId, FileKey, Offset, Limit) end).

%%--------------------------------------------------------------------
%% @doc
%% @equiv ls(SessId, FileKey, Offset, Limit, Token, undefined).
%% @end
%%--------------------------------------------------------------------
-spec get_children(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(), Limit :: integer(), Token :: undefined | binary()) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}], NewToken :: binary(),
        IsLast :: boolean()} | error_reply().
get_children(SessId, FileKey, Offset, Limit, Token) ->
    get_children(SessId, FileKey, Offset, Limit, Token, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory starting from specified startId.
%% Returns up to Limit of entries, starting with Offset-th entry.
%% @end
%%--------------------------------------------------------------------
-spec get_children(session:id(),
    FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(),
    Limit :: integer(),
    Token :: undefined | binary(),
    StartId :: undefined | file_meta:name()
) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}], NewToken :: binary(),
        IsLast :: boolean()} | error_reply().
get_children(SessId, FileKey, Offset, Limit, Token, StartId) ->
    ?run(fun() -> lfm_dirs:get_children(SessId, FileKey, Offset, Limit, Token, StartId) end).


%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory. Returns attributes of files.
%% Returns up to Limit of entries. Uses token to choose starting entry.
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs(
    session:id(),
    FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(),
    Limit :: integer()
) ->
    {ok, [#file_attr{}]} | error_reply().
get_children_attrs(SessId, FileKey, Offset, Limit) ->
    ?run(fun() -> lfm_dirs:get_children_attrs(SessId, FileKey, Offset, Limit) end).


%%--------------------------------------------------------------------
%% @doc
%% Lists some contents of a directory. Returns attributes of files.
%% Returns up to Limit of entries, starting with Offset-th entry.
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs(
    session:id(),
    FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(),
    Limit :: integer(),
    Token :: undefined | binary()
) ->
    {ok, [#file_attr{}], NewToken :: binary(), IsLast :: boolean()} |
    error_reply().
get_children_attrs(SessId, FileKey, Offset, Limit, Token) ->
    ?run(fun() -> lfm_dirs:get_children_attrs(SessId, FileKey, Offset, Limit, Token) end).


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
%% Lists some contents of a directory. Returns details of files.
%% Returns up to Limit of entries, starting with Offset-th entry.
%% @end
%%--------------------------------------------------------------------
-spec get_children_details(
    session:id(),
    FileKey :: fslogic_worker:file_guid_or_path(),
    Offset :: integer(),
    Limit :: integer(),
    StartId :: undefined | file_meta:name()
) ->
    {ok, [lfm_attrs:file_details()], IsLast :: boolean()} | error_reply().
get_children_details(SessId, FileKey, Offset, Limit, StartId) ->
    ?run(fun() -> lfm_dirs:get_children_details(SessId, FileKey, Offset, Limit, StartId) end).


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
%% Moves a file or directory to a new location.
%% @end
%%--------------------------------------------------------------------
-spec mv(session:id(), fslogic_worker:file_guid_or_path(), fslogic_worker:file_guid_or_path(),
    file_meta:name()) -> {ok, fslogic_worker:file_guid()} | error_reply().
mv(SessId, FileKey, TargetParentKey, TargetName) ->
    ?run(fun() -> lfm_files:mv(SessId, FileKey, TargetParentKey, TargetName) end).

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
%% Copies a file or directory to given location.
%% @end
%%--------------------------------------------------------------------
-spec cp(session:id(), fslogic_worker:file_guid_or_path(), fslogic_worker:file_guid_or_path(),
    file_meta:name()) -> {ok, fslogic_worker:file_guid()} | error_reply().
cp(SessId, FileKey, TargetParentKey, TargetName) ->
    ?run(fun() -> lfm_files:cp(SessId, FileKey, TargetParentKey, TargetName) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns full path of file
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(session:id(), fslogic_worker:file_guid()) ->
    {ok, file_meta:path()} | error_reply().
get_file_path(SessId, FileGuid) ->
    ?run(fun() -> lfm_files:get_file_path(SessId, FileGuid) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns guid of file
%% @end
%%--------------------------------------------------------------------
-spec get_file_guid(session:id(), fslogic_worker:file_guid_or_path()) ->
    {ok, fslogic_worker:file_guid()}.
get_file_guid(SessId, FilePath) ->
    ?run(fun() -> lfm_files:get_file_guid(SessId, FilePath) end).

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
%% Schedules file replication to given provider.
%% @end
%%--------------------------------------------------------------------
-spec schedule_file_replication(session:id(), fslogic_worker:file_guid_or_path(),
    TargetProviderId :: oneprovider:id(), transfer:callback()) ->
    {ok, transfer:id()} | error_reply().
schedule_file_replication(SessId, FileKey, TargetProviderId, Callback) ->
    ?run(fun() -> lfm_files:schedule_file_replication(
        SessId, FileKey, TargetProviderId, Callback
    ) end).

%%--------------------------------------------------------------------
%% @doc
%% Schedules file replication to given provider.
%% @end
%%--------------------------------------------------------------------
-spec schedule_replication_by_view(session:id(), TargetProviderId :: oneprovider:id(),
    transfer:callback(), od_space:id(), transfer:view_name(),
    transfer:query_view_params()) -> {ok, transfer:id()} | error_reply().
schedule_replication_by_view(SessId, TargetProviderId, Callback, SpaceId,
    ViewName, QueryParams
) ->
    ?run(fun() -> lfm_files:schedule_replication_by_view(
        SessId, TargetProviderId, Callback, SpaceId, ViewName, QueryParams
    ) end).

%%--------------------------------------------------------------------
%% @doc
%% Schedules file replica eviction on given provider, migrates unique data
%% to provider given as MigrateProviderId.
%% @end
%%--------------------------------------------------------------------
-spec schedule_replica_eviction(session:id(), fslogic_worker:file_guid_or_path(),
    SourceProviderId :: oneprovider:id(), TargetProviderId :: undefined | oneprovider:id()) ->
    {ok, transfer:id()} | error_reply().
schedule_replica_eviction(SessId, FileKey, SourceProviderId, TargetProviderId) ->
    ?run(fun() -> lfm_files:schedule_replica_eviction(
        SessId, FileKey, SourceProviderId, TargetProviderId
    ) end).

%%--------------------------------------------------------------------
%% @doc
%% Schedules file replica eviction on given provider, migrates unique data
%% to provider given as MigrateProviderId.
%% @end
%%--------------------------------------------------------------------
-spec schedule_replica_eviction_by_view(session:id(), oneprovider:id(),
    undefined | oneprovider:id(), od_space:id(),
    transfer:view_name(), transfer:query_view_params()) ->
    {ok, transfer:id()} | error_reply().
schedule_replica_eviction_by_view(SessId, EvictingProviderId, ReplicatingProviderId,
    SpaceId, ViewName, QueryViewParams
) ->
    ?run(fun() -> lfm_files:schedule_replica_eviction_by_view(
        SessId, EvictingProviderId, ReplicatingProviderId,
        SpaceId, ViewName, QueryViewParams
    ) end).

%%--------------------------------------------------------------------
%% @doc
%% Creates a new file
%% @end
%%--------------------------------------------------------------------
-spec create(session:id(), Path :: file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
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
    Mode :: undefined | file_meta:posix_permissions(), fslogic_worker:open_flag()) ->
    {ok, {fslogic_worker:file_guid(), handle()}}
    | error_reply().
create_and_open(SessId, Path, Mode, OpenFlag) ->
    ?run(fun() -> lfm_files:create_and_open(SessId, Path, Mode, OpenFlag) end).

-spec create_and_open(session:id(), ParentGuid :: fslogic_worker:file_guid(),
    Name :: file_meta:name(), Mode :: undefined | file_meta:posix_permissions(),
    fslogic_worker:open_flag()) ->
    {ok, {fslogic_worker:file_guid(), handle()}}
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
%% Returns location to file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, file_location:record()} | lfm:error_reply().
get_file_location(SessId, FileKey) ->
    ?run(fun() -> lfm_files:get_file_location(SessId, FileKey) end).

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
%% Reads requested part of a file with size check.
%% @end
%%--------------------------------------------------------------------
-spec check_size_and_read(FileHandle :: handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: handle(), binary()} | error_reply().
check_size_and_read(FileHandle, Offset, MaxSize) ->
    ?run(fun() -> lfm_files:check_size_and_read(FileHandle, Offset, MaxSize) end).

%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file (no events or prefetching).
%% @end
%%--------------------------------------------------------------------
-spec silent_read(FileHandle :: handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: handle(), binary()} | error_reply().
silent_read(FileHandle, Offset, MaxSize) ->
    ?run(fun() ->
        lfm_files:silent_read(FileHandle, Offset, MaxSize)
    end).

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
    {ok, Blocks :: [[non_neg_integer()]]} | error_reply().
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
    ok | error_reply().
check_perms(SessId, FileKey, PermType) ->
    ?run(fun() -> lfm_perms:check_perms(SessId, FileKey, PermType) end).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec get_acl(session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, acl:acl()} | error_reply().
get_acl(SessId, FileKey) ->
    ?run(fun() -> lfm_perms:get_acl(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Updates file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec set_acl(session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    acl:acl()) -> ok | error_reply().
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
%% Returns file details.
%% @end
%%--------------------------------------------------------------------
-spec get_details(session:id(), file_key()) ->
    {ok, lfm_attrs:file_details()} | error_reply().
get_details(SessId, FileKey) ->
    ?run(fun() -> lfm_attrs:get_details(SessId, FileKey) end).

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
-spec create_share(session:id(), fslogic_worker:file_guid_or_path(), od_share:name()) ->
    {ok, od_share:id()} | error_reply().
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


%%--------------------------------------------------------------------
%% @doc
%% Checks result of lfm call and if it's error throws ?ERROR_POSIX.
%% Otherwise returns it.
%% @end
%%--------------------------------------------------------------------
-spec check_result(OK | {error, term()}) -> OK | no_return() when
    OK :: ok | {ok, term()} | {ok, term(), term()} | {ok, term(), term(), term()}.
check_result(ok) -> ok;
check_result({ok, _} = Res) -> Res;
check_result({ok, _, _} = Res) -> Res;
check_result({ok, _, _, _} = Res) -> Res;
check_result({error, Errno}) -> throw(?ERROR_POSIX(Errno)).

%%--------------------------------------------------------------------
%% @doc
%% Adds new qos_entry for file or directory.
%% @end
%%--------------------------------------------------------------------
-spec add_qos_entry(session:id(), file_key(), qos_expression:raw(),
    qos_entry:replicas_num()) -> {ok, qos_entry:id()} | error_reply().
add_qos_entry(SessId, FileKey, Expression, ReplicasNum) ->
    ?run(fun() -> lfm_qos:add_qos_entry(SessId, FileKey, Expression, ReplicasNum) end).

%%--------------------------------------------------------------------
%% @doc
%% Gets effective QoS for file or directory.
%% @end
%%--------------------------------------------------------------------
-spec get_effective_file_qos(session:id(), file_key()) ->
    {ok, {[qos_entry:id()], file_qos:assigned_entries()}} | error_reply().
get_effective_file_qos(SessId, FileKey) ->
    ?run(fun() -> lfm_qos:get_effective_file_qos(SessId, FileKey) end).

%%--------------------------------------------------------------------
%% @doc
%% Get details of specified qos_entry.
%% @end
%%--------------------------------------------------------------------
-spec get_qos_entry(session:id(), qos_entry:id()) ->
    {ok, qos_entry:record()} | error_reply().
get_qos_entry(SessId, QosEntryId) ->
    ?run(fun() -> lfm_qos:get_qos_entry(SessId, QosEntryId) end).

%%--------------------------------------------------------------------
%% @doc
%% Remove qos_entry.
%% @end
%%--------------------------------------------------------------------
-spec remove_qos_entry(session:id(), qos_entry:id()) -> ok | error_reply().
remove_qos_entry(SessId, QosEntryId) ->
    ?run(fun() -> lfm_qos:remove_qos_entry(SessId, QosEntryId) end).

%%--------------------------------------------------------------------
%% @doc
%% Check if QoS requirements defined in qos_entry document are fulfilled.
%% @end
%%--------------------------------------------------------------------
-spec check_qos_fulfilled(session:id(), qos_entry:id()) -> {ok, boolean()} | error_reply().
check_qos_fulfilled(SessId, QosEntryId) ->
    ?run(fun() -> lfm_qos:check_qos_fulfilled(SessId, QosEntryId) end).

%%--------------------------------------------------------------------
%% @doc
%% Check if QoS requirements defined in qos_entry document/documents
%% are fulfilled for given file.
%% @end
%%--------------------------------------------------------------------
-spec check_qos_fulfilled(session:id(), qos_entry:id(), file_key()) ->
    {ok, boolean()} | error_reply().
check_qos_fulfilled(SessId, QosEntryId, FileKey) ->
    ?run(fun() -> lfm_qos:check_qos_fulfilled(SessId, QosEntryId, FileKey) end).
