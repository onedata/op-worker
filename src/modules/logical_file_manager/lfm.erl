%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module offers a high level API for operating on logical filesystem.
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
-author("Lukasz Opiola").

-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").


% General file related operations
-export([
    get_fs_stats/2,

    stat/2, stat/3,
    get_file_references/2,

    get_file_path/2,
    get_file_guid/2,
    resolve_guid_by_relative_path/3,
    get_parent/2,
    ensure_dir/4,

    is_dir/2,

    update_times/5,
    mv/4,
    cp/4,
    rm_recursive/2, unlink/3
]).
%% Hardlink/symlink specific operations
-export([
    make_link/4,
    make_symlink/4,
    read_symlink/2,
    resolve_symlink/2
]).
%% Regular file specific operations
-export([
    create/2, create/3, create/4,
    create_and_open/4, create_and_open/5,
    open/3, monitored_open/3,
    fsync/1, fsync/3,
    sync_block/4,
    write/3, read/3,
    check_size_and_read/3,
    silent_read/3,
    truncate/3,
    release/1, monitored_release/1,
    get_file_location/2
]).
%% Directory specific operations
-export([
    mkdir/3, mkdir/4,
    create_dir_at_path/3,
    get_child_attr/3,
    get_children_attrs/3,
    get_children_attrs/4,
    get_files_recursively/4,
    get_children_count/2
]).
%% Permissions related operations
-export([
    set_perms/3,
    check_perms/3,
    set_acl/3,
    get_acl/2,
    remove_acl/2
]).
%% Custom metadata related operations
-export([
    has_custom_metadata/2,
    list_xattr/4,
    set_xattr/3,
    set_xattr/5,
    get_xattr/4,
    remove_xattr/3
]).
%% Multipart upload related operations
-export([
    create_multipart_upload/3,
    abort_multipart_upload/2,
    complete_multipart_upload/2,
    list_multipart_uploads/4,
    upload_multipart_part/3,
    list_multipart_parts/4
]).

%% Utility functions
-export([check_result/1]).


-type file_ref() :: #file_ref{}.
-type file_key() :: {path, file_meta:path()} | file_ref().

-type handle() :: lfm_context:ctx().
-type error_reply() :: {error, term()}.

-export_type([handle/0, file_ref/0, file_key/0, error_reply/0]).


-define(run(Expr),
    try
        Expr
    catch
        _:{badmatch, {error, not_found}} ->
            {error, ?ENOENT};
        _:{badmatch, Error} ->
            Error;
        throw:Error ->
            Error;
        _:___Reason:Stacktrace ->
            ?error_stacktrace("logical_file_manager generic error: ~tp", [___Reason], Stacktrace),
            {error, ___Reason}
    end).


%%%===================================================================
%%% General file related operations
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns fs_stats() containing support e.g. size and occupied size.
%% @end
%%--------------------------------------------------------------------
-spec get_fs_stats(session:id(), file_key()) ->
    {ok, lfm_attrs:fs_stats()} | error_reply().
get_fs_stats(SessId, FileKey) ->
    ?run(lfm_attrs:get_fs_stats(SessId, FileKey)).


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes (see file_attr.hrl).
%% @end
%%--------------------------------------------------------------------
-spec stat(session:id(), file_key()) ->
    {ok, lfm_attrs:file_attributes()} | error_reply().
stat(SessId, FileKey) ->
    stat(SessId, FileKey, ?ONECLIENT_FILE_ATTRS).


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes (see file_attr.hrl).
%% @end
%%--------------------------------------------------------------------
-spec stat(session:id(), file_key(), [onedata_file:attr_name()]) ->
    {ok, lfm_attrs:file_attributes()} | error_reply().
stat(SessId, FileKey, Attributes) ->
    ?run(lfm_attrs:stat(SessId, FileKey, Attributes)).


-spec get_file_references(session:id(), file_key()) ->
    {ok, [file_id:file_guid()]} | error_reply().
get_file_references(SessId, FileKey) ->
    ?run(lfm_attrs:get_references(SessId, FileKey)).


-spec get_file_path(session:id(), fslogic_worker:file_guid()) ->
    {ok, file_meta:path()} | error_reply().
get_file_path(SessId, FileGuid) ->
    ?run(lfm_files:get_file_path(SessId, FileGuid)).


-spec get_file_guid(session:id(), file_meta:path()) ->
    {ok, fslogic_worker:file_guid()}.
get_file_guid(SessId, FilePath) ->
    ?run(lfm_files:get_file_guid(SessId, FilePath)).


-spec resolve_guid_by_relative_path(session:id(), fslogic_worker:file_guid(), file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
resolve_guid_by_relative_path(SessId, RelativeRootGuid, FilePath) ->
    ?run(lfm_files:resolve_guid_by_relative_path(SessId, RelativeRootGuid, FilePath)).


-spec get_parent(session:id(), file_key()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
get_parent(SessId, FileKey) ->
    ?run(lfm_files:get_parent(SessId, FileKey)).

-spec ensure_dir(session:id(), fslogic_worker:file_guid(), file_meta:path(), file_meta:mode()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
ensure_dir(SessId, RelativeRootGuid, FilePath, Mode) ->
    ?run(lfm_files:ensure_dir(SessId, RelativeRootGuid, FilePath, Mode)).

-spec is_dir(session:id(), file_key()) ->
    boolean() | error_reply().
is_dir(SessId, FileEntry) ->
    ?run(lfm_files:is_dir(SessId, FileEntry)).


-spec update_times(
    session:id(),
    file_key(),
    times:a_time() | undefined,
    times:m_time() | undefined,
    times:c_time() | undefined
) ->
    ok | error_reply().
update_times(SessId, FileKey, ATime, MTime, CTime) ->
    ?run(lfm_attrs:update_times(SessId, FileKey, ATime, MTime, CTime)).


-spec mv(session:id(), file_key(), file_key(), file_meta:name()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
mv(SessId, FileKey, TargetParentKey, TargetName) ->
    ?run(lfm_files:mv(SessId, FileKey, TargetParentKey, TargetName)).


-spec cp(session:id(), file_key(), file_key(), file_meta:name()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
cp(SessId, FileKey, TargetParentKey, TargetName) ->
    ?run(lfm_files:cp(SessId, FileKey, TargetParentKey, TargetName)).


%%--------------------------------------------------------------------
%% @doc
%% Deletes a directory with all its children asynchronously, moving
%% the directory to trash.
%% @end
%%--------------------------------------------------------------------
-spec rm_recursive(session:id(), file_key()) ->
    ok | error_reply().
rm_recursive(SessId, FileKey) ->
    ?run(lfm_files:rm_recursive(SessId, FileKey)).


-spec unlink(session:id(), file_key(), boolean()) ->
    ok | error_reply().
unlink(SessId, FileEntry, Silent) ->
    ?run(lfm_files:unlink(SessId, FileEntry, Silent)).


%%%===================================================================
%%% Hardlink/symlink specific operations
%%%===================================================================


-spec make_link(session:id(), file_key(), file_key(), file_meta:name()) ->
    {ok, #file_attr{}} | error_reply().
make_link(SessId, TargetFileKey, ParentKey, Name) ->
    ?run(lfm_files:make_link(SessId, TargetFileKey, ParentKey, Name)).


-spec make_symlink(session:id(), file_key(), file_meta:name(), file_meta_symlinks:symlink()) ->
    {ok, #file_attr{}} | lfm:error_reply().
make_symlink(SessId, ParentKey, Name, SymlinkValue) ->
    ?run(lfm_files:make_symlink(SessId, ParentKey, Name, SymlinkValue)).


-spec read_symlink(session:id(), file_key()) ->
    {ok, file_meta_symlinks:symlink()} | lfm:error_reply().
read_symlink(SessId, FileKey) ->
    ?run(lfm_files:read_symlink(SessId, FileKey)).


-spec resolve_symlink(session:id(), file_key()) ->
    {ok, file_id:file_guid()} | error_reply().
resolve_symlink(SessId, FileKey) ->
    ?run(lfm_attrs:resolve_symlink(SessId, FileKey)).


%%%===================================================================
%%% Regular file specific operations
%%%===================================================================


-spec create(session:id(), file_meta:path()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
create(SessId, Path) ->
    ?run(lfm_files:create(SessId, Path)).


-spec create(session:id(), file_meta:path(), file_meta:posix_permissions()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
create(SessId, Path, Mode) ->
    ?run(lfm_files:create(SessId, Path, Mode)).


-spec create(
    session:id(),
    fslogic_worker:file_guid(),
    file_meta:name(),
    undefined | file_meta:posix_permissions()
) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
create(SessId, ParentGuid, Name, Mode) ->
    ?run(lfm_files:create(SessId, ParentGuid, Name, Mode)).


-spec create_and_open(
    session:id(),
    file_meta:path(),
    undefined | file_meta:posix_permissions(),
    fslogic_worker:open_flag()
) ->
    {ok, {fslogic_worker:file_guid(), handle()}} | error_reply().
create_and_open(SessId, Path, Mode, OpenFlag) ->
    ?run(lfm_files:create_and_open(SessId, Path, Mode, OpenFlag)).


-spec create_and_open(
    session:id(),
    fslogic_worker:file_guid(),
    file_meta:name(),
    undefined | file_meta:posix_permissions(),
    fslogic_worker:open_flag()
) ->
    {ok, {fslogic_worker:file_guid(), handle()}} | error_reply().
create_and_open(SessId, ParentGuid, Name, Mode, OpenFlag) ->
    ?run(lfm_files:create_and_open(SessId, ParentGuid, Name, Mode, OpenFlag)).


-spec open(session:id(), file_key(), helpers:open_flag()) ->
    {ok, handle()} | error_reply().
open(SessId, FileKey, OpenType) ->
    ?run(lfm_files:open(SessId, FileKey, OpenType)).


%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode. The state of process opening file using this function
%% is monitored so that all opened handles can be closed when it unexpectedly dies
%% (e.g. client abruptly closes connection).
%% @end
%%--------------------------------------------------------------------
-spec monitored_open(session:id(), file_key(), helpers:open_flag()) ->
    {ok, handle()} | error_reply().
monitored_open(SessId, FileKey, OpenType) ->
    ?run(lfm_files:monitored_open(SessId, FileKey, OpenType)).


-spec fsync(handle()) -> ok | {error, Reason :: term()}.
fsync(FileHandle) ->
    ?run(lfm_files:fsync(FileHandle)).


%%--------------------------------------------------------------------
%% @doc
%% Flushes waiting events for session connected with handler.
%% @end
%%--------------------------------------------------------------------
-spec fsync(session:id(), file_key(), oneprovider:id()) ->
    ok | {error, Reason :: term()}.
fsync(SessId, FileKey, ProviderId) ->
    ?run(lfm_files:fsync(SessId, FileKey, ProviderId)).


-spec sync_block(session:id(), lfm:file_key(), fslogic_blocks:block(), 0..255) ->
    ok | {error, Reason :: term()}.
sync_block(SessionId, FileKey, Block, Priority) ->
    ?run(lfm_files:sync_block(SessionId, FileKey, Block, Priority)).


-spec write(FileHandle :: handle(), Offset :: integer(), Buffer :: binary()) ->
    {ok, NewHandle :: handle(), integer()} | error_reply().
write(FileHandle, Offset, Buffer) ->
    ?run(lfm_files:write(FileHandle, Offset, Buffer)).


-spec read(FileHandle :: handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: handle(), binary()} | error_reply().
read(FileHandle, Offset, MaxSize) ->
    ?run(lfm_files:read(FileHandle, Offset, MaxSize)).


-spec check_size_and_read(FileHandle :: handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: handle(), binary()} | error_reply().
check_size_and_read(FileHandle, Offset, MaxSize) ->
    ?run(lfm_files:check_size_and_read(FileHandle, Offset, MaxSize)).


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file (no events or prefetching).
%% @end
%%--------------------------------------------------------------------
-spec silent_read(FileHandle :: handle(), Offset :: integer(), MaxSize :: integer()) ->
    {ok, NewHandle :: handle(), binary()} | error_reply().
silent_read(FileHandle, Offset, MaxSize) ->
    ?run(lfm_files:silent_read(FileHandle, Offset, MaxSize)).


-spec truncate(session:id(), file_key(),
    Size :: non_neg_integer()) -> ok | error_reply().
truncate(SessId, FileKey, Size) ->
    ?run(lfm_files:truncate(SessId, FileKey, Size)).


-spec release(handle()) -> ok | error_reply().
release(FileHandle) ->
    ?run(lfm_files:release(FileHandle)).


%%--------------------------------------------------------------------
%% @doc
%% Releases previously opened file. If it is the last handle opened by this process
%% using `monitored_open` then the state of process will no longer be monitored
%% (even if process unexpectedly dies there are no handles to release).
%% @end
%%--------------------------------------------------------------------
-spec monitored_release(handle()) -> ok | error_reply().
monitored_release(FileHandle) ->
    ?run(lfm_files:monitored_release(FileHandle)).


%%--------------------------------------------------------------------
%% @doc
%% Returns location to file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(session:id(), file_key()) ->
    {ok, file_location:record()} | lfm:error_reply().
get_file_location(SessId, FileKey) ->
    ?run(lfm_files:get_file_location(SessId, FileKey)).


%%%===================================================================
%%% Directory specific operations
%%%===================================================================


-spec mkdir(session:id(), file_meta:path(), file_meta:posix_permissions() | undefined) ->
    {ok, DirGUID :: fslogic_worker:file_guid()} | error_reply().
mkdir(SessId, Path, Mode) ->
    ?run(lfm_dirs:mkdir(SessId, Path, Mode)).


-spec mkdir(
    session:id(),
    fslogic_worker:file_guid(),
    file_meta:name(),
    file_meta:posix_permissions() | undefined
) ->
    {ok, DirGuid :: fslogic_worker:file_guid()} | error_reply().
mkdir(SessId, ParentGuid, Name, Mode) ->
    ?run(lfm_dirs:mkdir(SessId, ParentGuid, Name, Mode)).


-spec create_dir_at_path(session:id(), fslogic_worker:file_guid(), file_meta:path()) -> {ok, #file_attr{}}.
create_dir_at_path(SessId, ParentGuid, Path) ->
    ?run(lfm_dirs:create_dir_at_path(SessId, ParentGuid, Path)).


%%--------------------------------------------------------------------
%% @doc
%% Gets basic file attributes (see file_attr.hrl) of a child with given name.
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(session:id(), fslogic_worker:file_guid(), file_meta:name()) ->
    {ok, #file_attr{}} | error_reply().
get_child_attr(SessId, ParentGuid, ChildName)  ->
    get_child_attr(SessId, ParentGuid, ChildName, ?ONECLIENT_FILE_ATTRS).


-spec get_child_attr(session:id(), fslogic_worker:file_guid(), file_meta:name(), [onedata_file:attr_name()]) ->
    {ok, #file_attr{}} | error_reply().
get_child_attr(SessId, ParentGuid, ChildName, Attributes)  ->
    ?run(lfm_dirs:get_child_attr(SessId, ParentGuid, ChildName, Attributes)).


%%--------------------------------------------------------------------
%% @doc
%% Gets file basic attributes (see file_attr.hrl) for each directory children.
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs(session:id(), file_key(), file_listing:options()) ->
    {ok, [#file_attr{}], file_listing:pagination_token()} | error_reply().
get_children_attrs(SessId, FileKey, ListOpts) ->
    get_children_attrs(SessId, FileKey, ListOpts, ?ONECLIENT_FILE_ATTRS).


-spec get_children_attrs(session:id(), file_key(), file_listing:options(), [onedata_file:attr_name()]) ->
    {ok, [#file_attr{}], file_listing:pagination_token()} | error_reply().
get_children_attrs(SessId, FileKey, ListOpts, Attributes) ->
    ?run(lfm_dirs:get_children_attrs(SessId, FileKey, ListOpts, Attributes)).


%%--------------------------------------------------------------------
%% @doc
%% Listing recursively non-directory files (i.e regular, symlinks and hardlinks) in subtree of 
%% given top directory. For each such file returns its file basic attributes (see file_attr.hrl) 
%% along with relative path to the given top directory.
%% @end
%%--------------------------------------------------------------------
-spec get_files_recursively(
    session:id(),
    lfm:file_key(),
    dir_req:recursive_listing_opts(),
    [onedata_file:attr_name()]
) ->
    {ok, [file_attr:record()], [file_meta:path()], recursive_listing:pagination_token()} | error_reply().
get_files_recursively(SessId, FileKey, Options, Attributes) ->
    ?run(lfm_dirs:get_files_recursively(SessId, FileKey, Options, Attributes)).


-spec get_children_count(session:id(), file_key()) ->
    {ok, integer()} | error_reply().
get_children_count(SessId, FileKey) ->
    ?run(lfm_dirs:get_children_count(SessId, FileKey)).


%%%===================================================================
%%% Permissions related operations
%%%===================================================================


-spec set_perms(session:id(), file_key(), file_meta:posix_permissions()) ->
    ok | error_reply().
set_perms(SessId, FileKey, NewPerms) ->
    ?run(lfm_perms:set_perms(SessId, FileKey, NewPerms)).


-spec check_perms(session:id(), file_key(), helpers:open_flag()) ->
    ok | error_reply().
check_perms(SessId, FileKey, PermType) ->
    ?run(lfm_perms:check_perms(SessId, FileKey, PermType)).


-spec set_acl(session:id(), file_key(), acl:acl()) ->
    ok | error_reply().
set_acl(SessId, FileKey, EntityList) ->
    ?run(lfm_perms:set_acl(SessId, FileKey, EntityList)).


-spec get_acl(session:id(), file_key()) ->
    {ok, acl:acl()} | error_reply().
get_acl(SessId, FileKey) ->
    ?run(lfm_perms:get_acl(SessId, FileKey)).


-spec remove_acl(session:id(), FileKey :: file_key()) ->
    ok | error_reply().
remove_acl(SessId, FileKey) ->
    ?run(lfm_perms:remove_acl(SessId, FileKey)).


%%%===================================================================
%%% Custom metadata related operations
%%%===================================================================


-spec has_custom_metadata(session:id(), file_key()) -> {ok, boolean()} | error_reply().
has_custom_metadata(SessId, FileKey) ->
    ?run(lfm_attrs:has_custom_metadata(SessId, FileKey)).


-spec list_xattr(session:id(), file_key(), boolean(), boolean()) ->
    {ok, [onedata_file:xattr_name()]} | error_reply().
list_xattr(SessId, FileKey, Inherited, ShowInternal) ->
    ?run(lfm_attrs:list_xattr(SessId, FileKey, Inherited, ShowInternal)).


-spec set_xattr(session:id(), file_key(), #xattr{}) ->
    ok | error_reply().
set_xattr(SessId, FileKey, Xattr) ->
    set_xattr(SessId, FileKey, Xattr, false, false).


-spec set_xattr(session:id(), file_key(), #xattr{}, boolean(), boolean()) ->
    ok | error_reply().
set_xattr(SessId, FileKey, Xattr, Create, Replace) ->
    ?run(lfm_attrs:set_xattr(SessId, FileKey, Xattr, Create, Replace)).


-spec get_xattr(session:id(), file_key(), onedata_file:xattr_name(), boolean()) ->
    {ok, #xattr{}} | error_reply().
get_xattr(SessId, FileKey, XattrName, Inherited) ->
    ?run(lfm_attrs:get_xattr(SessId, FileKey, XattrName, Inherited)).


-spec remove_xattr(session:id(), file_key(), onedata_file:xattr_name()) ->
    ok | error_reply().
remove_xattr(SessId, FileKey, XattrName) ->
    ?run(lfm_attrs:remove_xattr(SessId, FileKey, XattrName)).

%%%===================================================================
%%% Multipart upload related operations
%%%===================================================================

-spec create_multipart_upload(session:id(), od_space:id(), file_meta:path()) ->
    {ok, multipart_upload:id()} | {error, term()}.
create_multipart_upload(SessId, SpaceId, Path) ->
    ?run(lfm_multipart_upload:create(SessId, SpaceId, Path)).


-spec abort_multipart_upload(session:id(), multipart_upload:id()) -> ok | {error, term()}.
abort_multipart_upload(SessId, UploadId) ->
    ?run(lfm_multipart_upload:abort(SessId, UploadId)).


-spec complete_multipart_upload(session:id(), multipart_upload:id()) -> ok | {error, term()}.
complete_multipart_upload(SessId, UploadId) ->
    ?run(lfm_multipart_upload:complete(SessId, UploadId)).


-spec list_multipart_uploads(session:id(), od_space:id(), non_neg_integer(), multipart_upload:pagination_token() | undefined) ->
    {ok, [multipart_upload:record()], multipart_upload:pagination_token(), boolean()} | {error, term()}.
list_multipart_uploads(SessId, SpaceId, Limit, Token) ->
    ?run(lfm_multipart_upload:list(SessId, SpaceId, Limit, Token)).


-spec upload_multipart_part(session:id(), multipart_upload:id(), multipart_upload_part:record()) ->
    ok | {error, term()}.
upload_multipart_part(SessId, UploadId, Part) ->
    ?run(lfm_multipart_upload:upload_part(SessId, UploadId, Part)).


-spec list_multipart_parts(session:id(), multipart_upload:id(), non_neg_integer(), multipart_upload_part:part_number()) ->
    {ok, [multipart_upload_part:record()], boolean()} | {error, term()}.
list_multipart_parts(SessId, UploadId, Limit, StartAfter) ->
    ?run(lfm_multipart_upload:list_parts(SessId, UploadId, Limit, StartAfter)).

%%%===================================================================
%%% Utility functions
%%%===================================================================


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
check_result(?ERROR_NOT_FOUND) -> throw(?ERROR_NOT_FOUND);
check_result({error, Errno}) -> throw(?ERROR_POSIX(Errno)).
