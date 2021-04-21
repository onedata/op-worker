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
-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").


% General file related operations
-export([
    get_fs_stats/2,

    stat/2, stat/3,
    get_details/2,
    get_file_references/2,

    get_file_path/2,
    get_file_guid/2,
    get_parent/2,

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
    write/3, read/3,
    check_size_and_read/3,
    silent_read/3,
    truncate/3,
    release/1, monitored_release/1,
    get_file_location/2,
    get_file_distribution/2
]).
%% Directory specific operations
-export([
    mkdir/3, mkdir/4,
    get_children/3,
    get_child_attr/3,
    get_children_attrs/3,
    get_children_details/3,
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
    set_metadata/5,
    get_metadata/5,
    remove_metadata/3,

    list_xattr/4,
    set_xattr/3,
    set_xattr/5,
    get_xattr/4,
    remove_xattr/3
]).
%% CDMI related operations
-export([
    get_transfer_encoding/2,
    set_transfer_encoding/3,
    get_cdmi_completion_status/2,
    set_cdmi_completion_status/3,
    get_mimetype/2,
    set_mimetype/3
]).
%% Share related operations
-export([
    create_share/4,
    remove_share/2
]).
%% Transfer related operations
-export([
    schedule_file_transfer/5,
    schedule_view_transfer/7
]).
%% QoS related operations
-export([
    get_effective_file_qos/2,
    add_qos_entry/4, add_qos_entry/5,
    get_qos_entry/2,
    remove_qos_entry/2,
    check_qos_status/2, check_qos_status/3
]).
%% Dataset related operations
-export([
    establish_dataset/3, remove_dataset/2, update_dataset/5,
    get_dataset_info/2, get_file_eff_dataset_summary/2,
    list_top_datasets/4, list_top_datasets/5,
    list_children_datasets/3, list_children_datasets/4
]).
%% Archives related operations
-export([archive_dataset/3, update_archive/3, get_archive_info/2, list_archives/4, remove_archive/2]).

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
        _:___Reason ->
            ?error_stacktrace("logical_file_manager generic error: ~p", [___Reason]),
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
    stat(SessId, FileKey, false).


%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes (see file_attr.hrl).
%% @end
%%--------------------------------------------------------------------
-spec stat(session:id(), file_key(), boolean()) ->
    {ok, lfm_attrs:file_attributes()} | error_reply().
stat(SessId, FileKey, IncludeLinksCount) ->
    ?run(lfm_attrs:stat(SessId, FileKey, IncludeLinksCount)).


%%--------------------------------------------------------------------
%% @doc
%% Returns file details (see file_details.hrl).
%% @end
%%--------------------------------------------------------------------
-spec get_details(session:id(), file_key()) ->
    {ok, lfm_attrs:file_details()} | error_reply().
get_details(SessId, FileKey) ->
    ?run(lfm_attrs:get_details(SessId, FileKey)).


-spec get_file_references(session:id(), file_key()) ->
    {ok, [file_id:file_guid()]} | error_reply().
get_file_references(SessId, FileKey) ->
    ?run(lfm_attrs:get_references(SessId, FileKey)).


-spec get_file_path(session:id(), fslogic_worker:file_guid()) ->
    {ok, file_meta:path()} | error_reply().
get_file_path(SessId, FileGuid) ->
    ?run(lfm_files:get_file_path(SessId, FileGuid)).


-spec get_file_guid(session:id(), file_key()) ->
    {ok, fslogic_worker:file_guid()}.
get_file_guid(SessId, FilePath) ->
    ?run(lfm_files:get_file_guid(SessId, FilePath)).


-spec get_parent(session:id(), file_key()) ->
    {ok, fslogic_worker:file_guid()} | error_reply().
get_parent(SessId, FileKey) ->
    ?run(lfm_files:get_parent(SessId, FileKey)).


-spec is_dir(session:id(), file_key()) ->
    ok | error_reply().
is_dir(SessId, FileEntry) ->
    ?run(lfm_files:is_dir(SessId, FileEntry)).


-spec update_times(
    session:id(),
    file_key(),
    ATime :: file_meta:time() | undefined,
    MTime :: file_meta:time() | undefined,
    CTime :: file_meta:time() | undefined
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
make_link(SessId, FileKey, TargetParentKey, Name) ->
    ?run(lfm_files:make_link(SessId, FileKey, TargetParentKey, Name)).


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


%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(session:id(), file_key()) ->
    {ok, Blocks :: [[non_neg_integer()]]} | error_reply().
get_file_distribution(SessId, FileKey) ->
    ?run(lfm_files:get_file_distribution(SessId, FileKey)).


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


-spec get_children(session:id(), file_key(), file_meta:list_opts()) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}], file_meta:list_extended_info()} | error_reply().
get_children(SessId, FileKey, ListOpts) ->
    ?run(lfm_dirs:get_children(SessId, FileKey, ListOpts)).


%%--------------------------------------------------------------------
%% @doc
%% Gets basic file attributes (see file_attr.hrl) of a child with given name.
%% @end
%%--------------------------------------------------------------------
-spec get_child_attr(session:id(), fslogic_worker:file_guid(), file_meta:name()) ->
    {ok, #file_attr{}} | error_reply().
get_child_attr(SessId, ParentGuid, ChildName)  ->
    ?run(lfm_dirs:get_child_attr(SessId, ParentGuid, ChildName)).


%%--------------------------------------------------------------------
%% @doc
%% Gets file basic attributes (see file_attr.hrl) for each directory children
%% starting with Offset-th entry and up to Limit of entries.
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs(session:id(), file_key(), file_meta:list_opts()) ->
    {ok, [#file_attr{}], file_meta:list_extended_info()} | error_reply().
get_children_attrs(SessId, FileKey, ListOpts) ->
    ?run(lfm_dirs:get_children_attrs(SessId, FileKey, ListOpts)).


%%--------------------------------------------------------------------
%% @doc
%% Gets file details (see file_details.hrl) for each directory children
%% starting with Offset-th from specified StartId entry and up to Limit
%% of entries.
%% @end
%%--------------------------------------------------------------------
-spec get_children_details(session:id(), file_key(), file_meta:list_opts()) ->
    {ok, [lfm_attrs:file_details()], file_meta:list_extended_info()} | error_reply().
get_children_details(SessId, FileKey, ListOpts) ->
    ?run(lfm_dirs:get_children_details(SessId, FileKey, ListOpts)).


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


-spec set_metadata(
    session:id(),
    file_key(),
    custom_metadata:type(),
    custom_metadata:value(),
    custom_metadata:query()
) ->
    ok | error_reply().
set_metadata(SessId, FileKey, Type, Value, Query) ->
    ?run(lfm_attrs:set_metadata(SessId, FileKey, Type, Value, Query)).


-spec get_metadata(
    session:id(),
    file_key(),
    custom_metadata:type(),
    custom_metadata:query(),
    boolean()
) ->
    {ok, custom_metadata:value()} | error_reply().
get_metadata(SessId, FileKey, Type, Query, Inherited) ->
    ?run(lfm_attrs:get_metadata(SessId, FileKey, Type, Query, Inherited)).


-spec remove_metadata(session:id(), file_key(), custom_metadata:type()) ->
    ok | error_reply().
remove_metadata(SessId, FileKey, Type) ->
    ?run(lfm_attrs:remove_metadata(SessId, FileKey, Type)).


-spec list_xattr(session:id(), file_key(), boolean(), boolean()) ->
    {ok, [custom_metadata:name()]} | error_reply().
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


-spec get_xattr(session:id(), file_key(), custom_metadata:name(), boolean()) ->
    {ok, #xattr{}} | error_reply().
get_xattr(SessId, FileKey, XattrName, Inherited) ->
    ?run(lfm_attrs:get_xattr(SessId, FileKey, XattrName, Inherited)).


-spec remove_xattr(session:id(), file_key(), custom_metadata:name()) ->
    ok | error_reply().
remove_xattr(SessId, FileKey, XattrName) ->
    ?run(lfm_attrs:remove_xattr(SessId, FileKey, XattrName)).


%%%===================================================================
%%% CDMI related operations
%%%===================================================================


-spec get_transfer_encoding(session:id(), file_key()) ->
    {ok, custom_metadata:transfer_encoding()} | error_reply().
get_transfer_encoding(SessId, FileKey) ->
    ?run(lfm_attrs:get_transfer_encoding(SessId, FileKey)).


-spec set_transfer_encoding(session:id(), file_key(), custom_metadata:transfer_encoding()) ->
    ok | error_reply().
set_transfer_encoding(SessId, FileKey, Encoding) ->
    ?run(lfm_attrs:set_transfer_encoding(SessId, FileKey, Encoding)).


%%--------------------------------------------------------------------
%% @doc
%% Returns completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status(session:id(), file_key()) ->
    {ok, custom_metadata:cdmi_completion_status()} | error_reply().
get_cdmi_completion_status(SessId, FileKey) ->
    ?run(lfm_attrs:get_cdmi_completion_status(SessId, FileKey)).


%%--------------------------------------------------------------------
%% @doc
%% Sets completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status(session:id(), file_key(), custom_metadata:cdmi_completion_status()) ->
    ok | error_reply().
set_cdmi_completion_status(SessId, FileKey, CompletionStatus) ->
    ?run(lfm_attrs:set_cdmi_completion_status(SessId, FileKey, CompletionStatus)).


-spec get_mimetype(session:id(), file_key()) ->
    {ok, custom_metadata:mimetype()} | error_reply().
get_mimetype(SessId, FileKey) ->
    ?run(lfm_attrs:get_mimetype(SessId, FileKey)).


-spec set_mimetype(session:id(), file_key(), custom_metadata:mimetype()) ->
    ok | error_reply().
set_mimetype(SessId, FileKey, Mimetype) ->
    ?run(lfm_attrs:set_mimetype(SessId, FileKey, Mimetype)).


%%%===================================================================
%%% Shares related operations
%%%===================================================================


-spec create_share(session:id(), file_key(), od_share:name(), od_share:description()) ->
    {ok, od_share:id()} | error_reply().
create_share(SessId, FileKey, Name, Description) ->
    ?run(lfm_shares:create_share(SessId, FileKey, Name, Description)).


-spec remove_share(session:id(), od_share:id()) -> ok | error_reply().
remove_share(SessId, ShareID) ->
    ?run(lfm_shares:remove_share(SessId, ShareID)).


%%%===================================================================
%%% Transfer related operations
%%%===================================================================


-spec schedule_file_transfer(
    session:id(),
    file_key(),
    ReplicatingProviderId :: undefined | od_provider:id(),
    EvictingProviderId :: undefined | od_provider:id(),
    transfer:callback()
) ->
    {ok, transfer:id()} | lfm:error_reply().
schedule_file_transfer(SessId, FileKey, ReplicatingProviderId, EvictingProviderId, Callback) ->
    ?run(lfm_files:schedule_file_transfer(
        SessId, FileKey, ReplicatingProviderId, EvictingProviderId, Callback
    )).


-spec schedule_view_transfer(
    session:id(),
    od_space:id(),
    transfer:view_name(), transfer:query_view_params(),
    ReplicatingProviderId :: undefined | od_provider:id(),
    EvictingProviderId :: undefined | od_provider:id(),
    transfer:callback()
) ->
    {ok, transfer:id()} | lfm:error_reply().
schedule_view_transfer(
    SessId, SpaceId, ViewName, QueryViewParams,
    ReplicatingProviderId, EvictingProviderId, Callback
) ->
    ?run(lfm_files:schedule_view_transfer(
        SessId, SpaceId, ViewName, QueryViewParams,
        ReplicatingProviderId, EvictingProviderId, Callback
    )).


%%%===================================================================
%%% QoS related operations
%%%===================================================================


-spec get_effective_file_qos(session:id(), file_key()) ->
    {ok, {#{qos_entry:id() => qos_status:summary()}, file_qos:assigned_entries()}} | error_reply().
get_effective_file_qos(SessId, FileKey) ->
    ?run(lfm_qos:get_effective_file_qos(SessId, FileKey)).


-spec add_qos_entry(
    session:id(),
    file_key(),
    qos_expression:infix() | qos_expression:expression(),
    qos_entry:replicas_num()
) ->
    {ok, qos_entry:id()} | error_reply().
add_qos_entry(SessId, FileKey, Expression, ReplicasNum) ->
    add_qos_entry(SessId, FileKey, Expression, ReplicasNum, user_defined).


-spec add_qos_entry(
    session:id(),
    file_key(),
    qos_expression:infix() | qos_expression:expression(),
    qos_entry:replicas_num(),
    qos_entry:type()
) ->
    {ok, qos_entry:id()} | error_reply().
add_qos_entry(SessId, FileKey, Expression, ReplicasNum, EntryType) ->
    ?run(lfm_qos:add_qos_entry(SessId, FileKey, Expression, ReplicasNum, EntryType)).


-spec get_qos_entry(session:id(), qos_entry:id()) ->
    {ok, qos_entry:record()} | error_reply().
get_qos_entry(SessId, QosEntryId) ->
    ?run(lfm_qos:get_qos_entry(SessId, QosEntryId)).


-spec remove_qos_entry(session:id(), qos_entry:id()) -> ok | error_reply().
remove_qos_entry(SessId, QosEntryId) ->
    ?run(lfm_qos:remove_qos_entry(SessId, QosEntryId)).


-spec check_qos_status(session:id(), qos_entry:id()) ->
    {ok, qos_status:summary()} | error_reply().
check_qos_status(SessId, QosEntryId) ->
    ?run(lfm_qos:check_qos_status(SessId, QosEntryId)).


-spec check_qos_status(session:id(), qos_entry:id(), file_key()) ->
    {ok, qos_status:summary()} | error_reply().
check_qos_status(SessId, QosEntryId, FileKey) ->
    ?run(lfm_qos:check_qos_status(SessId, QosEntryId, FileKey)).


%%%===================================================================
%%% Datasets related operations
%%%===================================================================


-spec establish_dataset(session:id(), file_key(), data_access_control:bitmask()) ->
    {ok, dataset:id()} | error_reply().
establish_dataset(SessId, FileKey, ProtectionFlags) ->
    ?run(lfm_datasets:establish(SessId, FileKey, ProtectionFlags)).


-spec remove_dataset(session:id(), dataset:id()) -> ok | error_reply().
remove_dataset(SessId, DatasetId) ->
    ?run(lfm_datasets:remove(SessId, DatasetId)).


-spec update_dataset(
    session:id(),
    dataset:id(),
    undefined | dataset:state(),
    data_access_control:bitmask(),
    data_access_control:bitmask()
) ->
    ok | lfm:error_reply().
update_dataset(SessId, DatasetId, NewState, FlagsToSet, FlagsToUnset) ->
    ?run(lfm_datasets:update(SessId, DatasetId, NewState, FlagsToSet, FlagsToUnset)).


-spec get_dataset_info(session:id(), dataset:id()) ->
    {ok, lfm_datasets:info()} | error_reply().
get_dataset_info(SessId, DatasetId) ->
    ?run(lfm_datasets:get_info(SessId, DatasetId)).


-spec get_file_eff_dataset_summary(session:id(), file_key()) ->
    {ok, lfm_datasets:file_eff_summary()} | error_reply().
get_file_eff_dataset_summary(SessId, FileKey) ->
    ?run(lfm_datasets:get_file_eff_summary(SessId, FileKey)).


-spec list_top_datasets(session:id(), od_space:id(), dataset:state(), dataset_api:listing_opts()) ->
    {ok, dataset_api:entries(), boolean()} | error_reply().
list_top_datasets(SessId, SpaceId, State, Opts) ->
    list_top_datasets(SessId, SpaceId, State, Opts, undefined).

-spec list_top_datasets(session:id(), od_space:id(), dataset:state(), dataset_api:listing_opts(),
    undefined | dataset_api:listing_mode()) ->
    {ok, dataset_api:entries(), boolean()} | error_reply().
list_top_datasets(SessId, SpaceId, State, Opts, ListingMode) ->
    ?run(lfm_datasets:list_top_datasets(SessId, SpaceId, State, Opts, ListingMode)).


-spec list_children_datasets(session:id(), dataset:id(), dataset_api:listing_opts()) ->
    {ok, dataset_api:entries(), boolean()} | error_reply().
list_children_datasets(SessId, DatasetId, Opts) ->
    list_children_datasets(SessId, DatasetId, Opts, undefined).

-spec list_children_datasets(session:id(), dataset:id(), dataset_api:listing_opts(),
    undefined | dataset_api:listing_mode()) -> {ok, dataset_api:entries(), boolean()} | error_reply().
list_children_datasets(SessId, DatasetId, Opts, ListingMode) ->
    ?run(lfm_datasets:list_children_datasets(SessId, DatasetId, Opts, ListingMode)).

%%%===================================================================
%%% Archive related operations
%%%===================================================================

-spec archive_dataset(session:id(), dataset:id(), archive:params()) ->
    {ok, archive:id()} | error_reply().
archive_dataset(SessId, DatasetId, ArchivingParams) ->
    ?run(lfm_datasets:archive(SessId, DatasetId, ArchivingParams)).


-spec update_archive(session:id(), archive:id(), archive:params()) -> ok | error_reply().
update_archive(SessId, ArchiveId, Params) ->
    ?run(lfm_datasets:update_archive(SessId, ArchiveId, Params)).


-spec get_archive_info(session:id(), archive:id()) ->
    {ok, lfm_datasets:archive_info()} | error_reply().
get_archive_info(SessId, ArchiveId) ->
    ?run(lfm_datasets:get_archive_info(SessId, ArchiveId)).


-spec list_archives(session:id(), dataset:id(), dataset_api:listing_opts(), dataset_api:listing_mode()) ->
    {ok, dataset_api:archive_entries(), boolean()} | error_reply().
list_archives(SessId, DatasetId, Opts, ListingMode) ->
    lfm_datasets:list_archives(SessId, DatasetId, Opts, ListingMode).


-spec remove_archive(session:id(), archive:id()) -> ok | error_reply().
remove_archive(SessId, ArchiveId) ->
    ?run(lfm_datasets:remove_archive(SessId, ArchiveId)).

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
