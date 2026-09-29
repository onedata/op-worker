%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for creating files directly on a storage backend
%%% (bypassing the logical filesystem), e.g. to prepare data that is later
%%% imported/registered into a space. Operates via the storage helper, so it
%%% works uniformly across storage types (POSIX, S3, ...).
%%%
%%% Entries can be handled one by one (create_file/4,5, chmod/4, stat/3, ...) or
%%% declaratively, as whole trees (create_file_tree_on_storage/3 and its inverse
%%% delete_file_tree_from_storage/3) - the storage side counterpart of
%%% file_tree_test_utils, which builds the same kind of declared tree through lfm.
%%%
%%% NOTE: this module must be added to ?LOAD_MODULES of any suite that uses it,
%%% as the on-node routines are executed on the op_worker node via rpc.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_file_tree_test_utils).
-author("Bartosz Walkowicz").

-include("storage/storage_file_tree_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("kernel/include/file.hrl").

%% API
-export([create_file_tree_on_storage/3, delete_file_tree_from_storage/3]).
-export([create_file/4, create_file/5]).
-export([create_fifo/3, create_fifo/4]).
-export([create_dir/3, create_dir/4, chown/5]).
-export([write_file/5, read_file/5, delete_file/4]).
-export([chmod/4, truncate/5, rename/4, rmdir/3]).
-export([stat/3, list_dir/5, get_mtime/3, set_mtime/4, set_atime_and_mtime/5]).
%% on-node routines (executed on op_worker via rpc)
-export([create_file_on_storage/3, write_to_storage_file/4, delete_file_on_storage/3]).
-export([read_from_storage_file/4]).
-export([create_fifo_on_storage/3]).
-export([create_dir_on_storage/3, chown_on_storage/4]).
-export([chmod_on_storage/3, truncate_on_storage/4, rename_on_storage/3, rmdir_on_storage/2]).
-export([stat_on_storage/2, list_dir_on_storage/4]).
-export([get_mtime_on_storage/2, set_mtime_on_storage/3, set_atime_and_mtime_on_storage/4]).

% A single node of a declared storage file tree: either a generic onenv file/dir
% spec, or a storage specific FIFO spec (created on storage but not imported).
-type file_tree_node_spec() :: file_tree_test_utils:object_spec() | #storage_fifo_spec{}.
-type file_tree_spec() ::
    undefined
    | file_tree_node_spec()
    | [file_tree_node_spec()].

-export_type([file_tree_node_spec/0, file_tree_spec/0]).

% Max number of concurrent processes used to create/delete the top-level tree nodes
% on the storage. Only the top-level siblings are parallelized (each subtree is
% processed sequentially), so the total concurrency stays bounded by this value -
% parallelizing every level would multiply across levels and overload the provider.
-define(SETUP_PARALLELISM, 20).


%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Creates the declared file tree directly on the storage (bypassing the logical
%% filesystem) so that it can later be imported. Returns the spec with all file
%% names concretized (undefined names are replaced with random ones), so that the
%% same structure can be used for verification.
%% @end
%%--------------------------------------------------------------------
-spec create_file_tree_on_storage(oct_background:entity_selector(), storage:id(), file_tree_spec()) ->
    file_tree_spec().
create_file_tree_on_storage(_ProviderSelector, _StorageId, undefined) ->
    undefined;
create_file_tree_on_storage(ProviderSelector, StorageId, Specs) when is_list(Specs) ->
    lists_utils:pmap(fun(Spec) ->
        create_file_tree_on_storage(ProviderSelector, StorageId, Spec)
    end, Specs, ?SETUP_PARALLELISM);
create_file_tree_on_storage(ProviderSelector, StorageId, Spec) ->
    create_node_on_storage(ProviderSelector, StorageId, <<"/">>, Spec).


%%--------------------------------------------------------------------
%% @doc
%% Removes a declared file tree directly from the storage (bypassing the logical
%% filesystem) - the inverse of create_file_tree_on_storage/3, used by continuous
%% (update) scan tests to simulate whole (sub)trees disappearing from the storage.
%% Regular files are unlinked (their size taken from the declared content) and
%% directories are removed bottom-up (the rmdir is a no-op on object storages -
%% see rmdir_on_storage/2). The spec must be concretized (all names filled in) -
%% e.g. the one returned by create_file_tree_on_storage/3.
%% @end
%%--------------------------------------------------------------------
-spec delete_file_tree_from_storage(oct_background:entity_selector(), storage:id(), file_tree_spec()) ->
    ok.
delete_file_tree_from_storage(_ProviderSelector, _StorageId, undefined) ->
    ok;
delete_file_tree_from_storage(ProviderSelector, StorageId, Specs) when is_list(Specs) ->
    lists_utils:pforeach(fun(Spec) ->
        delete_file_tree_from_storage(ProviderSelector, StorageId, Spec)
    end, Specs, ?SETUP_PARALLELISM);
delete_file_tree_from_storage(ProviderSelector, StorageId, Spec) ->
    delete_node_from_storage(ProviderSelector, StorageId, <<"/">>, Spec).


-spec create_file(oct_background:node_selector(), storage:id(), helpers:file_id(), binary()) -> ok.
create_file(ProviderSelector, StorageId, StorageFileId, Content) ->
    create_file(ProviderSelector, StorageId, StorageFileId, Content, ?DEFAULT_FILE_PERMS).


-spec create_file(
    oct_background:node_selector(), storage:id(), helpers:file_id(), binary(), file_meta:mode()
) ->
    ok.
create_file(ProviderSelector, StorageId, StorageFileId, Content, Mode) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, create_file_on_storage, [StorageId, StorageFileId, Mode]
    ),
    write_file(ProviderSelector, StorageId, StorageFileId, 0, Content).


-spec create_fifo(oct_background:node_selector(), storage:id(), helpers:file_id()) -> ok.
create_fifo(ProviderSelector, StorageId, StorageFileId) ->
    create_fifo(ProviderSelector, StorageId, StorageFileId, ?DEFAULT_FILE_PERMS).


-spec create_fifo(
    oct_background:node_selector(), storage:id(), helpers:file_id(), file_meta:mode()
) ->
    ok.
create_fifo(ProviderSelector, StorageId, StorageFileId, Mode) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, create_fifo_on_storage, [StorageId, StorageFileId, Mode]
    ).


-spec create_dir(oct_background:node_selector(), storage:id(), helpers:file_id()) -> ok.
create_dir(ProviderSelector, StorageId, StorageFileId) ->
    create_dir(ProviderSelector, StorageId, StorageFileId, ?DEFAULT_DIR_PERMS).


-spec create_dir(oct_background:node_selector(), storage:id(), helpers:file_id(), file_meta:mode()) ->
    ok.
create_dir(ProviderSelector, StorageId, StorageFileId, Mode) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, create_dir_on_storage, [StorageId, StorageFileId, Mode]
    ).


-spec chown(oct_background:node_selector(), storage:id(), helpers:file_id(), luma:uid(), luma:gid()) ->
    ok.
chown(ProviderSelector, StorageId, StorageFileId, Uid, Gid) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, chown_on_storage, [StorageId, StorageFileId, Uid, Gid]
    ).


-spec write_file(
    oct_background:node_selector(), storage:id(), helpers:file_id(), non_neg_integer(), binary()
) ->
    ok.
write_file(ProviderSelector, StorageId, StorageFileId, Offset, Content) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, write_to_storage_file, [StorageId, StorageFileId, Offset, Content]
    ).


%% @doc Reads the file content directly from the storage - e.g. to assert that a
%% logical operation did (not) reach the storage.
-spec read_file(
    oct_background:node_selector(), storage:id(), helpers:file_id(), non_neg_integer(), non_neg_integer()
) ->
    {ok, binary()} | {error, term()}.
read_file(ProviderSelector, StorageId, StorageFileId, Offset, Size) ->
    opw_test_rpc:call(
        ProviderSelector, ?MODULE, read_from_storage_file, [StorageId, StorageFileId, Offset, Size]
    ).


-spec delete_file(oct_background:node_selector(), storage:id(), helpers:file_id(), non_neg_integer()) ->
    ok.
delete_file(ProviderSelector, StorageId, StorageFileId, CurrentSize) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, delete_file_on_storage, [StorageId, StorageFileId, CurrentSize]
    ).


-spec chmod(oct_background:node_selector(), storage:id(), helpers:file_id(), file_meta:mode()) -> ok.
chmod(ProviderSelector, StorageId, StorageFileId, Mode) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, chmod_on_storage, [StorageId, StorageFileId, Mode]
    ).


-spec truncate(
    oct_background:node_selector(), storage:id(), helpers:file_id(),
    non_neg_integer(), non_neg_integer()
) ->
    ok.
truncate(ProviderSelector, StorageId, StorageFileId, NewSize, CurrentSize) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, truncate_on_storage, [StorageId, StorageFileId, NewSize, CurrentSize]
    ).


-spec rename(oct_background:node_selector(), storage:id(), helpers:file_id(), helpers:file_id()) -> ok.
rename(ProviderSelector, StorageId, SrcStorageFileId, DstStorageFileId) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, rename_on_storage, [StorageId, SrcStorageFileId, DstStorageFileId]
    ).


-spec rmdir(oct_background:node_selector(), storage:id(), helpers:file_id()) -> ok.
rmdir(ProviderSelector, StorageId, StorageFileId) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, rmdir_on_storage, [StorageId, StorageFileId]
    ).


%% Stats the entry directly on the storage - e.g. to assert whether it exists
%% there at all ({error, ?ENOENT} when not).
-spec stat(oct_background:node_selector(), storage:id(), helpers:file_id()) ->
    {ok, helpers:stat()} | {error, term()}.
stat(ProviderSelector, StorageId, StorageFileId) ->
    opw_test_rpc:call(
        ProviderSelector, ?MODULE, stat_on_storage, [StorageId, StorageFileId]
    ).


%% @doc Lists the entries of a directory directly on the storage - e.g. to assert
%% that no extra file was created there. POSIX-only, as object storages have no
%% real directories to list this way (see rmdir_on_storage/2).
-spec list_dir(
    oct_background:node_selector(), storage:id(), helpers:file_id(), non_neg_integer(), non_neg_integer()
) ->
    {ok, [helpers:file_id()]} | {error, term()}.
list_dir(ProviderSelector, StorageId, StorageFileId, Offset, Count) ->
    opw_test_rpc:call(
        ProviderSelector, ?MODULE, list_dir_on_storage, [StorageId, StorageFileId, Offset, Count]
    ).


-spec get_mtime(oct_background:node_selector(), storage:id(), helpers:file_id()) -> non_neg_integer().
get_mtime(ProviderSelector, StorageId, StorageFileId) ->
    opw_test_rpc:call(
        ProviderSelector, ?MODULE, get_mtime_on_storage, [StorageId, StorageFileId]
    ).


%%--------------------------------------------------------------------
%% @doc
%% Overwrites a file's mtime directly on the host filesystem, bypassing the
%% storage helper (whose API has no "set times" operation) - only meaningful
%% for POSIX-compatible storages, which are locally mounted on the op_worker
%% node. Used to simulate a storage backend where a write does not bump mtime
%% at the resolution the scan relies on.
%% @end
%%--------------------------------------------------------------------
-spec set_mtime(oct_background:node_selector(), storage:id(), helpers:file_id(), non_neg_integer()) -> ok.
set_mtime(ProviderSelector, StorageId, StorageFileId, Mtime) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, set_mtime_on_storage, [StorageId, StorageFileId, Mtime]
    ).


%%--------------------------------------------------------------------
%% @doc
%% Like set_mtime/4, but also overwrites atime (set_mtime/4 leaves atime
%% untouched - file:write_file_info/3 only changes fields that are not
%% 'undefined' in the given #file_info{}). POSIX-only, for the same reason.
%% @end
%%--------------------------------------------------------------------
-spec set_atime_and_mtime(
    oct_background:node_selector(), storage:id(), helpers:file_id(),
    non_neg_integer(), non_neg_integer()
) ->
    ok.
set_atime_and_mtime(ProviderSelector, StorageId, StorageFileId, Atime, Mtime) ->
    ok = opw_test_rpc:call(
        ProviderSelector, ?MODULE, set_atime_and_mtime_on_storage, [StorageId, StorageFileId, Atime, Mtime]
    ).


%%%===================================================================
%%% On-node routines
%%%===================================================================


%% @doc Runs on the op_worker node.
-spec create_file_on_storage(storage:id(), helpers:file_id(), file_meta:mode()) -> ok.
create_file_on_storage(StorageId, StorageFileId, Mode) ->
    HelperHandle = get_helper_handle(StorageId),
    ok = helpers:mknod(HelperHandle, StorageFileId, Mode, reg).


%% @doc Runs on the op_worker node.
-spec write_to_storage_file(storage:id(), helpers:file_id(), non_neg_integer(), binary()) -> ok.
write_to_storage_file(_StorageId, _StorageFileId, _Offset, <<>>) ->
    ok;
write_to_storage_file(StorageId, StorageFileId, Offset, Content) ->
    HelperHandle = get_helper_handle(StorageId),
    {ok, FileHandle} = helpers:open(HelperHandle, StorageFileId, write),
    {ok, _} = helpers:write(FileHandle, Offset, Content),
    ok = helpers:release(FileHandle).


%% @doc Runs on the op_worker node.
-spec read_from_storage_file(storage:id(), helpers:file_id(), non_neg_integer(), non_neg_integer()) ->
    {ok, binary()} | {error, term()}.
read_from_storage_file(StorageId, StorageFileId, Offset, Size) ->
    HelperHandle = get_helper_handle(StorageId),
    case helpers:open(HelperHandle, StorageFileId, read) of
        {ok, FileHandle} ->
            Result = helpers:read(FileHandle, Offset, Size),
            ok = helpers:release(FileHandle),
            Result;
        {error, _} = Error ->
            Error
    end.


%% @doc Runs on the op_worker node.
-spec delete_file_on_storage(storage:id(), helpers:file_id(), non_neg_integer()) -> ok.
delete_file_on_storage(StorageId, StorageFileId, CurrentSize) ->
    HelperHandle = get_helper_handle(StorageId),
    ok = helpers:unlink(HelperHandle, StorageFileId, CurrentSize).


%% @doc Runs on the op_worker node.
-spec create_fifo_on_storage(storage:id(), helpers:file_id(), file_meta:mode()) -> ok.
create_fifo_on_storage(StorageId, StorageFileId, Mode) ->
    HelperHandle = get_helper_handle(StorageId),
    ok = helpers:mknod(HelperHandle, StorageFileId, Mode, fifo).


%% @doc Runs on the op_worker node.
-spec create_dir_on_storage(storage:id(), helpers:file_id(), file_meta:mode()) -> ok.
create_dir_on_storage(StorageId, StorageFileId, Mode) ->
    HelperHandle = get_helper_handle(StorageId),
    ok = helpers:mkdir(HelperHandle, StorageFileId, Mode).


%% @doc Runs on the op_worker node.
-spec chown_on_storage(storage:id(), helpers:file_id(), luma:uid(), luma:gid()) -> ok.
chown_on_storage(StorageId, StorageFileId, Uid, Gid) ->
    HelperHandle = get_helper_handle(StorageId),
    ok = helpers:chown(HelperHandle, StorageFileId, Uid, Gid).


%% @doc Runs on the op_worker node.
-spec chmod_on_storage(storage:id(), helpers:file_id(), file_meta:mode()) -> ok.
chmod_on_storage(StorageId, StorageFileId, Mode) ->
    HelperHandle = get_helper_handle(StorageId),
    ok = helpers:chmod(HelperHandle, StorageFileId, Mode).


%% @doc Runs on the op_worker node.
-spec truncate_on_storage(storage:id(), helpers:file_id(), non_neg_integer(), non_neg_integer()) -> ok.
truncate_on_storage(StorageId, StorageFileId, NewSize, CurrentSize) ->
    HelperHandle = get_helper_handle(StorageId),
    ok = helpers:truncate(HelperHandle, StorageFileId, NewSize, CurrentSize).


%% @doc Runs on the op_worker node.
-spec rename_on_storage(storage:id(), helpers:file_id(), helpers:file_id()) -> ok.
rename_on_storage(StorageId, SrcStorageFileId, DstStorageFileId) ->
    HelperHandle = get_helper_handle(StorageId),
    ok = helpers:rename(HelperHandle, SrcStorageFileId, DstStorageFileId).


%% @doc Runs on the op_worker node. Object storages (e.g. S3) have no real
%% directories to remove - a "directory" there is just an inferred prefix of
%% object keys, with nothing for the helper to call - so their helper
%% implementation returns {error, 'Function not implemented'} for this
%% operation, which is treated as a (trivial) success here.
-spec rmdir_on_storage(storage:id(), helpers:file_id()) -> ok.
rmdir_on_storage(StorageId, StorageFileId) ->
    HelperHandle = get_helper_handle(StorageId),
    case helpers:rmdir(HelperHandle, StorageFileId) of
        ok -> ok;
        {error, 'Function not implemented'} -> ok
    end.


%% @doc Runs on the op_worker node.
-spec stat_on_storage(storage:id(), helpers:file_id()) ->
    {ok, helpers:stat()} | {error, term()}.
stat_on_storage(StorageId, StorageFileId) ->
    HelperHandle = get_helper_handle(StorageId),
    helpers:getattr(HelperHandle, StorageFileId).


%% @doc Runs on the op_worker node.
-spec list_dir_on_storage(storage:id(), helpers:file_id(), non_neg_integer(), non_neg_integer()) ->
    {ok, [helpers:file_id()]} | {error, term()}.
list_dir_on_storage(StorageId, StorageFileId, Offset, Count) ->
    HelperHandle = get_helper_handle(StorageId),
    helpers:readdir(HelperHandle, StorageFileId, Offset, Count).


%% @doc Runs on the op_worker node.
-spec get_mtime_on_storage(storage:id(), helpers:file_id()) -> non_neg_integer().
get_mtime_on_storage(StorageId, StorageFileId) ->
    HelperHandle = get_helper_handle(StorageId),
    {ok, #statbuf{st_mtime = Mtime}} = helpers:getattr(HelperHandle, StorageFileId),
    Mtime.


%% @doc Runs on the op_worker node. POSIX-only - relies on the storage being
%% locally mounted on this node, unlike every other routine in this module
%% (which goes through the storage helper and therefore works uniformly
%% across storage types).
-spec set_mtime_on_storage(storage:id(), helpers:file_id(), non_neg_integer()) -> ok.
set_mtime_on_storage(StorageId, StorageFileId, Mtime) ->
    ok = file:write_file_info(
        local_path(StorageId, StorageFileId), #file_info{mtime = Mtime}, [{time, posix}]
    ).


%% @doc Runs on the op_worker node. POSIX-only, see set_mtime_on_storage/3.
-spec set_atime_and_mtime_on_storage(storage:id(), helpers:file_id(), non_neg_integer(), non_neg_integer()) ->
    ok.
set_atime_and_mtime_on_storage(StorageId, StorageFileId, Atime, Mtime) ->
    ok = file:write_file_info(
        local_path(StorageId, StorageFileId), #file_info{atime = Atime, mtime = Mtime}, [{time, posix}]
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_node_on_storage(
    oct_background:entity_selector(), storage:id(), file_meta:path(), file_tree_node_spec()
) ->
    file_tree_node_spec().
create_node_on_storage(ProviderSelector, StorageId, ParentPath, DirSpec = #dir_spec{}) ->
    #dir_spec{name = Name, mode = Mode, uid = Uid, gid = Gid, children = Children} =
        ConcreteDirSpec = ensure_name(DirSpec),
    StorageFileId = filepath_utils:join([ParentPath, Name]),
    ok = create_dir(ProviderSelector, StorageId, StorageFileId, Mode),
    maybe_chown(ProviderSelector, StorageId, StorageFileId, Uid, Gid),
    ConcreteChildren = [
        create_node_on_storage(ProviderSelector, StorageId, StorageFileId, ChildSpec)
        || ChildSpec <- Children
    ],
    ConcreteDirSpec#dir_spec{children = ConcreteChildren};

create_node_on_storage(ProviderSelector, StorageId, ParentPath, FileSpec = #file_spec{}) ->
    #file_spec{name = Name, mode = Mode, content = Content, uid = Uid, gid = Gid} =
        ConcreteFileSpec = ensure_name(FileSpec),
    StorageFileId = filepath_utils:join([ParentPath, Name]),
    ok = create_file(ProviderSelector, StorageId, StorageFileId, Content, Mode),
    maybe_chown(ProviderSelector, StorageId, StorageFileId, Uid, Gid),
    ConcreteFileSpec;

create_node_on_storage(ProviderSelector, StorageId, ParentPath, FifoSpec = #storage_fifo_spec{}) ->
    #storage_fifo_spec{name = Name} = ConcreteFifoSpec = ensure_name(FifoSpec),
    StorageFileId = filepath_utils:join([ParentPath, Name]),
    ok = create_fifo(ProviderSelector, StorageId, StorageFileId),
    ConcreteFifoSpec.


%% @private
-spec delete_node_from_storage(
    oct_background:entity_selector(), storage:id(), file_meta:path(), file_tree_node_spec()
) ->
    ok.
delete_node_from_storage(ProviderSelector, StorageId, ParentPath, #dir_spec{name = Name, children = Children}) ->
    StorageFileId = filepath_utils:join([ParentPath, Name]),
    % delete all children first (required on POSIX, where a non-empty directory
    % cannot be removed), then the now-empty directory itself
    lists:foreach(fun(ChildSpec) ->
        delete_node_from_storage(ProviderSelector, StorageId, StorageFileId, ChildSpec)
    end, Children),
    rmdir(ProviderSelector, StorageId, StorageFileId);
delete_node_from_storage(ProviderSelector, StorageId, ParentPath, #file_spec{name = Name, content = Content}) ->
    StorageFileId = filepath_utils:join([ParentPath, Name]),
    delete_file(ProviderSelector, StorageId, StorageFileId, byte_size(Content));
delete_node_from_storage(ProviderSelector, StorageId, ParentPath, #storage_fifo_spec{name = Name}) ->
    StorageFileId = filepath_utils:join([ParentPath, Name]),
    delete_file(ProviderSelector, StorageId, StorageFileId, 0).


%% @private
-spec maybe_chown(
    oct_background:entity_selector(), storage:id(), helpers:file_id(),
    luma:uid() | undefined, luma:gid() | undefined
) ->
    ok.
maybe_chown(_ProviderSelector, _StorageId, _StorageFileId, undefined, _Gid) ->
    ok;
maybe_chown(_ProviderSelector, _StorageId, _StorageFileId, _Uid, undefined) ->
    ok;
maybe_chown(ProviderSelector, StorageId, StorageFileId, Uid, Gid) ->
    ok = chown(ProviderSelector, StorageId, StorageFileId, Uid, Gid).


%% @private
-spec ensure_name(file_tree_node_spec()) -> file_tree_node_spec().
ensure_name(DirSpec = #dir_spec{name = undefined}) ->
    DirSpec#dir_spec{name = str_utils:rand_hex(20)};
ensure_name(FileSpec = #file_spec{name = undefined}) ->
    FileSpec#file_spec{name = str_utils:rand_hex(20)};
ensure_name(FifoSpec = #storage_fifo_spec{name = undefined}) ->
    FifoSpec#storage_fifo_spec{name = str_utils:rand_hex(20)};
ensure_name(Spec) ->
    Spec.



%% @private
%% StorageFileId is absolute (leading "/"), so it must be concatenated
%% (not filename:join/1-ed, which would treat it as an absolute path on its
%% own and discard the mount point).
-spec local_path(storage:id(), helpers:file_id()) -> file:filename_all().
local_path(StorageId, StorageFileId) ->
    HelperSpec = storage:get_helper_spec(StorageId),
    MountPoint = maps:get(<<"mountPoint">>, helper_spec:get_configuration(HelperSpec)),
    <<MountPoint/binary, StorageFileId/binary>>.


%% @private
-spec get_helper_handle(storage:id()) -> helpers:helper_handle().
get_helper_handle(StorageId) ->
    HelperSpec = storage:get_helper_spec(StorageId),
    helpers:get_helper_handle(HelperSpec, HelperSpec#helper_spec.credentials).
