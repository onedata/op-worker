%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 ACK CYFRONET AGH
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
%%% NOTE: this module must be added to ?LOAD_MODULES of any suite that uses it,
%%% as the on-node routines are executed on the op_worker node via rpc.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_file_setup_utils).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("kernel/include/file.hrl").

%% API
-export([create_file/4, create_file/5]).
-export([create_fifo/3, create_fifo/4]).
-export([create_dir/3, create_dir/4, chown/5]).
-export([write_file/5, delete_file/4]).
-export([chmod/4, truncate/5, rename/4, rmdir/3]).
-export([get_mtime/3, set_mtime/4, set_atime_and_mtime/5]).
%% on-node routines (executed on op_worker via rpc)
-export([create_file_on_storage/3, write_to_storage_file/4, delete_file_on_storage/3]).
-export([create_fifo_on_storage/3]).
-export([create_dir_on_storage/3, chown_on_storage/4]).
-export([chmod_on_storage/3, truncate_on_storage/4, rename_on_storage/3, rmdir_on_storage/2]).
-export([get_mtime_on_storage/2, set_mtime_on_storage/3, set_atime_and_mtime_on_storage/4]).


%%%===================================================================
%%% API functions
%%%===================================================================


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


%% @doc Runs on the op_worker node.
-spec rmdir_on_storage(storage:id(), helpers:file_id()) -> ok.
rmdir_on_storage(StorageId, StorageFileId) ->
    HelperHandle = get_helper_handle(StorageId),
    ok = helpers:rmdir(HelperHandle, StorageFileId).


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
%% StorageFileId is absolute (leading "/"), so it must be concatenated
%% (not filename:join/1-ed, which would treat it as an absolute path on its
%% own and discard the mount point).
-spec local_path(storage:id(), helpers:file_id()) -> file:filename_all().
local_path(StorageId, StorageFileId) ->
    Helper = storage:get_helper(StorageId),
    MountPoint = maps:get(<<"mountPoint">>, helper:get_args(Helper)),
    <<MountPoint/binary, StorageFileId/binary>>.


%% @private
-spec get_helper_handle(storage:id()) -> helpers:helper_handle().
get_helper_handle(StorageId) ->
    Helper = storage:get_helper(StorageId),
    helpers:get_helper_handle(Helper, Helper#helper.admin_ctx).
