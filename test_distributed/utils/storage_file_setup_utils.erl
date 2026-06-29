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

%% API
-export([create_file/4, create_file/5]).
-export([create_dir/3, create_dir/4, chown/5]).
-export([write_file/5, delete_file/4]).
-export([chmod/4, truncate/5, rename/4, rmdir/3]).
%% on-node routines (executed on op_worker via rpc)
-export([create_file_on_storage/3, write_to_storage_file/4, delete_file_on_storage/3]).
-export([create_dir_on_storage/3, chown_on_storage/4]).
-export([chmod_on_storage/3, truncate_on_storage/4, rename_on_storage/3, rmdir_on_storage/2]).


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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_helper_handle(storage:id()) -> helpers:helper_handle().
get_helper_handle(StorageId) ->
    Helper = storage:get_helper(StorageId),
    helpers:get_helper_handle(Helper, Helper#helper.admin_ctx).
