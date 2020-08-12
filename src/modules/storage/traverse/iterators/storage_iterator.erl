%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Behaviour defining callbacks used by storage_traverse module
%%% for listing and processing files visible on storage.
%%% Currently there are 2 modules implementing this behaviour:
%%%    * block_storage_traverse - used for posix compatible helpers:
%%%      ** posix
%%%      ** nulldevice
%%%      ** glusterfs
%%%    * canonical_object_storage_traverse - used for object helpers with
%%%        canonical storage path type:
%%%      ** s3
%%% NOTE:
%%% When implementing new iterator, function clause for it must be added
%%% to storage_traverse:get_iterator/1 function.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_iterator).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").

%%%===================================================================
%%% API callbacks
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This function should return initialized storage_file_ctx:ctx()
%% for root directory from which traverse should be started.
%% @end
%%-------------------------------------------------------------------
-callback init_root_storage_file_ctx(RootStorageFileId :: helpers:file_id(),
    SpaceId :: od_space:id(), StorageId :: storage:id()) ->
    RootStorageFileCtx :: storage_file_ctx:ctx().


%%-------------------------------------------------------------------
%% @doc
%% Returns next batch of children of directory associated with
%% StorageFileCtx stored in StorageTraverse record.
%% Each ChildId is associated with its depth in the tree structure.
%% @end
%%-------------------------------------------------------------------
-callback get_children_and_next_batch_job(MasterJob :: storage_traverse:master_job()) ->
    {ok, ChildrenBatch :: storage_traverse:children_batch(), NextBatchMasterJob :: storage_traverse:master_job() | undefined} |
    {error, term()}.


%%-------------------------------------------------------------------
%% @doc
%% Returns true if processing passed StorageFileCtx should result in
%% master job.
%% @end
%%-------------------------------------------------------------------
-callback should_generate_master_job(StorageFileCtx :: storage_file_ctx:ctx()) ->
    {boolean(), StorageFileCtx2 :: storage_file_ctx:ctx()}.
