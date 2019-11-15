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
%% Initialises iterator specific options.
%% @end
%%-------------------------------------------------------------------
-callback init(MasterJob :: storage_traverse:master_job(), RunOpts :: storage_traverse:run_opts()) ->
    InitialisedMasterJob :: storage_traverse:master_job().

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
%% Checks whether file associated with StorageFileCtx is a directory.
%% @end
%%-------------------------------------------------------------------
-callback is_dir(StorageFileCtx :: storage_file_ctx:ctx()) ->
    {boolean(), StorageFileCtx2 :: storage_file_ctx:ctx()}.
