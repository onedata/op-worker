%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module implementing storage_iterator for posix compatible
%%% helpers.
%%% @end
%%%-------------------------------------------------------------------
-module(block_storage_iterator).
-author("Jakub Kudzia").

-behaviour(storage_iterator).

-include("global_definitions.hrl").
-include("modules/storage/traverse/storage_traverse.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% storage_iterator callbacks
-export([init/2, get_children_and_next_batch_job/1, is_dir/1]).

%%%===================================================================
%%% storage_iterator callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link storage_iterator} callback init/2.
%% @end
%%--------------------------------------------------------------------
-spec init(storage_traverse:master_job(), storage_traverse:run_opts()) -> storage_traverse:master_job().
init(StorageTraverse = #storage_traverse_master{}, _Opts) ->
    StorageTraverse.

%%--------------------------------------------------------------------
%% @doc
%% {@link storage_iterator} callback get_children_and_next_batch_job/1.
%% @end
%%--------------------------------------------------------------------
-spec get_children_and_next_batch_job(storage_traverse:master_job()) ->
    {ok, storage_traverse:children_batch(), storage_traverse:master_job() | undefined} | {error, term()}.
get_children_and_next_batch_job(StorageTraverse = #storage_traverse_master{max_depth = 0}) ->
    {ok, [], StorageTraverse};
get_children_and_next_batch_job(StorageTraverse = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    depth = ParentDepth,
    offset = Offset,
    batch_size = BatchSize
}) ->
    Handle = storage_file_ctx:get_handle_const(StorageFileCtx),
    case storage_driver:readdir(Handle, Offset, BatchSize) of
        {ok, ChildrenNames} ->
            StorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            ChildDepth = ParentDepth + 1,
            ChildrenBatch = [{filename:join([StorageFileId, ChildName]), ChildDepth} || ChildName <- ChildrenNames],
            case length(ChildrenBatch) < BatchSize of
                true ->
                    {ok, ChildrenBatch, undefined};
                false ->
                    NextOffset = Offset + length(ChildrenNames),
                    {ok, ChildrenBatch, StorageTraverse#storage_traverse_master{offset = NextOffset}}
            end;
        Error = {error, _} ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link storage_iterator} callback is_dir/1.
%% @end
%%--------------------------------------------------------------------
-spec is_dir(StorageFileCtx :: storage_file_ctx:ctx()) ->
    {boolean(), StorageFileCtx2 :: storage_file_ctx:ctx()}.
is_dir(StorageFileCtx) ->
    {#statbuf{st_mode = Mode}, StorageFileCtx2} = storage_file_ctx:stat(StorageFileCtx),
    {file_meta:type(Mode) =:= ?DIRECTORY_TYPE, StorageFileCtx2}.