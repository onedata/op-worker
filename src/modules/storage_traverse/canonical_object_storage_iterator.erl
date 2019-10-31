%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module implementing storage_iterator for object helpers
%%% with canonical storage path type.
%%% @end
%%%-------------------------------------------------------------------
-module(canonical_object_storage_iterator).
-author("Jakub Kudzia").

-behaviour(storage_iterator).

-include("global_definitions.hrl").
-include("modules/storage_traverse/storage_traverse.hrl").

%% storage_iterator callbacks
-export([init/2, get_children_batch/1, is_dir/1]).

%%%===================================================================
%%% storage_iterator callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link storage_iterator} callback init/2.
%% @end
%%--------------------------------------------------------------------
-spec init(storage_traverse:master_job(), storage_traverse:run_opts()) -> storage_traverse:master_job().
init(StorageTraverse = #storage_traverse_master{storage_file_ctx = StorageFileCtx}, Opts) ->
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    case maps:get(marker, Opts, undefined) of
        undefined ->
            StorageTraverse#storage_traverse_master{marker = storage_file_id:space_id(SpaceId, StorageId)};
        Marker ->
            StorageTraverse#storage_traverse_master{marker = Marker}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link storage_iterator} callback get_children_batch/1.
%% @end
%%--------------------------------------------------------------------
-spec get_children_batch(storage_traverse:master_job()) ->
    {ok, storage_traverse:children_batch(), storage_traverse:master_job()} | {error, term()}.
get_children_batch(StorageTraverse = #storage_traverse_master{max_depth = 0}) ->
    {ok, [], StorageTraverse};
get_children_batch(StorageTraverse = #storage_traverse_master{
    storage_file_ctx = StorageFileCtx,
    offset = Offset,
    batch_size = BatchSize,
    marker = Marker
}) ->
    Handle = storage_file_ctx:get_handle_const(StorageFileCtx),
    case storage_file_manager:listobjects(Handle, Marker, Offset, BatchSize) of
        {ok, []} ->
            {ok, [], StorageTraverse};
        {ok, ChildrenIds} ->
            ParentStorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            ParentTokens = filename:split(ParentStorageFileId),
            ChildrenBatch = [{ChildId, depth(ChildId, ParentTokens)} || ChildId <- ChildrenIds],
            {ok, ChildrenBatch, StorageTraverse#storage_traverse_master{
                offset = Offset + length(ChildrenIds),
                marker = lists:last(ChildrenIds)
            }};
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
    {false, StorageFileCtx}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec depth(helpers:file_id(), [helpers:file_id()]) -> non_neg_integer().
depth(ChildId, ParentIdTokens) ->
    ChildTokens = filename:split(ChildId),
    length(ChildTokens) - length(ParentIdTokens).