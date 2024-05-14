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
-module(flat_storage_iterator).
-author("Jakub Kudzia").

-behaviour(storage_iterator).

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include("modules/storage/traverse/storage_traverse.hrl").
-include_lib("ctool/include/logging.hrl").

%% storage_iterator callbacks
-export([init_root_storage_file_ctx/3, get_children_and_next_batch_job/1, should_generate_master_job/1]).

%% API
-export([get_virtual_directory_ctx/3]).

%%%===================================================================
%%% storage_iterator callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link storage_iterator} callback init_root_storage_file_ctx/3.
%% @end
%%--------------------------------------------------------------------
-spec init_root_storage_file_ctx(helpers:file_id(), od_space:id(), storage:id()) -> storage_file_ctx:ctx().
init_root_storage_file_ctx(RootStorageFileId, SpaceId, StorageId) ->
    get_virtual_directory_ctx(RootStorageFileId, SpaceId, StorageId).

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
    offset = Offset,
    batch_size = BatchSize,
    marker = Marker
}) ->
    Handle = storage_file_ctx:get_handle_const(StorageFileCtx),
    SpaceId = storage_file_ctx:get_space_id_const(StorageFileCtx),
    StorageId = storage_file_ctx:get_storage_id_const(StorageFileCtx),
    case storage_driver:listobjects(Handle, Marker, Offset, BatchSize) of
        {ok, []} ->
            {ok, [], undefined};
        {ok, ChildrenIdsAndStats} ->
            ParentStorageFileId = storage_file_ctx:get_storage_file_id_const(StorageFileCtx),
            ParentTokens = filename:split(ParentStorageFileId),
            ChildrenBatch = lists:map(fun({ChildId, ChildStat}) ->
                ChildCtx = storage_file_ctx:new_with_stat(ChildId, SpaceId, StorageId, ChildStat),
                {ChildCtx, depth(ChildId, ParentTokens)}
            end, ChildrenIdsAndStats),
            case length(ChildrenBatch) < BatchSize of
                true ->
                    {ok, ChildrenBatch, undefined};
                false ->
                    NextOffset = Offset + length(ChildrenIdsAndStats),
                    {NextMarker, _} = lists:last(ChildrenIdsAndStats),
                    {ok, ChildrenBatch, StorageTraverse#storage_traverse_master{
                        offset = NextOffset,
                        marker = NextMarker
                    }}
            end;
        Error = {error, _} ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link storage_iterator} callback should_generate_master_job/1.
%% @end
%%--------------------------------------------------------------------
-spec should_generate_master_job(StorageFileCtx :: storage_file_ctx:ctx()) ->
    {boolean(), StorageFileCtx2 :: storage_file_ctx:ctx()}.
should_generate_master_job(StorageFileCtx) ->
    % only StorageFileCtx associated with space is processed in master job on object storages
    {false, StorageFileCtx}.

%%%===================================================================
%%% API functions
%%%===================================================================

-spec get_virtual_directory_ctx(helpers:file_id(), od_space:id(), storage:id()) -> storage_file_ctx:ctx().
get_virtual_directory_ctx(StorageFileId, SpaceId, StorageId) ->
    CurrentTime = global_clock:timestamp_seconds(),
    Stat = #statbuf{
        st_uid = ?ROOT_UID,
        st_gid = ?ROOT_GID,
        st_mode = ?DEFAULT_DIR_PERMS bor 8#40000,
        st_mtime = CurrentTime,
        st_atime = CurrentTime,
        st_ctime = CurrentTime,
        st_size = 0
    },
    storage_file_ctx:new_with_stat(StorageFileId, SpaceId, StorageId, Stat).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec depth(helpers:file_id(), [helpers:file_id()]) -> non_neg_integer().
depth(ChildId, ParentIdTokens) ->
    % ParentId is always storage file id of space
    % depending whether space is mounted in root, to calculate file depth
    % we have to subtract 1 (in case of imported_storage) or 2 from length of ChildTokens.
    ChildTokens = filename:split(ChildId),
    length(ChildTokens) - length(ParentIdTokens).