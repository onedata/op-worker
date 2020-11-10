%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling operations concerning QoS
%%% traverse requests management.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_traverse_req).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([
    build_traverse_reqs/2, remove_req/2,
    select_traverse_reqs/2, get_storage/1,
    get_traverse_ids/1,
    are_all_finished/1,
    start_applicable_traverses/3,
    split_local_and_remote/1
]).

-type id() :: datastore:key().
-type traverse_req() :: #qos_traverse_req{}.
-opaque traverse_reqs() :: #{id() => #qos_traverse_req{}}.

-export_type([id/0, traverse_req/0, traverse_reqs/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns new traverse reqs based on given storages list and file uuid.
%% @end
%%--------------------------------------------------------------------
-spec build_traverse_reqs(file_meta:uuid(), [storage:id()]) -> traverse_reqs().
build_traverse_reqs(FileUuid, StoragesList) ->
    lists:foldl(fun(Storage, Acc) ->
        TaskId = datastore_key:new(),
        Acc#{TaskId => #qos_traverse_req{
            start_file_uuid = FileUuid,
            storage_id = Storage
        }}
    end, #{}, StoragesList).


%%--------------------------------------------------------------------
%% @doc
%% Removes traverse req associated with given TraverseId from TraverseReqs.
%% @end
%%--------------------------------------------------------------------
-spec remove_req(id(), traverse_reqs()) -> traverse_reqs().
remove_req(TraverseId, TraverseReqs) ->
    maps:remove(TraverseId, TraverseReqs).


%%--------------------------------------------------------------------
%% @doc
%% Returns traverse reqs associated with given traverse ids.
%% @end
%%--------------------------------------------------------------------
-spec select_traverse_reqs([id()], traverse_reqs()) -> traverse_reqs().
select_traverse_reqs(TraverseIds, TraverseReqs) ->
    maps:with(TraverseIds, TraverseReqs).


-spec get_storage(traverse_req()) -> storage:id().
get_storage(#qos_traverse_req{storage_id = StorageId}) ->
    StorageId.


-spec get_traverse_ids(traverse_reqs()) -> [id()].
get_traverse_ids(TraverseReqs) ->
    maps:keys(TraverseReqs).


-spec are_all_finished(traverse_reqs()) -> boolean().
are_all_finished(AllTraverseReqs) ->
    maps:size(AllTraverseReqs) == 0.


%%--------------------------------------------------------------------
%% @doc
%% Checks whether any traverse is to be run on this provider
%% and starts applicable traverses.
%% @end
%%--------------------------------------------------------------------
-spec start_applicable_traverses(qos_entry:id(), od_space:id(), traverse_reqs()) -> ok.
start_applicable_traverses(QosEntryId, SpaceId, AllTraverseReqs) ->
    maps:fold(fun(TaskId, TraverseReq, _) ->
        #qos_traverse_req{
            start_file_uuid = StartFileUuid, storage_id = StorageId
        } = TraverseReq,

        FileCtx = file_ctx:new_by_guid(file_id:pack_guid(StartFileUuid, SpaceId)),
        storage:is_local(StorageId) andalso
            start_traverse(FileCtx, QosEntryId, StorageId, TaskId)
    end, ok, AllTraverseReqs),
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executed only by provider to which given storage belongs to.
%% Starts QoS traverse and updates file_qos accordingly.
%% @end
%%--------------------------------------------------------------------
-spec start_traverse(file_ctx:ctx(), qos_entry:id(), storage:id(), traverse:id()) -> ok.
start_traverse(FileCtx, QosEntryId, StorageId, TaskId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    ok = file_qos:add_qos_entry_id(SpaceId, FileUuid, QosEntryId, StorageId),
    case lookup_file_meta_doc(FileCtx) of
        {ok, FileCtx1} ->
            ok = qos_traverse:start_initial_traverse(FileCtx1, QosEntryId, TaskId);
        {error, not_found} ->
            % There is no need to start traverse as appropriate transfers will be started
            % when file is finally synced. If this is directory, then each child registered
            % file meta posthook that will fulfill QoS after all its ancestors are synced.
            ok = qos_entry:remove_traverse_req(QosEntryId, TaskId)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Splits given traverse reqs to those of current provider and those of remote providers.
%% @end
%%--------------------------------------------------------------------
-spec split_local_and_remote(traverse_reqs()) -> {Local :: [id()], Remote :: [id()]}.
split_local_and_remote(TraverseReqs) ->
    maps:fold(fun(TaskId, TraverseReq, {LocalTraverseReqs, RemoteTraverseReqs}) ->
        StorageId = qos_traverse_req:get_storage(TraverseReq),
        case storage:is_local(StorageId) of
            true -> {[TaskId | LocalTraverseReqs], RemoteTraverseReqs};
            false -> {LocalTraverseReqs, [TaskId | RemoteTraverseReqs]}
        end
    end, {[], []}, TraverseReqs).

%%%===================================================================
%%% Utility functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Returns file's file_meta document.
%% Returns error if there is no such file doc (it is not synced yet).
%% @end
%%--------------------------------------------------------------------
-spec lookup_file_meta_doc(file_ctx:ctx()) -> {ok, file_ctx:ctx()} | {error, not_found}.
lookup_file_meta_doc(FileCtx) ->
    try
        {_Doc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
        {ok, FileCtx2}
    catch
        _:{badmatch, {error, not_found} = Error}->
            Error
    end.
