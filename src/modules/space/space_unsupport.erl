%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% fixme
%%% @end
%%%--------------------------------------------------------------------
-module(space_unsupport).
-author("Michal Stanisz").

-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([run/2]).

-type stage() :: qos | clear_storage | wait_for_dbsync | clean_docs.

-export_type([stage/0]).

run(SpaceId, StorageId) ->
    run_internal(qos,SpaceId, StorageId).

run_internal(finished, _SpaceId, _StorageId) -> ok;
run_internal(CurrentStage, SpaceId, StorageId) -> 
    ?notice("Running stage: ~p", [CurrentStage]),
    % fixme log to zone
    ok = execute_stage(CurrentStage, SpaceId, StorageId),
    ?notice("Stage ~p finished", [CurrentStage]),
    NextStage = get_next_stage(CurrentStage),
    run_internal(NextStage, SpaceId, StorageId).


execute_stage(qos, SpaceId, StorageId) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    % fixme if preferred provider replace AllStorages with provider Id (not really if other providers still allowed)
    Expression = <<?ALL_STORAGES/binary, " - storage_id = ", StorageId/binary>>,
    {ok, QosEntryId} = lfm:add_qos_entry(?ROOT_SESS_ID, {guid, SpaceGuid}, Expression, 1),
    % fixme use qos status sub
    ok = wait_for_qos(QosEntryId),
    lfm:remove_qos_entry(?ROOT_SESS_ID, QosEntryId);
execute_stage(clear_storage, SpaceId, StorageId) ->
    % fixme remove file_location ()
    {ok, TaskId} = unsupport_traverse:start(SpaceId, StorageId),
    ok;
execute_stage(wait_for_dbsync, _SpaceId, _StorageId) -> 
    % fixme
%%    timer:sleep(timer:minutes(1)),
    ok;
execute_stage(clean_docs, _SpaceId, _StorageId) -> 
    % fixme remove all synced using changes stream
    % fixme find which local documents are to be deleted and to it here (storage:space_unsupported?)
    ok.

get_next_stage(qos) -> clear_storage;
get_next_stage(clear_storage) -> wait_for_dbsync;
get_next_stage(wait_for_dbsync) -> clean_docs;
get_next_stage(clean_docs) -> finished.


wait_for_qos(QosEntryId) ->
    case lfm:check_qos_fulfilled(?ROOT_SESS_ID, QosEntryId) of
        {ok, true} -> ok;
        {ok, false} -> 
            timer:sleep(timer:seconds(1)),
            wait_for_qos(QosEntryId)
    end.