%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Stores progress information needed to restart workflow.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_execution_state_dump).
-author("Michal Wrzeszcz").


-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([dump_workflow_execution_state/1, restore_workflow_execution_state_from_dump/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).


-define(CTX, #{
    model => ?MODULE
}).


%%%===================================================================
%%% API
%%%===================================================================

-spec dump_workflow_execution_state(workflow_engine:execution_id()) -> ok.
dump_workflow_execution_state(ExecutionId) ->
    case workflow_execution_state:get(ExecutionId) of
        {ok, #workflow_execution_state{
            snapshot_mode = SnapshotMode,
            execution_status = Status,
            failed_job_count = FailedCount,

            iteration_state = IterationState,
            jobs = Jobs,
            tasks_data_registry = TaskData
        }} ->
            TranslatedStatus = case Status of
                ?PREPARATION_FAILED -> ?NOT_PREPARED;
                #execution_cancelled{has_lane_preparation_failed = true} -> ?NOT_PREPARED;
                _ -> ?PREPARED
            end,

            Doc = #document{key = ExecutionId, value = #workflow_execution_state_dump{
                snapshot_mode = SnapshotMode,
                lane_status = TranslatedStatus,
                failed_job_count = FailedCount,

                iteration_state_dump = workflow_iteration_state:dump(IterationState),
                jobs_dump = workflow_jobs:dump(Jobs),
                tasks_data_registry_dump = workflow_tasks_data_registry:dump(TaskData)
            }},
            {ok, _} = datastore_model:save(?CTX, Doc),
            ok;
        ?ERROR_NOT_FOUND ->
            ok
    end.


-spec restore_workflow_execution_state_from_dump(workflow_execution_state:doc(), iterator:iterator()) ->
    ok | ?ERROR_NOT_FOUND.
restore_workflow_execution_state_from_dump(
    #document{key = ExecutionId, value = #workflow_execution_state{incarnation_tag = Tag} = StateBase} = DocBase,
    Iterator
) ->
    case datastore_model:get(?CTX, ExecutionId) of
        {ok, #document{value = #workflow_execution_state_dump{
            snapshot_mode = SnapshotMode,
            lane_status = LaneStatus,
            failed_job_count = FailedCount,

            iteration_state_dump = IterationStateDump,
            jobs_dump = JobsDump,
            tasks_data_registry_dump = TaskDataDump
        }}} ->
            TranslatedStatus = case LaneStatus of
                ?PREPARED -> ?RESUMING_FROM_ITERATOR(Iterator);
                ?NOT_PREPARED -> ?NOT_PREPARED
            end,

            Doc = DocBase#document{value = StateBase#workflow_execution_state{
                snapshot_mode = SnapshotMode,
                execution_status = TranslatedStatus,
                failed_job_count = FailedCount, % TODO VFS-7787 - maybe reset?

                iteration_state = workflow_iteration_state:from_dump(IterationStateDump),
                jobs = workflow_jobs:from_dump(JobsDump, Tag),
                tasks_data_registry = workflow_tasks_data_registry:from_dump(TaskDataDump)
            }},

            workflow_execution_state:save(Doc),
            ok = datastore_model:delete(?CTX, ExecutionId);
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND
    end.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {snapshot_mode, atom},
        {lane_status, atom},
        {failed_job_count, integer},

        {iteration_state, workflow_iteration_state:get_dump_struct()},
        {jobs, workflow_jobs:get_dump_struct()},
        {tasks_data_registry, workflow_tasks_data_registry:get_dump_struct()}
    ]}.
