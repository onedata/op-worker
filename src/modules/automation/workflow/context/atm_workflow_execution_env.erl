%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utility functions for management of automation
%%% workflow execution environment which consists of conditions in which
%%% specific workflow is being executed (e.g. mapping of store schema id to
%%% actual store id).
%%% Main uses of automation workflow environment are:
%%% 1) quick access to basic information about workflow (e.g. space id)
%%% 2) quick access to all global stores (store defined in schema and accessible
%%%    on all levels of execution) and other stores in current scope
%%%    (e.g. currently executed lane run exception store).
%%% 3) acquisition of automation workflow auth.
%%% 4) acquisition of automation workflow logger.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_env).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([
    build/4, build/5,
    add_global_store_mapping/3,
    set_workflow_audit_log_store_container/2,
    set_lane_run_exception_store_container/2,
    set_lane_run_instant_failure_exception_threshold/2,
    ensure_task_selector_registry_up_to_date/3,
    add_task_audit_log_store_container/3,
    add_task_time_series_store_id/3,
    ensure_task_registered/2
]).
-export([
    get_space_id/1,
    get_workflow_execution_id/1,
    get_workflow_execution_incarnation/1,

    list_global_stores/1,
    get_global_store_id/2,

    get_lane_run_exception_store_container/1,
    get_lane_run_instant_failure_exception_threshold/1,
    get_task_time_series_store_id/2,
    get_task_selector/2,

    acquire_auth/1,

    get_log_level_int/1,
    build_logger/3
]).


% See module doc for more information.
-record(atm_workflow_execution_env, {
    space_id :: od_space:id(),
    workflow_execution_id :: atm_workflow_execution:id(),
    workflow_execution_incarnation :: atm_workflow_execution:incarnation(),

    log_level :: audit_log:entry_severity_int(),

    % globally accessible stores
    global_store_registry :: atm_workflow_execution:store_registry(),
    workflow_audit_log_store_container :: undefined | atm_store_container:record(),

    % current lane run execution specific components
    lane_run_exception_store_container :: undefined | atm_store_container:record(),
    lane_run_instant_failure_exception_threshold :: undefined | float(),  %% 0.0 .. 1.0

    task_selector_registry :: #{atm_task_execution:id() => atm_workflow_execution_logger:task_selector()},

    task_audit_logs_registry :: #{atm_task_execution:id() => atm_store_container:record()},
    task_time_series_registry :: #{atm_task_execution:id() => atm_store:id()}
}).
-type record() :: #atm_workflow_execution_env{}.

-type diff() :: fun((record()) -> record()).

-export_type([record/0, diff/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    od_space:id(),
    atm_workflow_execution:id(),
    atm_workflow_execution:incarnation(),
    audit_log:entry_severity_int()
) ->
    record().
build(SpaceId, AtmWorkflowExecutionId, AtmWorkflowExecutionIncarnation, LogLevel) ->
    build(SpaceId, AtmWorkflowExecutionId, AtmWorkflowExecutionIncarnation, LogLevel, #{}).


-spec build(
    od_space:id(),
    atm_workflow_execution:id(),
    atm_workflow_execution:incarnation(),
    audit_log:entry_severity_int(),
    atm_workflow_execution:store_registry()
) ->
    record().
build(
    SpaceId,
    AtmWorkflowExecutionId,
    AtmWorkflowExecutionIncarnation,
    LogLevel,
    AtmGlobalStoreRegistry
) ->
    #atm_workflow_execution_env{
        space_id = SpaceId,
        workflow_execution_id = AtmWorkflowExecutionId,
        workflow_execution_incarnation = AtmWorkflowExecutionIncarnation,
        log_level = LogLevel,
        global_store_registry = AtmGlobalStoreRegistry,
        workflow_audit_log_store_container = undefined,
        lane_run_exception_store_container = undefined,
        lane_run_instant_failure_exception_threshold = undefined,
        task_audit_logs_registry = #{},
        task_time_series_registry = #{},
        task_selector_registry = #{}
    }.


-spec add_global_store_mapping(automation:id(), atm_store:id(), record()) -> record().
add_global_store_mapping(AtmStoreSchemaId, AtmStoreId, Record = #atm_workflow_execution_env{
    global_store_registry = AtmGlobalStoreRegistry
}) ->
    Record#atm_workflow_execution_env{global_store_registry = AtmGlobalStoreRegistry#{
        AtmStoreSchemaId => AtmStoreId
    }}.


-spec set_workflow_audit_log_store_container(undefined | atm_store_container:record(), record()) ->
    record().
set_workflow_audit_log_store_container(AtmWorkflowAuditLogStoreContainer, Record) ->
    Record#atm_workflow_execution_env{
        workflow_audit_log_store_container = AtmWorkflowAuditLogStoreContainer
    }.


-spec set_lane_run_exception_store_container(undefined | atm_store_container:record(), record()) ->
    record().
set_lane_run_exception_store_container(AtmLaneRunExceptionStoreContainer, Record) ->
    Record#atm_workflow_execution_env{
        lane_run_exception_store_container = AtmLaneRunExceptionStoreContainer
    }.


-spec set_lane_run_instant_failure_exception_threshold(float(), record()) -> record().
set_lane_run_instant_failure_exception_threshold(Threshold, Record) ->
    Record#atm_workflow_execution_env{lane_run_instant_failure_exception_threshold = Threshold}.


-spec ensure_task_selector_registry_up_to_date(
    atm_workflow_execution:id() | atm_workflow_execution:doc(),
    atm_lane_execution:lane_run_selector(),
    record()
) ->
    record().
ensure_task_selector_registry_up_to_date(
    #document{value = AtmWorkflowExecution = #atm_workflow_execution{
        schema_snapshot_id = AtmWorkflowSchemaSnapshotId
    }},
    OriginalAtmLaneRunSelector,
    Record
) ->
    AtmLaneRunSelector = {AtmLaneIndex, _} = atm_lane_execution:try_resolving_lane_run_selector(
        OriginalAtmLaneRunSelector, AtmWorkflowExecution
    ),
    {ok, AtmLaneRun} = atm_lane_execution:get_run(AtmLaneRunSelector, AtmWorkflowExecution),

    case should_renew_task_selector_registry(AtmLaneRunSelector, AtmLaneRun, Record) of
        false ->
            Record;
        true ->
            {ok, #document{value = #atm_workflow_schema_snapshot{
                revision = #atm_workflow_schema_revision{
                    lanes = AtmLaneSchemas
                }
            }}} = atm_workflow_schema_snapshot:get(AtmWorkflowSchemaSnapshotId),

            AtmLaneSchema = lists:nth(AtmLaneIndex, AtmLaneSchemas),

            Record#atm_workflow_execution_env{task_selector_registry = lists:foldl(fun(
                {AtmParallelBoxIndex, {AtmParallelBoxSchema, AtmParallelBoxExecution}},
                OuterAcc
            ) ->
                lists:foldl(fun({AtmTaskIndex, #atm_task_schema{id = AtmTaskSchemaId}}, InnerAcc) ->
                    AtmTaskExecutionId = atm_parallel_box_execution:get_task_id(
                        AtmTaskSchemaId, AtmParallelBoxExecution
                    ),
                    AtmTaskSelector = {AtmLaneRunSelector, AtmParallelBoxIndex, AtmTaskIndex},

                    InnerAcc#{AtmTaskExecutionId => AtmTaskSelector}
                end, OuterAcc, lists:enumerate(1, AtmParallelBoxSchema#atm_parallel_box_schema.tasks))
            end, #{}, lists:enumerate(1, lists:zip(
                AtmLaneSchema#atm_lane_schema.parallel_boxes,
                AtmLaneRun#atm_lane_execution_run.parallel_boxes
            )))}
    end;

ensure_task_selector_registry_up_to_date(AtmWorkflowExecutionId, OriginalAtmLaneRunSelector, Record) ->
    {ok, AtmWorkflowExecutionDoc} = atm_workflow_execution:get(AtmWorkflowExecutionId),
    ensure_task_selector_registry_up_to_date(AtmWorkflowExecutionDoc, OriginalAtmLaneRunSelector, Record).


-spec add_task_audit_log_store_container(
    atm_task_execution:id(),
    undefined | atm_store_container:record(),
    record()
) ->
    record().
add_task_audit_log_store_container(
    AtmTaskExecutionId,
    AtmTaskAuditLogStoreContainer,
    #atm_workflow_execution_env{task_audit_logs_registry = AtmTaskAuditLogsRegistry} = Record
) ->
    Record#atm_workflow_execution_env{task_audit_logs_registry = AtmTaskAuditLogsRegistry#{
        AtmTaskExecutionId => AtmTaskAuditLogStoreContainer
    }}.


-spec add_task_time_series_store_id(
    atm_task_execution:id(),
    undefined | atm_store_container:record(),
    record()
) ->
    record().
add_task_time_series_store_id(
    AtmTaskExecutionId,
    AtmTaskTSStoreId,
    Record = #atm_workflow_execution_env{task_time_series_registry = AtmTaskTSRegistry}
) ->
    Record#atm_workflow_execution_env{task_time_series_registry = AtmTaskTSRegistry#{
        AtmTaskExecutionId => AtmTaskTSStoreId
    }}.


-spec ensure_task_registered(atm_task_execution:id(), record()) -> record().
ensure_task_registered(AtmTaskExecutionId, Record = #atm_workflow_execution_env{
    task_audit_logs_registry = AtmTaskAuditLogsRegistry
}) ->
    case maps:is_key(AtmTaskExecutionId, AtmTaskAuditLogsRegistry) of
        true -> Record;
        false -> add_task_stores(AtmTaskExecutionId, Record)
    end.


-spec get_space_id(record()) -> od_space:id().
get_space_id(#atm_workflow_execution_env{space_id = SpaceId}) ->
    SpaceId.


-spec get_workflow_execution_id(record()) -> atm_workflow_execution:id().
get_workflow_execution_id(#atm_workflow_execution_env{
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    AtmWorkflowExecutionId.


-spec get_workflow_execution_incarnation(record()) ->
    undefined | atm_workflow_execution:incarnation().
get_workflow_execution_incarnation(#atm_workflow_execution_env{
    workflow_execution_incarnation = AtmWorkflowExecutionIncarnation
}) ->
    AtmWorkflowExecutionIncarnation.


-spec list_global_stores(record()) -> [atm_store:id()].
list_global_stores(#atm_workflow_execution_env{global_store_registry = AtmStoreRegistry}) ->
    maps:values(AtmStoreRegistry).


-spec get_global_store_id(automation:id(), record()) -> atm_store:id() | no_return().
get_global_store_id(AtmStoreSchemaId, #atm_workflow_execution_env{
    global_store_registry = AtmStoreRegistry
}) ->
    case maps:get(AtmStoreSchemaId, AtmStoreRegistry, undefined) of
        undefined ->
            throw(?ERROR_ATM_STORE_NOT_FOUND(AtmStoreSchemaId));
        AtmStoreId ->
            AtmStoreId
    end.


-spec get_lane_run_exception_store_container(record()) ->
    undefined | atm_store_container:record().
get_lane_run_exception_store_container(#atm_workflow_execution_env{
    lane_run_exception_store_container = AtmLaneRunExceptionStoreContainer
}) ->
    AtmLaneRunExceptionStoreContainer.


-spec get_lane_run_instant_failure_exception_threshold(record()) -> undefined | float().
get_lane_run_instant_failure_exception_threshold(#atm_workflow_execution_env{
    lane_run_instant_failure_exception_threshold = Threshold
}) ->
    Threshold.


-spec get_task_time_series_store_id(atm_task_execution:id(), record()) ->
    undefined | atm_store:id().
get_task_time_series_store_id(AtmTaskExecutionId, #atm_workflow_execution_env{
    task_time_series_registry = AtmTaskTSRegistry
}) ->
    maps:get(AtmTaskExecutionId, AtmTaskTSRegistry).


-spec get_task_selector(atm_task_execution:id(), record()) ->
    atm_workflow_execution_logger:task_selector().
get_task_selector(AtmTaskExecutionId, #atm_workflow_execution_env{
    task_selector_registry = AtmTaskSelectorRegistry
}) ->
    maps:get(AtmTaskExecutionId, AtmTaskSelectorRegistry).


-spec acquire_auth(record()) -> atm_workflow_execution_auth:record() | no_return().
acquire_auth(#atm_workflow_execution_env{
    space_id = SpaceId,
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    CreatorUserCtx = atm_workflow_execution_session:acquire(AtmWorkflowExecutionId),
    atm_workflow_execution_auth:build(SpaceId, AtmWorkflowExecutionId, CreatorUserCtx).


-spec get_log_level_int(record()) -> audit_log:entry_severity_int().
get_log_level_int(#atm_workflow_execution_env{log_level = LogLevel}) ->
    LogLevel.


-spec build_logger(
    undefined | atm_task_execution:id(),
    atm_workflow_execution_auth:record(),
    record()
) ->
    atm_workflow_execution_logger:record() | no_return().
build_logger(AtmTaskExecutionId, AtmWorkflowExecutionAuth, #atm_workflow_execution_env{
    log_level = LogLevel,
    workflow_audit_log_store_container = AtmWorkflowAuditLogStoreContainer,
    task_audit_logs_registry = AtmTaskAuditLogsRegistry,
    task_selector_registry = AtmTaskSelectorRegistry
}) ->
    atm_workflow_execution_logger:build(
        AtmWorkflowExecutionAuth,
        LogLevel,
        maps:get(AtmTaskExecutionId, AtmTaskAuditLogsRegistry, undefined),
        AtmWorkflowAuditLogStoreContainer,
        AtmTaskSelectorRegistry
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec should_renew_task_selector_registry(
    atm_lane_execution:lane_run_selector(),
    atm_lane_execution:run(),
    record()
) ->
    boolean().
should_renew_task_selector_registry(AtmLaneRunSelector, AtmLaneRun, Record) ->
    case maps:values(Record#atm_workflow_execution_env.task_selector_registry) of
        [{AtmLaneRunSelector, _, _} | _] ->
            % registry is actual
            false;
        _ ->
            AtmLaneRun#atm_lane_execution_run.parallel_boxes /= []
    end.


%% @private
-spec add_task_stores(atm_task_execution:id(), record()) -> record().
add_task_stores(AtmTaskExecutionId, Record0 = #atm_workflow_execution_env{
    workflow_execution_id = AtmWorkflowExecutionId
}) ->
    {ok, #document{
        value = #atm_task_execution{
            workflow_execution_id = AtmWorkflowExecutionId,
            system_audit_log_store_id = AtmSystemAuditLogStoreId,
            time_series_store_id = AtmTaskTSStoreId
        }
    }} = atm_task_execution:get(AtmTaskExecutionId),

    Record1 = add_task_time_series_store_id(AtmTaskExecutionId, AtmTaskTSStoreId, Record0),

    {ok, #atm_store{container = AtmTaskAuditLogStoreContainer}} = atm_store_api:get(
        AtmSystemAuditLogStoreId
    ),
    add_task_audit_log_store_container(AtmTaskExecutionId, AtmTaskAuditLogStoreContainer, Record1).
