%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles appending log entries to automation audit logs.
%%% Depending on logging level (task or workflow) simple passing `undefined`
%%% for missing audit log when building logger will be enough - later incorrect
%%% calls (for logging to missing audit log) will be accepted but nothing will
%%% be done.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_logger).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("cluster_worker/include/audit_log.hrl").

%% API
-export([build/4, should_log/2]).
-export([task_append_system_log/3, task_handle_logs/3]).
-export([workflow_append_system_log/3, workflow_handle_logs/3]).

-type severity() :: binary().  %% see audit_log.hrl

-type log_content() :: binary() | json_utils:json_map().
-type log() :: json_utils:json_map() | audit_log:append_request().

-type component_selector() ::
    workflow_engine |
    {lane, atm_lane_execution:selector()} |
    {lane_run, atm_lane_execution:lane_run_selector()} |
    {task,
        atm_lane_execution:lane_run_selector(),
        ParallelBoxIndex :: pos_integer(),
        TaskIndex :: pos_integer()
    } |
    {task, atm_task_execution:id()}.

-type workflow_log_schema() :: #atm_workflow_log_schema{}.


-record(atm_workflow_execution_logger, {
    atm_workflow_execution_auth :: atm_workflow_execution_auth:record(),
    log_level :: audit_log:entry_severity_int(),
    task_audit_log_store_container :: undefined | atm_store_container:record(),
    workflow_audit_log_store_container :: undefined | atm_store_container:record()
}).
-type record() :: #atm_workflow_execution_logger{}.

-export_type([
    severity/0, log_content/0, log/0,
    component_selector/0, workflow_log_schema/0,
    record/0
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    atm_workflow_execution_auth:record(),
    audit_log:entry_severity_int(),
    undefined | atm_store_container:record(),
    undefined | atm_store_container:record()
) ->
    record().
build(
    AtmWorkflowExecutionAuth,
    LogLevel,
    AtmTaskAuditLogStoreContainer,
    AtmWorkflowAuditLogStoreContainer
) ->
    #atm_workflow_execution_logger{
        atm_workflow_execution_auth = AtmWorkflowExecutionAuth,
        log_level = LogLevel,
        task_audit_log_store_container = AtmTaskAuditLogStoreContainer,
        workflow_audit_log_store_container = AtmWorkflowAuditLogStoreContainer
    }.


-spec should_log(record(), audit_log:entry_severity_int()) -> boolean().
should_log(#atm_workflow_execution_logger{log_level = LogLevel}, LogSeverityInt) ->
    audit_log:should_log(LogLevel, LogSeverityInt).


-spec task_append_system_log(record(), log_content(), severity()) -> ok.
task_append_system_log(Logger, LogContent, Severity) ->
    task_handle_logs(
        Logger,
        #atm_audit_log_store_content_update_options{function = append},
        ensure_system_audit_log_object(LogContent, Severity)
    ).


-spec task_handle_logs(
    record(),
    atm_audit_log_store_content_update_options:record(),
    log() | [log()]
) ->
    ok.
task_handle_logs(#atm_workflow_execution_logger{
    atm_workflow_execution_auth = AtmWorkflowExecutionAuth,
    task_audit_log_store_container = AtmTaskAuditLogStoreContainer
}, UpdateOptions, AuditLogObject) ->
    handle_logs(
        UpdateOptions, AuditLogObject, AtmWorkflowExecutionAuth,
        AtmTaskAuditLogStoreContainer
    ).


-spec workflow_append_system_log(
    record(),
    workflow_log_schema() | log_content(),
    severity()
) ->
    ok.
workflow_append_system_log(Logger, LogSchemaOrContent, Severity) ->
    LogContent = build_workflow_log_content(LogSchemaOrContent),

    workflow_handle_logs(
        Logger,
        #atm_audit_log_store_content_update_options{function = append},
        ensure_system_audit_log_object(LogContent, Severity)
    ).


-spec workflow_handle_logs(
    record(),
    atm_audit_log_store_content_update_options:record(),
    log() | [log()]
) ->
    ok.
workflow_handle_logs(#atm_workflow_execution_logger{
    atm_workflow_execution_auth = AtmWorkflowExecutionAuth,
    workflow_audit_log_store_container = AtmWorkflowAuditLogStoreContainer
}, UpdateOptions, AuditLogObject) ->
    handle_logs(
        UpdateOptions, AuditLogObject, AtmWorkflowExecutionAuth,
        AtmWorkflowAuditLogStoreContainer
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_workflow_log_content(workflow_log_schema() | log_content()) ->
    json_utils:json_map().
build_workflow_log_content(AtmWorkflowLogSchema = #atm_workflow_log_schema{
    details = Details,
    referenced_tasks = ReferencedTasks
}) ->
    Log = maps_utils:put_if_defined(
        #{<<"description">> => format_description(AtmWorkflowLogSchema)},
        <<"details">>,
        Details
    ),
    case ReferencedTasks of
        undefined -> Log;
        _ -> Log#{<<"referencedComponents">> => #{<<"tasks">> => ReferencedTasks}}
    end;

build_workflow_log_content(LogContent) ->
    LogContent.


%% @private
-spec format_description(workflow_log_schema()) -> binary().
format_description(#atm_workflow_log_schema{
    selector = undefined,
    description = Description
}) ->
    Description;

format_description(#atm_workflow_log_schema{
    selector = workflow_engine,
    description = Description
}) ->
    ?fmt_bin("[workflow engine] ~ts", [Description]);

format_description(#atm_workflow_log_schema{
    selector = {lane, LaneSelector},
    description = Description
}) ->
    ?fmt_bin("[~ts] ~ts", [
        format_lane_selector(LaneSelector),
        Description
    ]);

format_description(#atm_workflow_log_schema{
    selector = {lane_run, {LaneSelector, RunSelector}},
    description = Description
}) ->
    ?fmt_bin("[~ts, ~ts] ~ts", [
        format_lane_selector(LaneSelector),
        format_run_selector(RunSelector),
        Description
    ]);

format_description(#atm_workflow_log_schema{
    selector = {task, {LaneSelector, RunSelector}, ParallelBoxIndex, TaskIndex},
    description = Description
}) ->
    ?fmt_bin("[~ts, ~ts, PBox: ~B, Task: ~B] ~ts", [
        format_lane_selector(LaneSelector),
        format_run_selector(RunSelector),
        ParallelBoxIndex,
        TaskIndex,
        Description
    ]);

format_description(#atm_workflow_log_schema{
    selector = {task, AtmTaskExecutionId},
    description = Description
}) ->
    %% TODO VFS-11098 [Lane:2 ... Task: 6] selector
    ?fmt_bin("[Task: ~ts...] ~ts", [
        binary:part(AtmTaskExecutionId, 0, 4),
        Description
    ]).


%% TODO VFS-11098 is it possible to always have index?
%% @private
-spec format_lane_selector(atm_lane_execution:selector()) -> binary().
format_lane_selector(current) -> <<"Lane: ?">>;
format_lane_selector(Index) -> ?fmt_bin("Lane: ~B", [Index]).


%% TODO VFS-11098 is it possible to always have number except for preparing in advance?
%% @private
-spec format_run_selector(atm_lane_execution:run_selector()) -> binary().
format_run_selector(current) -> <<"Run: ?">>;
format_run_selector(Num) -> ?fmt_bin("Run: ~B", [Num]).


%% @private
-spec ensure_system_audit_log_object(log_content(), severity()) -> log().
ensure_system_audit_log_object(LogContent, Severity) when is_map(LogContent) ->
    #audit_log_append_request{
        severity = Severity,
        source = ?SYSTEM_AUDIT_LOG_ENTRY_SOURCE,
        content = LogContent
    };

ensure_system_audit_log_object(LogMsg, Severity) when is_binary(LogMsg) ->
    #audit_log_append_request{
        severity = Severity,
        source = ?SYSTEM_AUDIT_LOG_ENTRY_SOURCE,
        content = #{<<"description">> => LogMsg}
    }.


%% @private
-spec handle_logs(
    atm_audit_log_store_content_update_options:record(),
    log() | [log()],
    atm_workflow_execution_auth:record(),
    undefined | atm_audit_log_store_container:record()
) ->
    ok.
handle_logs(_UpdateOptions, _Logs, _AtmWorkflowExecutionAuth, undefined) ->
    ok;
handle_logs(UpdateOptions, Logs, AtmWorkflowExecutionAuth, AtmAuditLogStoreContainer) ->
    % NOTE: atm_store_api is bypassed for performance reasons. It is possible as
    % audit_log store update does not modify store document itself but only
    % referenced infinite log
    atm_audit_log_store_container:update_content(
        AtmAuditLogStoreContainer, #atm_store_content_update_req{
            workflow_execution_auth = AtmWorkflowExecutionAuth,
            argument = Logs,
            options = UpdateOptions
        }
    ),
    ok.
