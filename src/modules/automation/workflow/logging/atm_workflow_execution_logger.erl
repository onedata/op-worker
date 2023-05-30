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

-record(atm_workflow_execution_logger, {
    atm_workflow_execution_auth :: atm_workflow_execution_auth:record(),
    logging_level :: atm_audit_log_store_container:level(),
    task_audit_log_store_container :: undefined | atm_store_container:record(),
    workflow_audit_log_store_container :: undefined | atm_store_container:record()
}).
-type record() :: #atm_workflow_execution_logger{}.

-export_type([severity/0, log_content/0, log/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    atm_workflow_execution_auth:record(),
    atm_audit_log_store_container:level(),
    undefined | atm_store_container:record(),
    undefined | atm_store_container:record()
) ->
    record().
build(
    AtmWorkflowExecutionAuth,
    LoggingLevel,
    AtmTaskAuditLogStoreContainer,
    AtmWorkflowAuditLogStoreContainer
) ->
    #atm_workflow_execution_logger{
        atm_workflow_execution_auth = AtmWorkflowExecutionAuth,
        logging_level = LoggingLevel,
        task_audit_log_store_container = AtmTaskAuditLogStoreContainer,
        workflow_audit_log_store_container = AtmWorkflowAuditLogStoreContainer
    }.


-spec should_log(record(), atm_audit_log_store_container:level()) -> boolean().
should_log(#atm_workflow_execution_logger{logging_level = LoggingLevel}, LogLevel) ->
    LogLevel =< LoggingLevel.


-spec task_append_system_log(log_content(), severity(), record()) -> ok.
task_append_system_log(LogContent, Severity, AtmWorkflowExecutionLogger) ->
    task_handle_logs(
        #atm_audit_log_store_content_update_options{function = append},
        ensure_system_audit_log_object(LogContent, Severity),
        AtmWorkflowExecutionLogger
    ).


-spec task_handle_logs(
    atm_audit_log_store_content_update_options:record(),
    log() | [log()],
    record()
) ->
    ok.
task_handle_logs(UpdateOptions, AuditLogObject, #atm_workflow_execution_logger{
    atm_workflow_execution_auth = AtmWorkflowExecutionAuth,
    task_audit_log_store_container = AtmTaskAuditLogStoreContainer
}) ->
    handle_logs(
        UpdateOptions, AuditLogObject, AtmWorkflowExecutionAuth,
        AtmTaskAuditLogStoreContainer
    ).


-spec workflow_append_system_log(log_content(), severity(), record()) -> ok.
workflow_append_system_log(LogContent, Severity, AtmWorkflowExecutionLogger) ->
    workflow_handle_logs(
        #atm_audit_log_store_content_update_options{function = append},
        ensure_system_audit_log_object(LogContent, Severity),
        AtmWorkflowExecutionLogger
    ).


-spec workflow_handle_logs(
    atm_audit_log_store_content_update_options:record(),
    log() | [log()],
    record()
) ->
    ok.
workflow_handle_logs(UpdateOptions, AuditLogObject, #atm_workflow_execution_logger{
    atm_workflow_execution_auth = AtmWorkflowExecutionAuth,
    workflow_audit_log_store_container = AtmWorkflowAuditLogStoreContainer
}) ->
    handle_logs(
        UpdateOptions, AuditLogObject, AtmWorkflowExecutionAuth,
        AtmWorkflowAuditLogStoreContainer
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


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
