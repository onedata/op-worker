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
-export([build/4]).
-export([
    task_handle_logs/3,
    task_debug/2, task_debug/3,
    task_info/2, task_info/3,
    task_notice/2, task_notice/3,
    task_warning/2, task_warning/3,
    task_alert/2, task_alert/3,
    task_error/2, task_error/3,
    task_critical/2, task_critical/3,
    task_emergency/2, task_emergency/3
]).
-export([
    workflow_handle_logs/3,
    workflow_debug/2, workflow_debug/3,
    workflow_info/2, workflow_info/3,
    workflow_notice/2, workflow_notice/3,
    workflow_warning/2, workflow_warning/3,
    workflow_alert/2, workflow_alert/3,
    workflow_error/2, workflow_error/3,
    workflow_critical/2, workflow_critical/3,
    workflow_emergency/2, workflow_emergency/3
]).

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


-spec task_debug(log_content(), record()) -> ok.
task_debug(LogContent, AtmWorkflowExecutionLogger) ->
    task_append_system_log(LogContent, ?LOGGER_DEBUG, AtmWorkflowExecutionLogger).


-spec task_debug(string(), [term()], record()) -> ok.
task_debug(Format, Args, AtmWorkflowExecutionLogger) ->
    task_debug(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_info(log_content(), record()) -> ok.
task_info(LogContent, AtmWorkflowExecutionLogger) ->
    task_append_system_log(LogContent, ?LOGGER_INFO, AtmWorkflowExecutionLogger).


-spec task_info(string(), [term()], record()) -> ok.
task_info(Format, Args, AtmWorkflowExecutionLogger) ->
    task_info(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_notice(log_content(), record()) -> ok.
task_notice(LogContent, AtmWorkflowExecutionLogger) ->
    task_append_system_log(LogContent, ?LOGGER_NOTICE, AtmWorkflowExecutionLogger).


-spec task_notice(string(), [term()], record()) -> ok.
task_notice(Format, Args, AtmWorkflowExecutionLogger) ->
    task_notice(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_warning(log_content(), record()) -> ok.
task_warning(LogContent, AtmWorkflowExecutionLogger) ->
    task_append_system_log(LogContent, ?LOGGER_WARNING, AtmWorkflowExecutionLogger).


-spec task_warning(string(), [term()], record()) -> ok.
task_warning(Format, Args, AtmWorkflowExecutionLogger) ->
    task_warning(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_alert(log_content(), record()) -> ok.
task_alert(LogContent, AtmWorkflowExecutionLogger) ->
    task_append_system_log(LogContent, ?LOGGER_ALERT, AtmWorkflowExecutionLogger).


-spec task_alert(string(), [term()], record()) -> ok.
task_alert(Format, Args, AtmWorkflowExecutionLogger) ->
    task_alert(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_error(log_content(), record()) -> ok.
task_error(LogContent, AtmWorkflowExecutionLogger) ->
    task_append_system_log(LogContent, ?LOGGER_ERROR, AtmWorkflowExecutionLogger).


-spec task_error(string(), [term()], record()) -> ok.
task_error(Format, Args, AtmWorkflowExecutionLogger) ->
    task_error(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_critical(log_content(), record()) -> ok.
task_critical(LogContent, AtmWorkflowExecutionLogger) ->
    task_append_system_log(LogContent, ?LOGGER_CRITICAL, AtmWorkflowExecutionLogger).


-spec task_critical(string(), [term()], record()) -> ok.
task_critical(Format, Args, AtmWorkflowExecutionLogger) ->
    task_critical(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_emergency(log_content(), record()) -> ok.
task_emergency(LogContent, AtmWorkflowExecutionLogger) ->
    task_append_system_log(LogContent, ?LOGGER_EMERGENCY, AtmWorkflowExecutionLogger).


-spec task_emergency(string(), [term()], record()) -> ok.
task_emergency(Format, Args, AtmWorkflowExecutionLogger) ->
    task_emergency(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


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


-spec workflow_debug(log_content(), record()) -> ok.
workflow_debug(LogContent, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(LogContent, ?LOGGER_DEBUG, AtmWorkflowExecutionLogger).


-spec workflow_debug(string(), [term()], record()) -> ok.
workflow_debug(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_debug(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_info(log_content(), record()) -> ok.
workflow_info(LogContent, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(LogContent, ?LOGGER_INFO, AtmWorkflowExecutionLogger).


-spec workflow_info(string(), [term()], record()) -> ok.
workflow_info(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_info(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_notice(log_content(), record()) -> ok.
workflow_notice(LogContent, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(LogContent, ?LOGGER_NOTICE, AtmWorkflowExecutionLogger).


-spec workflow_notice(string(), [term()], record()) -> ok.
workflow_notice(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_notice(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_warning(log_content(), record()) -> ok.
workflow_warning(LogContent, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(LogContent, ?LOGGER_WARNING, AtmWorkflowExecutionLogger).


-spec workflow_warning(string(), [term()], record()) -> ok.
workflow_warning(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_warning(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_alert(log_content(), record()) -> ok.
workflow_alert(LogContent, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(LogContent, ?LOGGER_ALERT, AtmWorkflowExecutionLogger).


-spec workflow_alert(string(), [term()], record()) -> ok.
workflow_alert(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_alert(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_error(log_content(), record()) -> ok.
workflow_error(LogContent, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(LogContent, ?LOGGER_ERROR, AtmWorkflowExecutionLogger).


-spec workflow_error(string(), [term()], record()) -> ok.
workflow_error(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_error(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_critical(log_content(), record()) -> ok.
workflow_critical(LogContent, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(LogContent, ?LOGGER_CRITICAL, AtmWorkflowExecutionLogger).


-spec workflow_critical(string(), [term()], record()) -> ok.
workflow_critical(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_critical(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_emergency(log_content(), record()) -> ok.
workflow_emergency(LogContent, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(LogContent, ?LOGGER_EMERGENCY, AtmWorkflowExecutionLogger).


-spec workflow_emergency(string(), [term()], record()) -> ok.
workflow_emergency(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_emergency(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec task_append_system_log(log_content(), severity(), record()) -> ok.
task_append_system_log(LogContent, Severity, AtmWorkflowExecutionLogger) ->
    task_handle_logs(
        #atm_audit_log_store_content_update_options{function = append},
        ensure_system_audit_log_object(LogContent, Severity),
        AtmWorkflowExecutionLogger
    ).


%% @private
-spec workflow_append_system_log(log_content(), severity(), record()) -> ok.
workflow_append_system_log(LogContent, Severity, AtmWorkflowExecutionLogger) ->
    workflow_handle_logs(
        #atm_audit_log_store_content_update_options{function = append},
        ensure_system_audit_log_object(LogContent, Severity),
        AtmWorkflowExecutionLogger
    ).


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
