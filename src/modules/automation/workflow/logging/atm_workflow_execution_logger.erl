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

%% API
-export([build/3]).
-export([
    task_handle_logs/4,
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
    workflow_handle_logs/4,
    workflow_debug/2, workflow_debug/3,
    workflow_info/2, workflow_info/3,
    workflow_notice/2, workflow_notice/3,
    workflow_warning/2, workflow_warning/3,
    workflow_alert/2, workflow_alert/3,
    workflow_error/2, workflow_error/3,
    workflow_critical/2, workflow_critical/3,
    workflow_emergency/2, workflow_emergency/3
]).

%% Possible severities:
%% 1. ?LOGGER_DEBUG
%% 2. ?LOGGER_INFO
%% 3. ?LOGGER_NOTICE
%% 4. ?LOGGER_WARNING
%% 5. ?LOGGER_ALERT
%% 6. ?LOGGER_ERROR
%% 7. ?LOGGER_CRITICAL
%% 8. ?LOGGER_EMERGENCY
-type severity() :: binary().

-type entry() :: binary() | json_utils:json_map().
-type log() :: json_utils:json_map().

-record(atm_workflow_execution_logger, {
    atm_workflow_execution_auth :: atm_workflow_execution_auth:record(),
    task_audit_log_store_container :: undefined | atm_store_container:record(),
    workflow_audit_log_store_container :: undefined | atm_store_container:record()
}).
-type record() :: #atm_workflow_execution_logger{}.

-export_type([severity/0, entry/0, log/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    atm_workflow_execution_auth:record(),
    undefined | atm_store_container:record(),
    undefined | atm_store_container:record()
) ->
    record().
build(AtmWorkflowExecutionAuth, AtmTaskAuditLogStoreContainer, AtmWorkflowAuditLogStoreContainer) ->
    #atm_workflow_execution_logger{
        atm_workflow_execution_auth = AtmWorkflowExecutionAuth,
        task_audit_log_store_container = AtmTaskAuditLogStoreContainer,
        workflow_audit_log_store_container = AtmWorkflowAuditLogStoreContainer
    }.


-spec task_handle_logs(
    atm_store_container:operation_type(),
    log() | [log()],
    atm_store_container:operation_options(),
    record()
) ->
    ok.
task_handle_logs(OperationType, AuditLogObject, Options, #atm_workflow_execution_logger{
    atm_workflow_execution_auth = AtmWorkflowExecutionAuth,
    task_audit_log_store_container = AtmTaskAuditLogStoreContainer
}) ->
    handle_logs(
        OperationType, AuditLogObject, Options,
        AtmWorkflowExecutionAuth, AtmTaskAuditLogStoreContainer
    ).


-spec task_debug(entry(), record()) -> ok.
task_debug(Entry, AtmWorkflowExecutionLogger) ->
    task_append_system_log(Entry, ?LOGGER_DEBUG, AtmWorkflowExecutionLogger).


-spec task_debug(string(), [term()], record()) -> ok.
task_debug(Format, Args, AtmWorkflowExecutionLogger) ->
    task_debug(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_info(entry(), record()) -> ok.
task_info(Entry, AtmWorkflowExecutionLogger) ->
    task_append_system_log(Entry, ?LOGGER_INFO, AtmWorkflowExecutionLogger).


-spec task_info(string(), [term()], record()) -> ok.
task_info(Format, Args, AtmWorkflowExecutionLogger) ->
    task_info(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_notice(entry(), record()) -> ok.
task_notice(Entry, AtmWorkflowExecutionLogger) ->
    task_append_system_log(Entry, ?LOGGER_NOTICE, AtmWorkflowExecutionLogger).


-spec task_notice(string(), [term()], record()) -> ok.
task_notice(Format, Args, AtmWorkflowExecutionLogger) ->
    task_notice(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_warning(entry(), record()) -> ok.
task_warning(Entry, AtmWorkflowExecutionLogger) ->
    task_append_system_log(Entry, ?LOGGER_WARNING, AtmWorkflowExecutionLogger).


-spec task_warning(string(), [term()], record()) -> ok.
task_warning(Format, Args, AtmWorkflowExecutionLogger) ->
    task_warning(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_alert(entry(), record()) -> ok.
task_alert(Entry, AtmWorkflowExecutionLogger) ->
    task_append_system_log(Entry, ?LOGGER_ALERT, AtmWorkflowExecutionLogger).


-spec task_alert(string(), [term()], record()) -> ok.
task_alert(Format, Args, AtmWorkflowExecutionLogger) ->
    task_alert(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_error(entry(), record()) -> ok.
task_error(Entry, AtmWorkflowExecutionLogger) ->
    task_append_system_log(Entry, ?LOGGER_ERROR, AtmWorkflowExecutionLogger).


-spec task_error(string(), [term()], record()) -> ok.
task_error(Format, Args, AtmWorkflowExecutionLogger) ->
    task_error(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_critical(entry(), record()) -> ok.
task_critical(Entry, AtmWorkflowExecutionLogger) ->
    task_append_system_log(Entry, ?LOGGER_CRITICAL, AtmWorkflowExecutionLogger).


-spec task_critical(string(), [term()], record()) -> ok.
task_critical(Format, Args, AtmWorkflowExecutionLogger) ->
    task_critical(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec task_emergency(entry(), record()) -> ok.
task_emergency(Entry, AtmWorkflowExecutionLogger) ->
    task_append_system_log(Entry, ?LOGGER_EMERGENCY, AtmWorkflowExecutionLogger).


-spec task_emergency(string(), [term()], record()) -> ok.
task_emergency(Format, Args, AtmWorkflowExecutionLogger) ->
    task_emergency(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_handle_logs(
    atm_store_container:operation_type(),
    log() | [log()],
    atm_store_container:operation_options(),
    record()
) ->
    ok.
workflow_handle_logs(OperationType, AuditLogObject, Options, #atm_workflow_execution_logger{
    atm_workflow_execution_auth = AtmWorkflowExecutionAuth,
    workflow_audit_log_store_container = AtmWorkflowAuditLogStoreContainer
}) ->
    handle_logs(
        OperationType, AuditLogObject, Options,
        AtmWorkflowExecutionAuth, AtmWorkflowAuditLogStoreContainer
    ).


-spec workflow_debug(entry(), record()) -> ok.
workflow_debug(Entry, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(Entry, ?LOGGER_DEBUG, AtmWorkflowExecutionLogger).


-spec workflow_debug(string(), [term()], record()) -> ok.
workflow_debug(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_debug(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_info(entry(), record()) -> ok.
workflow_info(Entry, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(Entry, ?LOGGER_INFO, AtmWorkflowExecutionLogger).


-spec workflow_info(string(), [term()], record()) -> ok.
workflow_info(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_info(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_notice(entry(), record()) -> ok.
workflow_notice(Entry, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(Entry, ?LOGGER_NOTICE, AtmWorkflowExecutionLogger).


-spec workflow_notice(string(), [term()], record()) -> ok.
workflow_notice(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_notice(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_warning(entry(), record()) -> ok.
workflow_warning(Entry, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(Entry, ?LOGGER_WARNING, AtmWorkflowExecutionLogger).


-spec workflow_warning(string(), [term()], record()) -> ok.
workflow_warning(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_warning(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_alert(entry(), record()) -> ok.
workflow_alert(Entry, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(Entry, ?LOGGER_ALERT, AtmWorkflowExecutionLogger).


-spec workflow_alert(string(), [term()], record()) -> ok.
workflow_alert(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_alert(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_error(entry(), record()) -> ok.
workflow_error(Entry, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(Entry, ?LOGGER_ERROR, AtmWorkflowExecutionLogger).


-spec workflow_error(string(), [term()], record()) -> ok.
workflow_error(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_error(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_critical(entry(), record()) -> ok.
workflow_critical(Entry, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(Entry, ?LOGGER_CRITICAL, AtmWorkflowExecutionLogger).


-spec workflow_critical(string(), [term()], record()) -> ok.
workflow_critical(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_critical(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


-spec workflow_emergency(entry(), record()) -> ok.
workflow_emergency(Entry, AtmWorkflowExecutionLogger) ->
    workflow_append_system_log(Entry, ?LOGGER_EMERGENCY, AtmWorkflowExecutionLogger).


-spec workflow_emergency(string(), [term()], record()) -> ok.
workflow_emergency(Format, Args, AtmWorkflowExecutionLogger) ->
    workflow_emergency(str_utils:format_bin(Format, Args), AtmWorkflowExecutionLogger).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec task_append_system_log(entry(), severity(), record()) -> ok.
task_append_system_log(Entry, Severity, AtmWorkflowExecutionLogger) ->
    SystemLog = ensure_system_audit_log_object(Entry, Severity),
    task_handle_logs(append, SystemLog, #{}, AtmWorkflowExecutionLogger).


%% @private
-spec workflow_append_system_log(entry(), severity(), record()) -> ok.
workflow_append_system_log(Entry, Severity, AtmWorkflowExecutionLogger) ->
    SystemLog = ensure_system_audit_log_object(Entry, Severity),
    workflow_handle_logs(append, SystemLog, #{}, AtmWorkflowExecutionLogger).


%% @private
-spec ensure_system_audit_log_object(entry(), severity()) -> log().
ensure_system_audit_log_object(Entry, Severity) when is_map(Entry) ->
    Entry#{<<"severity">> => Severity};
ensure_system_audit_log_object(Entry, Severity) when is_binary(Entry) ->
    #{<<"severity">> => Severity, <<"entry">> => #{<<"description">> => Entry}}.


%% @private
-spec handle_logs(
    atm_store_container:operation_type(),
    log() | [log()],
    atm_store_container:operation_options(),
    atm_workflow_execution_auth:record(),
    undefined | atm_store_container:record()
) ->
    ok.
handle_logs(_OperationType, _Logs, _Options, _AtmWorkflowExecutionAuth, undefined) ->
    ok;
handle_logs(OperationType, Logs, Options, AtmWorkflowExecutionAuth, AtmAuditLogStoreContainer) ->
    Operation = #atm_store_container_operation{
        type = OperationType,
        options = Options,
        argument = Logs,
        workflow_execution_auth = AtmWorkflowExecutionAuth
    },
    atm_audit_log_store_container:apply_operation(AtmAuditLogStoreContainer, Operation),
    ok.
