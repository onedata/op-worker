%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by automation execution
%%% logging machinery.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_LOGGING_HRL).
-define(ATM_LOGGING_HRL, 1).


-include_lib("cluster_worker/include/audit_log.hrl").


%% atm task level logging related macros

-define(atm_task_system_log(__LOGGER, __LOG_CONTENT, __LOG_SEVERITY, __LOG_LEVEL),
    case atm_workflow_execution_logger:should_log(__LOGGER, __LOG_LEVEL) of
        true ->
            atm_workflow_execution_logger:task_append_system_log(__LOGGER, __LOG_CONTENT, __LOG_SEVERITY);
        false ->
            ok
    end
).

-define(atm_task_debug(__LOGGER, __LOG_CONTENT), ?atm_task_system_log(
    __LOGGER, __LOG_CONTENT, ?DEBUG_AUDIT_LOG_SEVERITY, ?DEBUG_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_task_debug(__LOGGER, __FORMAT, __ARGS), ?atm_task_debug(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_task_info(__LOGGER, __LOG_CONTENT), ?atm_task_system_log(
    __LOGGER, __LOG_CONTENT, ?INFO_AUDIT_LOG_SEVERITY, ?INFO_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_task_info(__LOGGER, __FORMAT, __ARGS), ?atm_task_info(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_task_notice(__LOGGER, __LOG_CONTENT), ?atm_task_system_log(
    __LOGGER, __LOG_CONTENT, ?NOTICE_AUDIT_LOG_SEVERITY, ?NOTICE_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_task_notice(__LOGGER, __FORMAT, __ARGS), ?atm_task_notice(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_task_warning(__LOGGER, __LOG_CONTENT), ?atm_task_system_log(
    __LOGGER, __LOG_CONTENT, ?WARNING_AUDIT_LOG_SEVERITY, ?WARNING_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_task_warning(__LOGGER, __FORMAT, __ARGS), ?atm_task_warning(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_task_error(__LOGGER, __LOG_CONTENT), ?atm_task_system_log(
    __LOGGER, __LOG_CONTENT, ?ERROR_AUDIT_LOG_SEVERITY, ?ERROR_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_task_error(__LOGGER, __FORMAT, __ARGS), ?atm_task_error(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_task_critical(__LOGGER, __LOG_CONTENT), ?atm_task_system_log(
    __LOGGER, __LOG_CONTENT, ?CRITICAL_AUDIT_LOG_SEVERITY, ?CRITICAL_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_task_critical(__LOGGER, __FORMAT, __ARGS), ?atm_task_critical(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_task_alert(__LOGGER, __LOG_CONTENT), ?atm_task_system_log(
    __LOGGER, __LOG_CONTENT, ?ALERT_AUDIT_LOG_SEVERITY, ?ALERT_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_task_alert(__LOGGER, __FORMAT, __ARGS), ?atm_task_alert(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_task_emergency(__LOGGER, __LOG_CONTENT), ?atm_task_system_log(
    __LOGGER, __LOG_CONTENT, ?EMERGENCY_AUDIT_LOG_SEVERITY, ?EMERGENCY_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_task_emergency(__LOGGER, __FORMAT, __ARGS), ?atm_task_emergency(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).


%% Atm workflow level logging related macros

-define(atm_workflow_system_log(__LOGGER, __LOG_CONTENT, __LOG_SEVERITY, __LOG_LEVEL),
    case atm_workflow_execution_logger:should_log(__LOGGER, __LOG_LEVEL) of
        true ->
            atm_workflow_execution_logger:workflow_append_system_log(__LOGGER, __LOG_CONTENT, __LOG_SEVERITY);
        false ->
            ok
    end
).

-define(atm_workflow_debug(__LOGGER, __LOG_CONTENT), ?atm_workflow_system_log(
    __LOGGER, __LOG_CONTENT, ?DEBUG_AUDIT_LOG_SEVERITY, ?DEBUG_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_workflow_debug(__LOGGER, __FORMAT, __ARGS), ?atm_workflow_debug(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_workflow_info(__LOGGER, __LOG_CONTENT), ?atm_workflow_system_log(
    __LOGGER, __LOG_CONTENT, ?INFO_AUDIT_LOG_SEVERITY, ?INFO_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_workflow_info(__LOGGER, __FORMAT, __ARGS), ?atm_workflow_info(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_workflow_notice(__LOGGER, __LOG_CONTENT), ?atm_workflow_system_log(
    __LOGGER, __LOG_CONTENT, ?NOTICE_AUDIT_LOG_SEVERITY, ?NOTICE_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_workflow_notice(__LOGGER, __FORMAT, __ARGS), ?atm_workflow_notice(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_workflow_warning(__LOGGER, __LOG_CONTENT), ?atm_workflow_system_log(
    __LOGGER, __LOG_CONTENT, ?WARNING_AUDIT_LOG_SEVERITY, ?WARNING_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_workflow_warning(__LOGGER, __FORMAT, __ARGS), ?atm_workflow_warning(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_workflow_error(__LOGGER, __LOG_CONTENT), ?atm_workflow_system_log(
    __LOGGER, __LOG_CONTENT, ?ERROR_AUDIT_LOG_SEVERITY, ?ERROR_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_workflow_error(__LOGGER, __FORMAT, __ARGS), ?atm_workflow_error(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_workflow_critical(__LOGGER, __LOG_CONTENT), ?atm_workflow_system_log(
    __LOGGER, __LOG_CONTENT, ?CRITICAL_AUDIT_LOG_SEVERITY, ?CRITICAL_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_workflow_critical(__LOGGER, __FORMAT, __ARGS), ?atm_workflow_critical(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_workflow_alert(__LOGGER, __LOG_CONTENT), ?atm_workflow_system_log(
    __LOGGER, __LOG_CONTENT, ?ALERT_AUDIT_LOG_SEVERITY, ?ALERT_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_workflow_alert(__LOGGER, __FORMAT, __ARGS), ?atm_workflow_alert(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(atm_workflow_emergency(__LOGGER, __LOG_CONTENT), ?atm_workflow_system_log(
    __LOGGER, __LOG_CONTENT, ?EMERGENCY_AUDIT_LOG_SEVERITY, ?EMERGENCY_AUDIT_LOG_SEVERITY_INT
)).
-define(atm_workflow_emergency(__LOGGER, __FORMAT, __ARGS), ?atm_workflow_emergency(
    __LOGGER, str_utils:format_bin(__FORMAT, __ARGS)
)).

-define(fmt_bin(Format, Args), str_utils:format_bin(Format, Args)).

-define(lane_run_selector_json(__ATM_LANE_RUN_SELECTOR),
    atm_lane_execution:lane_run_selector_to_json(__ATM_LANE_RUN_SELECTOR)
).

-record(atm_workflow_log_schema, {
    selector :: undefined | atm_workflow_execution_logger:component_selector(),
    description :: binary(),
    details :: undefined | json_utils:json_map(),
    referenced_tasks :: undefined | [atm_task_execution:id()]
}).

-define(ATM_WORKFLOW_TASK_LOG(__ATM_TASK_EXECUTION_ID, __DESCRIPTION),
    ?ATM_WORKFLOW_TASK_LOG(__ATM_TASK_EXECUTION_ID, __DESCRIPTION, undefined)
).
-define(ATM_WORKFLOW_TASK_LOG(__ATM_TASK_EXECUTION_ID, __DESCRIPTION, __DETAILS), #atm_workflow_log_schema{
    selector = {task, __ATM_TASK_EXECUTION_ID},
    description = __DESCRIPTION,
    details = __DETAILS,
    referenced_tasks = [__ATM_TASK_EXECUTION_ID]
}).


-endif.
