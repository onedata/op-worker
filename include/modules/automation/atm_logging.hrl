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


-define(atm_task_system_log(__LOG_CONTENT, __LOG_SEVERITY, __LOG_LEVEL, __LOGGER),
    case atm_workflow_execution_logger:should_log(__LOGGER, __LOG_LEVEL) of
        true ->
            atm_workflow_execution_logger:task_append_system_log(__LOG_CONTENT, __LOG_SEVERITY, __LOGGER);
        false ->
            ok
    end
).

-define(atm_task_debug(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?DEBUG_AUDIT_LOG_SEVERITY, ?DEBUG_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_debug(__FORMAT, __ARGS, __LOGGER), ?atm_task_debug(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_info(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?INFO_AUDIT_LOG_SEVERITY, ?INFO_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_info(__FORMAT, __ARGS, __LOGGER), ?atm_task_info(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_notice(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?NOTICE_AUDIT_LOG_SEVERITY, ?NOTICE_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_notice(__FORMAT, __ARGS, __LOGGER), ?atm_task_notice(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_warning(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?WARNING_AUDIT_LOG_SEVERITY, ?WARNING_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_warning(__FORMAT, __ARGS, __LOGGER), ?atm_task_warning(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_error(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?ERROR_AUDIT_LOG_SEVERITY, ?ERROR_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_error(__FORMAT, __ARGS, __LOGGER), ?atm_task_error(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_critical(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?CRITICAL_AUDIT_LOG_SEVERITY, ?CRITICAL_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_critical(__FORMAT, __ARGS, __LOGGER), ?atm_task_critical(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_alert(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?ALERT_AUDIT_LOG_SEVERITY, ?ALERT_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_alert(__FORMAT, __ARGS, __LOGGER), ?atm_task_alert(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_task_emergency(__LOG_CONTENT, __LOGGER), ?atm_task_system_log(
    __LOG_CONTENT, ?EMERGENCY_AUDIT_LOG_SEVERITY, ?EMERGENCY_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_task_emergency(__FORMAT, __ARGS, __LOGGER), ?atm_task_emergency(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_system_log(__LOG_CONTENT, __LOG_SEVERITY, __LOG_LEVEL, __LOGGER),
    case atm_workflow_execution_logger:should_log(__LOGGER, __LOG_LEVEL) of
        true ->
            atm_workflow_execution_logger:workflow_append_system_log(__LOG_CONTENT, __LOG_SEVERITY, __LOGGER);
        false ->
            ok
    end
).

-define(atm_workflow_debug(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?DEBUG_AUDIT_LOG_SEVERITY, ?DEBUG_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_debug(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_debug(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_info(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?INFO_AUDIT_LOG_SEVERITY, ?INFO_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_info(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_info(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_notice(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?NOTICE_AUDIT_LOG_SEVERITY, ?NOTICE_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_notice(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_notice(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_warning(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?WARNING_AUDIT_LOG_SEVERITY, ?WARNING_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_warning(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_warning(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_error(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?ERROR_AUDIT_LOG_SEVERITY, ?ERROR_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_error(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_error(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_critical(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?CRITICAL_AUDIT_LOG_SEVERITY, ?CRITICAL_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_critical(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_critical(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_alert(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?ALERT_AUDIT_LOG_SEVERITY, ?ALERT_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_alert(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_alert(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).

-define(atm_workflow_emergency(__LOG_CONTENT, __LOGGER), ?atm_workflow_system_log(
    __LOG_CONTENT, ?EMERGENCY_AUDIT_LOG_SEVERITY, ?EMERGENCY_AUDIT_LOG_SEVERITY_INT, __LOGGER
)).
-define(atm_workflow_emergency(__FORMAT, __ARGS, __LOGGER), ?atm_workflow_emergency(
    str_utils:format_bin(__FORMAT, __ARGS), __LOGGER
)).


-endif.
