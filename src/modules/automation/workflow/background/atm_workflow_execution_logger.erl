%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_logger).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([build/3]).
-export([
    task_debug/2, task_info/2, task_notice/2,
    task_warning/2, task_alert/2,
    task_error/2, task_critical/2, task_emergency/2
]).

-type severity() :: binary(). %% TODO enum

-record(atm_workflow_execution_logger, {
    atm_workflow_execution_auth :: atm_workflow_execution_auth:record(),
    task_audit_log_store_container :: undefined | atm_store_container:record(),
    workflow_audit_log_store_container :: undefined | atm_store_container:record()
}).
-type record() :: #atm_workflow_execution_logger{}.

-export_type([record/0]).


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


task_debug(Msg, AtmWorkflowExecutionLogger) ->
    task_append_log(Msg, <<"debug">>, AtmWorkflowExecutionLogger).


task_info(Msg, AtmWorkflowExecutionLogger) ->
    task_append_log(Msg, <<"info">>, AtmWorkflowExecutionLogger).


task_notice(Msg, AtmWorkflowExecutionLogger) ->
    task_append_log(Msg, <<"debug">>, AtmWorkflowExecutionLogger).


task_warning(Msg, AtmWorkflowExecutionLogger) ->
    task_append_log(Msg, <<"notice">>, AtmWorkflowExecutionLogger).


task_alert(Msg, AtmWorkflowExecutionLogger) ->
    task_append_log(Msg, <<"alert">>, AtmWorkflowExecutionLogger).


task_error(Msg, AtmWorkflowExecutionLogger) ->
    task_append_log(Msg, <<"error">>, AtmWorkflowExecutionLogger).


task_critical(Msg, AtmWorkflowExecutionLogger) ->
    task_append_log(Msg, <<"critical">>, AtmWorkflowExecutionLogger).


task_emergency(Msg, AtmWorkflowExecutionLogger) ->
    task_append_log(Msg, <<"emergency">>, AtmWorkflowExecutionLogger).


%%%===================================================================
%%% Internal functions
%%%===================================================================


task_append_log(Msg, Severity, #atm_workflow_execution_logger{
    atm_workflow_execution_auth = AtmWorkflowExecutionAuth,
    task_audit_log_store_container = AtmTaskAuditLogStoreContainer
}) ->
    append_log(Msg, Severity, AtmWorkflowExecutionAuth, AtmTaskAuditLogStoreContainer).


append_log(_Msg, _Severity, _AtmWorkflowExecutionAuth, undefined) ->
    ok;
append_log(Msg, Severity, AtmWorkflowExecutionAuth, AtmAuditLogStoreContainer) ->
    AtmAuditLogStoreContainerOperation = #atm_store_container_operation{
        type = append,
        options = #{},
        argument = ensure_system_audit_log_object(Msg, Severity),
        workflow_execution_auth = AtmWorkflowExecutionAuth
    },
    % TODO write in audit log store container that apply_operation should never change record or this code will break
    catch atm_audit_log_store_container:apply_operation(
        AtmAuditLogStoreContainer, AtmAuditLogStoreContainerOperation
    ),

    ok.


%% @private
-spec ensure_system_audit_log_object(term(), severity()) -> json_utils:json_map().
ensure_system_audit_log_object(Msg, Severity) when is_map(Msg) ->
    Msg#{<<"severity">> => Severity};
ensure_system_audit_log_object(Msg, Severity) when is_binary(Msg) ->
    #{<<"severity">> => Severity, <<"entry">> => Msg}.
