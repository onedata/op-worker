%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by automation execution
%%% openfaas machinery.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(ATM_OPENFAAS_HRL).
-define(ATM_OPENFAAS_HRL, 1).


% Record carrying an activity report of an OpenFaaS function
-record(atm_openfaas_activity_report, {
    type :: atm_openfaas_activity_report:type(),
    batch :: [atm_openfaas_activity_report:body()]
}).

% Record carrying a status report of a pod that executes given OpenFaaS function
% (currently the only possible type of OpenFaaS function activity report), used
% to build atm_openfaas_function_pod_status_summary
-record(atm_openfaas_function_pod_status_report, {
    function_id :: atm_openfaas_task_executor:function_id(),
    pod_id :: atm_openfaas_function_pod_status_registry:pod_id(),

    pod_status :: atm_openfaas_function_pod_status_report:pod_status(),
    containers_readiness :: atm_openfaas_function_pod_status_report:containers_readiness(),

    event_timestamp :: atm_openfaas_function_pod_status_report:event_timestamp(),
    event_type :: atm_openfaas_function_pod_status_report:event_type(),
    event_reason :: atm_openfaas_function_pod_status_report:event_reason(),
    event_message :: atm_openfaas_function_pod_status_report:event_message()
}).

% Record holding the summary of status changes for a single pod of an OpenFaaS function
% (single entry in the atm_openfaas_function_pod_status_registry)
-record(atm_openfaas_function_pod_status_summary, {
    current_status :: atm_openfaas_function_pod_status_report:pod_status(),
    current_containers_readiness :: atm_openfaas_function_pod_status_report:containers_readiness(),
    last_status_change_timestamp :: atm_openfaas_function_pod_status_report:event_timestamp(),
    event_log_id :: infinite_log:log_id()
}).

% Record carrying a generic result streamer report
-record(atm_openfaas_result_streamer_report, {
    id :: atm_openfaas_result_streamer_report:id(),
    body :: atm_openfaas_result_streamer_report:body()
}).

% Record carrying a status report of a lambda result streamer of type 'registration'
-record(atm_openfaas_result_streamer_registration_report, {
    workflow_execution_id :: atm_workflow_execution:id(),
    task_execution_id :: atm_task_execution:id(),
    result_streamer_id :: atm_openfaas_result_streamer_registry:result_streamer_id()
}).

% Record carrying a status report of a lambda result streamer of type 'chunk'
-record(atm_openfaas_result_streamer_chunk_report, {
    chunk :: atm_openfaas_result_streamer_chunk_report:chunk()
}).

% Record carrying a status report of a lambda result streamer of type 'invalidData'
-record(atm_openfaas_result_streamer_invalid_data_report, {
    result_name :: automation:name(),
    base_64_encoded_data :: binary()
}).

% Record carrying a status report of a lambda result streamer of type 'deregistration'
-record(atm_openfaas_result_streamer_deregistration_report, {
}).

% Record expressing the push message sent to lambda result streamers to
% acknowledge that a result streamer report has been processed
-record(atm_openfaas_result_streamer_report_ack, {
    id :: atm_openfaas_result_streamer_report:id()
}).

% Record expressing the push message sent to lambda result streamers to
% cue their finalization (flushing of all results and deregistering)
-record(atm_openfaas_result_streamer_finalization_signal, {
}).


-endif.
