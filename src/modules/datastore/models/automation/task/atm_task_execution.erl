%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for storing information about automation task execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([create/2, get/1, update/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).


-type id() :: binary().
-type record() :: #atm_task_execution{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type status() ::
    % waiting
    ?RESUMING_STATUS | ?PENDING_STATUS |
    % ongoing
    ?ACTIVE_STATUS | ?STOPPING_STATUS |
    % stopped
    ?FINISHED_STATUS | ?SKIPPED_STATUS |
    ?CANCELLED_STATUS | ?FAILED_STATUS | ?INTERRUPTED_STATUS | ?PAUSED_STATUS.

-type stopping_reason() :: cancel | failure | interrupt | pause.

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([status/0, stopping_reason/0]).


% get ctx via module call to allow mocking in ct tests
-define(CTX(), ?MODULE:get_ctx()).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(id(), record()) -> {ok, doc()} | {error, term()}.
create(AtmTaskExecutionId, AtmTaskExecutionRecord) ->
    datastore_model:create(?CTX(), #document{
        key = AtmTaskExecutionId,
        value = AtmTaskExecutionRecord
    }).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(AtmTaskExecutionId) ->
    datastore_model:get(?CTX(), AtmTaskExecutionId).


-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(AtmTaskExecutionId, Diff1) ->
    Diff2 = fun(#atm_task_execution{status = PrevStatus} = AtmTaskExecution) ->
        case Diff1(AtmTaskExecution#atm_task_execution{status_changed = false}) of
            {ok, #atm_task_execution{status = NewStatus} = NewAtmTaskExecution} ->
                {ok, NewAtmTaskExecution#atm_task_execution{
                    status_changed = NewStatus /= PrevStatus
                }};
            {error, _} = Error ->
                Error
        end
    end,
    datastore_model:update(?CTX(), AtmTaskExecutionId, Diff2).


-spec delete(id()) -> ok | {error, term()}.
delete(AtmStoreId) ->
    datastore_model:delete(?CTX(), AtmStoreId).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    #{model => ?MODULE}.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {workflow_execution_id, string},
        {lane_index, integer},
        {run_num, integer},
        {parallel_box_index, integer},

        {schema_id, string},

        {executor, {custom, string, {persistent_record, to_string, from_string, atm_task_executor}}},
        {lambda_execution_config_entries, [{custom, string, {
            persistent_record, to_string, from_string, atm_lambda_execution_config_entry
        }}]},
        {argument_specs, [{custom, string, {
            persistent_record, to_string, from_string, atm_task_execution_argument_spec
        }}]},
        %% FIX below field name mismatch (see record) on next model upgrade
        {job_result_specs, [{custom, string, {
            persistent_record, to_string, from_string, atm_task_execution_result_spec
        }}]},
        %% FIX below field name mismatch (see record) on next model upgrade
        {supplementary_result_specs, [{custom, string, {
            persistent_record, to_string, from_string, atm_task_execution_result_spec
        }}]},

        {system_audit_log_store_id, string},
        {time_series_store_id, string},

        {status, atom},
        {status_changed, boolean},
        {aborting_reason, atom},
        {stopping_incarnation, integer},

        {items_in_processing, integer},
        {items_processed, integer},
        {items_failed, integer}
    ]}.
