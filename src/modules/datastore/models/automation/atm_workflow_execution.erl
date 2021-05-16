%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about automation workflow execution.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_wokflow_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([create/1, get/1, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).


-type id() :: binary().
-type diff() :: datastore_doc:diff(record()).
-type record() :: #atm_workflow_execution{}.
-type doc() :: datastore_doc:doc(record()).

-type state() :: ?WAITING_STATE | ?ONGOING_STATE | ?ENDED_STATE.

-type status() ::
    ?SCHEDULED_STATUS | ?INITIALIZING_STATUS | ?ENQUEUED_STATUS |
    ?ACTIVE_STATUS |
    ?FINISHED_STATUS | ?FAILED_STATUS.

-type timestamp() :: time:seconds().

-export_type([id/0, record/0, doc/0, state/0, status/0, timestamp/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec create(doc()) -> ok | {error, term()}.
create(AtmWorkflowExecutionDoc) ->
  ?extract_ok(datastore_model:create(?CTX, AtmWorkflowExecutionDoc)).


-spec get(id()) -> {ok, record()} | {error, term()}.
get(AtmWorkflowExecutionId) ->
    case datastore_model:get(?CTX, AtmWorkflowExecutionId) of
        {ok, #document{value = AtmWorkflowExecutionRecord}} ->
            {ok, AtmWorkflowExecutionRecord};
        {error, _} = Error ->
            Error
    end.


-spec delete(id()) -> ok | {error, term()}.
delete(AtmWorkflowExecutionId) ->
    datastore_model:delete(?CTX, AtmWorkflowExecutionId).


%%%===================================================================
%%% Datastore callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {schema_id, string},
        {schema_state, atom},
        {name, string},
        {description, string},

        {space_id, string},
        {stores, [string]},
        {lanes, [{record, [
            {schema_id, string},
            {name, string},

            {status, atom},
            {parallel_boxes, [{record, [
                {schema_id, string},
                {name, string},
                {status, atom},
                {tasks, {custom, string, {json_utils, encode, decode}}}
            ]}]},

            {store_iterator_config,
                {custom, string, {persistent_record, encode, decode, atm_store_iterator_config}}
            }
        ]}]},

        {status, atom},
        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer}
    ]}.
