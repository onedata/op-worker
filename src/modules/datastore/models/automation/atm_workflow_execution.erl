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

-include("modules/automation/atm_execution.hrl").

%% API
-export([create/1, get/1, update/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).


-type id() :: binary().
-type diff() :: datastore_doc:diff(record()).
-type record() :: #atm_workflow_execution{}.
-type doc() :: datastore_doc:doc(record()).

-type state() :: ?WAITING_STATE | ?ONGOING_STATE | ?ENDED_STATE.

-type status() ::
    ?SCHEDULED_STATUS | ?PREPARING_STATUS | ?ENQUEUED_STATUS |
    ?ACTIVE_STATUS |
    ?FINISHED_STATUS | ?FAILED_STATUS.

-type timestamp() :: time:seconds().

-export_type([id/0, record/0, doc/0, state/0, status/0, timestamp/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec create(doc()) -> {ok, doc()} | {error, term()}.
create(AtmWorkflowExecutionDoc) ->
    datastore_model:create(?CTX, AtmWorkflowExecutionDoc).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(AtmWorkflowExecutionId) ->
    datastore_model:get(?CTX, AtmWorkflowExecutionId).


-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(AtmWorkflowExecutionId, Diff) ->
    datastore_model:update(?CTX, AtmWorkflowExecutionId, Diff).


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
        {space_id, string},
        {schema_snapshot_id, string},

        {store_registry, #{string => string}},

        {lanes, [{record, [
            {schema_id, string},
            {status, atom},
            {parallel_boxes, [{record, [
                {schema_id, string},
                {status, atom},
                {tasks, #{string => atom}}
            ]}]}
        ]}]},

        {status, atom},
        {status_changed, boolean},

        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer}
    ]}.
