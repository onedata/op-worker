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
-export([create/1, get/1, update/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).


-type id() :: binary().
-type record() :: #atm_task_execution{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type ctx() :: #atm_task_execution_ctx{}.
-type arg_spec() :: #atm_task_execution_argument_spec{}.
-type status() :: ?PENDING_STATUS | ?ACTIVE_STATUS | ?FINISHED_STATUS | ?FAILED_STATUS.

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([ctx/0, arg_spec/0, status/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(record()) -> {ok, doc()} | {error, term()}.
create(AtmTaskExecutionRecord) ->
    datastore_model:create(?CTX, #document{value = AtmTaskExecutionRecord}).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(AtmTaskExecutionId) ->
    datastore_model:get(?CTX, AtmTaskExecutionId).


-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(AtmTaskExecutionId, Diff) ->
    datastore_model:update(?CTX, AtmTaskExecutionId, Diff).


-spec delete(id()) -> ok | {error, term()}.
delete(AtmStoreId) ->
    datastore_model:delete(?CTX, AtmStoreId).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {workflow_execution_id, string},
        {lane_no, integer},
        {parallel_box_no, integer},

        {schema_id, string},

        {executor, {custom, string, {persistent_record, encode, decode, atm_task_executor}}},
        {argument_specs, [{record, [
            {name, string},
            {value_builder, {custom, string, {persistent_record, encode, decode, atm_task_argument_value_builder}}},
            {data_spec, {custom, string, {persistent_record, encode, decode, atm_data_spec}}},
            {is_batch, boolean}
        ]}]},

        {status, atom},
        {status_changed, boolean},

        {items_in_processing, integer},
        {items_processed, integer},
        {items_failed, integer}
    ]}.
