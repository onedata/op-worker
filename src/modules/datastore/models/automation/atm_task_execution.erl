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

-include("modules/automation/atm_task_execution.hrl").
-include("modules/automation/atm_wokflow_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([create/1, get/1, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).


-type id() :: binary().
-type record() :: #atm_task_execution{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-type ctx() :: #atm_task_execution_ctx{}.

%% Full 'arg_input_spec' format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying concrete binaries. Instead it is
%% shown below:
%%
%% #{
%%      <<"inputRefType">> := <<"const">> | <<"store">> | <<"item">>,
%%      <<"inputRef">> => case <<"inputRefType">> of
%%          <<"const">> -> ConstValue :: json_utils:json_term();
%%          <<"store">> -> StoreSchemaId :: binary();
%%          <<"item">> -> json_utils:query() % optional, if not specified entire
%%                                           % item is substituted
%%      end
%% }
-type arg_input_spec() :: json_utils:json_map().
-type arg_spec() :: #atm_task_execution_argument_spec{}.

-type status() :: ?PENDING_STATUS | ?ACTIVE_STATUS | ?FINISHED_STATUS | ?FAILED_STATUS.

-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([ctx/0, arg_input_spec/0, arg_spec/0, status/0]).


-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(record()) -> {ok, id()} | {error, term()}.
create(AtmTaskExecutionRecord) ->
    Doc = #document{value = AtmTaskExecutionRecord},
    ?extract_key(datastore_model:create(?CTX, Doc)).


-spec get(id()) -> {ok, record()} | {error, term()}.
get(AtmTaskExecutionId) ->
    case datastore_model:get(?CTX, AtmTaskExecutionId) of
        {ok, #document{value = AtmTaskExecutionRecord}} ->
            {ok, AtmTaskExecutionRecord};
        {error, _} = Error ->
            Error
    end.


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
        {schema_id, string},
        {name, string},
        {lambda_id, string},

        {workflow_execution_id, string},
        {lane_no, integer},
        {parallel_box_no, integer},

        {executor, {custom, string, {persistent_record, encode, decode, atm_task_executor}}},
        {argument_specs, [{record, [
            {name, string},
            {input_spec, {custom, string, {json_utils, encode, decode}}},
            {data_spec, {custom, string, {persistent_record, encode, decode, atm_data_spec}}},
            {is_batch, boolean}
        ]}]},

        {status, atom},
        {handled_items, integer},
        {processed_items, integer},
        {failed_items, integer}
    ]}.
