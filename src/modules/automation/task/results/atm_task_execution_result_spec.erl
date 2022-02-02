%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for operating on task execution result spec which is
%%% created based on 'atm_lambda_result_spec' and 'atm_task_schema_result_mapper'.
%%% It is done for performance reasons so as to not reference several more
%%% documents (workflow schema and lambda doc) when executing task for each item.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_result_spec).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([build/2, get_name/1, consume_result/3]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(dispatch_spec, {
    store_schema_id :: automation:id(),
    function :: atm_task_schema_result_mapper:dispatch_function()
}).
-type dispatch_spec() :: #dispatch_spec{}.

-record(atm_task_execution_result_spec, {
    name :: automation:name(),
    data_spec :: atm_data_spec:record(),
    dispatch_specs :: [dispatch_spec()]
}).
-type record() :: #atm_task_execution_result_spec{}.

-export_type([dispatch_spec/0, record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    atm_lambda_result_spec:record(),
    [atm_task_schema_result_mapper:record()]
) ->
    record().
build(AtmLambdaResultSpec, AtmTaskSchemaResultMappers) ->
    #atm_task_execution_result_spec{
        name = AtmLambdaResultSpec#atm_lambda_result_spec.name,
        data_spec = AtmLambdaResultSpec#atm_lambda_result_spec.data_spec,
        dispatch_specs = lists:map(fun build_dispatch_spec/1, AtmTaskSchemaResultMappers)
    }.


-spec get_name(record()) -> automation:name().
get_name(#atm_task_execution_result_spec{name = Name}) ->
    Name.


-spec consume_result(
    atm_workflow_execution_ctx:record(),
    record(),
    json_utils:json_term()
) ->
    ok | no_return().
consume_result(AtmWorkflowExecutionCtx, #atm_task_execution_result_spec{
    dispatch_specs = DispatchSpecs,
    data_spec = AtmDataSpec
}, Result) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),
    atm_value:validate(AtmWorkflowExecutionAuth, Result, AtmDataSpec),

    lists:foreach(fun(#dispatch_spec{store_schema_id = AtmStoreSchemaId} = DispatchSpec) ->
        try
            dispatch_result(AtmWorkflowExecutionCtx, Result, #{}, DispatchSpec)
        catch Type:Reason:Stacktrace ->
            Error = ?atm_examine_error(Type, Reason, Stacktrace),
            throw(?ERROR_ATM_TASK_RESULT_DISPATCH_FAILED(AtmStoreSchemaId, Error))
        end
    end, DispatchSpecs).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_task_execution_result_spec{
    name = Name,
    data_spec = AtmDataSpec,
    dispatch_specs = DispatchSpecs
}, NestedRecordEncoder) ->
    #{
        <<"name">> => Name,
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec),
        <<"dispatchSpecs">> => lists:map(fun dispatch_spec_to_json/1, DispatchSpecs)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"name">> := Name,
    <<"dataSpec">> := AtmDataSpecJson,
    <<"dispatchSpecs">> := DispatchSpecsJson
}, NestedRecordDecoder) ->
    #atm_task_execution_result_spec{
        name = Name,
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        dispatch_specs = lists:map(fun dispatch_spec_from_json/1, DispatchSpecsJson)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_dispatch_spec(atm_task_schema_result_mapper:record()) -> dispatch_spec().
build_dispatch_spec(#atm_task_schema_result_mapper{
    store_schema_id = AtmStoreSchemaId,
    dispatch_function = DispatchFunction
}) ->
    #dispatch_spec{
        store_schema_id = AtmStoreSchemaId,
        function = DispatchFunction
    }.


%% @private
-spec dispatch_spec_to_json(dispatch_spec()) -> json_utils:json_term().
dispatch_spec_to_json(#dispatch_spec{
    store_schema_id = AtmStoreSchemaId,
    function = DispatchFunction
}) ->
    #{
        <<"storeSchemaId">> => AtmStoreSchemaId,
        <<"function">> => atom_to_binary(DispatchFunction, utf8)
    }.


%% @private
-spec dispatch_spec_from_json(json_utils:json_term()) -> dispatch_spec().
dispatch_spec_from_json(#{
    <<"storeSchemaId">> := AtmStoreSchemaId,
    <<"function">> := DispatchFunctionBin
}) ->
    #dispatch_spec{
        store_schema_id = AtmStoreSchemaId,
        function = binary_to_atom(DispatchFunctionBin, utf8)
    }.


%% @private
-spec dispatch_result(
    atm_workflow_execution_ctx:record(),
    json_utils:json_term(),
    atm_store_container:operation_options(),
    dispatch_spec()
) ->
    ok | no_return().
dispatch_result(AtmWorkflowExecutionCtx, Result, Options, #dispatch_spec{
    store_schema_id = ?CURRENT_TASK_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID,
    function = DispatchFun
}) ->
    atm_workflow_execution_logger:task_handle_logs(
        DispatchFun, Result, Options, atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx)
    );

dispatch_result(AtmWorkflowExecutionCtx, Result, Options, #dispatch_spec{
    store_schema_id = ?WORKFLOW_SYSTEM_AUDIT_LOG_STORE_SCHEMA_ID,
    function = DispatchFun
}) ->
    atm_workflow_execution_logger:workflow_handle_logs(
        DispatchFun, Result, Options, atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx)
    );

dispatch_result(AtmWorkflowExecutionCtx, Result, Options, #dispatch_spec{
    store_schema_id = AtmStoreSchemaId,
    function = DispatchFun
}) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),
    AtmStoreId = atm_workflow_execution_ctx:get_global_store_id(
        AtmStoreSchemaId, AtmWorkflowExecutionCtx
    ),
    atm_store_api:apply_operation(
        AtmWorkflowExecutionAuth, DispatchFun, Result, Options, AtmStoreId
    ).
