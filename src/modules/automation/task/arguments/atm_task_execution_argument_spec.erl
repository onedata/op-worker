%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for operating on task execution argument spec which is
%%% 'atm_lambda_argument_spec' and 'atm_task_schema_argument_mapper' merged into
%%% one record. It is done for performance reasons so as to not reference
%%% several more documents (workflow schema and lambda doc) when executing
%%% task for each item.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_argument_spec).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([build/2, get_name/1, acquire_arg/3]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_task_execution_argument_spec, {
    name :: automation:name(),
    value_builder :: atm_task_argument_value_builder:record(),
    data_spec :: atm_data_spec:record()
}).
-type record() :: #atm_task_execution_argument_spec{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    atm_parameter_spec:record(),
    undefined | atm_task_schema_argument_mapper:record()
) ->
    record().
build(
    #atm_parameter_spec{name = Name, data_spec = AtmDataSpec, default_value = DefaultValue},
    undefined
) ->
    #atm_task_execution_argument_spec{
        name = Name,
        value_builder = #atm_task_argument_value_builder{type = const, recipe = DefaultValue},
        data_spec = AtmDataSpec
    };

build(
    #atm_parameter_spec{name = Name, data_spec = AtmDataSpec},
    #atm_task_schema_argument_mapper{value_builder = ValueBuilder}
) ->
    #atm_task_execution_argument_spec{
        name = Name,
        value_builder = ValueBuilder,
        data_spec = AtmDataSpec
    }.


-spec get_name(record()) -> automation:name().
get_name(#atm_task_execution_argument_spec{name = ArgName}) ->
    ArgName.


-spec acquire_arg(automation:item(), atm_run_job_batch_ctx:record(), record()) ->
    json_utils:json_term() | no_return().
acquire_arg(Item, AtmRunJobBatchCtx, #atm_task_execution_argument_spec{
    value_builder = ArgValueBuilder,
    data_spec = AtmDataSpec
}) ->
    atm_value:transform_to_data_spec_conformant(
        atm_run_job_batch_ctx:get_workflow_execution_auth(AtmRunJobBatchCtx),
        build_value(Item, AtmRunJobBatchCtx, ArgValueBuilder),
        AtmDataSpec
    ).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_task_execution_argument_spec{
    name = Name,
    value_builder = ValueBuilder,
    data_spec = AtmDataSpec
}, NestedRecordEncoder) ->
    #{
        <<"name">> => Name,
        <<"valueBuilder">> => NestedRecordEncoder(ValueBuilder, atm_task_argument_value_builder),
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"name">> := Name,
    <<"valueBuilder">> := ValueBuilderJson,
    <<"dataSpec">> := AtmDataSpecJson
}, NestedRecordDecoder) ->
    #atm_task_execution_argument_spec{
        name = Name,
        value_builder = NestedRecordDecoder(ValueBuilderJson, atm_task_argument_value_builder),
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_value(
    automation:item(),
    atm_run_job_batch_ctx:record(),
    atm_task_argument_value_builder:record()
) ->
    json_utils:json_term() | no_return().
build_value(Item, AtmRunJobBatchCtx, #atm_task_argument_value_builder{
    type = object,
    recipe = ObjectSpec
}) ->
    %% TODO VFS-7660 add path to errors when constructing nested arguments
    maps:map(fun(_Key, NestedBuilder = #atm_task_argument_value_builder{}) ->
        build_value(Item, AtmRunJobBatchCtx, NestedBuilder)
    end, ObjectSpec);

build_value(_Item, _AtmRunJobBatchCtx, #atm_task_argument_value_builder{
    type = const,
    recipe = ConstValue
}) ->
    ConstValue;

build_value(Item, _AtmRunJobBatchCtx, #atm_task_argument_value_builder{
    type = iterated_item,
    recipe = undefined
}) ->
    Item;

build_value(Item, _AtmRunJobBatchCtx, #atm_task_argument_value_builder{
    type = iterated_item,
    recipe = Query
}) ->
    % TODO VFS-7660 fix query in case of array indices
    case json_utils:query(Item, Query) of
        {ok, Value} -> Value;
        error -> throw(?ERROR_ATM_TASK_ARG_MAPPER_ITERATED_ITEM_QUERY_FAILED(Item, Query))
    end;

build_value(_Item, AtmRunJobBatchCtx, #atm_task_argument_value_builder{
    type = single_value_store_content,
    recipe = AtmSingleValueStoreSchemaId
}) ->
    AtmSingleValueStoreId = atm_workflow_execution_ctx:get_global_store_id(
        AtmSingleValueStoreSchemaId,
        atm_run_job_batch_ctx:get_workflow_execution_ctx(AtmRunJobBatchCtx)
    ),
    {ok, AtmStore} = atm_store_api:get(AtmSingleValueStoreId),

    case atm_store_container:get_store_type(AtmStore#atm_store.container) of
        single_value ->
            ok;
        _ ->
            throw(?ERROR_ATM_STORE_TYPE_DISALLOWED(AtmSingleValueStoreSchemaId, [single_value]))
    end,

    AtmWorkflowExecutionAuth = atm_run_job_batch_ctx:get_workflow_execution_auth(AtmRunJobBatchCtx),
    BrowseOpts = #atm_single_value_store_content_browse_options{},

    case atm_store_api:browse_content(AtmWorkflowExecutionAuth, BrowseOpts, AtmStore) of
        #atm_single_value_store_content_browse_result{item = Error = {error, _}} ->
            throw(Error);
        #atm_single_value_store_content_browse_result{item = {ok, Item}} ->
            Item
    end;

build_value(_Item, _AtmRunJobBatchCtx, #atm_task_argument_value_builder{
    type = ValueBuilderType
}) ->
    % TODO VFS-7660 handle rest of atm_task_argument_value_builder:type()
    throw(?ERROR_ATM_TASK_ARG_MAPPER_UNSUPPORTED_VALUE_BUILDER(ValueBuilderType, [
        const, iterated_item, object, single_value_store_content
    ])).
