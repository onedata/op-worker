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
-export([build/2, get_name/1, construct_arg/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_task_execution_argument_spec, {
    name :: automation:name(),
    value_builder :: atm_task_argument_value_builder:record(),
    data_spec :: atm_data_spec:record(),
    is_batch :: boolean()
}).
-type record() :: #atm_task_execution_argument_spec{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    atm_lambda_argument_spec:record(),
    undefined | atm_task_schema_argument_mapper:record()
) ->
    record().
build(#atm_lambda_argument_spec{
    name = Name,
    data_spec = AtmDataSpec,
    is_batch = IsBatch,
    default_value = DefaultValue
}, undefined) ->
    #atm_task_execution_argument_spec{
        name = Name,
        value_builder = #atm_task_argument_value_builder{type = const, recipe = DefaultValue},
        data_spec = AtmDataSpec,
        is_batch = IsBatch
    };

build(#atm_lambda_argument_spec{
    name = Name,
    data_spec = AtmDataSpec,
    is_batch = IsBatch
}, #atm_task_schema_argument_mapper{value_builder = ValueBuilder}) ->
    #atm_task_execution_argument_spec{
        name = Name,
        value_builder = ValueBuilder,
        data_spec = AtmDataSpec,
        is_batch = IsBatch
    }.


-spec get_name(record()) -> automation:name().
get_name(#atm_task_execution_argument_spec{name = ArgName}) ->
    ArgName.


-spec construct_arg(atm_job_ctx:record(), record()) ->
    json_utils:json_term() | no_return().
construct_arg(AtmJobCtx, AtmTaskExecutionArgSpec = #atm_task_execution_argument_spec{
    value_builder = ArgValueBuilder
}) ->
    ArgValue = build_value(AtmJobCtx, ArgValueBuilder),
    validate_value(AtmJobCtx, ArgValue, AtmTaskExecutionArgSpec),

    ArgValue.


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
    data_spec = AtmDataSpec,
    is_batch = IsBatch
}, NestedRecordEncoder) ->
    #{
        <<"name">> => Name,
        <<"valueBuilder">> => NestedRecordEncoder(ValueBuilder, atm_task_argument_value_builder),
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec),
        <<"isBatch">> => IsBatch
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"name">> := Name,
    <<"valueBuilder">> := ValueBuilderJson,
    <<"dataSpec">> := AtmDataSpecJson,
    <<"isBatch">> := IsBatch
}, NestedRecordDecoder) ->
    #atm_task_execution_argument_spec{
        name = Name,
        value_builder = NestedRecordDecoder(ValueBuilderJson, atm_task_argument_value_builder),
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        is_batch = IsBatch
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_value(atm_job_ctx:record(), atm_task_argument_value_builder:record()) ->
    json_utils:json_term() | no_return().
build_value(_AtmJobCtx, #atm_task_argument_value_builder{
    type = const,
    recipe = ConstValue
}) ->
    ConstValue;

build_value(AtmJobCtx, #atm_task_argument_value_builder{
    type = iterated_item,
    recipe = undefined
}) ->
    atm_job_ctx:get_item(AtmJobCtx);

build_value(AtmJobCtx, #atm_task_argument_value_builder{
    type = iterated_item,
    recipe = Query
}) ->
    Item = atm_job_ctx:get_item(AtmJobCtx),

    % TODO VFS-7660 fix query in case of array indices
    case json_utils:query(Item, Query) of
        {ok, Value} -> Value;
        error -> throw(?ERROR_ATM_TASK_ARG_MAPPER_ITERATED_ITEM_QUERY_FAILED(Item, Query))
    end;

build_value(AtmJobCtx, #atm_task_argument_value_builder{
    type = onedatafs_credentials
}) ->
    #{
        <<"host">> => oneprovider:get_domain(),
        <<"accessToken">> => atm_job_ctx:get_access_token(AtmJobCtx)
    };

build_value(AtmJobCtx, #atm_task_argument_value_builder{
    type = single_value_store_content,
    recipe = AtmSingleValueStoreSchemaId
}) ->
    AtmSingleValueStoreId = atm_workflow_execution_ctx:get_workflow_store_id(
        AtmSingleValueStoreSchemaId,
        atm_job_ctx:get_workflow_execution_ctx(AtmJobCtx)
    ),
    {ok, AtmStore} = atm_store_api:get(AtmSingleValueStoreId),

    case atm_store_container:get_store_type(AtmStore#atm_store.container) of
        single_value ->
            ok;
        _ ->
            throw(?ERROR_ATM_STORE_TYPE_DISALLOWED(AtmSingleValueStoreSchemaId, [single_value]))
    end,

    AtmWorkflowExecutionAuth = atm_job_ctx:get_workflow_execution_auth(AtmJobCtx),
    BrowseOpts = #{offset => 0, limit => 1},

    case atm_store_api:browse_content(AtmWorkflowExecutionAuth, BrowseOpts, AtmStore) of
        {[], true} ->
            throw(?ERROR_ATM_STORE_EMPTY(AtmSingleValueStoreSchemaId));
        {[{_Index, {error, _} = Error}], true} ->
            throw(Error);
        {[{_Index, {ok, Item}}], true} ->
            Item
    end;

build_value(_AtmJobCtx, #atm_task_argument_value_builder{
    type = ValueBuilderType
}) ->
    % TODO VFS-7660 handle rest of atm_task_argument_value_builder:type()
    throw(?ERROR_ATM_TASK_ARG_MAPPER_UNSUPPORTED_VALUE_BUILDER(ValueBuilderType, [
        const, iterated_item, onedatafs_credentials, single_value_store_content
    ])).


%% @private
-spec validate_value(
    atm_job_ctx:record(),
    json_utils:json_term() | [json_utils:json_term()],
    record()
) ->
    ok | no_return().
validate_value(AtmJobCtx, ArgValue, #atm_task_execution_argument_spec{
    data_spec = AtmDataSpec,
    is_batch = false
}) ->
    AtmWorkflowExecutionAuth = atm_job_ctx:get_workflow_execution_auth(AtmJobCtx),
    atm_value:validate(AtmWorkflowExecutionAuth, ArgValue, AtmDataSpec);

validate_value(AtmJobCtx, ArgsBatch, #atm_task_execution_argument_spec{
    data_spec = AtmDataSpec,
    is_batch = true
}) when is_list(ArgsBatch) ->
    AtmWorkflowExecutionAuth = atm_job_ctx:get_workflow_execution_auth(AtmJobCtx),

    lists:foreach(fun(ArgValue) ->
        atm_value:validate(AtmWorkflowExecutionAuth, ArgValue, AtmDataSpec)
    end, ArgsBatch);

validate_value(_AtmWorkflowExecutionAuth, _ArgsBatch, _AtmTaskExecutionArgSpec) ->
    throw(?ERROR_ATM_BAD_DATA(<<"value">>, <<"not a batch">>)).
