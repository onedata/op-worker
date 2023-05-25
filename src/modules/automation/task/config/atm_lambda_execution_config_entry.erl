%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for operating on lambda execution config entry which is
%%% 'atm_parameter_spec' and its value merged into one record. It is done
%%% for performance reasons so as to not reference several more documents
%%% (workflow schema and lambda doc) when executing task for each item.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lambda_execution_config_entry).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([build/3, get_name/1, acquire_value/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_lambda_execution_config_entry, {
    name :: automation:name(),
    compressed_value :: automation:item(),
    data_spec :: atm_data_spec:record()
}).
-type record() :: #atm_lambda_execution_config_entry{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(
    atm_workflow_execution_auth:record(),
    atm_parameter_spec:record(),
    undefined | json_utils:json_term()
) ->
    record() | no_return().
build(
    AtmWorkflowExecutionAuth,
    #atm_parameter_spec{name = Name, data_spec = AtmDataSpec, default_value = DefaultValue},
    UndefinedOrValue
) ->
    Value = utils:ensure_defined(UndefinedOrValue, DefaultValue),
    atm_value:validate(AtmWorkflowExecutionAuth, Value, AtmDataSpec),

    #atm_lambda_execution_config_entry{
        name = Name,
        compressed_value = atm_value:to_store_item(Value, AtmDataSpec),
        data_spec = AtmDataSpec
    }.


-spec get_name(record()) -> automation:name().
get_name(#atm_lambda_execution_config_entry{name = Name}) -> Name.


-spec acquire_value(atm_run_job_batch_ctx:record(), record()) ->
    json_utils:json_term() | no_return().
acquire_value(AtmRunJobBatchCtx, #atm_lambda_execution_config_entry{
    compressed_value = CompressedValue,
    data_spec = AtmDataSpec
}) ->
    AtmWorkflowExecutionAuth = atm_run_job_batch_ctx:get_workflow_execution_auth(AtmRunJobBatchCtx),
    Value = ?check(atm_value:from_store_item(AtmWorkflowExecutionAuth, CompressedValue, AtmDataSpec)),
    atm_value:resolve_lambda_parameter(AtmWorkflowExecutionAuth, Value, AtmDataSpec).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_lambda_execution_config_entry{
    name = Name,
    compressed_value = Value,
    data_spec = AtmDataSpec
}, NestedRecordEncoder) ->
    #{
        <<"name">> => Name,
        <<"compressedValue">> => Value,
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"name">> := Name,
    <<"compressedValue">> := Value,
    <<"dataSpec">> := AtmDataSpecJson
}, NestedRecordDecoder) ->
    #atm_lambda_execution_config_entry{
        name = Name,
        compressed_value = Value,
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec)
    }.
