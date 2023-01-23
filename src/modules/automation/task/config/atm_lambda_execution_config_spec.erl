%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for operating on lambda execution config spec which is
%%% 'atm_parameter_spec' and it's value merged into one record. It is done
%%% for performance reasons so as to not reference several more documents
%%% (workflow schema and lambda doc) when executing task for each item.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lambda_execution_config_spec).
-author("Bartosz Walkowicz").

-behaviour(persistent_record).

-include("modules/automation/atm_execution.hrl").

%% API
-export([build/2]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_lambda_execution_config_spec, {
    name :: automation:name(),
    value :: automation:item(),
    data_spec :: atm_data_spec:record()
}).
-type record() :: #atm_lambda_execution_config_spec{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build(atm_parameter_spec:record(), undefined | json_utils:json_term()) ->
    record().
build(
    #atm_parameter_spec{name = Name, data_spec = AtmDataSpec, default_value = DefaultValue},
    Value
) ->
    #atm_lambda_execution_config_spec{
        name = Name,
        value = utils:ensure_defined(Value, DefaultValue),
        data_spec = AtmDataSpec
    }.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_lambda_execution_config_spec{
    name = Name,
    value = Value,
    data_spec = AtmDataSpec
}, NestedRecordEncoder) ->
    #{
        <<"name">> => Name,
        <<"value">> => Value,
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"name">> := Name,
    <<"value">> := Value,
    <<"dataSpec">> := AtmDataSpecJson
}, NestedRecordDecoder) ->
    #atm_lambda_execution_config_spec{
        name = Name,
        value = Value,
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec)
    }.
