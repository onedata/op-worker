%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_container` functionality for `single_value`
%%% atm_store type.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_single_value_container).
-author("Bartosz Walkowicz").

-behaviour(atm_container).
-behaviour(persistent_record).

-include_lib("ctool/include/errors.hrl").

%% atm_container callbacks
-export([create/2, get_data_spec/1, acquire_iterator/1, apply_operation/4, delete/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type initial_value() :: undefined | atm_api:item().
-type apply_operation_options() :: #{binary() => boolean()}. 

-record(atm_single_value_container, {
    data_spec :: atm_data_spec:record(),
    value :: undefined | atm_api:item()
}).
-type record() :: #atm_single_value_container{}.

-export_type([initial_value/0, apply_operation_options/0, record/0]).


%%%===================================================================
%%% atm_container callbacks
%%%===================================================================


-spec create(atm_data_spec:record(), initial_value()) -> record() | no_return().
create(AtmDataSpec, InitialValue) ->
    InitialValue == undefined orelse atm_data_validator:validate(InitialValue, AtmDataSpec),

    #atm_single_value_container{
        data_spec = AtmDataSpec,
        value = InitialValue
    }.


-spec get_data_spec(record()) -> atm_data_spec:record().
get_data_spec(#atm_single_value_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec acquire_iterator(record()) -> atm_single_value_container_iterator:record().
acquire_iterator(#atm_single_value_container{value = Value}) ->
    atm_single_value_container_iterator:build(Value).


-spec apply_operation(record(), atm_container:operation(), apply_operation_options(), atm_api:item()) ->
    record() | no_return().
apply_operation(#atm_single_value_container{data_spec = AtmDataSpec} = Record, set, _Options, Item) ->
    atm_data_validator:validate(Item, AtmDataSpec),
    Record#atm_single_value_container{value = Item};
apply_operation(_Record, _Operation, _Options, _Item) ->
    throw(?ERROR_NOT_SUPPORTED).


-spec delete(record()) -> ok.
delete(_Record) ->
    ok.


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_single_value_container{
    data_spec = AtmDataSpec,
    value = Value
}, NestedRecordEncoder) ->
    maps_utils:put_if_defined(
        #{<<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec)},
        <<"value">>, Value
    ).


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{<<"dataSpec">> := AtmDataSpecJson} = AtmContainerJson, NestedRecordDecoder) ->
    #atm_single_value_container{
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        value = maps:get(<<"value">>, AtmContainerJson, undefined)
    }.
