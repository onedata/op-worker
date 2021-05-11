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
-export([create/2, get_data_spec/1, get_container_stream/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type init_args() :: undefined | json_utils:json_term().

-record(atm_single_value_container, {
    data_spec :: atm_data_spec:record(),
    value :: undefined | json_utils:json_term()
}).
-type container() :: #atm_single_value_container{}.

-export_type([init_args/0, container/0]).


%%%===================================================================
%%% atm_container callbacks
%%%===================================================================


-spec create(atm_data_spec:record(), init_args()) -> container() | no_return().
create(AtmDataSpec, InitArgs) ->
    InitArgs == undefined orelse atm_data_validator:assert_instance(InitArgs, AtmDataSpec),

    #atm_single_value_container{
        data_spec = AtmDataSpec,
        value = InitArgs
    }.


-spec get_data_spec(container()) -> atm_data_spec:record().
get_data_spec(#atm_single_value_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec get_container_stream(container()) -> atm_single_value_container_stream:stream().
get_container_stream(#atm_single_value_container{value = Value}) ->
    atm_single_value_container_stream:create(Value).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(container(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_single_value_container{
    data_spec = AtmDataSpec,
    value = undefined
}, NestedRecordEncoder) ->
    #{<<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec)};

db_encode(#atm_single_value_container{
    data_spec = AtmDataSpec,
    value = Value
}, NestedRecordEncoder) ->
    #{
        <<"dataSpec">> => NestedRecordEncoder(AtmDataSpec, atm_data_spec),
        <<"value">> => Value
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    container().
db_decode(#{<<"dataSpec">> := AtmDataSpecJson} = AtmContainerJson, NestedRecordDecoder) ->
    #atm_single_value_container{
        data_spec = NestedRecordDecoder(AtmDataSpecJson, atm_data_spec),
        value = maps:get(<<"value">>, AtmContainerJson, undefined)
    }.
