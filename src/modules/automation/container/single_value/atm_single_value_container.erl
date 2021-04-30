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

-include_lib("ctool/include/errors.hrl").

%% atm_container callbacks
-export([
    init/2,
    get_data_spec/1,
    get_container_stream/1,
    to_json/1,
    from_json/1
]).

-type init_args() :: undefined | json_utils:json_term().

-record(atm_single_value_container, {
    data_spec :: atm_data_spec:spec(),
    value :: undefined | json_utils:json_term()
}).
-type container() :: #atm_single_value_container{}.

-export_type([init_args/0, container/0]).


%%%===================================================================
%%% atm_container callbacks
%%%===================================================================


-spec init(atm_data_spec:spec(), init_args()) -> container() | no_return().
init(AtmDataSpec, InitArgs) ->
    InitArgs == undefined orelse atm_data_spec:assert_instance(InitArgs, AtmDataSpec),

    #atm_single_value_container{
        data_spec = AtmDataSpec,
        value = InitArgs
    }.


-spec get_data_spec(container()) -> atm_data_spec:spec().
get_data_spec(#atm_single_value_container{data_spec = AtmDataSpec}) ->
    AtmDataSpec.


-spec get_container_stream(container()) -> atm_single_value_container_stream:stream().
get_container_stream(#atm_single_value_container{value = Value}) ->
    atm_single_value_container_stream:init(Value).


-spec to_json(container()) -> json_utils:json_map().
to_json(#atm_single_value_container{data_spec = AtmDataSpec, value = undefined}) ->
    #{<<"dataSpec">> => atm_data_spec:to_json(AtmDataSpec)};
to_json(#atm_single_value_container{data_spec = AtmDataSpec, value = Value}) ->
    #{
        <<"dataSpec">> => atm_data_spec:to_json(AtmDataSpec),
        <<"value">> => Value
    }.


-spec from_json(json_utils:json_map()) -> container().
from_json(#{<<"dataSpec">> := AtmDataSpecJson} = AtmSingleValueContainerJson) ->
    #atm_single_value_container{
        data_spec = atm_data_spec:from_json(AtmDataSpecJson),
        value = maps:get(<<"value">>, AtmSingleValueContainerJson, undefined)
    }.
