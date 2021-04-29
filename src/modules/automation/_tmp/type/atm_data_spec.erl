%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Base model form data classes used in automation machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_data_spec).
-author("Bartosz Walkowicz").

-behaviour(jsonable).

-include("modules/automation/atm_tmp.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_type/1, assert_instance/2]).

%% Jsonable callbacks
-export([version/0, to_json/1, from_json/1]).


-type spec() :: #atm_data_spec{}.
-type type() :: atm_integer_type.


-export_type([spec/0, type/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback assert_instance(Value :: term(), spec()) ->
    true | no_return().


%%%===================================================================
%%% API functions
%%%===================================================================


-spec get_type(spec()) -> type().
get_type(#atm_data_spec{type = Type}) ->
    Type.


-spec assert_instance(term(), spec()) -> true | no_return().
assert_instance(Value, #atm_data_spec{type = Module} = Type) ->
    Module:assert_instance(Value, Type).


%%%===================================================================
%%% Jsonable callbacks
%%%===================================================================


-spec version() -> json_serializer:model_version().
version() ->
    1.


-spec to_json(spec()) -> json_utils:json_map().
to_json(#atm_data_spec{type = Type, value_constraints = ValueConstraints}) ->
    #{
        <<"type">> => atom_to_binary(Type, utf8),
        <<"valueConstraints">> => ValueConstraints
    }.


-spec from_json(json_utils:json_map()) -> spec().
from_json(#{
    <<"type">> := TypeBin,
    <<"valueConstraints">> := ValueConstraints
}) ->
    #atm_data_spec{
        type = binary_to_atom(TypeBin, utf8),
        value_constraints = ValueConstraints
    }.
