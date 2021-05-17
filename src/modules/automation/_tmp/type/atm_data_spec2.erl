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
-module(atm_data_spec2).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_tmp.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_type/1, get_value_constraints/1]).
-export([to_json/1, from_json/1]).


-type record() :: #atm_data_spec2{}.

-export_type([record/0]).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec get_type(record()) -> atm_data_type2:type().
get_type(#atm_data_spec2{type = Type}) ->
    Type.


-spec get_value_constraints(record()) -> map().
get_value_constraints(#atm_data_spec2{value_constraints = ValueConstraints}) ->
    ValueConstraints.


-spec to_json(record()) -> json_utils:json_map().
to_json(#atm_data_spec2{type = Type, value_constraints = ValueConstraints}) ->
    #{
        <<"type">> => atom_to_binary(Type, utf8),
        <<"valueConstraints">> => ValueConstraints
    }.


-spec from_json(json_utils:json_map()) -> record().
from_json(#{
    <<"type">> := TypeBin,
    <<"valueConstraints">> := ValueConstraints
}) ->
    #atm_data_spec2{
        type = binary_to_atom(TypeBin, utf8),
        value_constraints = ValueConstraints
    }.
