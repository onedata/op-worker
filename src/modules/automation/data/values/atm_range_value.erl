%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_value` functionality for `atm_range_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_value).
-author("Bartosz Walkowicz").

-behaviour(atm_value).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_value callbacks
-export([
    validate/3,
    to_store_item/2,
    from_store_item/3,
    describe/3,
    resolve_lambda_parameter/3
]).

%% Full 'initial_content' format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying concrete binaries ('initial_content' must be
%% proper json object which implies binaries as keys). Instead it is shown below:
%%
%% #{
%%      <<"end">> := integer(),
%%      <<"start">> => integer(),  % default `0`
%%      <<"step">> => integer()    % default `1`
%% }
-type range_json() :: #{binary() => integer()}.

%% Full 'initial_content' format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying lists of predefined length. Instead it is shown below:
%%
%% [Start :: integer(), End :: integer(), Step :: integer()]
-type range() :: [integer()].

-export_type([range_json/0, range/0]).


%%%===================================================================
%%% atm_value callbacks
%%%===================================================================


-spec validate(
    atm_workflow_execution_auth:record(),
    range_json(),
    atm_range_data_spec:record()
) ->
    ok | no_return().
validate(_AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    Range = to_store_item(Value, AtmDataSpec),

    try
        assert_valid_step_direction(Range)
    catch throw:{unverified_constraints, UnverifiedConstraints} ->
        throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
            Value, atm_range_type, UnverifiedConstraints
        ))
    end.


-spec to_store_item(range_json(), atm_range_data_spec:record()) ->
    range().
to_store_item(Value = #{<<"end">> := End}, _AtmDataSpec) ->
    Start = maps:get(<<"start">>, Value, 0),
    Step = maps:get(<<"step">>, Value, 1),

    [Start, End, Step].


-spec from_store_item(
    atm_workflow_execution_auth:record(),
    range(),
    atm_range_data_spec:record()
) ->
    {ok, range_json()}.
from_store_item(_AtmWorkflowExecutionAuth, [Start, End, Step], _AtmDataSpec) ->
    {ok, #{
        <<"start">> => Start,
        <<"end">> => End,
        <<"step">> => Step
    }}.


-spec describe(
    atm_workflow_execution_auth:record(),
    range(),
    atm_range_data_spec:record()
) ->
    {ok, automation:item()}.
describe(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    from_store_item(AtmWorkflowExecutionAuth, Value, AtmDataSpec).


-spec resolve_lambda_parameter(
    atm_workflow_execution_auth:record(),
    range_json(),
    atm_range_data_spec:record()
) ->
    automation:item().
resolve_lambda_parameter(AtmWorkflowExecutionAuth, Value, AtmParameterDataSpec) ->
    validate(AtmWorkflowExecutionAuth, Value, AtmParameterDataSpec),
    Value.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec assert_valid_step_direction(range()) -> ok | no_return().
assert_valid_step_direction([Start, End, Step]) when Start =< End, Step > 0 ->
    ok;
assert_valid_step_direction([Start, End, Step]) when Start >= End, Step < 0 ->
    ok;
assert_valid_step_direction(_) ->
    throw({unverified_constraints, <<"invalid step direction">>}).
