%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator` and `atm_data_compressor`
%%% functionality for `atm_range_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_value).
-author("Bartosz Walkowicz").

-behaviour(atm_data_validator).
-behaviour(atm_data_compressor).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_data_validator callbacks
-export([assert_meets_constraints/3, resolve/3]).

%% atm_data_compressor callbacks
-export([compress/2, expand/3]).

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
%%% atm_data_validator callbacks
%%%===================================================================


-spec assert_meets_constraints(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_range_data_spec:record()
) ->
    ok | no_return().
assert_meets_constraints(_AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    Range = compress(Value, AtmDataSpec),

    try
        assert_valid_step_direction(Range)
    catch throw:{unverified_constraints, UnverifiedConstraints} ->
        throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
            Value, atm_range_type, UnverifiedConstraints
        ))
    end.


-spec resolve(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_range_data_spec:record()
) ->
    atm_value:expanded() | no_return().
resolve(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    assert_meets_constraints(AtmWorkflowExecutionAuth, Value, AtmDataSpec),
    Value.


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(range_json(), atm_range_data_spec:record()) -> range().
compress(Value = #{<<"end">> := End}, _AtmDataSpec) ->
    Start = maps:get(<<"start">>, Value, 0),
    Step = maps:get(<<"step">>, Value, 1),

    [Start, End, Step].


-spec expand(atm_workflow_execution_auth:record(), range(), atm_range_data_spec:record()) ->
    {ok, range_json()}.
expand(_AtmWorkflowExecutionAuth, [Start, End, Step], _AtmDataSpec) ->
    {ok, #{
        <<"start">> => Start,
        <<"end">> => End,
        <<"step">> => Step
    }}.


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
