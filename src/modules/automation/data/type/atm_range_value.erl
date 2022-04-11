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
-export([assert_meets_constraints/3]).

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
    atm_data_type:value_constraints()
) ->
    ok | no_return().
assert_meets_constraints(AtmWorkflowExecutionAuth, Value, ValueConstraints) ->
    Range = compress(Value, ValueConstraints),

    try
        check_implicit_constraints(AtmWorkflowExecutionAuth, Range)
    catch
        throw:{unverified_constraints, UnverifiedConstraints} ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                Value, atm_range_type, UnverifiedConstraints
            ));
        throw:Error ->
            throw(Error);
        _:_ ->
            throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_range_type))
    end.


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(range_json(), atm_data_type:value_constraints()) -> range().
compress(Value = #{<<"end">> := End}, _ValueConstraints) ->
    Start = maps:get(<<"start">>, Value, 0),
    Step = maps:get(<<"step">>, Value, 1),

    [Start, End, Step].


-spec expand(atm_workflow_execution_auth:record(), range(), atm_data_type:value_constraints()) ->
    {ok, range_json()}.
expand(_AtmWorkflowExecutionAuth, [Start, End, Step], _ValueConstraints) ->
    {ok, #{
        <<"start">> => Start,
        <<"end">> => End,
        <<"step">> => Step
    }}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_implicit_constraints(atm_workflow_execution_auth:record(), range()) ->
    ok | no_return().
check_implicit_constraints(AtmWorkflowExecutionAuth, Range) ->
    assert_valid_field_types(AtmWorkflowExecutionAuth, Range),
    assert_valid_step_direction(Range).


%% @private
-spec assert_valid_field_types(atm_workflow_execution_auth:record(), range()) ->
    ok | no_return().
assert_valid_field_types(AtmWorkflowExecutionAuth, [Start, End, Step]) ->
    FieldDataSpec = #atm_data_spec{type = atm_integer_type},

    lists:foreach(fun({FieldPath, ArgValue}) ->
        try
            atm_value:validate(AtmWorkflowExecutionAuth, ArgValue, FieldDataSpec)
        catch Type:Reason:Stacktrace ->
            throw({unverified_constraints, #{
                FieldPath => errors:to_json(?atm_examine_error(Type, Reason, Stacktrace))
            }})
        end
    end, [
        {<<".start">>, Start},
        {<<".end">>, End},
        {<<".step">>, Step}
    ]).


%% @private
-spec assert_valid_step_direction(range()) -> ok | no_return().
assert_valid_step_direction([Start, End, Step]) when Start =< End, Step > 0 ->
    ok;
assert_valid_step_direction([Start, End, Step]) when Start >= End, Step < 0 ->
    ok;
assert_valid_step_direction(_) ->
    throw({unverified_constraints, <<"invalid step direction">>}).
