%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation values (instantiations of 'atm_data_type').
%%% @end
%%%-------------------------------------------------------------------
-module(atm_value_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    validate_atm_range_value/1
]).

groups() -> [
    {all_tests, [parallel], [
        validate_atm_range_value
    ]}
].

all() -> [
    {group, all_tests}
].


-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).
-define(erpc(Expr), ?erpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% API functions
%%%===================================================================


validate_atm_range_value(_Config) ->
    validate_value_test_base(
        #atm_data_spec{type = atm_range_type},
        [
            #{<<"end">> => 10},
            #{<<"start">> => 1, <<"end">> => 10},
            #{<<"start">> => -5, <<"end">> => 10, <<"step">> => 2},
            #{<<"start">> => 15, <<"end">> => -10, <<"step">> => -1},

            % Valid objects with excess fields are also accepted
            #{<<"end">> => 100, <<"key">> => <<"value">>}
        ],
        lists:flatten([
            lists:map(
                fun(Value) -> {Value, ?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_range_type)} end,
                [5, <<"NaN">>, [5], #{<<"key">> => 5}]
            ),

            lists:map(fun({Value, Field}) ->
                {Value, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                    Value, atm_range_type, #{<<".", Field/binary>> => errors:to_json(
                        ?ERROR_ATM_DATA_TYPE_UNVERIFIED(maps:get(Field, Value), atm_integer_type)
                    )}
                )}
            end, [
                {#{<<"end">> => <<"NaN">>}, <<"end">>},
                {#{<<"start">> => <<"NaN">>, <<"end">> => 10}, <<"start">>},
                {#{<<"start">> => 5, <<"end">> => 10, <<"step">> => <<"NaN">>}, <<"step">>}
            ]),

            lists:map(fun(Value) ->
                {Value, ?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                    Value, atm_range_type, <<"invalid step direction">>
                )}
            end, [
                #{<<"start">> => 5, <<"end">> => 10, <<"step">> => 0},
                #{<<"start">> => 15, <<"end">> => 10, <<"step">> => 1},
                #{<<"start">> => -15, <<"end">> => -10, <<"step">> => -1},
                #{<<"start">> => 10, <<"end">> => 15, <<"step">> => -1}
            ])
        ])
    ).


%% @private
validate_value_test_base(AtmDataSpec, ValidValues, InvalidValuesAndExpErrors) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),

    lists:foreach(fun(ValidValue) ->
        assert_valid_value(AtmWorkflowExecutionAuth, ValidValue, AtmDataSpec)
    end, ValidValues),

    lists:foreach(fun({InvalidValue, ExpError}) ->
        assert_invalid_value(AtmWorkflowExecutionAuth, InvalidValue, AtmDataSpec, ExpError)
    end, InvalidValuesAndExpErrors).


%% @private
-spec assert_valid_value(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_spec:record()
) ->
    ok.
assert_valid_value(AtmWorkflowExecutionAuth, Value, AtmDataSpec) ->
    ?assertEqual(ok, ?rpc(atm_value:validate(AtmWorkflowExecutionAuth, Value, AtmDataSpec))).


%% @private
-spec assert_invalid_value(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_spec:record(),
    errors:error()
) ->
    ok.
assert_invalid_value(AtmWorkflowExecutionAuth, Value, AtmDataSpec, ExpError) ->
    ?assertThrow(ExpError, ?erpc(atm_value:validate(
        AtmWorkflowExecutionAuth, Value, AtmDataSpec
    ))).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_workflow_execution_auth() -> atm_workflow_execution_auth:record().
create_workflow_execution_auth() ->
    atm_store_test_utils:create_workflow_execution_auth(
        ?PROVIDER_SELECTOR, user1, space_krk
    ).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, atm_store_test_utils],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(all_tests, Config) ->
    time_test_utils:freeze_time(Config),
    Config.


end_per_group(all_tests, Config) ->
    time_test_utils:unfreeze_time(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
