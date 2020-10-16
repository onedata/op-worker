%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Functions running test combinations as specified in scenario for API
%%% (REST + gs) tests.
%%%
%%%         /--------------------------------------------------\
%%%         | !!! COMMON PITFALLS (when writing api tests) !!! |
%%%         \--------------------------------------------------/
%%%
%%% 1) Using setup_fun() to renew the exact same data every time (e.g. json
%%%    metadata on specific file). In multi provider environment this can lead
%%%    to following example race condition:
%%%        1. setup_fun() renew file metadata META on provider A. In case when META
%%%           wasn't modified during previous test (unauthorized/forbidden clients)
%%%           this will update custom_metadata document but content itself will
%%%           not change. Because content wasn't modified checks on provider B will
%%%           pass even before changes from A would be propagated.
%%%        2. next test, for correct client this time, will start and succeed in
%%%           e.g removing META on provider B.
%%%        3. changes from provider A finally arrive at provider B which results
%%%           in restoring META on file.
%%%        4. verify_fun() checks whether META deletion is visible from providers
%%%           A and B and it fails.
%%%    To prevent such bugs it is recommended to checks in setup_fun() if data was
%%%    modified and only then renew it.
%%% @end
%%%-------------------------------------------------------------------
-module(onenv_api_test_runner).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").

-export([run_tests/2]).

-type ct_config() :: proplists:proplist().

-type scenario_type() ::
    % Standard rest scenario - using fileId in path so that no lookup
    % takes place
    rest |
    % Rest scenario using file path in URL - causes fileId lookup in
    % rest_handler. If path can't be resolved (this file/space is not
    % supported by specific provider) rather then concrete error a
    % ?ERROR_BAD_VALUE_IDENTIFIER(<<"urlFilePath">>) will be returned
    rest_with_file_path |
    % Rest scenario that results in ?ERROR_NOT_SUPPORTED regardless
    % of request auth and parameters.
    rest_not_supported |
    % Standard graph sync scenario
    gs |
    % Gs scenario with gri.scope == private and gri.id == SharedGuid.
    % Such requests should be rejected at auth steps resulting in
    % ?ERROR_UNAUTHORIZED.
    gs_with_shared_guid_and_aspect_private |
    % Gs scenario that results in ?ERROR_NOT_SUPPORTED regardless
    % of test case due to for example invalid aspect.
    gs_not_supported.

% List of nodes to which api calls can be directed. However only one node
% from this list will be chosen (randomly) for each test case.
% It greatly shortens time needed to run tests and also allows to test e.g.
% setting some value on one node and updating it on another node.
% Checks whether value set on one node was synced with other nodes can be
% performed using `verify_fun` callback.
-type target_nodes() :: [node()].

-type client_placeholder() :: atom().

-type client_spec() :: #client_spec{}.
-type data_spec() :: #data_spec{}.

% Function called before testcase. Can be used to create test environment.
-type setup_fun() :: fun(() -> ok).
% Function called after testcase. Can be used to clear up environment.
-type teardown_fun() :: fun(() -> ok).
% Function called after testcase. Can be used to check if test had desired effect
% on environment (e.g. check if resource deleted during test was truly deleted).
% First argument tells whether request made during testcase should succeed
-type verify_fun() :: fun(
    (RequestResultExpectation :: expected_success | expected_failure, api_test_ctx()) -> boolean()
).

-type rest_args() :: #rest_args{}.
-type gs_args() :: #gs_args{}.

-type api_test_ctx() :: #api_test_ctx{}.

% Function called during testcase to prepare call/request arguments. If test cannot
% be run due to e.g invalid combination of client, data and provider 'skip' atom
% should be returned instead to skip that specific testcase.
-type prepare_args_fun() :: fun((api_test_ctx()) -> skip | rest_args() | gs_args()).
% Function called after testcase to validate returned call/request result
-type validate_call_result_fun() :: fun((api_test_ctx(), Result :: term()) -> ok | no_return()).

-type scenario_spec() :: #scenario_spec{}.

% Template used to create scenario_spec(). It contains scenario specific data
% while args/params repeated for all scenarios of some type/group can be
% extracted and kept in e.g. suite_spec().
-type scenario_template() :: #scenario_template{}.

% Record used to group scenarios having common parameters like target nodes, client spec
% or check functions. List of scenario_spec() will be created from that common params as
% well as scenario specific data contained in each scenario_template().
-type suite_spec() :: #suite_spec{}.


-export_type([
    ct_config/0,
    scenario_type/0, target_nodes/0,
    client_placeholder/0, client_spec/0, data_spec/0,
    setup_fun/0, teardown_fun/0, verify_fun/0,
    rest_args/0, gs_args/0, api_test_ctx/0,
    prepare_args_fun/0, validate_call_result_fun/0,
    scenario_spec/0, scenario_template/0, suite_spec/0
]).


-type rest_response() :: {
    ok,
    RespCode :: non_neg_integer(),
    RespHeaders :: map(),
    RespBody :: binary() | map()
}.
-type invalid_client_type() :: unauthorized | forbidden_not_in_space | forbidden_in_space.

-define(NO_DATA, undefined).

-define(GS_RESP(Result), #gs_resp_graph{data = Result}).

-define(GS_SCENARIO_TYPES, [
    gs,
    gs_with_shared_guid_and_aspect_private,
    gs_not_supported
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec run_tests(ct_config(), [scenario_spec() | suite_spec()]) ->
    HasAllTestsPassed :: boolean().
run_tests(Config, SpecTemplates) ->
    lists:foldl(fun(SpecTemplate, AllPreviousTestsPassed) ->
        AllPreviousTestsPassed and try
            run_suite(Config, prepare_suite_spec(Config, SpecTemplate))
        catch
            throw:fail ->
                false;
            Type:Reason ->
                ct:pal("Unexpected error while running test suite ~p:~p ~p", [
                    Type, Reason, erlang:get_stacktrace()
                ]),
                false
        end
    end, true, SpecTemplates).


%%%===================================================================
%%% Run scenario test cases combinations functions
%%%===================================================================


%% @private
-spec run_suite(ct_config(), suite_spec()) -> HasAllTestsPassed :: boolean().
run_suite(Config, SuiteSpec) ->
    run_invalid_clients_test_cases(Config, unauthorized, SuiteSpec)
    and run_invalid_clients_test_cases(Config, forbidden_not_in_space, SuiteSpec)
    and run_invalid_clients_test_cases(Config, forbidden_in_space, SuiteSpec)
    and run_malformed_data_test_cases(Config, SuiteSpec)
    and run_missing_required_data_test_cases(Config, SuiteSpec)
    and run_expected_success_test_cases(Config, SuiteSpec).


%% @private
-spec run_invalid_clients_test_cases(ct_config(), invalid_client_type(), suite_spec()) ->
    HasAllTestsPassed :: boolean().
run_invalid_clients_test_cases(Config, InvalidClientsType, #suite_spec{
    target_nodes = TargetNodes,
    client_spec = ClientSpec,

    setup_fun = SetupFun,
    teardown_fun = TeardownFun,
    verify_fun = VerifyFun,

    scenario_templates = ScenarioTemplates,

    data_spec = DataSpec
}) ->
    TestCaseFun = fun(TargetNode, ClientWithError, DataSet, #scenario_template{
        type = ScenarioType
    } = ScenarioTemplate) ->
        {Client, Error} = get_scenario_specific_error_for_invalid_client(
            ScenarioType, InvalidClientsType, ClientWithError
        ),
        run_exp_error_testcase(
            TargetNode, Client, DataSet, Error, VerifyFun, ScenarioTemplate, Config
        )
    end,

    SetupFun(),
    TestsPassed = run_testcases(
        ScenarioTemplates,
        TargetNodes,
        get_invalid_clients(InvalidClientsType, ClientSpec),
        generate_required_data_sets(DataSpec),
        TestCaseFun
    ),
    TeardownFun(),

    TestsPassed.


%% @private
-spec get_invalid_clients(invalid_client_type(), client_spec()) ->
    [aai:auth() | {aai:auth(), errors:error()}].
get_invalid_clients(unauthorized, #client_spec{unauthorized = Clients}) ->
    Clients;
get_invalid_clients(forbidden_not_in_space, #client_spec{forbidden_not_in_space = Clients}) ->
    Clients;
get_invalid_clients(forbidden_in_space, #client_spec{forbidden_in_space = Clients}) ->
    Clients.


%% @private
-spec get_scenario_specific_error_for_invalid_client(
    scenario_type(),
    invalid_client_type(),
    aai:auth() | {aai:auth(), errors:error()}
) ->
    {aai:auth(), errors:error()}.
get_scenario_specific_error_for_invalid_client(rest_not_supported, _ClientType, ClientAndError) ->
    % Error thrown by middleware when checking if operation is supported -
    % before auth checks could be performed
    {extract_client(ClientAndError), ?ERROR_NOT_SUPPORTED};
get_scenario_specific_error_for_invalid_client(gs_not_supported, _ClientType, ClientAndError) ->
    % Error thrown by middleware when checking if operation is supported -
    % before auth checks could be performed
    {extract_client(ClientAndError), ?ERROR_NOT_SUPPORTED};
get_scenario_specific_error_for_invalid_client(rest_with_file_path, ClientType, ClientAndError) when
    ClientType =:= unauthorized;
    ClientType =:= forbidden_not_in_space
->
    % Error thrown by rest_handler (before middleware auth checks could be performed)
    % as invalid clients who doesn't belong to space can't resolve file path to guid
    {extract_client(ClientAndError), ?ERROR_BAD_VALUE_IDENTIFIER(<<"urlFilePath">>)};
get_scenario_specific_error_for_invalid_client(_ScenarioType, _, {_, {error, _}} = ClientAndError) ->
    ClientAndError;
get_scenario_specific_error_for_invalid_client(_ScenarioType, unauthorized, Client) ->
    {Client, ?ERROR_UNAUTHORIZED};
get_scenario_specific_error_for_invalid_client(_ScenarioType, forbidden_not_in_space, Client) ->
    {Client, ?ERROR_FORBIDDEN};
get_scenario_specific_error_for_invalid_client(_ScenarioType, forbidden_in_space, Client) ->
    {Client, ?ERROR_FORBIDDEN}.


%% @private
-spec extract_client(aai:auth() | {aai:auth(), errors:error()}) -> aai:auth().
extract_client({Client, {error, _}}) -> Client;
extract_client(Client)               -> Client.


%% @private
-spec run_malformed_data_test_cases(ct_config(), suite_spec()) -> HasAllTestsPassed :: boolean().
run_malformed_data_test_cases(Config, #suite_spec{
    target_nodes = TargetNodes,
    client_spec = #client_spec{correct = CorrectClients},

    setup_fun = SetupFun,
    teardown_fun = TeardownFun,
    verify_fun = VerifyFun,

    scenario_templates = ScenarioTemplates,

    data_spec = DataSpec
}) ->
    TestCaseFun = fun
        (_TargetNode, _Client, ?NO_DATA, _) ->
            % operations not requiring any data cannot be tested against
            % malformed data
            true;
        (TargetNode, Client, {DataSet, _BadKey, Error}, #scenario_template{
            name = ScenarioName,
            type = ScenarioType
        } = ScenarioTemplate) ->
            case is_data_error_applicable_to_scenario(Error, ScenarioType) of
                false ->
                    true;
                true ->
                    TestCaseCtx = build_test_ctx(
                        ScenarioName, ScenarioType, TargetNode, Client, DataSet
                    ),
                    run_exp_error_testcase(
                        TargetNode, Client, DataSet,
                        get_expected_malformed_data_error(Error, ScenarioType, TestCaseCtx),
                        VerifyFun, ScenarioTemplate, Config
                    )
            end
    end,

    SetupFun(),
    TestsPassed = run_testcases(
        ScenarioTemplates,
        TargetNodes,
        CorrectClients,
        generate_bad_data_sets(DataSpec),
        TestCaseFun
    ),
    TeardownFun(),

    TestsPassed.


%% @private
-spec is_data_error_applicable_to_scenario(
    errors:error() | {scenario_type(), errors:error()},
    scenario_type()
) ->
    boolean().
is_data_error_applicable_to_scenario({error, _}, _)                          -> true;
is_data_error_applicable_to_scenario({Scenario, _}, Scenario)                -> true;
is_data_error_applicable_to_scenario({rest_handler, _}, rest)                -> true;
is_data_error_applicable_to_scenario({rest_handler, _}, rest_with_file_path) -> true;
is_data_error_applicable_to_scenario({rest_handler, _}, rest_not_supported)  -> true;
is_data_error_applicable_to_scenario(_, _)                                   -> false.


%% @private
-spec get_expected_malformed_data_error(
    errors:error() | {scenario_type(), errors:error()},
    scenario_type(),
    api_test_ctx()
) ->
    errors:error().
get_expected_malformed_data_error({rest_handler, RestHandlerSpecificError}, _, _) ->
    % Rest handler errors takes precedence over anything else as it is
    % thrown before request is even passed to middleware
    RestHandlerSpecificError;
get_expected_malformed_data_error(_, rest_not_supported, _) ->
    % Operation not supported errors takes precedence over any data sanitization
    % or auth checks errors as it is done earlier
    ?ERROR_NOT_SUPPORTED;
get_expected_malformed_data_error(_, gs_not_supported, _) ->
    % Operation not supported errors takes precedence over any data sanitization
    % or auth checks errors as it is done earlier
    ?ERROR_NOT_SUPPORTED;
get_expected_malformed_data_error({error, _} = Error, _, _) ->
    Error;
get_expected_malformed_data_error({error_fun, ErrorFun}, _, TestCaseCtx) ->
    ErrorFun(TestCaseCtx);
get_expected_malformed_data_error({_ScenarioType, {error, _} = ScenarioSpecificError}, _, _) ->
    ScenarioSpecificError;
get_expected_malformed_data_error({_ScenarioType, {error_fun, ErrorFun}}, _, TestCaseCtx) ->
    ErrorFun(TestCaseCtx).


%% @private
-spec run_missing_required_data_test_cases(ct_config(), suite_spec()) ->
    HasAllTestsPassed :: boolean().
run_missing_required_data_test_cases(_Config, #suite_spec{data_spec = undefined}) ->
    true;
run_missing_required_data_test_cases(_Config, #suite_spec{data_spec = #data_spec{
    required = [],
    at_least_one = []
}}) ->
    true;
run_missing_required_data_test_cases(Config, #suite_spec{
    target_nodes = TargetNodes,
    client_spec = #client_spec{correct = CorrectClients},

    setup_fun = SetupFun,
    teardown_fun = TeardownFun,
    verify_fun = VerifyFun,

    scenario_templates = ScenarioTemplates,

    data_spec = DataSpec = #data_spec{
        required = RequiredParams,
        at_least_one = AtLeastOneParams
    }
}) ->
    RequiredDataSet = lists_utils:random_element(generate_required_data_sets(DataSpec)),

    MissingRequiredParamsDataSetsAndErrors = lists:map(fun(RequiredParam) ->
        {maps:remove(RequiredParam, RequiredDataSet), ?ERROR_MISSING_REQUIRED_VALUE(RequiredParam)}
    end, RequiredParams),

    MissingAtLeastOneParamsDataSetAndError = case AtLeastOneParams of
        [] ->
            [];
        _ ->
            ExpectedError = ?ERROR_MISSING_AT_LEAST_ONE_VALUE(lists:sort(AtLeastOneParams)),
            [{maps:without(AtLeastOneParams, RequiredDataSet), ExpectedError}]
    end,

    IncompleteDataSetsAndErrors = lists:flatten([
        MissingRequiredParamsDataSetsAndErrors,
        MissingAtLeastOneParamsDataSetAndError
    ]),

    TestCaseFun = fun(TargetNode, Client, {DataSet, MissingParamError}, #scenario_template{
        type = ScenarioType
    } = ScenarioTemplate) ->
        run_exp_error_testcase(
            TargetNode, Client, DataSet,
            get_scenario_specific_error_for_missing_data(ScenarioType, MissingParamError),
            VerifyFun, ScenarioTemplate, Config
        )
    end,

    SetupFun(),
    TestsPassed = run_testcases(
        ScenarioTemplates,
        TargetNodes,
        CorrectClients,
        IncompleteDataSetsAndErrors,
        TestCaseFun
    ),
    TeardownFun(),

    TestsPassed.


%% @private
-spec get_scenario_specific_error_for_missing_data(scenario_type(), errors:error()) ->
    errors:error().
get_scenario_specific_error_for_missing_data(rest_not_supported, _Error) ->
    % Error thrown by middleware when checking if operation is supported -
    % before sanitization could be performed
    ?ERROR_NOT_SUPPORTED;
get_scenario_specific_error_for_missing_data(gs_not_supported, _Error) ->
    % Error thrown by middleware when checking if operation is supported -
    % before sanitization could be performed
    ?ERROR_NOT_SUPPORTED;
get_scenario_specific_error_for_missing_data(_ScenarioType, Error) ->
    Error.


%% @private
-spec run_expected_success_test_cases(ct_config(), suite_spec()) -> HasAllTestsPassed :: boolean().
run_expected_success_test_cases(Config, #suite_spec{
    target_nodes = TargetNodes,
    client_spec = #client_spec{correct = CorrectClients},

    setup_fun = SetupFun,
    teardown_fun = TeardownFun,
    verify_fun = VerifyFun,

    scenario_templates = AvailableScenarios,
    randomly_select_scenarios = true,

    data_spec = DataSpec
}) ->
    CorrectDataSets = generate_correct_data_sets(DataSpec),

    lists:foldl(fun(Client, OuterAcc) ->
        CorrectDataSetsNum = length(CorrectDataSets),
        AvailableScenariosNum = length(AvailableScenarios),
        ScenarioPerDataSet = case AvailableScenariosNum > CorrectDataSetsNum of
            true ->
                ct:fail("Not enough data sets compared to available scenarios");
            false ->
                RandomizedScenarios = lists:flatmap(
                    fun(_) -> lists_utils:shuffle(AvailableScenarios) end,
                    lists:seq(1, CorrectDataSetsNum div AvailableScenariosNum + 1)
                ),
                lists:zip(
                    lists:sublist(RandomizedScenarios, CorrectDataSetsNum),
                    CorrectDataSets
                )
        end,
        OuterAcc and lists:foldl(fun({Scenario, DataSet}, InnerAcc) ->
            TargetNode = lists_utils:random_element(TargetNodes),

            SetupFun(),
            TestCasePassed = run_exp_success_testcase(
                TargetNode, Client, DataSet, VerifyFun, Scenario, Config
            ),
            TeardownFun(),

            InnerAcc and TestCasePassed
        end, true, ScenarioPerDataSet)
    end, true, CorrectClients);
run_expected_success_test_cases(Config, #suite_spec{
    target_nodes = TargetNodes,
    client_spec = #client_spec{correct = CorrectClients},

    setup_fun = SetupFun,
    teardown_fun = TeardownFun,
    verify_fun = VerifyFun,

    scenario_templates = ScenarioTemplates,
    randomly_select_scenarios = false,

    data_spec = DataSpec
}) ->
    TestCaseFun = fun(TargetNode, Client, DataSet, ScenarioTemplate) ->
        SetupFun(),
        TestCasePassed = run_exp_success_testcase(
            TargetNode, Client, DataSet, VerifyFun, ScenarioTemplate, Config
        ),
        TeardownFun(),

        TestCasePassed
    end,

    run_testcases(
        ScenarioTemplates,
        TargetNodes,
        CorrectClients,
        generate_correct_data_sets(DataSpec),
        TestCaseFun
    ).


%% @private
-spec run_testcases(
    [scenario_template()],
    [node()],
    [aai:auth() | {aai:auth(), errors:error()}],
    DataSets :: [undefined | map()],
    fun((node(), aai:auth() | {aai:auth(), errors:error()}, undefined | map(), scenario_template()) ->
        boolean()
    )
) ->
    AllTestCasesPassed :: boolean().
run_testcases(ScenarioTemplates, TargetNodes, Clients, DataSets, TestCaseFun) ->
    lists:foldl(fun(ScenarioTemplate, PrevScenariosPassed) ->
        PrevScenariosPassed and lists:foldl(fun(Client, PrevClientsPassed) ->
            PrevClientsPassed and lists:foldl(fun(DataSet, PrevDataSetsPassed) ->
                TargetNode = lists_utils:random_element(TargetNodes),
                PrevDataSetsPassed and TestCaseFun(
                    TargetNode, Client, DataSet, ScenarioTemplate
                )
            end, true, DataSets)
        end, true, Clients)
    end, true, ScenarioTemplates).


%% @private
-spec run_exp_error_testcase(node(), aai:auth(), undefined | map(), errors:error(), verify_fun(),
    scenario_template(), ct_config()) -> TestCasePassed :: boolean().
run_exp_error_testcase(TargetNode, Client, DataSet, ScenarioError, VerifyFun, #scenario_template{
    name = ScenarioName,
    type = ScenarioType,
    prepare_args_fun = PrepareArgsFun
}, Config) ->
    ExpError = case is_client_supported_by_node(Client, TargetNode, Config) of
        true -> ScenarioError;
        false -> ?ERROR_UNAUTHORIZED(?ERROR_USER_NOT_SUPPORTED)
    end,
    TestCaseCtx = build_test_ctx(ScenarioName, ScenarioType, TargetNode, Client, DataSet),

    case PrepareArgsFun(TestCaseCtx) of
        skip ->
            true;
        Args ->
            RequestResult = make_request(Config, TargetNode, Client, Args),
            try
                validate_error_result(ScenarioType, ExpError, RequestResult),
                VerifyFun(expected_failure, TestCaseCtx)
            catch T:R ->
                log_failure(ScenarioName, TestCaseCtx, Args, ExpError, RequestResult, T, R, Config),
                false
            end
    end.


%% @private
-spec run_exp_success_testcase(node(), aai:auth(), undefined | map(), verify_fun(),
    scenario_template(), ct_config()) -> TestCasePassed :: boolean().
run_exp_success_testcase(TargetNode, Client, DataSet, VerifyFun, #scenario_template{
    name = ScenarioName,
    type = ScenarioType,
    prepare_args_fun = PrepareArgsFun,
    validate_result_fun = ValidateResultFun
}, Config) ->
    TestCaseCtx = build_test_ctx(ScenarioName, ScenarioType, TargetNode, Client, DataSet),
    case PrepareArgsFun(TestCaseCtx) of
        skip ->
            true;
        Args ->
            Result = make_request(Config, TargetNode, Client, Args),
            try
                case is_client_supported_by_node(Client, TargetNode, Config) of
                    true ->
                        ValidateResultFun(TestCaseCtx, Result),
                        VerifyFun(expected_success, TestCaseCtx);
                    false ->
                        validate_error_result(
                            ScenarioType, ?ERROR_UNAUTHORIZED(?ERROR_USER_NOT_SUPPORTED), Result
                        ),
                        VerifyFun(expected_failure, TestCaseCtx)
                end
            catch T:R ->
                log_failure(ScenarioName, TestCaseCtx, Args, success, Result, T, R, Config),
                false
            end
    end.


%% @private
-spec validate_error_result(
    scenario_type(),
    errors:error(),
    errors:error() | rest_response()  % gs | REST
) ->
    ok.
validate_error_result(Type, ExpError, {ok, RespCode, _RespHeaders, RespBody}) when
    Type == rest;
    Type == rest_with_file_path;
    Type == rest_not_supported
->
    ?assertEqual(
        {errors:to_http_code(ExpError), ?REST_ERROR(ExpError)},
        {RespCode, RespBody}
    );

validate_error_result(Type, ExpError, Result) when
    Type == gs;
    Type == gs_with_shared_guid_and_aspect_private;
    Type == gs_not_supported
->
    ?assertEqual(ExpError, Result).


%% @private
-spec log_failure(
    ScenarioName :: binary(),
    api_test_ctx(),
    Args :: rest_args() | gs_args(),
    Expected :: term(),
    Got :: term(),
    ErrType :: error | exit | throw,
    ErrReason :: term(),
    ct_config()
) ->
    ok.
log_failure(
    ScenarioName,
    #api_test_ctx{node = TargetNode, client = Client},
    Args,
    Expected,
    Got,
    ErrType,
    ErrReason,
    Config
) ->
    ct:pal("~s test case failed:~n"
    "Node: ~p~n"
    "Client: ~p~n"
    "Args: ~s~n"
    "Expected: ~p~n"
    "Got: ~p~n"
    "Error: ~p:~p~n"
    "Stacktrace: ~p~n", [
        ScenarioName,
        TargetNode,
        client_to_placeholder(Client, Config),
        io_lib_pretty:print(Args, fun get_record_def/2),
        Expected,
        Got,
        ErrType, ErrReason,
        erlang:get_stacktrace()
    ]).


%%%===================================================================
%%% Prepare data combinations functions
%%%===================================================================


%% @private
-spec generate_correct_data_sets
    (undefined) -> [?NO_DATA];
    (data_spec()) -> [Data :: map()].
generate_correct_data_sets(undefined) ->
    [?NO_DATA];
generate_correct_data_sets(DataSpec) ->
    RequiredParamsDataSets = generate_required_data_sets(DataSpec),

    RequiredParamsDataSet = case RequiredParamsDataSets of
        [] -> #{};
        _ -> lists_utils:random_element(RequiredParamsDataSets)
    end,
    RequiredAndOptionalParamsDataSets = lists:map(fun(OptionalDataSet) ->
        maps:merge(RequiredParamsDataSet, OptionalDataSet)
    end, generate_optional_data_sets(DataSpec)),

    RequiredParamsDataSets ++ RequiredAndOptionalParamsDataSets.


%% @private
-spec generate_required_data_sets
    (undefined) -> [?NO_DATA];
    (data_spec()) -> [Data :: map()].
generate_required_data_sets(undefined) ->
    [?NO_DATA];
generate_required_data_sets(#data_spec{
    required = Required,
    at_least_one = AtLeastOne
} = DataSpec) ->

    AtLeastOneWithValues = lists:flatten(lists:map(
        fun(Key) ->
            [#{Key => Val} || Val <- get_correct_values(Key, DataSpec)]
        end, AtLeastOne)
    ),
    RequiredWithValues = lists:map(
        fun(Key) ->
            [#{Key => Val} || Val <- get_correct_values(Key, DataSpec)]
        end, Required
    ),
    RequiredCombinations = lists:foldl(
        fun(ValuesForKey, Acc) ->
            [maps:merge(A, B) || A <- ValuesForKey, B <- Acc]
        end, [#{}], RequiredWithValues
    ),
    RequiredWithOne = lists:flatten(lists:map(
        fun(ReqMap) ->
            [maps:merge(AtLeastOneMap, ReqMap) || AtLeastOneMap <- AtLeastOneWithValues]
        end, RequiredCombinations
    )),
    AllAtLeastOneMap = lists:foldl(fun maps:merge/2, #{}, AtLeastOneWithValues),
    RequiredWithAll = lists:map(
        fun(ReqMap) ->
            maps:merge(AllAtLeastOneMap, ReqMap)
        end, RequiredCombinations
    ),
    case AtLeastOne of
        [] -> RequiredCombinations;
        [_] -> RequiredWithOne;
        _ -> RequiredWithAll ++ RequiredWithOne
    end.


%% @private
-spec generate_optional_data_sets
    (undefined) -> [?NO_DATA];
    (data_spec()) -> [Data :: map()].
generate_optional_data_sets(undefined) ->
    [?NO_DATA];
generate_optional_data_sets(#data_spec{optional = []}) ->
    [];
generate_optional_data_sets(#data_spec{optional = Optional} = DataSpec) ->
    OptionalParamsWithValues = lists:flatten(lists:map(fun(Key) ->
        [#{Key => Val} || Val <- get_correct_values(Key, DataSpec)]
    end, Optional)),

    OptionalParamsCombinations = lists:usort(lists:foldl(fun(ParamWithValue, Acc) ->
        [maps:merge(Combination, ParamWithValue) || Combination <- Acc] ++ Acc
    end, [#{}], OptionalParamsWithValues)),

    lists:delete(#{}, OptionalParamsCombinations).


%% @private
-spec generate_bad_data_sets
    (undefined) -> [?NO_DATA];
    (data_spec()) -> [{Data :: map(), InvalidParam :: binary(), ExpError :: errors:error()}].
generate_bad_data_sets(undefined) ->
    [?NO_DATA];
generate_bad_data_sets(#data_spec{required = Required, bad_values = BadValues} = DataSpec) ->
    CorrectDataSet = lists:foldl(fun(Param, Acc) ->
        Acc#{Param => lists_utils:random_element(get_correct_values(Param, DataSpec))}
    end, #{}, Required),

    lists:map(fun({Param, InvalidValue, ExpError}) ->
        Data = CorrectDataSet#{Param => InvalidValue},
        {Data, Param, ExpError}
    end, BadValues).


%% @private
-spec get_correct_values(Key :: binary(), data_spec()) -> CorrectValues :: [term()].
get_correct_values(Key, #data_spec{correct_values = CorrectValues}) ->
    case maps:get(Key, CorrectValues) of
        Fun when is_function(Fun, 0) ->
            Fun();
        Value ->
            Value
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec prepare_suite_spec(ct_config(), scenario_spec() | suite_spec()) -> suite_spec().
prepare_suite_spec(Config, #suite_spec{client_spec = ClientSpecTemplate} = SuiteSpec) ->
    SuiteSpec#suite_spec{
        client_spec = prepare_client_spec(Config, ClientSpecTemplate)
    };
prepare_suite_spec(Config, #scenario_spec{} = ScenarioSpec) ->
    prepare_suite_spec(Config, scenario_spec_to_suite_spec(ScenarioSpec)).


%% @private
-spec prepare_client_spec(ct_config(), client_spec()) -> client_spec().
prepare_client_spec(Config, #client_spec{
    correct = CorrectClients,
    unauthorized = UnauthorizedClient,
    forbidden_not_in_space = ForbiddenClientsNotInSpace,
    forbidden_in_space = ForbiddenClientsInSpace
}) ->
    #client_spec{
        correct = substitute_client_placeholders(CorrectClients, Config),
        unauthorized = substitute_client_placeholders(UnauthorizedClient, Config),
        forbidden_not_in_space = substitute_client_placeholders(ForbiddenClientsNotInSpace, Config),
        forbidden_in_space = substitute_client_placeholders(ForbiddenClientsInSpace, Config)
    }.


%% @private
-spec substitute_client_placeholders(
    [aai:auth() | client_placeholder() | {aai:auth() | client_placeholder(), errors:error()}],
    ct_config()
) ->
    [aai:auth() | {aai:auth(), errors:error()}].
substitute_client_placeholders(ClientsAndPlaceholders, Config) ->
    lists:map(fun
        ({ClientOrPlaceholder, {error, _} = Error}) ->
            {substitute_client_placeholder(ClientOrPlaceholder, Config), Error};
        (ClientOrPlaceholder) ->
            substitute_client_placeholder(ClientOrPlaceholder, Config)
    end, ClientsAndPlaceholders).


%% @private
-spec substitute_client_placeholder(aai:auth() | client_placeholder(), ct_config()) -> aai:auth().
substitute_client_placeholder(#auth{} = AaiClient, _Config) ->
    AaiClient;
substitute_client_placeholder(Placeholder, Config) ->
    placeholder_to_client(Placeholder, Config).


%% @private
-spec scenario_spec_to_suite_spec(scenario_spec()) -> suite_spec().
scenario_spec_to_suite_spec(#scenario_spec{
    name = ScenarioName,
    type = ScenarioType,
    target_nodes = TargetNodes,
    client_spec = ClientSpec,

    setup_fun = SetupFun,
    teardown_fun = TeardownFun,
    verify_fun = VerifyFun,

    prepare_args_fun = PrepareArgsFun,
    validate_result_fun = ValidateResultFun,

    data_spec = DataSpec
}) ->
    #suite_spec{
        target_nodes = TargetNodes,
        client_spec = ClientSpec,

        setup_fun = SetupFun,
        teardown_fun = TeardownFun,
        verify_fun = VerifyFun,

        scenario_templates = [#scenario_template{
            name = ScenarioName,
            type = ScenarioType,
            prepare_args_fun = PrepareArgsFun,
            validate_result_fun = ValidateResultFun
        }],
        randomly_select_scenarios = false,

        data_spec = DataSpec
    }.


%% @private
-spec placeholder_to_client(client_placeholder(), ct_config()) -> aai:auth().
placeholder_to_client(nobody, _Config) ->
    ?NOBODY;
placeholder_to_client(Username, Config) when is_atom(Username) ->
    ?USER(api_test_env:get_user_id(Username, Config)).


%% @private
-spec client_to_placeholder(aai:auth(), ct_config()) -> client_placeholder().
client_to_placeholder(?NOBODY, _Config) ->
    nobody;
client_to_placeholder(?USER(UserId), Config) ->
    api_test_env:to_entity_placeholder(UserId, Config).


%% @private
-spec build_test_ctx(binary(), scenario_type(), node(), aai:auth(), map()) ->
    api_test_ctx().
build_test_ctx(ScenarioName, ScenarioType, TargetNode, Client, DataSet) ->
    #api_test_ctx{
        scenario_name = ScenarioName,
        scenario_type = ScenarioType,
        node = TargetNode,
        client = Client,
        data = DataSet
    }.


%% @private
-spec is_client_supported_by_node(aai:auth(), node(), ct_config()) ->
    boolean().
is_client_supported_by_node(?NOBODY, _Node, _Config) ->
    true;
is_client_supported_by_node(?USER(UserId), Node, Config) ->
    ProvId = op_test_rpc:get_provider_id(Node),
    lists:member(UserId, api_test_env:get_provider_eff_users(ProvId, Config)).


%% @private
-spec make_request(ct_config(), node(), aai:auth(), rest_args() | gs_args()) ->
    {ok, GsCallResult :: map()} |
    {ok, RespCode :: non_neg_integer(), RespHeaders :: map(), RespBody :: binary() | map()} |
    {error, term()}.
make_request(Config, Node, Client, #rest_args{} = Args) ->
    make_rest_request(Config, Node, Client, Args);
make_request(Config, Node, Client, #gs_args{} = Args) ->
    make_gs_request(Config, Node, Client, Args).


%% @private
-spec make_gs_request(ct_config(), node(), aai:auth(), gs_args()) ->
    {ok, Result :: map()} | {error, term()}.
make_gs_request(Config, Node, Client, #gs_args{
    operation = Operation,
    gri = GRI,
    auth_hint = AuthHint,
    data = Data
}) ->
    case connect_via_gs(Node, Client, Config) of
        {ok, GsClient} ->
            case gs_client:graph_request(GsClient, GRI, Operation, Data, false, AuthHint) of
                {ok, ?GS_RESP(Result)} ->
                    {ok, Result};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec connect_via_gs(node(), aai:auth(), ct_config()) ->
    {ok, GsClient :: pid()} | errors:error().
connect_via_gs(Node, Client, Config) ->
    {Auth, ExpIdentity} = case Client of
        ?NOBODY ->
            {undefined, ?SUB(nobody)};
        ?USER(UserId) ->
            TokenAuth = {token, api_test_env:get_user_access_token(UserId, Config)},
            {TokenAuth, ?SUB(user, UserId)}
    end,
    GsEndpoint = gs_endpoint(Node),
    GsSupportedVersions = op_test_rpc:gs_protocol_supported_versions(Node),
    Opts = [{cacerts, op_test_rpc:get_cert_chain_pems(Node)}],

    case gs_client:start_link(GsEndpoint, Auth, GsSupportedVersions, fun(_) -> ok end, Opts) of
        {ok, GsClient, #gs_resp_handshake{identity = ExpIdentity}} ->
            {ok, GsClient};
        {error, _} = Error ->
            Error
    end.


%% @private
-spec gs_endpoint(node()) -> URL :: binary().
gs_endpoint(Node) ->
    Port = get_https_server_port_str(Node),
    Domain = op_test_rpc:get_provider_domain(Node),

    str_utils:join_as_binaries(
        ["wss://", Domain, Port, "/graph_sync/gui"],
        <<>>
    ).


%% @private
-spec make_rest_request(ct_config(), node(), aai:auth(), rest_args()) ->
    rest_response() | {error, term()}.
make_rest_request(Config, Node, Client, #rest_args{
    method = Method,
    path = Path,
    headers = Headers,
    body = Body
}) ->
    URL = get_rest_endpoint(Node, Path),
    HeadersWithAuth = maps:merge(Headers, get_rest_auth_headers(Client, Config)),
    CaCerts = op_test_rpc:get_cert_chain_pems(Node),
    Opts = [{ssl_options, [{cacerts, CaCerts}]}],

    case http_client:request(Method, URL, HeadersWithAuth, Body, Opts) of
        {ok, RespCode, RespHeaders, RespBody} ->
            case maps:get(<<"content-type">>, RespHeaders, undefined) of
                <<"application/json">> ->
                    {ok, RespCode, RespHeaders, json_utils:decode(RespBody)};
                _ ->
                    {ok, RespCode, RespHeaders, RespBody}
            end;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec get_rest_auth_headers(aai:auth(), ct_config()) -> AuthHeaders :: map().
get_rest_auth_headers(?NOBODY, _Config) ->
    #{};
get_rest_auth_headers(?USER(UserId), Config) ->
    #{?HDR_X_AUTH_TOKEN => api_test_env:get_user_access_token(UserId, Config)}.


%% @private
-spec get_rest_endpoint(node(), ResourcePath :: string() | binary()) ->
    URL :: binary().
get_rest_endpoint(Node, ResourcePath) ->
    Port = get_https_server_port_str(Node),
    Domain = op_test_rpc:get_provider_domain(Node),

    str_utils:join_as_binaries(
        ["https://", Domain, Port, "/api/v3/oneprovider/", ResourcePath],
        <<>>
    ).


%% @private
-spec get_https_server_port_str(node()) -> PortStr :: string().
get_https_server_port_str(Node) ->
    case get(port) of
        undefined ->
            {ok, Port} = test_utils:get_env(Node, ?APP_NAME, https_server_port),
            PortStr = case Port of
                443 -> "";
                _ -> ":" ++ integer_to_list(Port)
            end,
            put(port, PortStr),
            PortStr;
        Port ->
            Port
    end.


% Returns information about chosen records, such as fields,
% required to for example pretty print it
get_record_def(rest_args, N) ->
    case record_info(size, rest_args) - 1 of
        N -> record_info(fields, rest_args);
        _ -> no
    end;
get_record_def(gs_args, N) ->
    case record_info(size, gs_args) - 1 of
        N -> record_info(fields, gs_args);
        _ -> no
    end;
get_record_def(data_spec, N) ->
    case record_info(size, data_spec) - 1 of
        N -> record_info(fields, data_spec);
        _ -> no
    end;
get_record_def(gri, N) ->
    case record_info(size, gri) - 1 of
        N -> record_info(fields, gri);
        _ -> no
    end;
get_record_def(_, _) ->
    no.
