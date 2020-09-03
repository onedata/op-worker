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
%%%    modified is any and only then renew it.
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

-type config() :: proplists:proplist().

-export_type([config/0]).

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


-spec run_tests(config(), [scenario_spec() | suite_spec()]) ->
    boolean().
run_tests(Config, Specs) ->
    lists:foldl(fun
        (#scenario_spec{} = ScenarioSpec, AllTestsPassed) ->
            AllTestsPassed and run_suite(Config, scenario_spec_to_suite_spec(ScenarioSpec));
        (#suite_spec{} = SuiteSpec, AllTestsPassed) ->
            AllTestsPassed and run_suite(Config, SuiteSpec)
    end, true, Specs).


%%%===================================================================
%%% Run scenario test cases combinations functions
%%%===================================================================


%% @private
run_suite(Config, #suite_spec{
    client_spec = ClientSpecWithPlaceholders
} = SuiteSpecWithClientPlaceholders) ->
    try
        SuiteSpec = SuiteSpecWithClientPlaceholders#suite_spec{
            client_spec = prepare_client_spec(ClientSpecWithPlaceholders, Config)
        },

        run_invalid_clients_test_cases(Config, unauthorized, SuiteSpec)
        and run_invalid_clients_test_cases(Config, forbidden_not_in_space, SuiteSpec)
        and run_invalid_clients_test_cases(Config, forbidden_in_space, SuiteSpec)
        and run_malformed_data_test_cases(Config, SuiteSpec)
        and run_missing_required_data_test_cases(Config, SuiteSpec)
        and run_expected_success_test_cases(Config, SuiteSpec)
    catch
        throw:fail ->
            false;
        Type:Reason ->
            ct:pal("Unexpected error while running test suite ~p:~p ~p", [
                Type, Reason, erlang:get_stacktrace()
            ]),
            false
    end.


%% @private
prepare_client_spec(#client_spec{
    correct = CorrectClientsAndPlaceholders,
    unauthorized = UnauthorizedClientsAndPlaceholders,
    forbidden_not_in_space = ForbiddenClientsNotInSpaceAndPlaceholders,
    forbidden_in_space = ForbiddenClientsInSpaceAndPlaceholders
}, Config) ->
    #client_spec{
        correct = replace_client_placeholders_with_aai_auths(
            CorrectClientsAndPlaceholders, Config
        ),
        unauthorized = replace_client_placeholders_with_aai_auths(
            UnauthorizedClientsAndPlaceholders, Config
        ),
        forbidden_not_in_space = replace_client_placeholders_with_aai_auths(
            ForbiddenClientsNotInSpaceAndPlaceholders, Config
        ),
        forbidden_in_space = replace_client_placeholders_with_aai_auths(
            ForbiddenClientsInSpaceAndPlaceholders, Config
        )
    }.


%% @private
replace_client_placeholders_with_aai_auths(ClientsAndPlaceholders, Config) ->
    lists:map(fun
        ({ClientOrPlaceholder, {error, _} = Error}) ->
            {replace_client_placeholder_with_aai_auth(ClientOrPlaceholder, Config), Error};
        (ClientOrPlaceholder) ->
            replace_client_placeholder_with_aai_auth(ClientOrPlaceholder, Config)
    end, ClientsAndPlaceholders).


%% @private
replace_client_placeholder_with_aai_auth(nobody, _Config) ->
    ?NOBODY;
replace_client_placeholder_with_aai_auth(Username, Config) when is_atom(Username) ->
    ?USER(api_test_env:get_user_id(Username, Config));
replace_client_placeholder_with_aai_auth(#auth{} = ValidClient, _Config) ->
    ValidClient.


%% @private
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
    TestsPassed = run_scenarios(
        ScenarioTemplates, TargetNodes,
        get_invalid_clients(InvalidClientsType, ClientSpec),
        required_data_sets(DataSpec),
        TestCaseFun
    ),
    TeardownFun(),

    TestsPassed.


%% @private
get_invalid_clients(unauthorized, #client_spec{unauthorized = Clients}) ->
    Clients;
get_invalid_clients(forbidden_not_in_space, #client_spec{forbidden_not_in_space = Clients}) ->
    Clients;
get_invalid_clients(forbidden_in_space, #client_spec{forbidden_in_space = Clients}) ->
    Clients.


%% @private
get_scenario_specific_error_for_invalid_client(rest_not_supported, _ClientType, ClientAndError) ->
    % Error thrown by middleware when checking if operation is supported -
    % before auth checks could be performed
    {get_client(ClientAndError), ?ERROR_NOT_SUPPORTED};
get_scenario_specific_error_for_invalid_client(gs_not_supported, _ClientType, ClientAndError) ->
    % Error thrown by middleware when checking if operation is supported -
    % before auth checks could be performed
    {get_client(ClientAndError), ?ERROR_NOT_SUPPORTED};
get_scenario_specific_error_for_invalid_client(rest_with_file_path, ClientType, ClientAndError) when
    ClientType =:= unauthorized;
    ClientType =:= forbidden_not_in_space
->
    % Error thrown by rest_handler (before middleware auth checks could be performed)
    % as invalid clients who doesn't belong to space can't resolve file path to guid
    {get_client(ClientAndError), ?ERROR_BAD_VALUE_IDENTIFIER(<<"urlFilePath">>)};
get_scenario_specific_error_for_invalid_client(_ScenarioType, _ClientType, {_, {error, _}} = ClientAndError) ->
    ClientAndError;
get_scenario_specific_error_for_invalid_client(_ScenarioType, unauthorized, Client) ->
    {Client, ?ERROR_UNAUTHORIZED};
get_scenario_specific_error_for_invalid_client(_ScenarioType, forbidden_not_in_space, Client) ->
    {Client, ?ERROR_FORBIDDEN};
get_scenario_specific_error_for_invalid_client(_ScenarioType, forbidden_in_space, Client) ->
    {Client, ?ERROR_FORBIDDEN}.


%% @private
get_client({Client, {error, _}}) -> Client;
get_client(Client) -> Client.


%% @private
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
    TestsPassed = run_scenarios(
        ScenarioTemplates, TargetNodes, CorrectClients, bad_data_sets(DataSpec),
        TestCaseFun
    ),
    TeardownFun(),

    TestsPassed.


%% @private
is_data_error_applicable_to_scenario({error, _}, _)                          -> true;
is_data_error_applicable_to_scenario({Scenario, _}, Scenario)                -> true;
is_data_error_applicable_to_scenario({rest_handler, _}, rest)                -> true;
is_data_error_applicable_to_scenario({rest_handler, _}, rest_with_file_path) -> true;
is_data_error_applicable_to_scenario({rest_handler, _}, rest_not_supported)  -> true;
is_data_error_applicable_to_scenario(_, _)                                   -> false.


%% @private
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
    RequiredDataSet = hd(required_data_sets(DataSpec)),

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
    TestsPassed = run_scenarios(
        ScenarioTemplates, TargetNodes, CorrectClients, IncompleteDataSetsAndErrors,
        TestCaseFun
    ),
    TeardownFun(),

    TestsPassed.


%% @private
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
run_expected_success_test_cases(Config, #suite_spec{
    target_nodes = TargetNodes,
    client_spec = #client_spec{correct = CorrectClients},

    setup_fun = SetupFun,
    teardown_fun = TeardownFun,
    verify_fun = VerifyFun,

    scenario_templates = ScenarioTemplates,
    randomly_select_scenarios = true,

    data_spec = DataSpec
}) ->
    CorrectDataSets = correct_data_sets(DataSpec),

    lists:foldl(fun(Client, OuterAcc) ->
        case filter_scenarios_available_for_client(Client, ScenarioTemplates) of
            [] ->
                OuterAcc;
            AvailableScenarios ->
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
        end
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

    run_scenarios(
        ScenarioTemplates, TargetNodes, CorrectClients, correct_data_sets(DataSpec),
        TestCaseFun
    ).


%% @private
run_scenarios(ScenarioTemplates, TargetNodes, Clients, DataSets, TestCaseFun) ->
    lists:foldl(fun(#scenario_template{type = ScenarioType} = ScenarioTemplate, PrevScenariosPassed) ->
        PrevScenariosPassed and lists:foldl(fun(Client, PrevClientsPassed) ->
            PrevClientsPassed and lists:foldl(fun(DataSet, PrevDataSetsPassed) ->
                TargetNode = lists_utils:random_element(TargetNodes),
                PrevDataSetsPassed and TestCaseFun(
                    TargetNode, Client, DataSet, ScenarioTemplate
                )
            end, true, DataSets)
        end, true, filter_available_clients(ScenarioType, Clients))
    end, true, ScenarioTemplates).


%% @private
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


% TODO VFS-6201 rm when connecting via gs as nobody becomes possible
%% @private
filter_available_clients(Type, Clients) when
    Type == gs;
    Type == gs_with_shared_guid_and_aspect_private;
    Type == gs_not_supported
->
    Clients -- [?NOBODY];
filter_available_clients(_ScenarioType, Clients) ->
    Clients.


% TODO VFS-6201 rm when connecting via gs as nobody becomes possible
%% @private
filter_scenarios_available_for_client(?NOBODY, ScenarioTemplates) ->
    lists:filter(fun(#scenario_template{type = Type}) ->
        not lists:member(Type, ?GS_SCENARIO_TYPES)
    end, ScenarioTemplates);
filter_scenarios_available_for_client(_, ScenarioTemplates) ->
    ScenarioTemplates.


%% @private
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
        client_to_atom(Client, Config),
        io_lib_pretty:print(Args, fun get_record_def/2),
        Expected,
        Got,
        ErrType, ErrReason,
        erlang:get_stacktrace()
    ]).


%% @private
client_to_atom(?NOBODY, _Config) ->
    nobody;
client_to_atom(?USER(UserId), Config) ->
    api_test_env:resolve_entity_name(UserId, Config).


%%%===================================================================
%%% Prepare data combinations functions
%%%===================================================================


% Returns data sets that are correct
correct_data_sets(undefined) ->
    [?NO_DATA];
correct_data_sets(DataSpec) ->
    RequiredDataSets = required_data_sets(DataSpec),

    AllRequiredParamsDataSet = case RequiredDataSets of
        [] -> #{};
        _ -> hd(RequiredDataSets)
    end,
    AllRequiredWithOptionalDataSets = lists:map(fun(OptionalDataSet) ->
        maps:merge(AllRequiredParamsDataSet, OptionalDataSet)
    end, optional_data_sets(DataSpec)),

    RequiredDataSets ++ AllRequiredWithOptionalDataSets.


% Generates all combinations of "required" params and one "at_least_one" param
required_data_sets(undefined) ->
    [?NO_DATA];
required_data_sets(DataSpec) ->
    #data_spec{
        required = Required,
        at_least_one = AtLeastOne
    } = DataSpec,

    AtLeastOneWithValues = lists:flatten(lists:map(
        fun(Key) ->
            [#{Key => Val} || Val <- get_correct_value(Key, DataSpec)]
        end, AtLeastOne)
    ),
    RequiredWithValues = lists:map(
        fun(Key) ->
            [#{Key => Val} || Val <- get_correct_value(Key, DataSpec)]
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


% Generates all combinations for optional params
optional_data_sets(undefined) ->
    [?NO_DATA];
optional_data_sets(#data_spec{optional = []}) ->
    [];
optional_data_sets(#data_spec{optional = Optional} = DataSpec) ->
    OptionalParamsWithValues = lists:flatten(lists:map(fun(Key) ->
        [#{Key => Val} || Val <- get_correct_value(Key, DataSpec)]
    end, Optional)),

    OptionalParamsCombinations = lists:usort(lists:foldl(fun(ParamWithValue, Acc) ->
        [maps:merge(Combination, ParamWithValue) || Combination <- Acc] ++ Acc
    end, [#{}], OptionalParamsWithValues)),

    lists:delete(#{}, OptionalParamsCombinations).


% Generates combinations of bad data sets by adding wrong values to
% correct data set (one set with correct values for all params).
bad_data_sets(undefined) ->
    [?NO_DATA];
bad_data_sets(#data_spec{
    required = Required,
    at_least_one = AtLeastOne,
    optional = Optional,
    bad_values = BadValues
} = DataSpec) ->
    AllCorrect = lists:foldl(fun(Param, Acc) ->
        Acc#{Param => hd(get_correct_value(Param, DataSpec))}
    end, #{}, Required ++ AtLeastOne ++ Optional),

    lists:map(fun({Param, InvalidValue, ExpError}) ->
        Data = AllCorrect#{Param => InvalidValue},
        {Data, Param, ExpError}
    end, BadValues).


% Converts correct value spec into a value
get_correct_value(Key, #data_spec{correct_values = CorrectValues}) ->
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
-spec is_client_supported_by_node(aai:auth(), node(), config()) ->
    boolean().
is_client_supported_by_node(?NOBODY, _Node, _Config) ->
    true;
is_client_supported_by_node(?USER(UserId), Node, Config) ->
    ProvId = op_test_rpc:get_provider_id(Node),
    lists:member(UserId, api_test_env:get_provider_eff_users(ProvId, Config)).


%% @private
-spec make_request(config(), node(), aai:auth(), rest_args() | gs_args()) ->
    {ok, Result :: term()} | {error, term()}.
make_request(Config, Node, Client, #rest_args{} = Args) ->
    make_rest_request(Config, Node, Client, Args);
make_request(Config, Node, Client, #gs_args{} = Args) ->
    make_gs_request(Config, Node, Client, Args).


%% @private
-spec make_gs_request(config(), node(), aai:auth(), gs_args()) ->
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
-spec connect_via_gs(node(), aai:auth(), config()) ->
    {ok, GsClient :: pid()} | errors:error().
connect_via_gs(_Node, ?NOBODY, _Config) ->
    % TODO VFS-6201 fix when connecting as nobody via gs becomes possible
    throw(fail);
connect_via_gs(Node, ?USER(UserId), Config) ->
    connect_via_gs(
        Node,
        ?SUB(user, UserId),
        {token, api_test_env:get_user_access_token(UserId, Config)},
        [{cacerts, op_test_rpc:get_cert_chain_pems(Node)}]
    ).


%% @private
-spec connect_via_gs(
    node(),
    aai:subject(),
    gs_protocol:client_auth(),
    ConnectionOpts :: proplists:proplist()
) ->
    {ok, GsClient :: pid()} | errors:error().
connect_via_gs(Node, ExpIdentity, Authorization, Opts) ->
    case gs_client:start_link(
        gs_endpoint(Node),
        Authorization,
        op_test_rpc:gs_protocol_supported_versions(Node),
        fun(_) -> ok end,
        Opts
    ) of
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
-spec make_rest_request(config(), node(), aai:auth(), rest_args()) ->
    {ok, RespCode :: non_neg_integer(), RespBody :: binary() | map()} |
    {error, term()}.
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
-spec get_rest_auth_headers(aai:auth(), config()) -> AuthHeaders :: map().
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
