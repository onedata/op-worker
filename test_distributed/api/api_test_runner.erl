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
%%% @end
%%%-------------------------------------------------------------------
-module(api_test_runner).
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

-define(NO_DATA, undefined).
-define(GS_RESP(Result), #gs_resp_graph{data = Result}).


%%%===================================================================
%%% API
%%%===================================================================


-spec run_tests(config(), [scenario_spec() | suite_spec()]) ->
    boolean().
run_tests(Config, Specs) ->
    lists:foldl(fun
        (#scenario_spec{} = ScenarioSpec, AllTestsPassed) ->
            AllTestsPassed and run_scenario(Config, ScenarioSpec);
        (#suite_spec{} = SuiteSpec, AllTestsPassed) ->
            AllTestsPassed and run_suite(Config, SuiteSpec)
    end, true, Specs).


%%%===================================================================
%%% Run scenario test cases combinations functions
%%%===================================================================


run_suite(Config, #suite_spec{
    target_nodes = TargetNodes,
    client_spec = ClientSpec,

    setup_fun = EnvSetupFun,
    teardown_fun = EnvTeardownFun,
    verify_fun = EnvVerifyFun,

    scenario_templates = ScenarioSchemes,
    data_spec = DataSpec
}) ->
    lists:foldl(fun(#scenario_template{
        name = Name,
        type = Type,
        prepare_args_fun = PrepareArgsFun,
        validate_result_fun = ValidateCallResultFun
    }, AllScenariosPassed) ->
        AllScenariosPassed and run_scenario(Config, #scenario_spec{
            name = Name,
            type = Type,
            target_nodes = TargetNodes,
            client_spec = ClientSpec,
            setup_fun = EnvSetupFun,
            teardown_fun = EnvTeardownFun,
            verify_fun = EnvVerifyFun,
            prepare_args_fun = PrepareArgsFun,
            validate_result_fun = ValidateCallResultFun,
            data_spec = DataSpec
        })
    end, true, ScenarioSchemes).


run_scenario(Config, ScenarioSpec) ->
    try
        true
        and run_unauthorized_clients_test_cases(Config, ScenarioSpec)
        and run_forbidden_clients_test_cases(Config, ScenarioSpec)
        and run_malformed_data_test_cases(Config, ScenarioSpec)
        and run_expected_success_test_cases(Config, ScenarioSpec)
    catch
        throw:fail ->
            false;
        Type:Reason ->
            ct:pal("Unexpected error while running test scenario ~p:~p ~p", [
                Type, Reason, erlang:get_stacktrace()
            ]),
            false
    end.


run_unauthorized_clients_test_cases(Config, #scenario_spec{
    type = ScenarioType,
    client_spec = #client_spec{
        unauthorized = UnauthorizedClients,
        supported_clients_per_node = SupportedClientsPerNode
    },
    data_spec = DataSpec
} = ScenarioSpec) ->
    run_invalid_clients_test_cases(
        Config,
        SupportedClientsPerNode,
        ScenarioSpec,
        UnauthorizedClients,
        required_data_sets(DataSpec),
        case ScenarioType of
            rest_not_supported ->
                ?ERROR_NOT_SUPPORTED;
            gs_not_supported ->
                ?ERROR_NOT_SUPPORTED;
            rest_with_file_path ->
                ?ERROR_BAD_VALUE_IDENTIFIER(<<"urlFilePath">>);
            _ ->
                ?ERROR_UNAUTHORIZED
        end
    ).


run_forbidden_clients_test_cases(Config, #scenario_spec{
    type = ScenarioType,
    client_spec = #client_spec{
        forbidden = ForbiddenClients,
        supported_clients_per_node = SupportedClientsPerNode
    },
    data_spec = DataSpec
} = ScenarioSpec) ->
    run_invalid_clients_test_cases(
        Config,
        SupportedClientsPerNode,
        ScenarioSpec,
        ForbiddenClients,
        required_data_sets(DataSpec),
        case ScenarioType of
            rest_not_supported ->
                ?ERROR_NOT_SUPPORTED;
            gs_not_supported ->
                ?ERROR_NOT_SUPPORTED;
            rest_with_file_path ->
                ?ERROR_BAD_VALUE_IDENTIFIER(<<"urlFilePath">>);
            _ ->
                ?ERROR_FORBIDDEN
        end
    ).


run_invalid_clients_test_cases(Config, SupportedClientsPerNode, #scenario_spec{
    name = ScenarioName,
    type = ScenarioType,
    target_nodes = TargetNodes,

    setup_fun = EnvSetupFun,
    teardown_fun = EnvTeardownFun,
    verify_fun = EnvVerifyFun,

    prepare_args_fun = PrepareArgsFun
}, Clients, DataSets, Error) ->
    Env = EnvSetupFun(),

    Result = run_test_cases(
        ScenarioType,
        TargetNodes,
        Clients,
        DataSets,
        fun(TargetNode, Client, DataSet) ->
            ExpError = case is_client_supported_by_node(Client, TargetNode, SupportedClientsPerNode) of
                true -> Error;
                false -> ?ERROR_USER_NOT_SUPPORTED
            end,
            TestCaseCtx = #api_test_ctx{
                node = TargetNode,
                client = Client,
                env = Env,
                data = DataSet
            },
            Args = PrepareArgsFun(TestCaseCtx),
            Result = make_request(Config, TargetNode, Client, Args),
            try
                validate_error_result(ScenarioType, ExpError, Result),
                EnvVerifyFun(expected_failure, TestCaseCtx)
            catch _:_ ->
                log_failure(ScenarioName, TargetNode, Client, Args, ExpError, Result),
                false
            end
        end
    ),

    EnvTeardownFun(Env),
    Result.


run_malformed_data_test_cases(Config, #scenario_spec{
    name = ScenarioName,
    type = ScenarioType,
    target_nodes = TargetNodes,
    client_spec = #client_spec{
        correct = CorrectClients,
        supported_clients_per_node = SupportedClientsPerNode
    },

    setup_fun = EnvSetupFun,
    teardown_fun = EnvTeardownFun,
    verify_fun = EnvVerifyFun,

    prepare_args_fun = PrepareArgsFun,
    data_spec = DataSpec
}) ->
    Env = EnvSetupFun(),

    Result = run_test_cases(
        ScenarioType,
        TargetNodes,
        CorrectClients,
        bad_data_sets(DataSpec),
        fun
            (_TargetNode, _Client, ?NO_DATA) ->
                % operations not requiring any data cannot be tested against
                % malformed data
                true;
            (TargetNode, Client, {DataSet, _BadKey, Error}) ->
                case is_error_applicable_to_scenario(Error, ScenarioType) of
                    false ->
                        true;
                    true ->
                        ExpError = case is_client_supported_by_node(Client, TargetNode, SupportedClientsPerNode) of
                            false ->
                                ?ERROR_USER_NOT_SUPPORTED;
                            true ->
                                get_expected_malformed_data_error(Error, ScenarioType)
                        end,
                        TestCaseCtx = #api_test_ctx{
                            node = TargetNode,
                            client = Client,
                            env = Env,
                            data = DataSet
                        },
                        Args = PrepareArgsFun(TestCaseCtx),
                        Result = make_request(Config, TargetNode, Client, Args),
                        try
                            validate_error_result(ScenarioType, ExpError, Result),
                            EnvVerifyFun(expected_failure, TestCaseCtx)
                        catch _:_ ->
                            log_failure(ScenarioName, TargetNode, Client, Args, ExpError, Result),
                            false
                        end
                end
        end
    ),

    EnvTeardownFun(Env),
    Result.


is_error_applicable_to_scenario({error, _}, _)                          -> true;
is_error_applicable_to_scenario({rest_handler, _}, rest)                -> true;
is_error_applicable_to_scenario({rest_handler, _}, rest_with_file_path) -> true;
is_error_applicable_to_scenario({rest_handler, _}, rest_not_supported)  -> true;
is_error_applicable_to_scenario({rest, _}, rest)                        -> true;
is_error_applicable_to_scenario({rest, _}, rest_with_file_path)         -> true;
is_error_applicable_to_scenario({rest, _}, rest_not_supported)          -> true;
is_error_applicable_to_scenario({gs, _}, gs)                            -> true;
is_error_applicable_to_scenario({gs, _}, gs_not_supported)              -> true;
is_error_applicable_to_scenario(_, _)                                   -> false.


get_expected_malformed_data_error({rest_handler, RestHandlerSpecificError}, _) ->
    % Rest handler errors takes precedence other anything else as it is
    % thrown before request is even passed to middleware
    RestHandlerSpecificError;
get_expected_malformed_data_error(_, rest_not_supported) ->
    ?ERROR_NOT_SUPPORTED;
get_expected_malformed_data_error(_, gs_not_supported) ->
    ?ERROR_NOT_SUPPORTED;
get_expected_malformed_data_error({error, _} = Error, _) ->
    Error;
get_expected_malformed_data_error({_Type, TypeSpecificError}, _) ->
    TypeSpecificError.


run_expected_success_test_cases(Config, #scenario_spec{
    name = ScenarioName,
    type = ScenarioType,
    target_nodes = TargetNodes,
    client_spec = #client_spec{
        correct = CorrectClients,
        supported_clients_per_node = SupportedClientsPerNode
    },

    setup_fun = EnvSetupFun,
    teardown_fun = EnvTeardownFun,
    verify_fun = EnvVerifyFun,

    prepare_args_fun = PrepareArgsFun,
    validate_result_fun = ValidateResultFun,
    data_spec = DataSpec
}) ->
    run_test_cases(
        ScenarioType,
        TargetNodes,
        CorrectClients,
        correct_data_sets(DataSpec),
        fun(TargetNode, Client, DataSet) ->
            Env = EnvSetupFun(),
            TestCaseCtx = #api_test_ctx{
                node = TargetNode,
                client = Client,
                env = Env,
                data = DataSet
            },
            Args = PrepareArgsFun(TestCaseCtx),
            Result = make_request(Config, TargetNode, Client, Args),
            try
                case is_client_supported_by_node(Client, TargetNode, SupportedClientsPerNode) of
                    true ->
                        ValidateResultFun(TestCaseCtx, Result),
                        EnvVerifyFun(expected_success, TestCaseCtx);
                    false ->
                        validate_error_result(ScenarioType, ?ERROR_USER_NOT_SUPPORTED, Result),
                        EnvVerifyFun(expected_failure, TestCaseCtx)
                end
            catch _:_ ->
                log_failure(ScenarioName, TargetNode, Client, Args, succes, Result),
                false
            after
                EnvTeardownFun(Env)
            end
        end
    ).


run_test_cases(ScenarioType, TargetNodes, Clients, DataSets, TestCaseFun) ->
    lists:foldl(fun(TargetNode, OuterAcc) ->
        OuterAcc and lists:foldl(fun(Client, MiddleAcc) ->
            MiddleAcc and lists:foldl(fun(DataSet, InnerAcc) ->
                InnerAcc and TestCaseFun(TargetNode, Client, DataSet)
            end, true, DataSets)
        end, true, filter_available_clients(ScenarioType, Clients))
    end, true, TargetNodes).


filter_available_clients(Type, Clients) when
    Type == gs;
    Type == gs_not_supported
->
    % TODO VFS-6201 rm when connecting via gs as nobody becomes possible
    Clients -- [?NOBODY];
filter_available_clients(_ScenarioType, Clients) ->
    Clients.


validate_error_result(Type, ExpError, {ok, RespCode, RespBody}) when
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
    Type == gs_not_supported
->
    ?assertEqual(ExpError, Result).


log_failure(ScenarioName, TargetNode, Client, Args, Expected, Got) ->
    ct:pal("~s test case failed:~n"
    "Node: ~p~n"
    "Client: ~p~n"
    "Args: ~s~n"
    "Expected: ~p~n"
    "Got: ~p~n", [
        ScenarioName,
        TargetNode,
        aai:auth_to_printable(Client),
        io_lib_pretty:print(Args, fun get_record_def/2),
        Expected,
        Got
    ]).


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
-spec is_client_supported_by_node(aai:auth(), node(), #{node() => [aai:auth()]}) ->
    boolean().
is_client_supported_by_node(?NOBODY, _Node, _SupportedClientsPerNode) ->
    true;
is_client_supported_by_node(Client, Node, SupportedClientsPerNode) ->
    lists:member(Client, maps:get(Node, SupportedClientsPerNode)).


%% @private
-spec make_request(config(), node(), aai:auth(), rest_args() | gs_args()) ->
    {ok, Result :: map()} | {error, term()}.
make_request(Config, Node, Client, #rest_args{} = Args) ->
    make_rest_request(Config, Node, Client, Args);
make_request(Config, Node, Client, #gs_args{} = Args) ->
    make_gs_request(Config, Node, Client, Args).


%% @private
-spec make_gs_request(config(), node(), aai:auth(), gs_args()) ->
    {ok, Result :: map()} | {error, term()}.
make_gs_request(_Config, Node, Client, #gs_args{
    operation = Operation,
    gri = GRI,
    auth_hint = AuthHint,
    data = Data
}) ->
    case connect_via_gs(Node, Client) of
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
-spec connect_via_gs(node(), aai:auth()) ->
    {ok, GsClient :: pid()} | errors:error().
connect_via_gs(_Node, ?NOBODY) ->
    % TODO VFS-6201 fix when connecting as nobody via gs becomes possible
    throw(fail);
connect_via_gs(Node, ?USER(UserId)) ->
    connect_via_gs(
        Node,
        ?SUB(user, UserId),
        {token, initializer:create_access_token(UserId)},
        [{cacerts, rpc:call(Node, https_listener, get_cert_chain_pems, [])}]
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
        rpc:call(Node, gs_protocol, supported_versions, []),
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
    {ok, Domain} = test_utils:get_env(Node, ?APP_NAME, test_web_cert_domain),

    str_utils:join_as_binaries(
        ["wss://", Domain, Port, "/graph_sync/gui"],
        <<>>
    ).


%% @private
-spec make_rest_request(config(), node(), aai:auth(), rest_args()) ->
    {ok, RespCode :: non_neg_integer(), RespBody :: binary() | map()} |
    {error, term()}.
make_rest_request(_Config, Node, Client, #rest_args{
    method = Method,
    path = Path,
    headers = Headers,
    body = Body
}) ->
    URL = get_rest_endpoint(Node, Path),
    HeadersWithAuth = maps:merge(Headers, get_rest_auth_headers(Client)),
    CaCerts = rpc:call(Node, https_listener, get_cert_chain_pems, []),
    Opts = [{ssl_options, [{cacerts, CaCerts}]}],

    case http_client:request(Method, URL, HeadersWithAuth, Body, Opts) of
        {ok, RespCode, RespHeaders, RespBody} ->
            case maps:get(<<"content-type">>, RespHeaders, undefined) of
                <<"application/json">> ->
                    {ok, RespCode, json_utils:decode(RespBody)};
                _ ->
                    {ok, RespCode, RespBody}
            end;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec get_rest_auth_headers(aai:auth()) -> AuthHeaders :: map().
get_rest_auth_headers(?NOBODY) ->
    #{};
get_rest_auth_headers(?USER(UserId)) ->
    #{?HDR_X_AUTH_TOKEN => initializer:create_access_token(UserId)}.


%% @private
-spec get_rest_endpoint(node(), ResourcePath :: string() | binary()) ->
    URL :: binary().
get_rest_endpoint(Node, ResourcePath) ->
    Port = get_https_server_port_str(Node),
    {ok, Domain} = test_utils:get_env(Node, ?APP_NAME, test_web_cert_domain),

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
