%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Utility functions used in API (REST + gs) tests.
%%% @end
%%%-------------------------------------------------------------------
-module(api_test_utils).
-author("Bartosz Walkowicz").

-include("api_test_utils.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").

-export([run_scenarios/2]).

-type config() :: proplists:proplist().

-define(NO_DATA, undefined).
-define(GS_RESP(Result), #gs_resp_graph{data = Result}).


%%%===================================================================
%%% API
%%%===================================================================


run_scenarios(Config, ScenariosSpecs) ->
    lists:foldl(fun(ScenarioSpec, AllScenariosPassed) ->
        AllScenariosPassed and try
            run_scenario(Config, ScenarioSpec)
        catch
            throw:fail ->
                false;
            Type:Reason ->
                ct:pal("Unexpected error while running test scenarios ~p:~p ~p", [
                    Type, Reason, erlang:get_stacktrace()
                ]),
                false
        end
    end, true, ScenariosSpecs).


%%%===================================================================
%%% Run scenario test cases combinations functions
%%%===================================================================


run_scenario(Config, ScenarioSpec) ->
    true
    and run_unauthorized_clients_test_cases(Config, ScenarioSpec)
    and run_forbidden_clients_test_cases(Config, ScenarioSpec)
    and run_malformed_data_test_cases(Config, ScenarioSpec)
    and run_expected_success_test_cases(Config, ScenarioSpec).


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
                EnvVerifyFun(false, Env, DataSet)
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
                ExpError = case is_client_supported_by_node(Client, TargetNode, SupportedClientsPerNode) of
                    true ->
                        case ScenarioType of
                            rest_not_supported -> ?ERROR_NOT_SUPPORTED;
                            _ -> Error
                        end;
                    false ->
                        ?ERROR_USER_NOT_SUPPORTED
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
                    EnvVerifyFun(false, Env, DataSet)
                catch _:_ ->
                    log_failure(ScenarioName, TargetNode, Client, Args, ExpError, Result),
                    false
                end
        end
    ),

    EnvTeardownFun(Env),
    Result.


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
                        EnvVerifyFun(true, Env, DataSet);
                    false ->
                        validate_error_result(ScenarioType, ?ERROR_USER_NOT_SUPPORTED, Result),
                        EnvVerifyFun(false, Env, DataSet)
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


filter_available_clients(gs, Clients) ->
    % TODO rm when connecting via gs as nobody becomes possible
    Clients -- [nobody];
filter_available_clients(_ScenarioType, Clients) ->
    Clients.


make_request(Config, Node, Client, #rest_args{} = Args) ->
    make_rest_request(Config, Node, Client, Args);
make_request(Config, Node, Client, #gs_args{} = Args) ->
    make_gs_request(Config, Node, Client, Args).


validate_error_result(Type, ExpError, {ok, RespCode, RespBody}) when
    Type == rest;
    Type == rest_with_file_path;
    Type == rest_not_supported
->
    ExpCode = errors:to_http_code(ExpError),
    ExpBody = #{<<"error">> => errors:to_json(ExpError)},

    ?assertEqual({ExpCode, ExpBody}, {RespCode, RespBody});

validate_error_result(gs, ExpError, Result) ->
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
        aai:auth_to_printable(client_to_auth(Client)),
        io_lib_pretty:print(Args, fun get_record_def/2),
        Expected,
        Got
    ]).


%%%===================================================================
%%% Prepare data combinations functions
%%%===================================================================


% Generates all combinations of "required" and "at_least_one" data
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


% Data sets wih required params and one or all optional params
% (e.g. returns 5 data sets for 4 optional params).
optional_data_sets(undefined, _) ->
    [?NO_DATA];
optional_data_sets(DataSpec, RequiredWithAll) ->
    #data_spec{
        optional = Optional
    } = DataSpec,

    OptionalWithValues = lists:flatten(lists:map(
        fun(Key) ->
            [#{Key => Val} || Val <- get_correct_value(Key, DataSpec)]
        end, Optional)
    ),
    RequiredWithOneOptional = lists:map(
        fun(OneOptionalMap) ->
            maps:merge(OneOptionalMap, RequiredWithAll)
        end, OptionalWithValues
    ),
    AllOptionalsMap = lists:foldl(fun maps:merge/2, #{}, OptionalWithValues),
    RequiredWithAllOptional = maps:merge(RequiredWithAll, AllOptionalsMap),

    case Optional of
        [] -> [];
        [_] -> RequiredWithOneOptional;
        _ -> [RequiredWithAllOptional | RequiredWithOneOptional]
    end.


% Returns all data sets that are correct
correct_data_sets(undefined) ->
    [?NO_DATA];
correct_data_sets(DataSpec) ->
    RequiredDataSets = required_data_sets(DataSpec),
    AllRequired = case RequiredDataSets of
        [] -> #{};
        _ -> hd(RequiredDataSets)
    end,
    OptionalDataSets = optional_data_sets(DataSpec, AllRequired),
    RequiredDataSets ++ OptionalDataSets.


% Generates all combinations of bad data sets by adding wrong values to
% correct data sets.
bad_data_sets(undefined) ->
    [?NO_DATA];
bad_data_sets(DataSpec) ->
    #data_spec{
        required = Required,
        at_least_one = AtLeastOne,
        optional = Optional,
        bad_values = BadValues
    } = DataSpec,
    AllCorrect = maps:from_list(lists:map(fun(Key) ->
        {Key, hd(get_correct_value(Key, DataSpec))}
    end, Required ++ AtLeastOne ++ Optional)),
    lists:map(
        fun({Key, Value, ErrorType}) ->
            Data = AllCorrect#{Key => Value},
            {Data, Key, ErrorType}
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
-spec is_client_supported_by_node(client(), node(), #{node() => [client()]}) ->
    boolean().
is_client_supported_by_node(nobody, _Node, _SupportedClientsPerNode) ->
    true;
is_client_supported_by_node(Client, Node, SupportedClientsPerNode) ->
    lists:member(Client, maps:get(Node, SupportedClientsPerNode)).


%% @private
-spec make_gs_request(config(), node(), client(), #gs_args{}) ->
    {ok, Result :: map()} | {error, term()}.
make_gs_request(_Config, Node, Client, #gs_args{
    operation = Operation,
    gri = GRI,
    subscribe = Subscribe,
    auth_hint = AuthHint,
    data = Data
}) ->
    case connect_via_gs(Node, Client) of
        {ok, GsClient} ->
            case gs_client:graph_request(GsClient, GRI, Operation, Data, Subscribe, AuthHint) of
                {ok, ?GS_RESP(Result)} ->
                    {ok, Result};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec connect_via_gs(node(), client()) ->
    {ok, GsClient :: pid()} | errors:error().
connect_via_gs(_Node, nobody) ->
    % TODO fix when connecting as nobody via gs becomes possible
    throw(fail);
connect_via_gs(Node, {user, UserId}) ->
    AccessToken = initializer:create_access_token(UserId),
    CaCerts = rpc:call(Node, https_listener, get_cert_chain_pems, []),
    connect_via_gs(
        Node,
        ?SUB(user, UserId),
        {token, AccessToken},
        [{cacerts, CaCerts}]
    ).


%% @private
-spec connect_via_gs(node(), aai:subject(), client(), ConnectionOpts :: proplists:proplist()) ->
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
-spec make_rest_request(config(), node(), client(), #rest_args{}) ->
    {ok, RespCode :: non_neg_integer(), RespBody :: binary() | map()} |
    {error, term()}.
make_rest_request(_Config, Node, Client, #rest_args{
    method = Method,
    path = Path,
    headers = Headers,
    body = Body
}) ->
    URL = get_rest_endpoint(Node, Path),
    HeadersWithAuth = maps:merge(
        utils:ensure_defined(Headers, undefined, #{}),
        get_auth_headers(Client)
    ),
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
-spec get_auth_headers(client()) -> AuthHeaders :: map().
get_auth_headers(nobody) ->
    #{};
get_auth_headers({user, UserId}) ->
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


%% @private
-spec client_to_auth(client()) -> aai:auth().
client_to_auth(nobody) ->
    ?NOBODY;
client_to_auth(root) ->
    ?ROOT;
client_to_auth({user, UserId}) ->
    ?USER(UserId).


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
