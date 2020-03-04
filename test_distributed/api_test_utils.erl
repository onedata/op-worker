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
-include("http/rest.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").

-export([run_scenarios/2]).

-define(NO_DATA, undefined).
-define(GS_RESP(Result), #gs_resp_graph{data = Result}).


%%%===================================================================
%%% API
%%%===================================================================


run_scenarios(Config, ScenariosSpecs) ->
    lists:foldl(fun(ScenarioSpec, AllScenariosPassed) ->
        AllScenariosPassed andalso try
            run_scenario(Config, ScenarioSpec)
        catch
            throw:fail ->
                false;
            Type:Reason ->
                ct:pal("Unexpected error while running test scenarios ~p:~p", [
                    Type, Reason
                ]),
                false
        end
    end, true, ScenariosSpecs).


%%%===================================================================
%%% Internal functions
%%%===================================================================


run_scenario(Config, ScenarioSpec) ->
    Res1 = run_unauthorized_test_cases(Config, ScenarioSpec),
    Res2 = run_forbidden_test_cases(Config, ScenarioSpec),
    Res3 = run_malformed_data_test_cases(Config, ScenarioSpec),

    Res1 andalso Res2 andalso Res3.


run_unauthorized_test_cases(Config, #scenario_spec{
    client_spec = #client_spec{unauthorized = UnauthorizedClients},
    params_spec = ParamsSpec
} = ScenarioSpec) ->
    run_invalid_clients_test_cases(
        Config,
        ScenarioSpec,
        UnauthorizedClients,
        required_data_sets(ParamsSpec),
        ?ERROR_UNAUTHORIZED
    ).


run_forbidden_test_cases(Config, #scenario_spec{
    client_spec = #client_spec{forbidden = ForbiddenClients},
    params_spec = ParamsSpec
} = ScenarioSpec) ->
    run_invalid_clients_test_cases(
        Config,
        ScenarioSpec,
        ForbiddenClients,
        required_data_sets(ParamsSpec),
        ?ERROR_FORBIDDEN
    ).


run_invalid_clients_test_cases(Config, #scenario_spec{
    type = ScenarioType,
    target_node = TargetNode,

    setup_fun = EnvSetupFun,
    teardown_fun = EnvTeardownFun,
    verify_fun = EnvVerifyFun,

    prepare_args_fun = PrepareArgsFun
}, Clients, DataSets, ExpError) ->
    Env = EnvSetupFun(),
    Result = lists:foldl(fun(Client, OuterAcc) ->
        OuterAcc andalso lists:foldl(fun(DataSet, InnerAcc) ->
                Args = PrepareArgsFun(Env, DataSet),
                Result = call_api(Config, TargetNode, Client, Args),
                InnerAcc andalso try
                    validate_error_result(ScenarioType, ExpError, Result),
                    EnvVerifyFun(false, Env, Args)
                catch _:_ ->
                    log_failure(TargetNode, Client, Args, ExpError, Result),
                    false
                end
        end, true, DataSets)
    end, true, Clients),
    EnvTeardownFun(Env),
    Result.


run_malformed_data_test_cases(Config, #scenario_spec{
    type = ScenarioType,
    target_node = TargetNode,
    client_spec = #client_spec{correct = CorrectClients},

    setup_fun = EnvSetupFun,
    teardown_fun = EnvTeardownFun,
    verify_fun = EnvVerifyFun,

    prepare_args_fun = PrepareArgsFun,
    params_spec = ParamsSpec
}) ->
    ct:pal("~p:~p", [?MODULE, ?LINE]),
    BadDataSets = bad_data_sets(ParamsSpec),

    Env = EnvSetupFun(),
    Result = lists:foldl(fun(Client, OuterAcc) ->
        OuterAcc andalso lists:foldl(fun
            % operations not requiring any data cannot be tested against
            % malformed data
            (?NO_DATA, InnerAcc) ->
                InnerAcc;
            ({DataSet, _BadKey, ExpError}, InnerAcc) ->
                Args = PrepareArgsFun(Env, DataSet),
                Result = call_api(Config, TargetNode, Client, Args),
                InnerAcc andalso try
                    validate_error_result(ScenarioType, ExpError, Result),
                    EnvVerifyFun(false, Env, Args)
                catch _:_ ->
                    log_failure(TargetNode, Client, Args, ExpError, Result),
                    false
                end
        end, true, BadDataSets)
    end, true, CorrectClients),
    EnvTeardownFun(Env),

    Result.


call_api(_Config, Node, Client, #rest_args{
    method = Method,
    path = Path,
    headers = Headers0,
    body = Body
}) ->
    URL = rest_endpoint(Node, Path),

    Headers = utils:ensure_defined(Headers0, undefined, #{}),
    HeadersWithAuth = case Client of
        nobody ->
            Headers;
        {user, UserId} ->
            Headers#{
                ?HDR_X_AUTH_TOKEN => initializer:create_access_token(UserId)
            }
    end,

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


validate_error_result(rest, ExpError, {ok, RespCode, RespBody}) ->
    ExpCode = errors:to_http_code(ExpError),
    ExpBody = #{<<"error">> => errors:to_json(ExpError)},

    ?assertEqual({ExpCode, ExpBody}, {RespCode, RespBody}).


log_failure(TargetNode, Client, Args, Expected, Got) ->
    ct:pal("API test case failed:~n"
    "Node: ~p~n"
    "Client: ~p~n"
    "Args: ~s~n"
    "Expected: ~p~n"
    "Got: ~p~n", [
        TargetNode,
        aai:auth_to_printable(client_to_aai_auth(Client)),
        io_lib_pretty:print(Args, fun get_record_def/2),
        Expected,
        Got
    ]).


%%%===================================================================
%%% Prepare data combinations
%%%===================================================================


% Generates all combinations of "required" and "at_least_one" data
required_data_sets(undefined) ->
    [?NO_DATA];
required_data_sets(DataSpec) ->
    #params_spec{
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
    #params_spec{
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
    #params_spec{
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
get_correct_value(Key, #params_spec{correct_values = CorrectValues}) ->
    case maps:get(Key, CorrectValues) of
        Fun when is_function(Fun, 0) ->
            Fun();
        Value ->
            Value
    end.


%%%===================================================================
%%% General use private functions
%%%===================================================================


rest_endpoint(Node, Path) ->
    Port = case get(port) of
        undefined ->
            {ok, P} = test_utils:get_env(Node, ?APP_NAME, https_server_port),
            PStr = case P of
                443 -> "";
                _ -> ":" ++ integer_to_list(P)
            end,
            put(port, PStr),
            PStr;
        P ->
            P
    end,
    {ok, Domain} = test_utils:get_env(Node, ?APP_NAME, test_web_cert_domain),

    str_utils:join_as_binaries(
        ["https://", Domain, Port, "/api/v3/oneprovider/", Path],
        <<>>
    ).


% Converts auth used in tests into aai auth
client_to_aai_auth(nobody) ->
    ?NOBODY;
client_to_aai_auth(root) ->
    ?ROOT;
client_to_aai_auth({user, UserId}) ->
    ?USER(UserId).


% Returns information about chosen records, such as fields,
% required to for example pretty print it
get_record_def(rest_args, N) ->
    case record_info(size, rest_args) - 1 of
        N ->
            record_info(fields, rest_args);
        _ ->
            no
    end;
get_record_def(params_spec, N) ->
    case record_info(size, params_spec) - 1 of
        N ->
            record_info(fields, params_spec);
        _ ->
            no
    end;
get_record_def(gri, N) ->
    case record_info(size, gri) - 1 of
        N ->
            record_info(fields, gri);
        _ ->
            no
    end;
get_record_def(_, _) ->
    no.
