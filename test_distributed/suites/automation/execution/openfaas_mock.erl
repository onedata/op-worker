%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements http server simulating OpenFaaS service.
%%% @end
%%%-------------------------------------------------------------------
-module(openfaas_mock).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("inets/include/httpd.hrl").

%% API
-export([
    start/1,
    get_config/0,
    update_config/1,
    stop/0,

    get_ip/0,
    get_port/0,
    get_admin_user/0,
    get_admin_password/0,
    get_function_namespace/0
]).

%% httpd callback
-export([do/1]).


-type config() :: #{
    health := ready | not_ready | {error, ErrorMsg :: binary()},
    docker_mocks_module := module()
}.

-export_type([config/0]).


-define(HTTP_SERVER_PORT, 8080).


%%%===================================================================
%%% API
%%%===================================================================


-spec start(config()) -> pid() | no_return().
start(Config) ->
    inets:start(),
    {ok, Pid} = inets:start(httpd, [
        {port, ?HTTP_SERVER_PORT},
        {server_name, "openfaas_mock"},
        {server_root, "/tmp"},
        {document_root, "/tmp"},
        {modules, [?MODULE]}
    ]),

    node_cache:put(openfaas_mock_config, Config),

    Pid.


-spec get_config() -> config().
get_config() ->
    node_cache:get(openfaas_mock_config).


-spec update_config(config()) -> ok.
update_config(ConfigDiff) ->
    node_cache:put(openfaas_mock_config, maps:merge(get_config(), ConfigDiff)).


-spec stop() -> ok.
stop() ->
    inets:stop().


-spec get_ip() -> binary().
get_ip() ->
    {ok, IpAddressBin} = ip_utils:to_binary(initializer:local_ip_v4()),
    IpAddressBin.


-spec get_port() -> non_neg_integer().
get_port() ->
    ?HTTP_SERVER_PORT.


-spec get_admin_user() -> binary().
get_admin_user() ->
    <<"admin">>.


-spec get_admin_password() -> binary().
get_admin_password() ->
    <<"password">>.


-spec get_function_namespace() -> binary().
get_function_namespace() ->
    <<"openfaas">>.


%%%===================================================================
%%% httpd callback
%%%===================================================================


do(Info = #mod{method = Method, request_uri = Path}) ->
    handle(str_utils:to_binary(Method), str_utils:to_binary(Path), Info).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-define(RESPONSE(__STATUS_CODE), ?RESPONSE(__STATUS_CODE, nobody)).
-define(RESPONSE(__STATUS_CODE, __BODY), [{response, {response, [{code, __STATUS_CODE}], __BODY}}]).

handle(<<"GET">>, <<"/healthz">>, _) ->
%%    httpd_response:send_body()
    case get_config() of
        #{health := ready} ->
            {proceed, ?RESPONSE(?HTTP_200_OK)};
        #{health := not_ready} ->
            {proceed, ?RESPONSE(?HTTP_503_SERVICE_UNAVAILABLE)};
        #{health := {error, ErrorMsg}} ->
            {proceed, ?RESPONSE(?HTTP_500_INTERNAL_SERVER_ERROR, [ErrorMsg])}
    end;

handle(<<"POST">>, <<"/system/functions">>, #mod{entity_body = Body}) ->
    % register function
    FunctionDefinition = json_utils:decode(Body),
    FunctionName = maps:get(<<"service">>, FunctionDefinition),
    DockerImage = maps:get(<<"image">>, FunctionDefinition),

    %% TODO test malformed request/error when registering openfaas fun
    node_cache:put({openfaas_fun, FunctionName}, DockerImage),
    {proceed, ?RESPONSE(?HTTP_202_ACCEPTED)};

handle(<<"GET">>, <<"/system/function/", FunctionName/binary>>, _) ->
    % get function
    case node_cache:get({openfaas_fun, FunctionName}, undefined) of
        undefined ->
            {proceed, ?RESPONSE(?HTTP_404_NOT_FOUND)};
        _ ->
            %% TODO test error when getting openfaas fun info
            Info = #{<<"availableReplicas">> => 1},
            {proceed, ?RESPONSE(?HTTP_200_OK, [json_utils:encode(Info)])}
    end;

handle(<<"DELETE">>, <<"/system/functions">>, #mod{entity_body = Body}) ->
    % deregister function
    Data = json_utils:decode(Body),
    node_cache:clear({openfaas_fun, maps:get(<<"functionName">>, Data)}),

    %% TODO test error when deleting openfaas fun
    {proceed, ?RESPONSE(?HTTP_200_OK)};

handle(<<"POST">>, <<"/async-function/", FunctionName/binary>>, #mod{
    parsed_header = Headers,
    entity_body = Body
}) ->
    % schedule function execution
    CallbackUrl = str_utils:to_binary(proplists:get_value("X-Callback-Url", Headers)),

    case node_cache:get({openfaas_fun, FunctionName}, undefined) of
        undefined ->
            {proceed, ?RESPONSE(?HTTP_404_NOT_FOUND)};
        DockerImage ->
            #{docker_mocks_module := Mod} = get_config(),
            spawn(fun() ->
                Result = try
                    Mod:exec(DockerImage, json_utils:decode(Body))
                catch _:_ ->
                    %% TODO error
                    ok
                end,

                Response = case is_map(Result) of
                    true -> json_utils:encode(Result);
                    false -> Result
                end,
                http_client:post(CallbackUrl, #{}, Response)
            end),

            %% TODO test malformed request/error when scheduling fun execution
            {proceed, ?RESPONSE(?HTTP_202_ACCEPTED)}
    end.
