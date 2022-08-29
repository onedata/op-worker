%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_monitor).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% API
-export([is_openfaas_available/0, assert_openfaas_available/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec is_openfaas_available() -> boolean().
is_openfaas_available() ->
    case check_openfaas_status() of
        healthy -> true;
        _ -> false
    end.


-spec assert_openfaas_available() -> ok | no_return().
assert_openfaas_available() ->
    case check_openfaas_status() of
        healthy -> ok;
        unhealthy -> throw(?ERROR_ATM_OPENFAAS_UNHEALTHY);
        unreachable -> throw(?ERROR_ATM_OPENFAAS_UNREACHABLE);
        _ -> throw(?ERROR_ATM_OPENFAAS_NOT_CONFIGURED)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_openfaas_status() -> atm_openfaas_status:status().
check_openfaas_status() ->
    {ok, Result} = node_cache:acquire(?FUNCTION_NAME, fun() ->
        HealthcheckResult = try
            OpenfaasConfig = atm_openfaas_config:get(),

            % /healthz is proper Openfaas endpoint defined in their swagger:
            % https://raw.githubusercontent.com/openfaas/faas/master/api-docs/swagger.yml
            Endpoint = atm_openfaas_config:get_openfaas_endpoint(OpenfaasConfig, <<"/healthz">>),
            Headers = atm_openfaas_config:get_basic_auth_header(OpenfaasConfig),

            case http_client:get(Endpoint, Headers) of
                {ok, ?HTTP_200_OK, _RespHeaders, _RespBody} ->
                    healthy;
                {ok, ?HTTP_500_INTERNAL_SERVER_ERROR, _RespHeaders, _RespBody} ->
                    unhealthy;
                _ ->
                    unreachable
            end
        catch throw:?ERROR_ATM_OPENFAAS_NOT_CONFIGURED ->
            not_configured
        end,
        {ok, HealthcheckResult, 60}  %% TODO rm
    end),
    Result.
