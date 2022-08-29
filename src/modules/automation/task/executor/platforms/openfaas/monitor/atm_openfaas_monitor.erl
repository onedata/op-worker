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

-behaviour(gen_server).

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% API
-export([spec/0, start_link/0]).
-export([is_openfaas_available/0, assert_openfaas_available/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).


-define(SERVER, {global, ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================


-spec spec() -> supervisor:child_spec().
spec() -> #{
    id => ?MODULE,
    start => {?MODULE, start_link, []},
    restart => permanent,
    shutdown => timer:seconds(10),
    type => worker,
    modules => [?MODULE]
}.


-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link(?SERVER, ?MODULE, [], []).


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
%%% gen_server callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, state(), hibernate}.
init(_) ->
    process_flag(trap_exit, true),
    {ok, undefined, hibernate}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), hibernate}.
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {reply, {error, wrong_request}, State, hibernate}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), hibernate}.
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State, hibernate}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: term(), state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), hibernate}.
handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State, hibernate}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    state()) -> term().
terminate(_Reason, _State) ->
    ok.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, state(), Extra :: term()) ->
    {ok, NewState :: state()} | error().
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


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
