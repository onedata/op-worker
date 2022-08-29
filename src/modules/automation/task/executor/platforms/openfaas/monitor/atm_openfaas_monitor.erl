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
-export([
    is_openfaas_available/0,
    assert_openfaas_available/0,
    get_openfaas_status/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).


-type state() :: undefined.

-define(CHECK_STATUS_INTERVAL_SECONDS, op_worker:get_env(
    openfaas_status_check_interval_seconds, 60
)).

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
    case get_openfaas_status() of
        healthy -> true;
        _ -> false
    end.


-spec assert_openfaas_available() -> ok | no_return().
assert_openfaas_available() ->
    case get_openfaas_status() of
        healthy -> ok;
        unhealthy -> throw(?ERROR_ATM_OPENFAAS_UNHEALTHY);
        unreachable -> throw(?ERROR_ATM_OPENFAAS_UNREACHABLE);
        not_configured -> throw(?ERROR_ATM_OPENFAAS_NOT_CONFIGURED)
    end.


-spec get_openfaas_status() -> atm_openfaas_status:status().
get_openfaas_status() ->
    case atm_openfaas_status:get() of
        {ok, #document{value = #atm_openfaas_status{status = Status}}} -> Status;
        {error, not_found} -> not_configured
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
-spec init(Args :: term()) -> {ok, state()}.
init(_) ->
    process_flag(trap_exit, true),
    self() ! timeout,
    {ok, undefined}.


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
    {noreply, NewState :: state(), non_neg_integer()} |
    {noreply, NewState :: state(), hibernate}.
handle_info(timeout, State) ->
    atm_openfaas_status:save(check_openfaas_status()),
    {noreply, State, timer:seconds(?CHECK_STATUS_INTERVAL_SECONDS)};

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
    {ok, NewState :: state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_openfaas_status() -> atm_openfaas_status:status().
check_openfaas_status() ->
    try
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
    end.
