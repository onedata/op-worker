%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for monitoring status of OpenFaaS service.
%%% If its status transitions from 'healthy' to any other status and remains
%%% as such for prolonged period of time (grace attempts), then OpenFaaS will
%%% be considered as down. This will be reported to atm workflow execution layer.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_monitor).
-author("Bartosz Walkowicz").

-behaviour(gen_server).

-include("modules/automation/atm_execution.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% API
-export([setup_internal_service/0, start_link/0]).
-export([
    is_openfaas_healthy/0,
    assert_openfaas_healthy/0,
    get_openfaas_status/0
]).

%% Internal Service callbacks
-export([start_service/0, stop_service/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3
]).


-type status() :: not_configured | unreachable | unhealthy | healthy.
-type grace_attempts() :: undefined | non_neg_integer().

-record(state, {
    status :: status(),
    grace_attempts = undefined :: grace_attempts()
}).
-type state() :: #state{}.

-export_type([status/0]).


-define(STATUS_CHECK_INTERVAL_SECONDS, op_worker:get_env(
    openfaas_status_check_interval_seconds, 60
)).
-define(STATUS_CHECK_GRACE_ATTEMPTS, op_worker:get_env(
    openfaas_status_check_grace_attempts, 5
)).

-define(REPORT_OPENFAAS_DOWN_TO_ATM_WORKFLOW_EXECUTION_LAYER,
    report_openfaas_down_to_atm_workflow_execution_layer
).
-define(REPORT_OPENFAAS_DOWN_TO_ATM_WORKFLOW_EXECUTION_RETRY_INTERVAL, op_worker:get_env(
    report_openfaas_down_to_atm_workflow_execution_layer_retry_interval, 10000
)).

-define(SERVER, {global, ?MODULE}).

-define(SERVICE_NAME, <<"atm-openfaas-monitor-watcher-service">>).


%%%===================================================================
%%% API
%%%===================================================================


-spec setup_internal_service() -> ok.
setup_internal_service() ->
    ok = internal_services_manager:start_service(?MODULE, ?SERVICE_NAME, ?SERVICE_NAME, #{
        start_function => start_service,
        stop_function => stop_service
    }).


-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link(?SERVER, ?MODULE, [], []).


-spec is_openfaas_healthy() -> boolean().
is_openfaas_healthy() ->
    case get_openfaas_status() of
        healthy -> true;
        _ -> false
    end.


-spec assert_openfaas_healthy() -> ok | no_return().
assert_openfaas_healthy() ->
    case get_openfaas_status() of
        healthy -> ok;
        DownStatus -> throw(down_status_to_error(DownStatus))
    end.


-spec get_openfaas_status() -> status().
get_openfaas_status() ->
    case atm_openfaas_status_cache:get() of
        {ok, #document{value = #atm_openfaas_status_cache{status = Status}}} ->
            Status;
        {error, not_found} ->
            check_openfaas_status()
    end.


%%%===================================================================
%%% Internal services API
%%%===================================================================


-spec start_service() -> ok | abort.
start_service() ->
    Spec = #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => timer:seconds(10),
        type => worker,
        modules => [?MODULE]
    },
    case catch supervisor:start_child(?FSLOGIC_WORKER_SUP, Spec) of
        {ok, _} ->
            ok;
        Error ->
            ?debug("Failed to start atm_openfaas_monitor due to: ~p", [Error]),
            abort
    end.


-spec stop_service() -> ok.
stop_service() ->
    ok = supervisor:terminate_child(?FSLOGIC_WORKER_SUP, ?MODULE).


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

    Status = check_openfaas_status(),
    atm_openfaas_status_cache:save(Status),

    State = #state{status = Status, grace_attempts = undefined},
    {ok, State, timer:seconds(?STATUS_CHECK_INTERVAL_SECONDS)}.


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
handle_info(timeout, State = #state{status = CurrentStatus}) ->
    NewStatus = check_openfaas_status(),
    NewStatus /= CurrentStatus andalso atm_openfaas_status_cache:save(NewStatus),

    NewGraceAttempts = case infer_grace_attempts(NewStatus, State) of
        0 ->
            report_openfaas_down_to_atm_workflow_execution_layer(NewStatus),
            undefined;
        Attempts ->
            Attempts
    end,

    NewState = #state{status = NewStatus, grace_attempts = NewGraceAttempts},
    {noreply, NewState, timer:seconds(?STATUS_CHECK_INTERVAL_SECONDS)};

handle_info(?REPORT_OPENFAAS_DOWN_TO_ATM_WORKFLOW_EXECUTION_LAYER, State = #state{
    status = CurrentStatus
}) ->
    report_openfaas_down_to_atm_workflow_execution_layer(CurrentStatus),
    {noreply, State, timer:seconds(?STATUS_CHECK_INTERVAL_SECONDS)};

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
-spec down_status_to_error(status()) -> errors:error().
down_status_to_error(unhealthy) -> ?ERROR_ATM_OPENFAAS_UNHEALTHY;
down_status_to_error(unreachable) -> ?ERROR_ATM_OPENFAAS_UNREACHABLE;
down_status_to_error(not_configured) -> ?ERROR_ATM_OPENFAAS_NOT_CONFIGURED.


%% @private
-spec check_openfaas_status() -> status().
check_openfaas_status() ->
    try
        OpenfaasConfig = atm_openfaas_config:get(),

        % /healthz is proper Openfaas endpoint defined in their swagger:
        % https://raw.githubusercontent.com/openfaas/faas/master/api-docs/swagger.yml
        Endpoint = atm_openfaas_config:get_endpoint(OpenfaasConfig, <<"/healthz">>),
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


%% @private
-spec infer_grace_attempts(NewStatus :: status(), state()) -> grace_attempts().
infer_grace_attempts(healthy, _State) ->
    undefined;

infer_grace_attempts(_NewStatus, #state{status = healthy}) ->
    max(0, ?STATUS_CHECK_GRACE_ATTEMPTS);

infer_grace_attempts(_NewStatus, #state{grace_attempts = undefined}) ->
    undefined;

infer_grace_attempts(_NewStatus, #state{grace_attempts = GraceAttempts}) ->
    GraceAttempts - 1.


%% @private
-spec report_openfaas_down_to_atm_workflow_execution_layer(status()) -> ok.
report_openfaas_down_to_atm_workflow_execution_layer(Status) ->
    try provider_logic:get_spaces() of
        {ok, SpaceIds} ->
            Error = down_status_to_error(Status),
            lists:foreach(fun(SpaceId) ->
                atm_workflow_execution_api:report_openfaas_down(SpaceId, Error)
            end, SpaceIds);
        ?ERROR_UNREGISTERED_ONEPROVIDER ->
            schedule_openfaas_down_report_to_atm_workflow_execution_layer();
        ?ERROR_NO_CONNECTION_TO_ONEZONE ->
            schedule_openfaas_down_report_to_atm_workflow_execution_layer();
        Error = {error, _} ->
            ?error(
                "Unable to report OpenFaaS down to atm workflow execution layer due to: ~p",
                [Error]
            )
    catch Type:Reason:Stacktrace ->
        ?error_stacktrace(
            "Unable to report OpenFaaS down to atm workflow execution layer due to ~w:~p",
            [Type, Reason],
            Stacktrace
        )
    end.


%% @private
-spec schedule_openfaas_down_report_to_atm_workflow_execution_layer() -> reference().
schedule_openfaas_down_report_to_atm_workflow_execution_layer() ->
    erlang:send_after(
        ?REPORT_OPENFAAS_DOWN_TO_ATM_WORKFLOW_EXECUTION_RETRY_INTERVAL,
        self(),
        ?REPORT_OPENFAAS_DOWN_TO_ATM_WORKFLOW_EXECUTION_LAYER
    ).
