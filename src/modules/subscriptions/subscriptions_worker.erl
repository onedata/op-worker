%%%--------------------------------------------------------------------
%%% @author Michal Żmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This worker manages subscriptions of the provider.
%%% Subscriptions ensure, that provider receives updates from the OZ.
%%% Updates concern the provider itself
%%% or the users that work with the provider.
%%% @end
%%%--------------------------------------------------------------------
-module(subscriptions_worker).
-author("Michal Zmuda").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include("modules/subscriptions/subscriptions.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_runner.hrl").

% Definitions of reconnect intervals for subscriptions websocket client.
-define(INITIAL_RECONNECT_INTERVAL, 1000).
-define(RECONNECT_INTERVAL_INCREASE_RATE, 2).
-define(MAX_RECONNECT_INTERVAL, timer:minutes(15)).

-export([init/1, handle/1, cleanup/0]).
-export([refresh_subscription/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends subscription renew message & updates subscription progress.
%% @end
%%--------------------------------------------------------------------
-spec refresh_subscription() -> ok.
refresh_subscription() ->
    {Missing, ResumeAt} = subscriptions:get_missing(),
    Users = subscriptions:get_users(),
    ?info("Subscription progress - last_seq: ~p, missing: ~p, users: ~p ",
        [ResumeAt, Missing, Users]),

    Message = json_utils:encode([
        {users, Users},
        {resume_at, ResumeAt},
        {missing, Missing}
    ]),

    ensure_connection_running(),
    subscription_wss:push(Message).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init([]) ->
    schedule_subscription_renew(),
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: healthcheck | start_provider_connection | refresh_subscription |
    {process_updates, Updates} | {'EXIT', pid(), ExitReason :: term()},
    Updates :: [#sub_update{}],
    Result :: nagios_handler:healthcheck_response() | ok | {ok, Response} |
    {error, Reason},
    Response :: term(),
    Reason :: term().
handle(healthcheck) ->
    case subscription_wss:healthcheck() of
        ok -> ok;
        %% Active connection to the OZ isn't required to assume worker is ok
        {error, Reason} -> ?warning("Connection error:~p", [Reason])
    end;

handle(ensure_connection_running) ->
    case whereis(subscription_wss) of
        undefined -> start_provider_connection();
        _ -> ok
    end;

handle(refresh_subscription) ->
    Self = node(),
    case subscriptions:get_refreshing_node() of
        {ok, Self} -> refresh_subscription();
        {ok, Node} ->
            ?debug("Pid ~p does not match dedicated ~p", [Self, Node]),
            ensure_connection_running()
    end,
    ok;

handle({process_updates, Updates}) ->
    utils:pforeach(fun(Update) -> handle_update(Update) end, Updates),
    Seqs = lists:map(fun(#sub_update{seq = Seq}) -> Seq end, Updates),
    subscriptions:account_updates(ordsets:from_list(Seqs)),
    ok;

handle({'EXIT', _Pid, _Reason} = Req) ->
    %% todo: ensure VFS-1877 is resolved (otherwise it probably isn't working)
    %% Handle possible websocket crashes
    case subscription_wss:healthcheck() of
        {error, _} -> ?error("Connection crashed: ~p", [_Reason]);
        _ -> ?log_bad_request(Req)
    end,
    ok;

handle(Req) ->
    ?log_bad_request(Req).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok.
cleanup() ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @private
%% Process update.
%% @end
%%--------------------------------------------------------------------
-spec handle_update(#sub_update{}) -> ok.
handle_update(#sub_update{ignore = true}) -> ok;
handle_update(#sub_update{delete = true, id = ID, model = Model}) ->
    subscription_conflicts:delete_model(Model, ID);
handle_update(#sub_update{model = Model, doc = Doc, revs = Revs}) ->
    subscription_conflicts:update_model(Model, Doc, Revs).

%%--------------------------------------------------------------------
%% @doc @private
%% Schedule renewing the subscription at fixed interval.
%% As connection check is performed during subscription renewals
%% are sufficient to maintain healthy connection.
%% @end
%%--------------------------------------------------------------------
-spec schedule_subscription_renew() -> ok.
schedule_subscription_renew() ->
    whereis(?MODULE) ! {timer, refresh_subscription},

    {ok, Seconds} = application:get_env(?APP_NAME,
        subscription_renew_seconds),

    {ok, _} = timer:send_interval(timer:seconds(Seconds), whereis(?MODULE),
        {timer, refresh_subscription}),
    ok.

%%--------------------------------------------------------------------
%% @doc @private
%% Ensures if websocket is running & registered.
%% @end
%%--------------------------------------------------------------------
-spec ensure_connection_running() -> ok | {error, Reason :: term()}.
ensure_connection_running() ->
    worker_proxy:call(?MODULE, ensure_connection_running).

%%--------------------------------------------------------------------
%% @doc @private
%% Attempts to start the connection. The procedure is wrapped in a global lock
%% and reattempted in increasing intervals to prevent the provider from
%% flooding OneZone with connection requests.
%% @end
%%--------------------------------------------------------------------
-spec start_provider_connection() -> ok.
start_provider_connection() ->
    critical_section:run(subscriptions_wss, fun() ->
        Result = try
            case subscription_wss:start_link() of
                {ok, Pid} ->
                    ?info("Subscriptions connection started ~p", [Pid]),
                    ok;
                {error, R1} ->
                    {error, R1}
            end
        catch
            E:R2 ->
                {E, R2}
        end,
        case Result of
            ok ->
                reset_reconnect_interval();
            {Type, Reason} ->
                Interval = increase_reconnect_interval(),
                ?error("Subscriptions connection failed to start - ~p:~p. "
                "Next retry not sooner than ~p seconds.", [
                    Type, Reason, round(Interval / 1000)
                ]),
                % Sleep, blocking the lock for some time - this ensures that
                % next attempt will occur after intended interval.
                timer:sleep(Interval)
        end
    end).


%%--------------------------------------------------------------------
%% @doc @private
%% Increases the reconnect interval according to RECONNECT_INTERVAL_INCREASE_RATE,
%% saves the new value to worker host state and returns the old interval.
%% @end
%%--------------------------------------------------------------------
-spec increase_reconnect_interval() -> integer().
increase_reconnect_interval() ->
    Interval = case worker_host:state_get(?MODULE, reconnect_interval) of
        undefined -> ?INITIAL_RECONNECT_INTERVAL;
        Value -> Value
    end,
    worker_host:state_put(?MODULE, reconnect_interval, min(
        Interval * ?RECONNECT_INTERVAL_INCREASE_RATE,
        ?MAX_RECONNECT_INTERVAL
    )),
    Interval.


%%--------------------------------------------------------------------
%% @doc @private
%% Resets the reconnect interval to its initial value.
%% @end
%%--------------------------------------------------------------------
-spec reset_reconnect_interval() -> ok.
reset_reconnect_interval() ->
    worker_host:state_put(?MODULE, reconnect_interval, ?INITIAL_RECONNECT_INTERVAL).
