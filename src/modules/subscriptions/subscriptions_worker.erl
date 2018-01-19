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
-define(INITIAL_RECONNECT_INTERVAL_SEC, 2).
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
    ?debug("Subscription progress - last_seq: ~p, missing: ~p, users: ~p ",
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
    Request :: healthcheck | ensure_connection_running | refresh_subscription |
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
        {error, Reason} -> ?debug("Connection error:~p", [Reason])
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
            ensure_connection_running();
        {error, dispatcher_out_of_sync} ->
            % Possible error during node init - log only debug
            ?debug("Cannot refresh subscriptions: dispatcher_out_of_sync"),
            ok
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
%% Attempts to start a new WebSocket subscriptions connection to OneZone.
%% Check first if version of OneZone is supported and exit if not.
%% The procedure is wrapped in a global lock and reattempted in increasing
%% intervals to prevent the provider from flooding OneZone with
%% connection requests. If a reconnect is attempted during the grace period
%% (after failed connection), the function returns immediately.
%% @end
%%--------------------------------------------------------------------
-spec start_provider_connection() -> ok.
start_provider_connection() ->
    assert_zone_compatibility(),
    critical_section:run(subscriptions_wss, fun() ->
        case timestamp_in_seconds() >= get_next_reconnect() of
            false ->
                ?debug("Discarding subscriptions connection request as the "
                "grace period has not passed yet.");
            true ->
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
                        Interval = postpone_next_reconnect(),
                        ?debug("Subscriptions connection failed to start - ~p:~p. "
                        "Next retry not sooner than ~p seconds.", [
                            Type, Reason, Interval
                        ])
                end
        end
    end),
    ok.


%%--------------------------------------------------------------------
%% @doc @private
%% Check compatibility of Onezone and exit if it is not compatible.
%% @end
%%--------------------------------------------------------------------
-spec assert_zone_compatibility() -> ok | no_return().
assert_zone_compatibility() ->
    {ok, SupportedZoneVersions} = application:get_env(
        ?APP_NAME, supported_oz_versions
    ),
    {ok, Code, _RespHeaders, ResponseBody} = http_client:get(
        oneprovider:get_oz_url() ++ "/get_zone_version", #{}, <<>>, [insecure]
    ),
    case Code of
        200 ->
            ZoneVersion = binary_to_list(ResponseBody),
            case lists:member(ZoneVersion, SupportedZoneVersions) of
                false ->
                    ?critical("Exiting due to connection attempt with unsupported "
                              "version of Onezone: ~p. Supported ones: ~p", [
                        ZoneVersion, SupportedZoneVersions
                    ]),
                    init:stop();
                true ->
                    ok
            end;
        _ ->
            ?critical("Exiting due to inability to check Onezone version before "
                      "attempting conection."),
            init:stop()
    end.


%%--------------------------------------------------------------------
%% @doc @private
%% Postpones the time of next reconnect in an increasing manner,
%% according to RECONNECT_INTERVAL_INCREASE_RATE. Saves the 'next_reconnect'
%% time to worker proxy state.
%% @end
%%--------------------------------------------------------------------
-spec postpone_next_reconnect() -> integer().
postpone_next_reconnect() ->
    Interval = get_reconnect_interval(),
    set_next_reconnect(timestamp_in_seconds() + Interval),
    NewInterval = min(
        Interval * ?RECONNECT_INTERVAL_INCREASE_RATE,
        ?MAX_RECONNECT_INTERVAL
    ),
    set_reconnect_interval(NewInterval),
    Interval.


%%--------------------------------------------------------------------
%% @doc @private
%% Resets the reconnect interval to its initial value and next reconnect to
%% current time (which means next reconnect can be performed immediately).
%% @end
%%--------------------------------------------------------------------
-spec reset_reconnect_interval() -> ok.
reset_reconnect_interval() ->
    set_reconnect_interval(?INITIAL_RECONNECT_INTERVAL_SEC),
    set_next_reconnect(timestamp_in_seconds()).


%%--------------------------------------------------------------------
%% @doc @private
%% Retrieves reconnect interval from worker host state. It is an integer
%% value indicating duration in seconds of the grace period after a failed
%% connection attempt.
%% @end
%%--------------------------------------------------------------------
-spec get_reconnect_interval() -> integer().
get_reconnect_interval() ->
    case worker_host:state_get(?MODULE, reconnect_interval) of
        undefined -> ?INITIAL_RECONNECT_INTERVAL_SEC;
        Value -> Value
    end.


%%--------------------------------------------------------------------
%% @doc @private
%% Saves reconnect interval to worker host state.
%% @end
%%--------------------------------------------------------------------
-spec set_reconnect_interval(integer()) -> ok.
set_reconnect_interval(Interval) ->
    worker_host:state_put(?MODULE, reconnect_interval, Interval).


%%--------------------------------------------------------------------
%% @doc @private
%% Retrieves next reconnect time from worker host state. It indicates
%% when the grace period ends and new connection attempts can be made.
%% @end
%%--------------------------------------------------------------------
-spec get_next_reconnect() -> integer().
get_next_reconnect() ->
    case worker_host:state_get(?MODULE, next_reconnect) of
        undefined -> timestamp_in_seconds();
        Value -> Value
    end.


%%--------------------------------------------------------------------
%% @doc @private
%% Saves next reconnect time to worker host state.
%% @end
%%--------------------------------------------------------------------
-spec set_next_reconnect(integer()) -> ok.
set_next_reconnect(Timestamp) ->
    worker_host:state_put(?MODULE, next_reconnect, Timestamp).


%%--------------------------------------------------------------------
%% @doc @private
%% Returns current timestamp rounded to seconds.
%% @end
%%--------------------------------------------------------------------
-spec timestamp_in_seconds() -> integer().
timestamp_in_seconds() ->
    {MegaSeconds, Seconds, MicroSeconds} = erlang:timestamp(),
    MegaSeconds * 1000000 + Seconds + round(MicroSeconds / 1000000).

