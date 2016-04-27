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


-export([init/1, handle/1, cleanup/0]).

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
    schedule_connection_start(),
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: healthcheck | start_provider_connection |refresh_subscription |
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

handle(start_provider_connection) ->
    try
        subscriptions:ensure_initialised(),
        case subscription_wss:start_link() of
            {ok, Pid} ->
                ?info("Subscriptions connection started ~p", [Pid]);
            {error, Reason} ->
                ?error("Subscriptions connection failed to start: ~p", [Reason]),
                schedule_connection_start()
        end
    catch
        E:R ->
            ?error("Connection not started: ~p:~p", [E, R]),
            schedule_connection_start()
    end,
    ok;

handle(refresh_subscription) ->
    Self = node(),
    case subscriptions:get_refreshing_node() of
        {ok, Self} -> refresh_subscription();
        {ok, Node} -> ?debug("Pid ~p does not match dedicated ~p", [Self, Node])
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
        {error, _} ->
            ?error("Subscriptions connection crashed: ~p", [_Reason]),
            schedule_connection_start();
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
%% Send subscription renew message.
%% @end
%%--------------------------------------------------------------------
-spec refresh_subscription() -> ok.
refresh_subscription() ->
    {Missing, ResumeAt} = subscriptions:get_missing(),
    Users = subscriptions:get_users(),
    ?info("Subscription progress - last_seq: ~p, missing: ~p, users: ~p ", [
        ResumeAt, Missing, Users
    ]),

    Message = json_utils:encode([
        {users, Users},
        {resume_at, ResumeAt},
        {missing, Missing}
    ]),

    %% todo: remove once VFS-1877 is resolved (and exit handler does it's job)
    ensure_connection_running(),
    subscription_wss:push(Message).

%%--------------------------------------------------------------------
%% @doc @private
%% Schedule renewing the subscription at fixed interval.
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
%% Schedule restart of the provider - OZ link.
%% Restart is delayed as conditions which led to loosing the connection
%% may still apply.
%% @end
%%--------------------------------------------------------------------
-spec schedule_connection_start() -> ok.
schedule_connection_start() ->
    {ok, Delay} = application:get_env(?APP_NAME,
        subscriptions_connection_restart_interval),

    {ok, _} = timer:send_after(Delay, whereis(?MODULE),
        {timer, start_provider_connection}),
    ok.


%%--------------------------------------------------------------------
%% @doc @private
%% Ensures if websocket is running & registered.
%% @end
%%--------------------------------------------------------------------
-spec ensure_connection_running() -> ok | {error, Reason :: term()}.
ensure_connection_running() ->
    case whereis(subscription_wss) of
        undefined ->
            worker_proxy:call(subscriptions_worker, start_provider_connection);
        _ -> ok
    end.