%%%--------------------------------------------------------------------
%%% @author Michal Żmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% @end
%%%--------------------------------------------------------------------
-module(subscriptions_worker).
-author("Michal Zmuda").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_runner.hrl").


-export([init/1, handle/1, cleanup/0]).

init([]) ->
    schedule_connection_start(),
    {ok, #{}}.

handle(healthcheck) ->
    case whereis(subscription_wss) of
        undefined -> ?warning("Subscriptions connection not running"), ok;
        _ -> ok
    end;

handle(start_provider_connection) ->
    try
        subscription_monitor:ensure_initialised(),
        case subscription_wss:start_link() of
            {ok, Pid} ->
                ?info("Subscriptions connection started ~p", [Pid]);
            {error, Reason} ->
                ?error("Subscriptions connection failed to start: ~p", [Reason]),
                schedule_connection_start()
        end
    catch
        E:R ->
            ?error("Subscriptions connection not started: ~p:~p", [E, R]),
            schedule_connection_start()
    end;

handle(renew) ->
    ok;

handle({update, Updates}) ->
    utils:pforeach(fun(Update) -> handle_update(Update) end, Updates);

%% Handle stream crashes
handle({'EXIT', _Pid, _Reason} = Req) ->
    case whereis(subscription_wss) of
        undefined ->
            ?error("Subscriptions connection crashed: ~p", [_Reason]),
            schedule_connection_start();
        _ -> ?log_bad_request(Req)
    end;

handle(Req) ->
    ?log_bad_request(Req).

cleanup() ->
    ok.

handle_update({Doc, Type, Revs, Seq}) ->
    ?info("UPDATE ~p", [{Doc, Type, Revs, Seq}]),
    ok.


schedule_connection_start() ->
    timer:send_after(timer:seconds(5), whereis(?MODULE), {timer, start_provider_connection}).