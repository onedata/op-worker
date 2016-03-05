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
    try
%%        subscription_monitor:ensure_initialised(),
%%        subscription_wss:start_link(),
        {ok, #{}}
    catch
        E:R ->
            ?error("Failed to connect with OZ ~p:~p", [E, R]),
            {E, R}
    end.

handle(healthcheck) ->
    ok;

handle(renew) ->
    ok;

handle({update, Updates}) ->
    utils:pforeach(fun(Update) -> handle_update(Update) end, Updates);

handle(Req) ->
    ?log_bad_request(Req).

cleanup() ->
    ok.

handle_update({Doc, Type, Revs, Seq}) ->
    ?info("UPDATE ~p", [{Doc, Type, Revs, Seq}]),
    ok.