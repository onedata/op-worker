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
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_runner.hrl").


-export([init/1, handle/1, cleanup/0]).

init([]) ->
    {ok, #{
        last_seq => 1 %% todo last_seq
    }}.

handle(healthcheck) ->
    case is_pid(global:whereis_name(subscriptions_bridge)) of
        true -> ok;
        false -> {error, subscriptions_bridge_not_running}
    end;

handle(renew) ->
    renew();

handle(Req) ->
    ?log_bad_request(Req).

cleanup() ->
    ok.


renew() ->
    ?run(fun() ->
        URN = "/subscription",
        Data = json_utils:encode([
            {<<"last_seq">>, get_seq()},
            {<<"endpoint">>, get_endpoint()}
        ]),
        {ok, 204, _ResponseHeaders, _ResponseBody} =
            oz_endpoint:auth_request(provider, URN, post, Data),
        ok
    end).

get_seq() ->
    worker_host:state_get(?MODULE, last_seq).

get_endpoint() ->
    Endpoint = worker_host:state_get(?MODULE, endpoint),
    case Endpoint of
        undefined ->
            IP = inet_parse:ntoa(oneprovider:get_node_ip()),
            Prepared = list_to_binary("https://" ++ IP ++ "/updates"),
            worker_host:state_put(?MODULE, endpoint, Prepared),
            Prepared;
        _ -> Endpoint
    end.