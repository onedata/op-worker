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
    {ok, #{
        last_seq => 1, %% todo last_seq
        clients_expiry => #{}
    }}.

handle(healthcheck) ->
    case is_pid(global:whereis_name(subscriptions_bridge)) of
        true -> ok;
        false -> {error, subscriptions_bridge_not_running}
    end;

handle(renew) ->
    renew(),
    renew_users();

handle(Req) ->
    ?log_bad_request(Req).

cleanup() ->
    ok.

renew_users() ->
    Now = erlang:system_time(seconds),
    ExpiryMap = worker_host:state_get(?MODULE, clients_expiry),
    {ok, Sessions} = session:get_active(),
    lists:foreach(fun(#document{value = #session{
        identity = #identity{user_id = UserID}, auth = Auth}}) ->
        case maps:get(UserID, ExpiryMap, 0) of
            Expiry when Expiry < Now -> do_renew_user(Auth);
            _ -> ok
        end
    end, Sessions).


do_renew_user(#auth{macaroon = Macaroon, disch_macaroons = DMacaroons}) ->
    TTL = application:get_env(?APP_NAME, client_subscription_ttl_seconds, timer:minutes(5)),
    ?run(fun() ->
        URN = "/subscription",
        Data = json_utils:encode([
            {<<"ttl_seconds">>, TTL},
            {<<"provider">>, oneprovider:get_provider_id()}
        ]),

        Client = {user, {Macaroon, DMacaroons}},
        {ok, 204, _ResponseHeaders, _ResponseBody} =
            oz_endpoint:auth_request(Client, URN, post, Data),
        ok
    end).

update_expiry(UserID, ExpiresAtSeconds) ->
    worker_host:state_update(?MODULE, clients_expiry, fun(Map) ->
        maps:put(UserID, ExpiresAtSeconds, Map)
    end).

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