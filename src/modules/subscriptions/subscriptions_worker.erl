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

-define(STATE_KEY, <<"current_state">>).

-export([init/1, handle/1, cleanup/0]).

init([]) ->
    ensure_state_initialised(),
    {ok, #{}}.

handle(healthcheck) ->
    case is_pid(global:whereis_name(subscriptions_bridge)) of
        true -> ok;
        false -> {error, subscriptions_bridge_not_running}
    end;

handle(renew) ->
    renew();

handle({update, Updates}) ->
    utils:pforeach(fun(Update) -> handle_update(Update) end, Updates);

handle(Req) ->
    ?log_bad_request(Req).

cleanup() ->
    ok.

renew() ->
    URN = "/subscription",
    Data = json_utils:encode([
        {<<"last_seq">>, get_seq()},
        {<<"endpoint">>, get_endpoint()}
    ]),
    ?run(fun() ->
        {ok, 204, _ResponseHeaders, _ResponseBody} =
            oz_endpoint:auth_request(provider, URN, post, Data),
        ok
    end).

get_seq() ->
    {ok, #document{value = #subscriptions_state{last_seq = Seq}}} =
        subscriptions_state:get(?STATE_KEY),
    Seq.

get_endpoint() ->
    Endpoint = worker_host:state_get(?MODULE, endpoint),
    case Endpoint of
        undefined ->
            IP = inet_parse:ntoa(oneprovider:get_node_ip()),
            Port = integer_to_list(rest_listener:port()),
            Prepared = list_to_binary("https://" ++ IP ++ ":" ++ Port ++ "/updates"),
            worker_host:state_put(?MODULE, endpoint, Prepared),
            Prepared;
        _ -> Endpoint
    end.

ensure_state_initialised() ->
    subscriptions_state:create_or_update(#document{
        key = ?STATE_KEY,
        value = #subscriptions_state{
            last_seq = 1
        }
    }, fun(State) -> {ok, State} end).

update_last_seq(Seq) ->
    subscriptions_state:update(?STATE_KEY, fun
        (#subscriptions_state{last_seq = Curr} = State) when Curr < Seq ->
            {ok, State#subscriptions_state{last_seq = Seq}};
        (S) -> {ok, S}
    end).

handle_update({Doc, Type, Revs, Seq}) ->
    ?info("UPDATE ~p", [{Doc, Type, Revs, Seq}]), %% todo remove
    update_last_seq(Seq),
    ID = Doc#document.key,
    subscriptions_history:create_or_update(#document{
        key = {Type, ID},
        value = #subscriptions_history{revisions = Revs}
    }, fun(#subscriptions_history{revisions = Curr}) ->
        CurrRev = hd(Revs),
        case lists:member(CurrRev, Curr) of
            true -> {error, obsolete_version};
            false ->
                NotIncludedYet = Revs -- Curr,
                FinalRevs = NotIncludedYet ++ Curr,
                {ok, _} = Type:save(Doc),
                {ok, #subscriptions_history{revisions = FinalRevs}}
        end
    end).