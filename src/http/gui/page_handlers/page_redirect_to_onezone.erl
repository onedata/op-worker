%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This page redirects to the Oneprovider GUI served by Onezone.
%%% @end
%%%-------------------------------------------------------------------
-module(page_redirect_to_onezone).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/api_errors.hrl").

-export([handle/2]).
-export([redirect/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(gui:method(), cowboy_req:req()) -> cowboy_req:req().
handle(<<"GET">>, Req) ->
    redirect(Req, <<"/i">>).


%%--------------------------------------------------------------------
%% @doc
%% Redirects to Oneprovider GUI located in Onezone, provided that the cluster id
%% can be resolved.
%% @end
%%--------------------------------------------------------------------
-spec redirect(cowboy_req:req(), Path :: binary()) -> cowboy_req:req().
redirect(Req, Path) ->
    OzUrl = oneprovider:get_oz_url(),
    case provider_logic:get_cluster() of
        {ok, ClusterId} ->
            cowboy_req:reply(307, #{<<"location">> => str_utils:format_bin("~s/~s/~s~s", [
                OzUrl, onedata:service_shortname(?OP_WORKER), ClusterId, Path
            ])}, Req);
        ?ERROR_UNREGISTERED_PROVIDER ->
            cowboy_req:reply(200, #{
                <<"content-type">> => <<"text/plain">>
            }, <<"This Oneprovider instance is not yet configured.">>, Req);
        {error, _} ->
            cowboy_req:reply(307, #{
                <<"location">> => OzUrl
            }, Req)
    end.
