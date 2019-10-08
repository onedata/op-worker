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

-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/onedata.hrl").
-include_lib("ctool/include/errors.hrl").

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
    case oneprovider:get_id_or_undefined() of
        undefined ->
            cowboy_req:reply(?HTTP_200_OK, #{
                <<"content-type">> => <<"text/plain">>
            }, <<"This Oneprovider instance is not yet configured.">>, Req);
        ProviderId ->
            cowboy_req:reply(?HTTP_302_FOUND, #{
                <<"location">> => str_utils:format_bin("~s/~s/~s~s", [
                    OzUrl, onedata:gui_prefix(?OP_WORKER_GUI), ProviderId, Path
                ]),
                <<"cache-control">> => <<"max-age=3600">>
            }, Req)
    end.
