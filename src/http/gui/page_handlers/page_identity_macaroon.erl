%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when identity macaroon page is visited.
%%% @end
%%%-------------------------------------------------------------------
-module(page_identity_macaroon).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include("http/rest/http_status.hrl").

-export([handle/2]).

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
    {ok, IdentityMacaroon} = provider_auth:get_identity_macaroon(),
    cowboy_req:reply(
        ?HTTP_200_OK,
        #{<<"content-type">> => <<"text/plain">>},
        IdentityMacaroon,
        Req
    ).
