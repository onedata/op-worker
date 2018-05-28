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

-export([handle/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(new_gui:method(), cowboy_req:req()) -> cowboy_req:req().
handle(<<"GET">>, Req) ->
    {ok, IdentityMacaroon} = provider_auth:get_identity_macaroon(),
    cowboy_req:reply(200,
        #{<<"content-type">> => <<"text/plain">>},
        IdentityMacaroon,
        Req
    ).
