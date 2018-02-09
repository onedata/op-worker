%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles HTTP requests to retrieve provider's identity macaroon.
%%% @end
%%%-------------------------------------------------------------------
-module(get_identity_macaroon_handler).
-author("Lukasz Opiola").

-behaviour(cowboy_handler).

-include_lib("ctool/include/logging.hrl").

%% API
-export([init/2]).


%%--------------------------------------------------------------------
%% @doc Cowboy handler callback.
%% Handles a request returning provider's identity macaroon.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), term()) -> {ok, cowboy_req:req(), term()}.
init(#{method := <<"GET">>} = Req, State) ->
    {ok, IdentityMacaroon} = provider_auth:get_identity_macaroon(),
    NewReq = cowboy_req:reply(
        200, #{<<"content-type">> => <<"text/plain">>}, IdentityMacaroon, Req),
    {ok, NewReq, State};
init(Req, State) ->
    NewReq = cowboy_req:reply(405, #{<<"allow">> => <<"GET">>}, Req),
    {ok, NewReq, State}.
