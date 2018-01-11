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
-behaviour(cowboy_http_handler).

-include_lib("ctool/include/logging.hrl").

%% API
-export([init/3, handle/2, terminate/3]).

%% ====================================================================
%% Cowboy API functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback.
%% @end
%%--------------------------------------------------------------------
-spec init({TransportName :: atom(), ProtocolName :: http},
    Req :: cowboy_req:req(), Opts :: any()) ->
    {ok, cowboy_req:req(), []}.
init(_Type, Req, _Opts) ->
    {ok, Req, []}.


%%--------------------------------------------------------------------
%% @doc
%% Handles a request returning provider's id.
%% @end
%%--------------------------------------------------------------------
-spec handle(term(), term()) -> {ok, cowboy_req:req(), term()}.
handle(Req, State) ->
    {ok, IdentityMacaroon} = provider_auth:get_identity_macaroon(),
    {ok, NewReq} = cowboy_req:reply(
        200, [{<<"content-type">>, <<"text/plain">>}], IdentityMacaroon, Req),
    {ok, NewReq, State}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, no cleanup needed
%% @end
%%--------------------------------------------------------------------
-spec terminate(term(), term(), term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.
