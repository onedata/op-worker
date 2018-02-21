%% ====================================================================
%%% @author Jakub Kudzia
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%% ====================================================================
%%% @doc
%%% This module handles request that resolve provider id.
%%% @end
%% ====================================================================
-module(get_provider_id_handler).
-author("Jakub Kudzia").

-behaviour(cowboy_handler).

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([init/2]).


%%--------------------------------------------------------------------
%% @doc Cowboy handler callback.
%% Handles a request returning provider's id.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), term()) -> {ok, cowboy_req:req(), term()}.
init(#{method := <<"GET">>} = Req, State) ->
    Body = try
        oneprovider:get_id()
    catch _:_ ->
        <<"This provider is not registered yet">>
    end,
    NewReq = cowboy_req:reply(
        200, #{<<"content-type">> => <<"text/plain">>}, Body, Req
    ),
    {ok, NewReq, State};
init(Req, State) ->
    NewReq = cowboy_req:reply(405, #{<<"allow">> => <<"GET">>}, Req),
    {ok, NewReq, State}.
