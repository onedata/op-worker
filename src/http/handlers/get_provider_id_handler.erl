%% ====================================================================
%%% @author Jakub Kudzia
%%% @copyright (C): 2015, ACK CYFRONET AGH
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
-spec init(any(), term(), any()) -> {ok, term(), atom()}.
init(_Type, Req, _Opts) ->
    {ok, Req, []}.


%%--------------------------------------------------------------------
%% @doc
%% Handles a request producing an XML response
%% @end
%%--------------------------------------------------------------------
-spec handle(term(), term()) -> {ok, term(), term()}.
handle(Req, State) ->
    ProviderId = oneprovider:get_provider_id(),
    {ok, Req1} = cowboy_req:reply(
        200, [{<<"content-type">>, <<"text/plain">>}], [ProviderId], Req),
    {ok, Req1, State}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, no cleanup needed
%% @end
%%--------------------------------------------------------------------
-spec terminate(term(), term(), term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.
