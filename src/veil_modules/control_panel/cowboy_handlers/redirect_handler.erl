%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module handles requests directed to http and returns a 301 redirect to https.
%% @end
%% ===================================================================
-module(redirect_handler).

-include_lib("ctool/include/logging.hrl").

-export([init/3, handle/2, terminate/3]).


%% init/3
%% ====================================================================
%% @doc Cowboy handler callback, no state is required
-spec init(any(), term(), any()) -> {ok, term(), []}.
%% ====================================================================
init(_Type, Req, _Opts) ->
	{ok, Req, []}.


%% handle/2
%% ====================================================================
%% @doc Handles a request returning a HTTP Redirect (301 - Moved permanently).
%% @end
-spec handle(term(), term()) -> {ok, term(), term()}.
%% ====================================================================
handle(Req, State) ->
	{Hostname, _} = cowboy_req:header(<<"host">>, Req),
	{Path, _} = cowboy_req:path(Req),
	{ok, Req2} = cowboy_req:reply(301, 
		[
			{<<"location">>, <<"https://", Hostname/binary, Path/binary>>},
			{<<"content-type">>, <<"text/html">>}
		], <<"">>, Req),
  	{ok, Req2, State}.


%% terminate/3
%% ====================================================================
%% @doc Cowboy handler callback, no cleanup needed
-spec terminate(term(), term(), term()) -> ok.
%% ====================================================================
terminate(_Reason, _Req, _State) ->
	ok.