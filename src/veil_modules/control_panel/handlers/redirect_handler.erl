%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module requests directed to http:// and returns a 304 redirect to https://.
%% @end
%% ===================================================================
-module(nagios_handler).

-include("logging.hrl").

-export([init/3, handle/2, terminate/3]).


%% init/3
%% ====================================================================
%% @doc Cowboy handler callback, no state is required
-spec init(any(), term(), any()) -> {ok, term(), []}.
%% ====================================================================
init(_Type, Req, _Opts) ->
	?dump(cowboy_req:get_binding(path, Req)),
	?dump(cowboy_req:get_path(Req)),
	?dump(cowboy_req:get_path_info(Req)),
	{ok, Req, []}.


%% handle/2
%% ====================================================================
%% @doc Handles a request producing an XML response
%% @end
-spec handle(term(), term()) -> {ok, term(), term()}.
%% ====================================================================
handle(Req, State) ->
	{ok, Req2} = cowboy_req:reply(200, [{<<"content-type">>, <<"application/json">>}], "dupa", Req),
  	{ok, Req2, State}.


%% terminate/3
%% ====================================================================
%% @doc Cowboy handler callback, no cleanup needed
-spec terminate(term(), term(), term()) -> ok.
%% ====================================================================
terminate(_Reason, _Req, _State) ->
	ok.