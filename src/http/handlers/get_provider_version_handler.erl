%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module handles requests for current version of Oneprovider.
%%% @end
%%%-------------------------------------------------------------------
-module(get_provider_version_handler).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/logging.hrl").
-include_lib("hackney/include/hackney_lib.hrl").
-include("global_definitions.hrl").


-export([init/3, handle/2, terminate/3]).


%%--------------------------------------------------------------------
%% @doc Cowboy handler callback, no state is required
%% @end
%%--------------------------------------------------------------------
-spec init(any(), term(), any()) -> {ok, term(), []}.
init(_Type, Req, State) ->
    {ok, Req, State}.


%%--------------------------------------------------------------------
%% @doc Handles a request returning current version of Oneprovider.
%% @end
%%--------------------------------------------------------------------
-spec handle(term(), term()) -> {ok, cowboy_req:req(), term()}.
handle(Req, State) ->
    {_AppId, _AppName, AppVersion} = lists:keyfind(
        ?APP_NAME, 1, application:loaded_applications()
    ),
    {ok, NewReq} = cowboy_req:reply(
        200, [{<<"content-type">>, <<"text/plain">>}], AppVersion, Req),
    {ok, NewReq, State}.


%%--------------------------------------------------------------------
%% @doc Cowboy handler callback, no cleanup needed
%% @end
%%--------------------------------------------------------------------
-spec terminate(term(), term(), term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.
