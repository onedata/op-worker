%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is a cowboy handler and processes remote control requests.
%%% @end
%%%-------------------------------------------------------------------
-module(remote_control_handler).

-include_lib("ctool/include/logging.hrl").
-include("appmock_internal.hrl").

%% Cowboy API
-export([init/3, handle/2, terminate/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback, called to initialize the state of the handler.
%% @end
%%--------------------------------------------------------------------
-spec init(Type :: term(), Req :: cowboy_req:req(), Args :: term()) -> {ok, term(), Args :: term()}.
init(_Type, Req, Args) ->
    {ok, Req, Args}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback, called to process a request.
%% Handles remote control requests by delegating them to appmock_logic.
%% @end
%%--------------------------------------------------------------------
-spec handle(Req :: cowboy_req:req(), State :: term()) -> {ok, term(), State :: term()}.
handle(Req, State) ->
    [Path] = State,
    {ok, Req2} =
        try
            case Path of
                ?VERIFY_ALL_PATH ->
                    appmock_logic:verify_all_mocks(Req);
                ?VERIFY_MOCK_PATH ->
                    appmock_logic:verify_mock(Req)
            end
        catch T:M ->
            ?error_stacktrace("Error in remote_control_handler. Path: ~p. ~p:~p.",
                [Path, T, M]),
            {ok, _ErrorReq} = cowboy_req:reply(500, Req)
        end,
    {ok, Req2, State}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback, called to perform cleanup after the request is handled.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), Req :: cowboy_req:req(), State :: term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.