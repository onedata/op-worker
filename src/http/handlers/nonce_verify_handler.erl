%% ====================================================================
%%% @author Lukasz Opiola
%%% @copyright (C) 2018, ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This handler serves as a public endpoint to check OP's connectivity to OZ.
%%% @end
%%%-------------------------------------------------------------------
-module(nonce_verify_handler).
-author("Lukasz Opiola").
-behaviour(cowboy_http_handler).

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

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
    {Nonce, _} = cowboy_req:qs_val(<<"nonce">>, Req, undefined),

    %% If connection is established, OP should be able to retrieve its own
    %% data. Any other result implies connection error.
    Status = case authorization_nonce:verify(Nonce) of
        true -> <<"ok">>;
        false -> <<"error">>
    end,
    {ok, Req1} = cowboy_req:reply(
        200,
        [{<<"content-type">>, <<"application/json">>}],
        json_utils:encode_map(#{<<"status">> => Status}),
        Req
    ),
    {ok, Req1, State}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy handler callback, no cleanup needed
%% @end
%%--------------------------------------------------------------------
-spec terminate(term(), term(), term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.
