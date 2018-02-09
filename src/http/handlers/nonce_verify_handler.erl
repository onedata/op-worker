%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles requests that verify authorization nonce, which is used
%%% during handshake in inter-provider communication.
%%% @end
%%%-------------------------------------------------------------------
-module(nonce_verify_handler).
-author("Lukasz Opiola").

-behaviour(cowboy_handler).

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([init/2]).


%%--------------------------------------------------------------------
%% @doc Cowboy handler callback.
%% Handles a request returning status of nonce authorization.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), term()) -> {ok, cowboy_req:req(), term()}.
init(#{method := <<"GET">>} = Req, State) ->
    Nonce = proplists:get_value(<<"nonce">>, cowboy_req:parse_qs(Req)),
    Status = case authorization_nonce:verify(Nonce) of
        true -> <<"ok">>;
        false -> <<"error">>
    end,
    NewReq = cowboy_req:reply(200,
        #{<<"content-type">> => <<"application/json">>},
        json_utils:encode_map(#{<<"status">> => Status}),
        Req
    ),
    {ok, NewReq, State};
init(Req, State) ->
    NewReq = cowboy_req:reply(405, #{<<"allow">> => <<"GET">>}, Req),
    {ok, NewReq, State}.
