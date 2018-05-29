%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when nonce verify page is visited.
%%% @end
%%%-------------------------------------------------------------------
-module(page_nonce_verify).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-export([handle/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(new_gui:method(), cowboy_req:req()) -> cowboy_req:req().
handle(<<"GET">>, Req) ->
    Nonce = proplists:get_value(<<"nonce">>, cowboy_req:parse_qs(Req), <<"">>),
    Status = case authorization_nonce:verify(Nonce) of
        true -> <<"ok">>;
        false -> <<"error">>
    end,
    cowboy_req:reply(200,
        #{<<"content-type">> => <<"application/json">>},
        json_utils:encode(#{<<"status">> => Status}),
        Req
    ).
