%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when OZ connectivity page is visited.
%%% @end
%%%-------------------------------------------------------------------
-module(page_oz_connectivity).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").

-export([handle/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(gui:method(), cowboy_req:req()) -> cowboy_req:req().
handle(<<"GET">>, Req) ->
    Status = case gs_channel_service:is_connected() of
        true -> <<"ok">>;
        false -> <<"error">>
    end,
    cowboy_req:reply(
        ?HTTP_200_OK,
        #{?HDR_CONTENT_TYPE => <<"application/json">>},
        json_utils:encode(#{<<"status">> => Status}),
        Req
    ).
