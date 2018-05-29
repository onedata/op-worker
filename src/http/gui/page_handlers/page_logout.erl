%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when logout page is visited.
%%% @end
%%%-------------------------------------------------------------------
-module(page_logout).
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
    Req2 = op_gui_session:log_out(Req),
    cowboy_req:reply(307, #{
        <<"location">> => oneprovider:get_oz_logout_page()
    }, Req2).
