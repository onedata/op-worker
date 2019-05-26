%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when provider configuration page is visited.
%%% This is the legacy configuration endpoint,
%%% not documented in swagger.
%%% @end
%%%-------------------------------------------------------------------
-module(page_provider_configuration).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include("global_definitions.hrl").

-define(to_binaries(__List), [list_to_binary(V) || V <- __List]).

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
    cowboy_req:reply(200,
        #{<<"content-type">> => <<"application/json">>},
        json_utils:encode(configuration:gather_configuration()),
        Req
    ).
