%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when identity token page is visited.
%%% @todo VFS-5554 Deprecated, included for backward compatibility
%%% Must be supported in the 19.09.* line, later the counterpart in the
%%% configuration endpoint can be used.
%%% @end
%%%-------------------------------------------------------------------
-module(page_identity_token).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include_lib("ctool/include/http/codes.hrl").

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
    {ok, IdentityToken} = provider_auth:get_identity_token(),
    cowboy_req:reply(
        ?HTTP_200_OK,
        #{<<"content-type">> => <<"text/plain">>},
        IdentityToken,
        Req
    ).
