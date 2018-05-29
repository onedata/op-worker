%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license 
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements dynamic_page_behaviour and is called
%%% when validate login page is visited.
%%% @end
%%%-------------------------------------------------------------------
-module(page_validate_login).
-author("Lukasz Opiola").

-behaviour(dynamic_page_behaviour).

-include_lib("ctool/include/logging.hrl").
-include("proto/common/credentials.hrl").
-include("global_definitions.hrl").

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
    QueryString = cowboy_req:parse_qs(Req),
    MacaroonBin = proplists:get_value(<<"code">>, QueryString, <<"">>),

    case gui_auth_manager:authenticate(MacaroonBin) of
        {error, _} ->
            cowboy_req:reply(401, Req);
        {ok, Auth} ->
            {ok, #document{value = Identity}} = user_identity:get_or_fetch(Auth),

            Req2 = op_gui_session:log_out(Req),
            Req3 = op_gui_session:log_in(Identity, Auth, Req2),

            RedirectPath = case proplists:get_value(<<"redirect-path">>, QueryString) of
                undefined -> <<"/">>;
                Path -> Path
            end,
            cowboy_req:reply(307, #{<<"location">> => RedirectPath}, Req3)
    end.
