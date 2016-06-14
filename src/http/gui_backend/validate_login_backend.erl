%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements page_backend_behaviour and is called
%%% when validate_login page is visited - it contains the logic
%%% that validates a redirect for login from GR.
%%% THIS IS A PROTOTYPE AND AN EXAMPLE OF IMPLEMENTATION.
%%% @end
%%%-------------------------------------------------------------------
-module(validate_login_backend).
-author("Lukasz Opiola").
-behaviour(page_backend_behaviour).

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([page_init/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link page_backend_behaviour} callback page_init/0.
%% @end
%%--------------------------------------------------------------------
-spec page_init() -> gui_html_handler:page_init_result().
page_init() ->
    case g_session:is_logged_in() of
        true ->
            ok;
        false ->
            SrlzdMacaroon = g_ctx:get_url_param(<<"code">>),
            {ok, Macaroon} = macaroon:deserialize(SrlzdMacaroon),
            {ok, Auth = #token_auth{}} = gui_auth_manager:authenticate(Macaroon),
            {ok, #document{value = #identity{user_id = UserId} = Identity}} =
                identity:get_or_fetch(Auth),
            {ok, _} = g_session:log_in(UserId, [Identity, Auth])
    end,
    {redirect_relative, <<"/">>}.
