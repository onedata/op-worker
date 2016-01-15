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

-compile([export_all]).

-include("global_definitions.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

page_init() ->
    case g_session:is_logged_in() of
        true ->
            ok;
        false ->
            SrlzdMacaroon = g_ctx:get_url_param(<<"code">>),
            {ok, Auth = #auth{}} = gui_auth_manager:authenticate(SrlzdMacaroon),
            {ok, _} = g_session:log_in([Auth])
    end,
    {redirect_relative, <<"/">>}.