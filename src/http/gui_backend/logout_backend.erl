%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements page_backend_behaviour and is called
%%% when logout page is visited - it contains logout logic (redirects to GR).
%%% @end
%%%-------------------------------------------------------------------
-module(logout_backend).
-author("Lukasz Opiola").
-behaviour(page_backend_behaviour).

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
    g_session:log_out(),
    {redirect_absolute, str_utils:to_binary(oneprovider:get_oz_logout_page())}.