%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Aug 2015 15:25
%%%-------------------------------------------------------------------
-module(validate_login_backend).
-author("lopiola").

-compile([export_all]).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

page_init() ->
    SrlzdMacaroon = g_ctx:get_url_param(<<"code">>),
    {ok, Auth = #auth{}} = gui_auth_manager:authorize(SrlzdMacaroon),
    {ok, _} = g_session:log_in([Auth]),
    {redirect, "/"}.