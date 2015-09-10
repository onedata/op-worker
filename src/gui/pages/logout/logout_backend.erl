%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Sep 2015 11:52
%%%-------------------------------------------------------------------
-module(logout_backend).
-author("lopiola").

-include_lib("ctool/include/logging.hrl").

%% API
-export([page_init/0]).


page_init() ->
    ?dump(logout),
    g_session:log_out(),
    {serve_body, <<"ierozki">>}.