%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Sep 2015 11:52
%%%-------------------------------------------------------------------
-module(ver_login_backend).
-author("lopiola").

-include_lib("ctool/include/logging.hrl").

%% API
-export([page_init/0]).


page_init() ->
    ?dump(page_init),
    g_session:log_in(),
    g_session:put_value(key, <<"blebelbleb">>),
    {serve_body, <<"">>}.