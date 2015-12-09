%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. Sep 2015 15:04
%%%-------------------------------------------------------------------
-module(op_gui_utils).
-author("lopiola").

-include("global_definitions.hrl").

%% API
-export([get_user_id/0]).

get_user_id() ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} =
        session:get(g_session:get_session_id()),
    UserId.
