%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Aug 2015 15:25
%%%-------------------------------------------------------------------
-module(static_data_backend).
-author("lopiola").

-compile([export_all]).

-include_lib("ctool/include/logging.hrl").
-include("global_definitions.hrl").

%% API
-export([find/1]).

find(<<"user_name">>) ->
    {ok, op_gui_utils:get_user_id()}.
