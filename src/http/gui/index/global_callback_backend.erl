%%%-------------------------------------------------------------------
%%% @author lopiola
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Aug 2015 15:25
%%%-------------------------------------------------------------------
-module(global_callback_backend).
-author("lopiola").

-compile([export_all]).

-include_lib("ctool/include/logging.hrl").
-include("global_definitions.hrl").

%% API
-export([callback/2]).

callback(<<"userName">>, _) ->
    {ok, op_gui_utils:get_user_id()};

callback(<<"sync">>, _) ->
    sync:ensure_started("../../build"),
    sync:track_gui(),
    Res = case sync:sync() of
              true -> <<"ok">>;
              false -> <<"error">>
          end,
    {ok, Res}.
