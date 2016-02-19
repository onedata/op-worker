%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements callback_backend_behaviour.
%%% It is used to handle callbacks from the client - such requests that do not
%%% correspond to underlying models. For example -
%%% 'give me the name of current user'.
%%% THIS IS A PROTOTYPE AND AN EXAMPLE OF IMPLEMENTATION.
%%% @end
%%%-------------------------------------------------------------------
-module(global_callback_backend).
-author("Lukasz Opiola").

-compile([export_all]).

-include_lib("ctool/include/logging.hrl").
-include("global_definitions.hrl").

%% API
-export([callback/2]).

callback(<<"userName">>, _) ->
    {ok, op_gui_utils:get_user_id()}.