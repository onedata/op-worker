%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module includes utility funcitons used in gui modules.
%%% @end
%%%-------------------------------------------------------------------
-module(op_gui_utils).
-author("Lukasz Opiola").

-include("global_definitions.hrl").

%% API
-export([get_user_id/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns the user id of currently logged in user, or undefined if
%% this is not a logged in session.
%% @end
%%--------------------------------------------------------------------
-spec get_user_id() -> binary() | undefined.
get_user_id() ->
    case session:get(g_session:get_session_id()) of
        {ok, #document{value = #session{identity = #identity{
            user_id = UserId}}}} ->
            UserId;
        _ ->
            undefined
    end.
