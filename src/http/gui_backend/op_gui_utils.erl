%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module includes utility functions used in gui modules.
%%% @end
%%%-------------------------------------------------------------------
-module(op_gui_utils).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_user_rest_auth/0, get_users_default_space/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a tuple that can be used directly in REST operations on behalf of
%% current user.
%% @end
%%--------------------------------------------------------------------
-spec get_user_rest_auth() -> {user, {
    Macaroon :: macaroon:macaroon(),
    DischargeMacaroons :: [macaroon:macaroon()]}}.
get_user_rest_auth() ->
    SessionId = g_session:get_session_id(),
    {ok, #document{value = #session{auth = Auth}}} = session:get(SessionId),
    #auth{macaroon = Mac, disch_macaroons = DMacs} = Auth,
    {user, {Mac, DMacs}}.


%%--------------------------------------------------------------------
%% @doc
%% Returns the default space of current user (the one in GUI context).
%% @end
%%--------------------------------------------------------------------
-spec get_users_default_space() -> binary().
get_users_default_space() ->
    % @TODO VFS-1860 this should be taken from onedata_user record and
    % changes of this should be pushed.
    {ok, DefaultSpaceId} = oz_users:get_default_space(get_user_rest_auth()),
    DefaultSpaceId.
