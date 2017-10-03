%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Space membership checker.
%%% @end
%%%--------------------------------------------------------------------
-module(space_membership).
-author("Tomasz Lichon").

-include("http/http_common.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

%% API
-export([check_with_auth/2, check_with_user/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if user is in space using his auth.
%% @end
%%--------------------------------------------------------------------
-spec check_with_auth(onedata_auth_api:auth(), od_space:id()) -> ok | no_return().
check_with_auth(SessionId, SpaceId) ->
    {ok, UserId} = session:get_user_id(SessionId),
    check_with_user(SessionId, UserId, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Check if user is in space.
%% @end
%%--------------------------------------------------------------------
-spec check_with_user(SessionId :: session:id(), od_user:id(), od_space:id()) ->
    ok | no_return().
check_with_user(SessionId, UserId, SpaceId) ->
    case user_logic:has_eff_space(SessionId, UserId, SpaceId) of
        true ->
            ok;
        false ->
            throw(?ERROR_PERMISSION_DENIED)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
