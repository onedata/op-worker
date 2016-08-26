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
-export([check_with_auth/2, check_with_user/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Check if user is in space using his auth.
%% @end
%%--------------------------------------------------------------------
-spec check_with_auth(onedata_auth_api:auth(), space_info:id()) -> ok | no_return().
check_with_auth(Auth, SpaceId) ->
    {ok, UserId} = session:get_user_id(Auth),
    check_with_auth(UserId, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Check if user is in space.
%% @end
%%--------------------------------------------------------------------
-spec check_with_user(onedata_user:id(), space_info:id()) -> ok | no_return().
check_with_user(UserId, SpaceId) ->
    {ok, #document{value = #onedata_user{spaces = Spaces}}} = onedata_user:get(UserId),
    case lists:member(SpaceId, Spaces) of
        true ->
            ok;
        false ->
            throw(?ERROR_PERMISSION_DENIED)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================