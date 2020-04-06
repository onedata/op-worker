%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utils functions for middleware plugins.
%%% @end
%%%-------------------------------------------------------------------
-module(middleware_utils).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include_lib("ctool/include/errors.hrl").

-export([
    is_eff_space_member/2,
    assert_space_supported_locally/1, assert_space_supported_by/2,
    assert_file_exists/2,
    check_object_id/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec is_eff_space_member(aai:auth(), od_space:id()) -> boolean().
is_eff_space_member(?GUEST, _SpaceId) ->
    false;
is_eff_space_member(?USER(UserId, SessionId), SpaceId) ->
    user_logic:has_eff_space(SessionId, UserId, SpaceId).


-spec assert_space_supported_locally(od_space:id()) -> ok | no_return().
assert_space_supported_locally(SpaceId) ->
    assert_space_supported_by(SpaceId, oneprovider:get_id()).


-spec assert_space_supported_by(od_space:id(), od_provider:id()) ->
    ok | no_return().
assert_space_supported_by(SpaceId, ProviderId) ->
    case space_logic:is_supported(?ROOT_SESS_ID, SpaceId, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_SPACE_NOT_SUPPORTED_BY(ProviderId))
    end.


-spec assert_file_exists(aai:auth(), file_id:file_guid()) ->
    ok | no_return().
assert_file_exists(#auth{session_id = SessionId}, FileGuid) ->
    case lfm:stat(SessionId, {guid, FileGuid}) of
        {ok, _} ->
            ok;
        {error, Errno} ->
            throw(?ERROR_POSIX(Errno))
    end.


-spec check_object_id(file_id:objectid(), binary()) -> 
    {true, file_id:file_guid()} | no_return().
check_object_id(ObjectId, Key) ->
    case catch file_id:objectid_to_guid(ObjectId) of
        {ok, Guid} ->
            {true, Guid};
        _Error ->
            throw(?ERROR_BAD_VALUE_IDENTIFIER(Key))
    end.
