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
    decode_object_id/2,

    is_shared_file_request/3
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


-spec decode_object_id(file_id:objectid(), binary()) -> 
    {true, file_id:file_guid()} | no_return().
decode_object_id(ObjectId, Key) ->
    try
        {ok, Guid} = file_id:objectid_to_guid(ObjectId),
        {_, _, _} = file_id:unpack_share_guid(Guid),
        {true, Guid}
    catch _:_ ->
        throw(?ERROR_BAD_VALUE_IDENTIFIER(Key))
    end.


-spec is_shared_file_request(gri:entity_type(), gri:aspect(), gri:entity_id()) ->
    boolean().
is_shared_file_request(op_file, _, Id) when is_binary(Id) ->
    file_id:is_share_guid(Id);
is_shared_file_request(op_replica, As, Id) when
    As =:= instance;
    As =:= distribution
    ->
    file_id:is_share_guid(Id);
is_shared_file_request(_, _, _) ->
    false.
