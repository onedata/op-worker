%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utils functions for op logic plugins.
%%% @end
%%%-------------------------------------------------------------------
-module(op_logic_utils).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/api_errors.hrl").

-export([
    is_eff_space_member/2,
    ensure_space_support/2,
    ensure_space_supported_locally/1, ensure_space_supported_by/2,

    ensure_file_exists/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec is_eff_space_member(op_logic:client(), op_space:id()) -> boolean().
is_eff_space_member(?NOBODY, _SpaceId) ->
    false;
is_eff_space_member(#client{id = SessionId}, SpaceId) ->
    {ok, UserId} = session:get_user_id(SessionId),
    user_logic:has_eff_space(SessionId, UserId, SpaceId).


-spec ensure_space_support(od_space:id(), undefined | [Provider]) ->
    ok | no_return() when
    Provider :: undefined | od_provider:id().
ensure_space_support(SpaceId, undefined) ->
    ensure_space_supported_locally(SpaceId);
ensure_space_support(SpaceId, TargetProviders) ->
    ensure_space_supported_locally(SpaceId),
    lists:foreach(fun(ProviderId) ->
        ensure_space_supported_by(SpaceId, ProviderId)
    end, TargetProviders).


-spec ensure_space_supported_locally(od_space:id()) -> ok | no_return().
ensure_space_supported_locally(SpaceId) ->
    case provider_logic:supports_space(SpaceId) of
        true -> ok;
        false -> throw(?ERROR_SPACE_NOT_SUPPORTED)
    end.


-spec ensure_space_supported_by(od_space:id(),
    undefined | od_provider:id()) -> ok | no_return().
ensure_space_supported_by(SpaceId, ProviderId) ->
    case space_logic:is_supported(?ROOT_SESS_ID, SpaceId, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_SPACE_NOT_SUPPORTED_BY(ProviderId))
    end.


-spec ensure_file_exists(op_logic:client(), file_id:file_guid()) ->
    ok | no_return().
ensure_file_exists(#client{id = SessionId}, FileGuid) ->
    case logical_file_manager:stat(SessionId, {guid, FileGuid}) of
        {ok, _} ->
            ok;
        _ ->
            throw(?ERROR_NOT_FOUND)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================
