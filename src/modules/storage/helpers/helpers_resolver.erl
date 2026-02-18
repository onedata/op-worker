%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions for resolving storage helper.
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_resolver).
-author("Bartosz Walkowicz").

%% API
-export([resolve/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec resolve(session:id(), od_space:id(), storage:id()) ->
    {ok, helpers:helper_handle()} | errors:error().
resolve(SessionId, SpaceId, StorageId) ->
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, Storage} = storage:get(StorageId),
    HelperConfig = storage:get_helper_config(Storage),
    case luma:map_to_storage_credentials(SessionId, UserId, SpaceId, Storage) of
        {ok, UserCtx} ->
            Handle = helpers:get_helper_handle(HelperConfig, UserCtx),
            {ok, Handle};
        {error, _} = Error ->
            Error
    end.
