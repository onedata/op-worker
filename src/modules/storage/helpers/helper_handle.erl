%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles helper handle management.
%%% @end
%%%-------------------------------------------------------------------
-module(helper_handle).
-author("Bartosz Walkowicz").

%% API
-export([get/3, refresh/4]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get(session:id(), od_space:id(), storage:id()) ->
    {ok, helpers:helper_handle()} | errors:error().
get(SessionId, SpaceId, StorageId) ->
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, Storage} = storage:get(StorageId),
    HelperConfig = storage:get_helper_config(Storage),
    case luma:map_to_storage_credentials(SessionId, UserId, SpaceId, Storage) of
        {ok, UserCtx} ->
            HelperHandle = helpers:get_helper_handle(HelperConfig, UserCtx),
            {ok, HelperHandle};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Regenerates user context with up-to-date args for given storage
%% and calls nif to update them in the existing helper.
%% @end
%%--------------------------------------------------------------------
-spec refresh(
    helpers:helper_handle() | helpers:file_handle(),
    session:id(),
    od_space:id(),
    storage:data() | storage:id()
) ->
    ok.
refresh(Handle, SessionId, SpaceId, StorageId) when is_binary(StorageId) ->
    {ok, Storage} = storage:get(StorageId),
    refresh(Handle, SessionId, SpaceId, Storage);

refresh(Handle, SessionId, SpaceId, Storage) ->
    HelperConfig = storage:get_helper_config(Storage),
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, UserCtx} = luma:map_to_storage_credentials(SessionId, UserId, SpaceId, Storage),
    {ok, ArgsWithUserCtx} = helper_config:build_helper_nif_args(HelperConfig, UserCtx),
    ArgsWithUserCtxAndType = maps:put(<<"type">>, helper_config:get_name(HelperConfig), ArgsWithUserCtx),
    ok = helpers:refresh_params(Handle, ArgsWithUserCtxAndType).
