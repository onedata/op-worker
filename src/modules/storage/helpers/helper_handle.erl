%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles helper handle management.
%%% TODO VFS-13846 cache get(SessionId, SpaceId, StorageId)
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
    HelperSpec = storage:get_helper_spec(Storage),
    case luma:map_to_storage_credentials(SessionId, UserId, SpaceId, Storage) of
        {ok, StorageCredentials} ->
            HelperHandle = helpers:get_helper_handle(HelperSpec, StorageCredentials),
            {ok, HelperHandle};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Regenerates user credentials with up-to-date configuration for given storage
%% and calls nif to update them in the existing helper.
%%
%% NOTE: meant for the handle of an open file - the one place whose params cannot
%% be resolved anew, as they were frozen in the helper instance the file was
%% opened with. A helper handle must go through get/3 instead: refreshing it in
%% place swaps the instance behind a C++ helper cache entry that stays keyed by
%% the previous params.
%% @end
%%--------------------------------------------------------------------
-spec refresh(helpers:file_handle(), session:id(), od_space:id(), storage:data()) ->
    ok.
refresh(Handle, SessionId, SpaceId, Storage) ->
    HelperSpec = storage:get_helper_spec(Storage),
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, StorageCredentials} = luma:map_to_storage_credentials(SessionId, UserId, SpaceId, Storage),
    {ok, HelperParams} = helper_spec:build_helper_params(HelperSpec, StorageCredentials),
    HelperParamsWithType = maps:put(<<"type">>, helper_spec:get_name(HelperSpec), HelperParams),
    ok = helpers:refresh_params(Handle, HelperParamsWithType).
