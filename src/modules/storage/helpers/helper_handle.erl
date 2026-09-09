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
    HelperSpec = storage:get_helper_spec(Storage),
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, StorageCredentials} = luma:map_to_storage_credentials(SessionId, UserId, SpaceId, Storage),
    {ok, HelperParams} = helper_spec:build_helper_params(HelperSpec, StorageCredentials),
    HelperParamsWithType = maps:put(<<"type">>, helper_spec:get_name(HelperSpec), HelperParams),
    % @TODO VFS-12931 Propagate storage update errors to onepanel and roll back
    ok = helpers:refresh_params(Handle, HelperParamsWithType).
