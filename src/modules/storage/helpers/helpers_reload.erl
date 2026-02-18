%%%-------------------------------------------------------------------
%%% @author Wojciech Geisler
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains functions for reloading storage helper
%%% parameters after storage configuration has changed.
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_reload).
-author("Wojciech Geisler").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([refresh_handle_params/4]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Regenerates user context with up-to-date args for given storage
%% and calls nif to update them in the existing helper.
%% @end
%%--------------------------------------------------------------------
-spec refresh_handle_params(helpers:helper_handle() | helpers:file_handle(),
    session:id(), od_space:id(), storage:data() | storage:id()) -> ok.
refresh_handle_params(Handle, SessionId, SpaceId, StorageId) when is_binary(StorageId) ->
    {ok, Storage} = storage:get(StorageId),
    refresh_handle_params(Handle, SessionId, SpaceId, Storage);
refresh_handle_params(Handle, SessionId, SpaceId, Storage) ->
    % gather information
    HelperConfig = storage:get_helper_config(Storage),
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, UserCtx} = luma:map_to_storage_credentials(SessionId, UserId, SpaceId, Storage),
    {ok, ArgsWithUserCtx} = helper_config:build_helper_nif_args(HelperConfig, UserCtx),
    ArgsWithUserCtxAndType = maps:put(<<"type">>, helper_config:get_name(HelperConfig), ArgsWithUserCtx),
    % do the refresh
    % @TODO VFS-12677 Propagate storage update errors to onepanel and roll back
    helpers:refresh_params(Handle, ArgsWithUserCtxAndType).
