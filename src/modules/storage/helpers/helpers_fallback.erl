%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains fallback that is called when helper operations
%%% return errors.
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_fallback).
-author("Jakub Kudzia").

%% API
-export([apply_and_maybe_handle_ekeyexpired/3]).

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/errors.hrl").


%%%===================================================================
%%% API functions
%%%===================================================================

-spec apply_and_maybe_handle_ekeyexpired
    (SfmHandle, Operation, HelperOrFileHandle) -> Result when
    SfmHandle :: storage_driver:handle(),
    Operation :: fun(() -> Result),
    HelperOrFileHandle :: helpers:helper_handle() | helpers:file_handle(),
    Result :: ok | {ok, term()} | {error, term()}.
apply_and_maybe_handle_ekeyexpired(#sd_handle{
    session_id = SessionId,
    space_id = SpaceId,
    storage_id = StorageId
}, Operation, HelperOrFileHandle) ->
    case Operation() of
        Result = {error, ?EKEYEXPIRED} ->
            {ok, StorageRecord} = storage:get(StorageId),
            Helper = storage:get_helper(StorageRecord),
            case helper:get_name(Helper) of
                ?WEBDAV_HELPER_NAME ->
                    % called by module for CT tests
                    helpers_reload:refresh_handle_params(HelperOrFileHandle,
                        SessionId, SpaceId, StorageRecord),
                    Operation();
                _ ->
                    Result
            end;
        OtherResult ->
            OtherResult
    end.