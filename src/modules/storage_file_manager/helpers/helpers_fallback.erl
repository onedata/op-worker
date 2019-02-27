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
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include_lib("ctool/include/posix/errors.hrl").


%%%===================================================================
%%% API functions
%%%===================================================================

-spec apply_and_maybe_handle_ekeyexpired(storage_file_manager:handle(),
    fun(() -> ok | {ok, term()} | {error, term()}), helpers:helper_handle() | helpers:file_handle()) ->
    ok | {ok, term()} | {error, term()}.
apply_and_maybe_handle_ekeyexpired(#sfm_handle{
    session_id = SessionId,
    space_id = SpaceId,
    storage = Storage
    },Operation, HelperOrFileHandle) ->
    case Operation() of
        Result = {error, ?EKEYEXPIRED} ->
            {ok, Helper} = fslogic_storage:select_helper(Storage),
            case helper:get_name(Helper) of
                ?WEBDAV_HELPER_NAME ->
                    helper_handle:refresh_params(HelperOrFileHandle, SessionId, SpaceId, Storage),
                    Operation();
                _ ->
                 Result
            end;
        OtherResult ->
            OtherResult
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
