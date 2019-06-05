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

% exported for CT tests
-export([refresh_params/4]).

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage_file_manager/helpers/helpers.hrl").
-include_lib("ctool/include/posix/errors.hrl").


%%%===================================================================
%%% API functions
%%%===================================================================

-spec apply_and_maybe_handle_ekeyexpired
    (SfmHandle, Operation, HelperOrFileHandle) -> Result when
    SfmHandle :: storage_file_manager:handle(),
    Operation :: fun(() -> Result),
    HelperOrFileHandle :: helpers:helper_handle() | helpers:file_handle(),
    Result :: ok | {ok, term()} | {error, term()}.
apply_and_maybe_handle_ekeyexpired(#sfm_handle{
    session_id = SessionId,
    space_id = SpaceId,
    storage = Storage
}, Operation, HelperOrFileHandle) ->
    case Operation() of
        Result = {error, ?EKEYEXPIRED} ->
            {ok, Helper} = fslogic_storage:select_helper(Storage),
            case helper:get_name(Helper) of
                ?WEBDAV_HELPER_NAME ->
                    % called by module for CT tests
                    helpers_fallback:refresh_params(HelperOrFileHandle, SessionId,
                        SpaceId, Storage),
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

-spec refresh_params(helpers:helper_handle() | helpers:file_handle(),
    session:id(), od_space:id(), storage:doc()) -> ok.
refresh_params(Handle, SessionId, SpaceId, StorageDoc) ->
    {ok, Helper} = fslogic_storage:select_helper(StorageDoc),
    HelperName = helper:get_name(Helper),
    {ok, UserId} = session:get_user_id(SessionId),
    {ok, UserCtx} = luma:get_server_user_ctx(SessionId, UserId, undefined,
        SpaceId, StorageDoc, HelperName),
    {ok, Helper2} = helper:insert_user_ctx(Helper, UserCtx),
    ArgsWithUserCtx = helper:get_args(Helper2),
    helpers:refresh_params(Handle, ArgsWithUserCtx).