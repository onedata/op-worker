%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for performing operations on helpers.
%%% It is possible to hook custom (for specific helper) error handling here.
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_runner).
-author("Jakub Kudzia").

%% API
-export([run_and_handle_error/2, run_with_file_handle_and_handle_error/2]).

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-type handle() :: helpers:helper_handle() | helpers:file_handle().

%%%===================================================================
%%% API functions
%%%===================================================================


-spec run_and_handle_error
    (SDHandle, Operation) -> Result when
    SDHandle :: storage_driver:handle(),
    Operation :: fun((helpers:helper_handle()) -> Result),
    Result :: ok | {ok, term()} | {error, term()}.
run_and_handle_error(SDHandle = #sd_handle{
    session_id = SessionId,
    space_id = SpaceId,
    storage_id = StorageId
}, Operation) ->
    case session_helpers:get_helper(SessionId, SpaceId, StorageId) of
        {ok, HelperHandle} ->
            run_and_handle_error(SDHandle, HelperHandle, Operation);
        {error, not_found} ->
            throw(?EACCES);
        {error, Reason} ->
            throw(Reason)
    end.

-spec run_with_file_handle_and_handle_error
    (SDHandle, Operation) -> Result when
    SDHandle :: storage_driver:handle(),
    Operation :: fun((helpers:file_handle()) -> Result),
    Result :: ok | {ok, term()} | {error, term()}.
run_with_file_handle_and_handle_error(SDHandle = #sd_handle{file_handle = FileHandle}, Operation) ->
    run_and_handle_error(SDHandle, FileHandle, Operation).

-spec run_and_handle_error
    (SDHandle, Handle, Operation) -> Result when
    SDHandle :: storage_driver:handle(),
    Handle :: handle(),
    Operation :: fun((handle()) -> Result),
    Result :: ok | {ok, term()} | {error, term()}.
run_and_handle_error(SDHandle, FileOrHelperHandle, Operation) ->
    case Operation(FileOrHelperHandle) of
        Error = {error, _} ->
            case handle_error(Error, FileOrHelperHandle, SDHandle) of
                retry ->
                    Operation(FileOrHelperHandle);
                Other ->
                    Other
            end;
        OtherResult ->
            OtherResult
    end.

-spec handle_error({error, term()}, handle(), storage_driver:handle()) ->
    {error, term()} | retry.
handle_error({error, ?EKEYEXPIRED}, FileOrHelperHandle, SDHandle) ->
    handle_ekeyexpired(FileOrHelperHandle, SDHandle);
handle_error(Error, _, _) ->
    Error.

-spec handle_ekeyexpired(handle(), storage_driver:handle()) ->
    {error, term()} | retry.
handle_ekeyexpired(FileOrHelperHandle, #sd_handle{
    session_id = SessionId,
    space_id = SpaceId,
    storage_id = StorageId
}) ->
    {ok, Storage} = storage:get(StorageId),
    Helper = storage:get_helper(Storage),
    case helper:get_name(Helper) of
        ?WEBDAV_HELPER_NAME ->
            % called by module for CT tests
            helpers_reload:refresh_handle_params(FileOrHelperHandle, SessionId, SpaceId, Storage),
            retry;
        _ ->
            {error, ?EKEYEXPIRED}
    end.
