%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for performing operations on helpers.
%%% It also contains error handling logic which may depend on helper
%%% type.
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_runner).
-author("Jakub Kudzia").

%% API
-export([run_and_handle_error/3, run_with_file_handle_and_handle_error/3]).

-include("modules/datastore/datastore_models.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-type handle() :: helpers:helper_handle() | helpers:file_handle().

%%%===================================================================
%%% API functions
%%%===================================================================


-spec run_and_handle_error
    (SDHandle, Operation, SufficientAccessType) -> Result when
    SDHandle :: storage_driver:handle(),
    Operation :: fun((helpers:helper_handle()) -> Result),
    SufficientAccessType :: storage:access_type(),
    Result :: ok | {ok, term()} | {error, term()}.
run_and_handle_error(SDHandle = #sd_handle{
    session_id = SessionId,
    space_id = SpaceId,
    storage_id = StorageId,
    file = StorageFileId,
    file_uuid = FileUuid
}, Operation, SufficientAccessType) ->
    case session_helpers:get_helper(SessionId, SpaceId, StorageId) of
        {ok, HelperHandle} ->
            run_and_handle_error(SDHandle, HelperHandle, Operation, SufficientAccessType);
        {error, not_found} ->
            case session:get(SessionId) of
                {ok, Session} ->
                    ?error(?autoformat_with_msg("Helper not found:",
                        [Session, SpaceId, StorageId, StorageFileId, FileUuid]));
                {error, not_found} ->
                    ?warning(?autoformat_with_msg("Helper for nonexistent session not found:",
                        [SessionId, SpaceId, StorageId, StorageFileId, FileUuid]))
            end,
            throw(?EACCES);
        {error, Reason} ->
            throw(Reason)
    end.

-spec run_with_file_handle_and_handle_error
    (SDHandle, Operation, SufficientAccessType) -> Result when
    SDHandle :: storage_driver:handle(),
    Operation :: fun((helpers:file_handle()) -> Result),
    SufficientAccessType :: storage:access_type(),
    Result :: ok | {ok, term()} | {error, term()}.
run_with_file_handle_and_handle_error(SDHandle = #sd_handle{file_handle = FileHandle}, Operation, SufficientAccessType) ->
    run_and_handle_error(SDHandle, FileHandle, Operation, SufficientAccessType).

-spec run_and_handle_error
    (SDHandle, Handle, Operation, SufficientAccessType) -> Result when
    SDHandle :: storage_driver:handle(),
    Handle :: handle(),
    Operation :: fun((handle()) -> Result),
    SufficientAccessType :: storage:access_type(),
    Result :: ok | {ok, term()} | {error, term()}.
run_and_handle_error(SDHandle = #sd_handle{storage_id = StorageId, space_id = SpaceId}, FileOrHelperHandle, Operation,
    SufficientAccessType
) ->
    case storage_logic:supports_access_type(StorageId, SpaceId, SufficientAccessType) of
        true ->
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
            end;
        false ->
            {error, ?EROFS}
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