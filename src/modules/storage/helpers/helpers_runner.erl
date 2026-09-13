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
    case helper_handle:get(SessionId, SpaceId, StorageId) of
        {ok, HelperHandle} ->
            run_and_handle_error(SDHandle, HelperHandle, Operation, SufficientAccessType);
        {error, not_found} ->
            % LUMA has no mapping for this user on this storage - typically the
            % local feed, which serves only what an administrator has entered
            case session:get(SessionId) of
                {ok, Session} ->
                    ?error(?autoformat_with_msg("Failed to resolve storage credentials:", [
                        Session, SpaceId, StorageId, StorageFileId, FileUuid
                    ]));
                {error, not_found} ->
                    ?warning(?autoformat_with_msg(
                        "Failed to resolve storage credentials for nonexistent session:",
                        [SessionId, SpaceId, StorageId, StorageFileId, FileUuid]
                    ))
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
                        {retry, RetryHandle} ->
                            Operation(RetryHandle);
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
    {error, term()} | {retry, handle()}.
handle_error({error, ?EKEYEXPIRED}, FileOrHelperHandle, SDHandle) ->
    handle_ekeyexpired(FileOrHelperHandle, SDHandle);
handle_error(Error, _, _) ->
    Error.


-spec handle_ekeyexpired(handle(), storage_driver:handle()) ->
    {error, term()} | {retry, handle()}.
handle_ekeyexpired(FileOrHelperHandle, #sd_handle{
    session_id = SessionId,
    space_id = SpaceId,
    storage_id = StorageId
}) ->
    {ok, Storage} = storage:get(StorageId),
    HelperSpec = storage:get_helper_spec(Storage),
    case helper_spec:is_oauth2_supported(HelperSpec) of
        true ->
            renew_expired_handle(FileOrHelperHandle, SessionId, SpaceId, Storage);
        false ->
            {error, ?EKEYEXPIRED}
    end.


%% @private
-spec renew_expired_handle(handle(), session:id(), od_space:id(), storage:data()) ->
    {error, term()} | {retry, handle()}.
renew_expired_handle(#helper_handle{}, SessionId, SpaceId, Storage) ->
    case helper_handle:get(SessionId, SpaceId, storage:get_id(Storage)) of
        {ok, HelperHandle} ->
            {retry, HelperHandle};
        {error, _} ->
            {error, ?EKEYEXPIRED}
    end;

renew_expired_handle(#file_handle{} = FileHandle, SessionId, SpaceId, Storage) ->
    % an open file is the one place that cannot be resolved anew: its params were
    % frozen in the helper instance it was opened with, so they have to be pushed
    % into that very instance
    %
    % NOTE: this mutates an instance shared by every handle built from the same
    % params. It is correct only because all files are opened with the root
    % session, so open handles carry the storage's own credentials. Should proxy
    % opens ever stop using the root session, this must be revisited - it would
    % then mutate a helper shared between users.
    %
    % NOTE: called by module for CT tests
    helper_handle:refresh(FileHandle, SessionId, SpaceId, Storage),
    {retry, FileHandle}.
