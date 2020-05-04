%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO
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


%%%===================================================================
%%% API functions
%%%===================================================================


%%-spec apply_and_maybe_handle_ekeyexpired
%%    (SfmHandle, Operation, HelperOrFileHandle) -> Result when
%%    SfmHandle :: storage_driver:handle(),
%%    Operation :: fun(() -> Result),
%%    HelperOrFileHandle :: helpers:helper_handle() | helpers:file_handle(),
%%    Result :: ok | {ok, term()} | {error, term()}.
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

run_with_file_handle_and_handle_error(SDHandle = #sd_handle{file_handle = FileHandle}, Operation) ->
    run_and_handle_error(SDHandle, FileHandle, Operation).

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

handle_error({error, ?EKEYEXPIRED}, FileOrHelperHandle, SDHandle) ->
    handle_ekeyexpired(FileOrHelperHandle, SDHandle);
handle_error(Error, _, _) ->
    Error.

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
