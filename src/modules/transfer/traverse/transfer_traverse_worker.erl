%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains functions responsible for transferring regular files.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_traverse_worker).
-author("Bartosz Walkowicz").

-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([process_file/3]).


-type traverse_info() :: #{
    space_id := od_space:id(),
    transfer_id := transfer:id(),
    user_ctx := user_ctx:ctx(),
    worker_module := module(),
    view_name => transfer:view_name(),
    replica_holder_provider_id => undefined | od_provider:id()
}.
-export_type([traverse_info/0]).


-define(MAX_TRANSFER_RETRIES, op_worker:get_env(max_transfer_retries_per_file, 3)).


%%%===================================================================
%%% Callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Callback called to get permissions required to check before starting transfer.
%% @end
%%--------------------------------------------------------------------
-callback required_permissions() -> [data_access_control:requirement()].


%%--------------------------------------------------------------------
%% @doc
%% Callback called when transferring regular file.
%% @end
%%--------------------------------------------------------------------
-callback transfer_regular_file(file_ctx:ctx(), traverse_info()) ->
    ok | {error, term()}.


%%%===================================================================
%%% API
%%%===================================================================


-spec process_file(transfer:id(), traverse_info(), file_ctx:ctx()) -> ok.
process_file(TransferId, TraverseInfo, FileCtx) ->
    case process_file(TransferId, TraverseInfo, FileCtx, ?MAX_TRANSFER_RETRIES) of
        ok ->
            ok;
        {error, not_found} ->
            % todo VFS-4218 currently we ignore this case
            {ok, _} = transfer:increment_files_processed_counter(TransferId);
        {error, cancelled} ->
            {ok, _} = transfer:increment_files_processed_counter(TransferId);
        {error, already_ended} ->
            {ok, _} = transfer:increment_files_processed_counter(TransferId);
        {error, _Reason} ->
            {ok, _} = transfer:increment_files_failed_and_processed_counters(TransferId)
    end,

    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec process_file(transfer:id(), traverse_info(), file_ctx:ctx(), non_neg_integer()) ->
    ok | {error, term()}.
process_file(TransferId, TraverseInfo, FileCtx0, RetriesLeft) ->
    case process_result(TransferId, FileCtx0, RetriesLeft, transfer_data(
        TransferId, TraverseInfo, FileCtx0
    )) of
        ok ->
            ok;
        {retry, FileCtx1} ->
            process_file(TransferId, TraverseInfo, FileCtx1, RetriesLeft - 1);
        {error, _} = Error ->
            Error
    end.


%% @private
-spec process_result(transfer:id(), file_ctx:ctx(), non_neg_integer(), term()) ->
    ok | {retry, file_ctx:ctx()} | {error, term()}.
process_result(_TransferId, _FileCtx, _RetriesLeft, ok) ->
    ok;

process_result(_TransferId, _FileCtx, _RetriesLeft, Error = {error, Reason}) when
    Reason =:= cancelled;
    Reason =:= already_ended
->
    Error;

process_result(TransferId, FileCtx, 0, Error = {error, not_found}) ->
    is_file_deleted(FileCtx) orelse ?error(
        "Data transfer in scope of transfer ~tp failed due to ~w~n"
        "No retries left", [TransferId, Error]
    ),
    Error;

process_result(TransferId, FileCtx, Retries, Error = {error, not_found}) ->
    is_file_deleted(FileCtx) orelse ?warning(
        "Data transfer in scope of transfer ~tp failed due to ~w~n"
        "File transfer will be retried (attempts left: ~tp)",
        [TransferId, Error, Retries - 1]
    ),
    {retry, FileCtx};

process_result(TransferId, FileCtx, 0, Error) ->
    {Path, _FileCtx2} = get_file_path(FileCtx),

    ?error(
        "Transfer of file ~tp in scope of transfer ~tp failed~n"
        "FilePath: ~ts~n"
        "Error was: ~tp~n"
        "No retries left", [
            file_ctx:get_logical_guid_const(FileCtx), TransferId,
            Path,
            Error
        ]
    ),
    {error, retries_per_file_transfer_exceeded};

process_result(TransferId, FileCtx, Retries, Error) ->
    {Path, FileCtx2} = get_file_path(FileCtx),

    ?warning(
        "Transfer of file ~tp in scope of transfer ~tp failed~n"
        "FilePath: ~ts~n"
        "Error was: ~tp~n"
        "File transfer will be retried (attempts left: ~tp)", [
            file_ctx:get_logical_guid_const(FileCtx), TransferId,
            Path,
            Error,
            Retries - 1
        ]
    ),
    {retry, FileCtx2}.


%% @private
-spec is_file_deleted(file_ctx:ctx()) -> boolean().
is_file_deleted(FileCtx) ->
    case file_ctx:file_exists_or_is_deleted(FileCtx) of
        {?FILE_DELETED, _} -> true;
        _ -> false
    end.


%% @private
-spec get_file_path(file_ctx:ctx()) -> {file_meta:path(), file_ctx:ctx()}.
get_file_path(FileCtx) ->
    try
        file_ctx:get_canonical_path(FileCtx)
    catch _:_ ->
        {<<"unknown">>, FileCtx}
    end.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Applies permissions check and if passed transfers data.
%% In case of error checks if transfer should be retried and if so returns
%% retry request and caught error otherwise.
%% @end
%%-------------------------------------------------------------------
-spec transfer_data(transfer:id(), traverse_info(), file_ctx:ctx()) ->
    ok | {error, term()}.
transfer_data(TransferId, TraverseInfo, FileCtx0) ->
    UserCtx = maps:get(user_ctx, TraverseInfo),
    WorkerModule = maps:get(worker_module, TraverseInfo),
    AccessDefinitions = WorkerModule:required_permissions(),

    try
        assert_transfer_is_ongoing(TransferId),

        assert_file_exists(FileCtx0),
        FileCtx1 = fslogic_authz:ensure_authorized(UserCtx, FileCtx0, AccessDefinitions),

        case fslogic_file_id:is_symlink_uuid(file_ctx:get_logical_uuid_const(FileCtx1)) of
            true -> ok;
            false -> WorkerModule:transfer_regular_file(FileCtx1, TraverseInfo)
        end
    of
        ok ->
            ok;
        {error, _Reason} = Error ->
            Error
    catch
        throw:{error, _} = Error ->
            Error;
        throw:Reason ->
            {error, Reason};
        error:{badmatch, Error = {error, not_found}} ->
            Error;
        Class:Reason:Stacktrace ->
            ?error_exception(
                "Unexpected error during transfer ~tp", [TransferId],
                Class, Reason, Stacktrace
            ),
            {Class, Reason}
    end.


%% @private
-spec assert_transfer_is_ongoing(transfer:id()) -> ok | no_return().
assert_transfer_is_ongoing(TransferId) ->
    case transfer:is_ongoing(TransferId) of
        true -> ok;
        false -> throw({error, already_ended})
    end.


%% @private
-spec assert_file_exists(file_ctx:ctx()) -> ok | no_return().
assert_file_exists(FileCtx) ->
    case file_ctx:file_exists_const(FileCtx) of
        true -> ok;
        false -> throw(not_found)
    end.
