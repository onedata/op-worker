%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains functions responsible for traversing file tree and
%%% transferring regular files.
%%% @end
%%%--------------------------------------------------------------------
-module(transfer_file_tree_traverse).
-author("Bartosz Walkowicz").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/qos.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% Traverse behaviour callbacks
-export([
    do_master_job/2, do_slave_job/2, task_finished/2, task_canceled/2,
    get_job/1, update_job_progress/5
]).

-type traverse_info() :: #{
    transfer_id := transfer:id(),
    user_ctx := user_ctx:ctx(),
    worker_module := module()
}.
-export_type([traverse_info/0]).


-define(TRANSFER_RETRIES, 2).  %% TODO app.config??


%%%===================================================================
%%% Traverse callbacks
%%%===================================================================


-spec get_job(traverse:job_id()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), tree_traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).


-spec task_finished(transfer:id(), traverse:pool()) -> ok.
task_finished(TransferId, _PoolName) ->
    transfer:mark_traverse_finished(TransferId),
    ok.


-spec task_canceled(transfer:id(), traverse:pool()) -> ok.
task_canceled(TransferId, PoolName) ->
    task_finished(TransferId, PoolName).


-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), transfer:id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TransferId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TransferId, Status, ?MODULE).


-spec do_master_job(tree_traverse:master_job() | tree_traverse:slave_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job = #tree_traverse_slave{}, #{task_id := TransferId}) ->
    % Transfer root file is regular file
    transfer:increment_files_to_process_counter(TransferId, 1),
    do_slave_job(Job, TransferId);
do_master_job(Job = #tree_traverse{}, MasterJobArgs = #{task_id := TransferId}) ->
    BatchProcessingPreHook = fun(SlaveJobs, _MasterJobs, _ListingToken, _SubtreeProcessingStatus) ->
        transfer:increment_files_to_process_counter(TransferId, length(SlaveJobs)),
        ok
    end,
    tree_traverse:do_master_job(Job, MasterJobArgs, BatchProcessingPreHook).


-spec do_slave_job(tree_traverse:slave_job(), transfer:id()) -> ok.
do_slave_job(#tree_traverse_slave{
    file_ctx = FileCtx,
    traverse_info = TraverseInfo
}, TransferId) ->
    case transfer_data(TransferId, TraverseInfo, FileCtx, ?TRANSFER_RETRIES) of
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
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec transfer_data(transfer:id(), traverse_info(), file_ctx:ctx(), non_neg_integer()) ->
    ok | {error, term()}.
transfer_data(TransferId, TraverseInfo, FileCtx0, RetriesLeft) ->
    case process_result(TransferId, FileCtx0, RetriesLeft, transfer_data(
        TransferId, TraverseInfo, FileCtx0
    )) of
        ok ->
            ok;
        {retry, FileCtx1} ->
            transfer_data(TransferId, TraverseInfo, FileCtx1, RetriesLeft - 1);
        {error, _} = Error ->
            Error
    end.


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether data transfer can be retried and if so returns retry request.
%% Otherwise returns given error.
%% @end
%%-------------------------------------------------------------------
-spec process_result(transfer:id(), file_ctx:ctx(), non_neg_integer(), term()) ->
    ok | {retry, file_ctx:ctx()} | {error, term()}.
process_result(_TransferId, _FileCtx, _RetriesLeft, ok) ->
    ok;

process_result(TransferId, _FileCtx, 0, Error = {error, not_found}) ->
    ?error(
        "Data transfer in scope of transfer ~p failed due to ~w~n"
        "No retries left", [TransferId, Error]
    ),
    Error;

process_result(TransferId, FileCtx, Retries, Error = {error, not_found}) ->
    ?warning(
        "Data transfer in scope of transfer ~p failed due to ~w~n"
        "File transfer will be retried (attempts left: ~p)",
        [TransferId, Error, Retries - 1]
    ),
    {retry, FileCtx};

process_result(TransferId, FileCtx, 0, Error) ->
    {Path, _FileCtx2} = file_ctx:get_canonical_path(FileCtx),

    ?error(
        "Transfer of file ~p in scope of transfer ~p failed~n"
        "FilePath: ~ts~n"
        "Error was: ~p~n"
        "No retries left", [
            file_ctx:get_logical_guid_const(FileCtx), TransferId,
            Path,
            Error
        ]
    ),
    {error, retries_per_file_transfer_exceeded};

process_result(TransferId, FileCtx, Retries, Error) ->
    {Path, FileCtx2} = file_ctx:get_canonical_path(FileCtx),

    ?warning(
        "Transfer of file ~p in scope of transfer ~p failed~n"
        "FilePath: ~ts~n"
        "Error was: ~p~n"
        "File transfer will be retried (attempts left: ~p)", [
            file_ctx:get_logical_guid_const(FileCtx), TransferId,
            Path,
            Error,
            Retries - 1
        ]
    ),
    {retry, FileCtx2}.


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
        {ok, #document{value = Transfer}} = transfer:get(TransferId),

        assert_transfer_is_ongoing(Transfer),
        FileCtx1 = fslogic_authz:ensure_authorized(UserCtx, FileCtx0, AccessDefinitions),

        case file_ctx:file_exists_const(FileCtx1) of
            true ->
                case fslogic_file_id:is_symlink_uuid(file_ctx:get_logical_uuid_const(FileCtx1)) of
                    true -> ok;
                    false -> WorkerModule:transfer_regular_file(FileCtx1, TraverseInfo)
                end;
            false ->
                {error, not_found}
        end
    of
        ok ->
            ok;
        {error, _Reason} = Error ->
            Error
    catch
        throw:cancelled ->
            {error, cancelled};
        throw:already_ended ->
            {error, already_ended};
        error:{badmatch, Error = {error, not_found}} ->
            Error;
        Class:Reason:Stacktrace ->
            ?error_exception(
                "Unexpected error during transfer ~p", [TransferId],
                Class, Reason, Stacktrace
            ),
            {Class, Reason}
    end.


%% @private
-spec assert_transfer_is_ongoing(transfer:transfer()) -> ok | no_return().
assert_transfer_is_ongoing(Transfer) ->
    case transfer:is_ongoing(Transfer) of
        true -> ok;
        false -> throw(already_ended)
    end.
