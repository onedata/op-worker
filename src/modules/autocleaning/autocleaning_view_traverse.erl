%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements view_traverse behaviour (see view_traverse.erl).
%%% It is used by autocleaning_run_controller to traverse over file_popularity_view
%%% in given space and schedule deletions of the least popular file replicas.
%%% Note: hardlinks are ignored during the traverse because they share content with
%%% original file. The content will be cleaned with original file cleaning.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_view_traverse).
-author("Jakub Kudzia").

-behaviour(view_traverse).

-include("modules/file_popularity/file_popularity_view.hrl").
-include("modules/replica_deletion/replica_deletion.hrl").
-include_lib("cluster_worker/include/traverse/view_traverse.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_pool/0, stop_pool/0, run/5, cancel/1, task_canceled/1]).

%% view_traverse callbacks
-export([process_row/3, batch_prehook/4, on_batch_canceled/4, task_finished/1]).

-type task_id() :: binary().

%% @formatter:off
-type info() :: #{
    batch_size := non_neg_integer(),
    run_id := autocleaning:run_id(),
    space_id := od_space:id(),
    rules := autocleaning_config:rules()
}.
%% @formatter:on

-define(AUTOCLEANING_MASTER_JOBS_NUM,
    application:get_env(?APP_NAME, autocleaning_master_jobs_num, 10)).
-define(AUTOCLEANING_SLAVE_JOBS_NUM,
    application:get_env(?APP_NAME, autocleaning_slave_jobs_num, 50)).
-define(AUTOCLEANING_PARALLEL_ORDERS_LIMIT,
    application:get_env(?APP_NAME, autocleaning_parallel_orders_limit, 10)).

-define(TASK_ID_SEPARATOR, <<"$$">>).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec init_pool() -> ok.
init_pool() ->
    view_traverse:init(?MODULE, ?AUTOCLEANING_MASTER_JOBS_NUM, ?AUTOCLEANING_SLAVE_JOBS_NUM,
        ?AUTOCLEANING_PARALLEL_ORDERS_LIMIT, false).

-spec stop_pool() -> ok.
stop_pool() ->
    view_traverse:stop(?MODULE).

-spec run(od_space:id(), autocleaning:run_id(), autocleaning_config:rules(), non_neg_integer(),
    view_traverse:token() | undefined) -> {ok, view_traverse:task_id()} | {error, term()}.
run(SpaceId, AutocleaningRunId, AutocleaningRules, BatchSize, Token) ->
    TaskId = pack_task_id(SpaceId, AutocleaningRunId),
    view_traverse:run(?MODULE, ?FILE_POPULARITY_VIEW(SpaceId), TaskId, #{
        query_opts => #{limit => BatchSize},
        async_next_batch_job => true,
        info => #{
            batch_size => BatchSize,
            run_id => AutocleaningRunId,
            space_id => SpaceId,
            rules => AutocleaningRules
        },
        token => Token
    }).


-spec cancel(task_id()) -> ok.
cancel(TaskId) ->
    ok = view_traverse:cancel(?MODULE, TaskId).


%%%===================================================================
%%% view_traverse callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link view_traverse} callback process_row/3.
%% @end
%%--------------------------------------------------------------------
-spec process_row(json_utils:json_map(), info(), non_neg_integer()) -> ok.
process_row(Row, #{
    rules := AutocleaningRules,
    run_id := AutocleaningRunId,
    space_id := SpaceId,
    batch_size := BatchSize
}, RowNumber) ->
    FileId = maps:get(<<"value">>, Row),
    {ok, Guid} = file_id:objectid_to_guid(FileId),
    FileCtx = file_ctx:new_by_guid(Guid),
    BatchNo = autocleaning_run_controller:batch_no(RowNumber, BatchSize),
    % TODO VFS-7440 - Can we clean hardlink pointing on deleted file?
    Continue = not file_ctx:is_hardlink_const(FileCtx) andalso
        autocleaning_rules:are_all_rules_satisfied(FileCtx, AutocleaningRules) andalso
        autocleaning_run:is_active(AutocleaningRunId),
    try Continue of
        true ->
            maybe_schedule_replica_deletion_task(FileCtx, AutocleaningRunId, SpaceId, BatchNo);
        _ ->
            autocleaning_run_controller:notify_processed_file(SpaceId, AutocleaningRunId, BatchNo)
        catch
            Error:Reason ->
                Uuid = file_ctx:get_uuid_const(FileCtx),
                SpaceId = file_ctx:get_space_id_const(FileCtx),
                autocleaning_run_controller:notify_processed_file(SpaceId, AutocleaningRunId, BatchNo),
                ?error_stacktrace("Filtering preselected file with uuid ~p in space ~p failed due to ~p:~p",
                    [Uuid, SpaceId, Error, Reason]),
                ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link view_traverse} callback batch_prehook/4.
%% @end
%%--------------------------------------------------------------------
-spec batch_prehook(non_neg_integer(), [json_utils:json_map()], view_traverse:token(), info()) -> ok.
batch_prehook(_BatchOffset, [], _Token, _Info) ->
    ok;
batch_prehook(BatchOffset, Rows, Token, #{
    run_id := AutocleaningRunId,
    space_id := SpaceId,
    batch_size := BatchSize
}) ->
    BatchNo = autocleaning_run_controller:batch_no(BatchOffset, BatchSize),
    autocleaning_run_controller:notify_files_to_process(SpaceId, AutocleaningRunId, length(Rows), BatchNo, Token).

%%--------------------------------------------------------------------
%% @doc
%% {@link view_traverse} callback on_batch_canceled/4.
%% @end
%%--------------------------------------------------------------------
-spec on_batch_canceled(non_neg_integer(), non_neg_integer(), view_traverse:token(), info()) -> ok.
on_batch_canceled(_BatchOffset, 0, _Token, _Info) ->
    ok;
on_batch_canceled(BatchOffset, RowJobsCancelled, _Token, #{
    run_id := AutocleaningRunId,
    space_id := SpaceId,
    batch_size := BatchSize
}) ->
    BatchNo = autocleaning_run_controller:batch_no(BatchOffset, BatchSize),
    autocleaning_run_controller:notify_processed_files(SpaceId, AutocleaningRunId, RowJobsCancelled, BatchNo).

%%--------------------------------------------------------------------
%% @doc
%% {@link view_traverse} callback task_finished/1.
%% @end
%%--------------------------------------------------------------------
-spec task_finished(task_id()) -> ok.
task_finished(TaskId) ->
    notify_finished_traverse(TaskId).

%%--------------------------------------------------------------------
%% @doc
%% {@link view_traverse} callback task_canceled/1.
%% @end
%%--------------------------------------------------------------------
-spec task_canceled(task_id()) -> ok.
task_canceled(TaskId) ->
    notify_finished_traverse(TaskId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec pack_task_id(od_space:id(), autocleaning:run_id()) -> task_id().
pack_task_id(SpaceId, AutocleaningRunId) ->
    str_utils:join_binary([SpaceId, AutocleaningRunId, datastore_key:new()], ?TASK_ID_SEPARATOR).

-spec unpack_task_id(task_id()) -> {od_space:id(), autocleaning:run_id()}.
unpack_task_id(TaskId) ->
    [SpaceId, AutocleaningId, _RandSuffix] = binary:split(TaskId, ?TASK_ID_SEPARATOR, [global]),
    {SpaceId, AutocleaningId}.

-spec maybe_schedule_replica_deletion_task(file_ctx:ctx(), autocleaning_run:id(), od_space:id(), non_neg_integer()) -> ok.
maybe_schedule_replica_deletion_task(FileCtx, ARId, SpaceId, BatchNo) ->
    case replica_deletion_master:find_supporter_and_prepare_deletion_request(FileCtx) of
        undefined ->
            autocleaning_run_controller:notify_processed_file(SpaceId, ARId, BatchNo);
        DeletionRequest ->
            BatchId = autocleaning_run_controller:pack_batch_id(ARId, BatchNo),
            ok = replica_deletion_master:request_deletion(SpaceId, DeletionRequest, BatchId, ?AUTOCLEANING_JOB)
    end.

-spec notify_finished_traverse(task_id()) -> ok.
notify_finished_traverse(TaskId) ->
    {SpaceId, AutocleaningRunId} = unpack_task_id(TaskId),
    autocleaning_run_controller:notify_finished_traverse(SpaceId, AutocleaningRunId).