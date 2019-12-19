%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_view_traverse).
-author("Jakub Kudzia").

-behaviour(view_traverse).

-include("modules/fslogic/file_popularity_view.hrl").
-include_lib("cluster_worker/include/traverse/view_traverse.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init_pool/0, stop_pool/0, run/5, cancel/1]).

%% view_traverse callbacks
-export([process_row/3, batch_prehook/3, task_finished/1]).

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
    application:get_env(?APP_NAME, autocleaning_slave_jobs_num, 20)).
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
    view_traverse:token() | undefined) -> ok | {error, term()}.
run(SpaceId, AutocleaningRunId, AutocleaningRules, BatchSize, Token) ->
    TaskId = pack_task_id(SpaceId, AutocleaningRunId),
    view_traverse:run(?MODULE, ?FILE_POPULARITY_VIEW(SpaceId), TaskId, #{
        query_opts => #{limit => BatchSize},
        info => #{
            batch_size => BatchSize,
            run_id => AutocleaningRunId,
            space_id => SpaceId,
            rules => AutocleaningRules
        },
        token => Token
    }).

-spec cancel(autocleaning:run_id()) -> ok.
cancel(AutocleaningRunId) ->
    view_traverse:cancel(?MODULE, AutocleaningRunId).

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
    BatchNo = RowNumber div BatchSize,
    try autocleaning_rules:are_all_rules_satisfied(FileCtx, AutocleaningRules) of
        true ->
            maybe_schedule_replica_deletion_task(FileCtx, AutocleaningRunId, SpaceId, BatchNo);
        false ->
            autocleaning_controller:notify_processed_file(SpaceId, AutocleaningRunId, BatchNo)
        catch
        Error:Reason ->
            Uuid = file_ctx:get_uuid_const(FileCtx),
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            ?error_stacktrace("Filtering preselected file with uuid ~p in space ~p failed due to ~p:~p",
                [Uuid, SpaceId, Error, Reason]),
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link view_traverse} callback batch_prehook/3.
%% @end
%%--------------------------------------------------------------------
-spec batch_prehook([json_utils:json_map()], view_traverse:token(), info()) -> ok.
batch_prehook([], _Token, _Info) ->
    ok;
batch_prehook(Rows, Token, #{
    run_id := AutocleaningRunId,
    space_id := SpaceId
}) ->
    autocleaning_controller:notify_files_to_process(SpaceId, AutocleaningRunId, length(Rows), Token).

%%--------------------------------------------------------------------
%% @doc
%% {@link view_traverse} callback task_finished/1.
%% @end
%%--------------------------------------------------------------------
-spec task_finished(task_id()) -> ok.
task_finished(TaskId) ->
    notify_finished_traverse(TaskId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec pack_task_id(od_space:id(), autocleaning:run_id()) -> task_id().
pack_task_id(SpaceId, AutocleaningRunId) ->
    str_utils:join_binary([SpaceId, AutocleaningRunId], ?TASK_ID_SEPARATOR).

-spec unpack_task_id(task_id()) -> {od_space:id(), autocleaning:run_id()}.
unpack_task_id(TaskId) ->
    [SpaceId, AutocleaningId] = binary:split(TaskId, ?TASK_ID_SEPARATOR),
    {SpaceId, AutocleaningId}.

-spec maybe_schedule_replica_deletion_task(file_ctx:ctx(), autocleaning_run:id(), od_space:id(), non_neg_integer()) -> ok.
maybe_schedule_replica_deletion_task(FileCtx, ARId, SpaceId, BatchNo) ->
    case replica_deletion_master:find_supporter_and_prepare_deletion_request(FileCtx) of
        undefined ->
            autocleaning_controller:notify_processed_file(SpaceId, ARId, BatchNo);
        DeletionRequest ->
            BatchId = autocleaning_controller:pack_batch_id(ARId, BatchNo),
            ok = replica_deletion_master:request_autocleaning_deletion(SpaceId, DeletionRequest, BatchId)
    end.

-spec notify_finished_traverse(task_id()) -> ok.
notify_finished_traverse(TaskId) ->
    {SpaceId, AutocleaningRunId} = unpack_task_id(TaskId),
    autocleaning_controller:notify_finished_traverse(SpaceId, AutocleaningRunId).