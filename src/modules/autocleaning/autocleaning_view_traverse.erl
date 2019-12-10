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
-export([init/0, stop/0, run/3, cancel/1]).

%% view_traverse callbacks
-export([process_row/3, task_finished/1, task_canceled/1]).

-type task_id() :: binary().

% todo ogarnac restartowanie -> musi zostac wystartwoany autocleaning_controller !!!!
% todo ogarnac restartowanie -> czy trzeba czytac 1 batch w przód???

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

init() ->
    view_traverse:init(?MODULE, ?AUTOCLEANING_MASTER_JOBS_NUM, ?AUTOCLEANING_SLAVE_JOBS_NUM,
        ?AUTOCLEANING_PARALLEL_ORDERS_LIMIT).

stop() ->
    view_traverse:stop(?MODULE).

-spec run(od_space:id(), autocleaning:run_id(), autocleaning_config:rules()) -> ok | {error, term()}.
run(SpaceId, AutocleaningRunId, AutocleaningRules) ->
    TaskId = pack_task_id(SpaceId, AutocleaningRunId),
    view_traverse:run(?MODULE, ?FILE_POPULARITY_VIEW(SpaceId), TaskId, #{
        run_id => AutocleaningRunId,
        space_id => SpaceId,
        rules => AutocleaningRules
    }).

cancel(AutocleaningRunId) ->
    view_traverse:cancel(?MODULE, AutocleaningRunId).

%%%===================================================================
%%% view_traverse callbacks
%%%===================================================================

%%% todo spec
process_row(Row, #{
    rules := AutocleaningRules,
    run_id := AutocleaningRunId,
    space_id := SpaceId
}, _RowNumber) ->
    {<<"value">>, FileId} = lists:keyfind(<<"value">>, 1, Row),
    {ok, Guid} = file_id:objectid_to_guid(FileId),
    FileCtx = file_ctx:new_by_guid(Guid),
    try autocleaning_rules:are_all_rules_satisfied(FileCtx, AutocleaningRules) of
        true ->
            maybe_schedule_replica_deletion_task(FileCtx, AutocleaningRunId, SpaceId);
        false ->
            ok
    catch
        Error:Reason ->
            Uuid = file_ctx:get_uuid_const(FileCtx),
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            ?error_stacktrace("Filtering preselected file with uuid ~p in space ~p failed due to ~p:~p",
                [Uuid, SpaceId, Error, Reason]),
            ok
    end.


task_finished(TaskId) ->
    notify_finished_traverse(TaskId).

task_canceled(TaskId) ->
    notify_finished_traverse(TaskId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec pack_task_id(od_space:id(), autocleaning:run_id()) -> task_id().
pack_task_id(SpaceId, AutocleaningRunId) ->
    str_utils:join_binary([SpaceId, AutocleaningRunId, ?TASK_ID_SEPARATOR]).

-spec unpack_task_id(task_id()) -> {od_space:id(), autocleaning:run_id()}.
unpack_task_id(TaskId) ->
    [SpaceId, AutocleaningId] = binary:split(TaskId, ?TASK_ID_SEPARATOR),
    {SpaceId, AutocleaningId}.

-spec maybe_schedule_replica_deletion_task(file_ctx:ctx(), autocleaning_run:id(), od_space:id()) -> ok.
maybe_schedule_replica_deletion_task(FileCtx, ARId, SpaceId) ->
    case replica_deletion_master:find_supporter_and_prepare_deletion_request(FileCtx) of
        undefined ->
            ok;
        DeletionRequest ->
            ok = replica_deletion_master:request_autocleaning_deletion(SpaceId, DeletionRequest, ARId),
            autocleaning_controller:notify_file_to_process(SpaceId, ARId)
    end.

notify_finished_traverse(TaskId) ->
    {SpaceId, AutocleaningRunId} = unpack_task_id(TaskId),
    autocleaning_controller:notify_finished_traverse(SpaceId, AutocleaningRunId).