%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module responsible for traversing file tree. It is required to be executed
%%% once per space so missing link documents (due to bug in previous versions)
%%% can be fetched and properly saved in datastore.
%%% @end
%%%--------------------------------------------------------------------
-module(file_links_reconciliation_traverse).
-author("Michal Stanisz").

-behavior(traverse_behaviour).

-include("global_definitions.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("tree_traverse.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([start/0]).
-export([init_pool/0, stop_pool/0]).
-export([pool_name/0]).

%% Traverse behaviour callbacks
-export([do_master_job/2, do_slave_job/2, task_started/2, task_finished/2,
    get_job/1, update_job_progress/5]).

-type id() :: od_space:id().

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, op_worker:get_env(file_links_reconciliation_traverse_batch_size, 40)).

-define(LISTING_ERROR_RETRY_INITIAL_SLEEP, timer:seconds(2)).
-define(LISTING_ERROR_RETRY_MAX_SLEEP, timer:hours(2)).

%%%===================================================================
%%% API
%%%===================================================================

-spec start() -> ok.
start() ->
    {ok, Spaces} = provider_logic:get_spaces(),
    lists:foreach(fun(SpaceId) ->
        case space_logic:get_provider_ids(SpaceId) of
            [_] ->
                ok; % no need to execute on spaces supported by just one provider
            _ ->
                FileCtx = file_ctx:new_by_guid(fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)),
                try
                    ok = ?extract_ok(tree_traverse:run(?POOL_NAME, FileCtx, #{
                        task_id => SpaceId,
                        batch_size => ?TRAVERSE_BATCH_SIZE
                    }))
                catch Class:Reason ->
                    case datastore_runner:normalize_error(Reason) of
                        already_exists -> ok;
                        _ ->
                            ?error_stacktrace("Unexpected error in file_links_reconciliation_traverse", Class, Reason),
                            erlang:apply(erlang, Class, [Reason])
                    end
                end
        end
    end, Spaces).


-spec init_pool() -> ok  | no_return().
init_pool() ->
    % Get pool limits from app.config
    MasterJobsLimit = op_worker:get_env(file_links_reconciliation_traverse_master_jobs_limit, 10),
    SlaveJobsLimit = op_worker:get_env(file_links_reconciliation_traverse_slave_jobs_limit, 20),
    ParallelismLimit = op_worker:get_env(file_links_reconciliation_traverse_parallelism_limit, 20),
    
    tree_traverse:init(?MODULE, MasterJobsLimit, SlaveJobsLimit, ParallelismLimit).


-spec stop_pool() -> ok.
stop_pool() ->
    tree_traverse:stop(?POOL_NAME).


-spec pool_name() -> traverse:pool().
pool_name() ->
    ?POOL_NAME.


%%%===================================================================
%%% Traverse callbacks
%%%===================================================================

-spec get_job(traverse:job_id()) ->
    {ok, tree_traverse:master_job(), traverse:pool(), tree_traverse:id()}  | {error, term()}.
get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).


-spec task_started(id(), traverse:pool()) -> ok.
task_started(TaskId, _PoolName) ->
    ?notice("File tree links reconciliation traverse started for space ~p.", [TaskId]).


-spec task_finished(id(), traverse:pool()) -> ok.
task_finished(TaskId, _PoolName) ->
    ?notice("File tree links reconciliation traverse finished for space ~p.", [TaskId]).


-spec update_job_progress(undefined | main_job | traverse:job_id(),
    tree_traverse:master_job(), traverse:pool(), id(),
    traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.
update_job_progress(Id, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(Id, Job, Pool, TaskId, Status, ?MODULE).


-spec do_master_job(tree_traverse:master_job() | tree_traverse:slave_job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()}.
do_master_job(Job = #tree_traverse_slave{}, #{task_id := TaskId}) ->
    do_slave_job(Job, TaskId);
do_master_job(Job, MasterJobArgs) ->
    do_master_job_internal(Job, MasterJobArgs, ?LISTING_ERROR_RETRY_INITIAL_SLEEP).


do_master_job_internal(Job, MasterJobArgs, Sleep) ->
    try
        tree_traverse:do_master_job(Job, MasterJobArgs)
    catch Class:Reason ->
        ?error_stacktrace("Error when listing children in file_links_reconciliation_traverse", Class, Reason),
        timer:sleep(Sleep),
        do_master_job_internal(Job, MasterJobArgs, min(Sleep * 2, ?LISTING_ERROR_RETRY_MAX_SLEEP))
    end.


-spec do_slave_job(tree_traverse:slave_job(), id()) -> ok.
do_slave_job(#tree_traverse_slave{}, _TaskId) ->
    ok.