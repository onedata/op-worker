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

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type id() :: od_space:id().

-define(POOL_NAME, atom_to_binary(?MODULE, utf8)).
-define(TRAVERSE_BATCH_SIZE, op_worker:get_env(file_links_reconciliation_traverse_batch_size, 40)).
-define(MASTER_JOBS_LIMIT, op_worker:get_env(file_links_reconciliation_traverse_master_jobs_limit, 10)).
-define(SLAVE_JOBS_LIMIT, op_worker:get_env(file_links_reconciliation_traverse_slave_jobs_limit, 20)).
-define(PARALLELISM_LIMIT, op_worker:get_env(file_links_reconciliation_traverse_parallelism_limit, 20)).

-define(LISTING_ERROR_RETRY_INITIAL_SLEEP, timer:seconds(2)).
-define(LISTING_ERROR_RETRY_MAX_SLEEP, timer:hours(2)).

-define(STATUS_DOCUMENT_KEY, <<"file_links_reconciliation_traverse_doc">>).

-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start() -> ok.
start() ->
    {ok, Spaces} = provider_logic:get_spaces(),
    SpacesToStart = lists:filter(fun(SpaceId) ->
        case space_logic:get_provider_ids(SpaceId) of
            {ok, [_]} ->
                false; % no need to execute on spaces supported by just one provider
            {ok, _} ->
                true
        end
    end, Spaces),
    ok = ?extract_ok(?ok_if_exists(datastore_model:create(?CTX, #document{
        key = ?STATUS_DOCUMENT_KEY,
        value = #file_links_reconciliation_traverse{ongoing_spaces = SpacesToStart}
    }))),
    lists:foreach(fun(SpaceId) ->
        FileCtx = file_ctx:new_by_guid(fslogic_file_id:spaceid_to_space_dir_guid(SpaceId)),
        try
            ok = ?extract_ok(tree_traverse:run(?POOL_NAME, FileCtx, #{
                task_id => SpaceId,
                batch_size => ?TRAVERSE_BATCH_SIZE,
                handle_interrupted_call => false
            }))
        catch Class:Reason ->
            case datastore_runner:normalize_error(Reason) of
                already_exists -> ok;
                [{error, already_exists}] -> ok;
                _ ->
                    ?error_stacktrace("Unexpected error in file_links_reconciliation_traverse", Class, Reason),
                    erlang:apply(erlang, Class, [Reason])
            end
        end
    end, SpacesToStart).


-spec init_pool() -> ok  | no_return().
init_pool() ->
    case datastore_model:get(?CTX, ?STATUS_DOCUMENT_KEY) of
        {ok, _} ->
            ok;
        {error, not_found} ->
            tree_traverse:init(?MODULE, ?MASTER_JOBS_LIMIT, ?SLAVE_JOBS_LIMIT, ?PARALLELISM_LIMIT)
    end.


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
    Res = datastore_model:update(?CTX, ?STATUS_DOCUMENT_KEY,
        fun(#file_links_reconciliation_traverse{ongoing_spaces = OngoingSpaces} = Record) ->
            {ok, Record#file_links_reconciliation_traverse{ongoing_spaces = OngoingSpaces -- [TaskId]}}
        end
    ),
    case Res of
        {ok, #document{value = #file_links_reconciliation_traverse{ongoing_spaces = []}}} ->
            spawn(fun() ->
                % sleep to ensure that task_finished function has time to finish,
                timer:sleep(timer:seconds(10)),
                stop_pool()
            end);
        _ ->
            ok
    end,
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


%%%===================================================================
%%% Datastore model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {ongoing_spaces, [string]}
    ]}.
