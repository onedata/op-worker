%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Utility functions for traverse and effective value stress testing.
%%% @end
%%%--------------------------------------------------------------------
-module(stress_test_traverse_pool).
-author("Michal Wrzeszcz").

-behaviour(traverse_behaviour).

-include("tree_traverse.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% Pool callbacks
-export([do_master_job/2, do_slave_job/2, update_job_progress/5, get_job/1, get_sync_info/1]).

%% SetUp and TearDown functions
-export([init_pool/2, stop_pool/1]).

%% Test steps
-export([test_step/3]).

%% Helper functions
-export([cache_proc/1, start_traverse/3, process_task_description/3]).

-define(CACHE, test_cache).
-define(CALL_CACHE(Worker, Op, Args), rpc:call(Worker, effective_value, Op, [?CACHE | Args])).

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

do_master_job(Job = #tree_traverse{file_ctx = FileCtx}, TaskId) ->
    case tree_traverse:get_traverse_info(Job) of
        #{mode := precalculate_dir} ->
            {Doc, _} = file_ctx:get_file_doc(FileCtx),
            Callback = fun(Args) -> process_file(Args) end,
            {ok, _, CalculationInfo} = effective_value:get_or_calculate(?CACHE, Doc, Callback, set_ev_options(Job)),
            case CalculationInfo of
                {0, _} ->
                    tree_traverse:do_master_job(Job, TaskId);
                {ItemsTraversed, _} ->
                    {ok, Info} = tree_traverse:do_master_job(Job, TaskId),
                    Info2 = Info#{description => #{dirs_evaluation => ItemsTraversed}},
                    {ok, Info2}
            end;
        _ ->
            tree_traverse:do_master_job(Job, TaskId)
    end.

do_slave_job(Job = #tree_traverse_slave{file_ctx = FileCtx}, _TaskId) ->
    Callback = fun(Args) -> process_file(Args) end,
    {Doc, _} = file_ctx:get_file_doc(FileCtx),
    {ok, _, CalculationInfo} = effective_value:get_or_calculate(?CACHE, Doc, Callback, set_ev_options(Job)),
    case CalculationInfo of
        {0, _} -> ok; % Get from cache
        {ItemsTraversed, ItemsTraversed} -> ok; % All dirs have been get from cache
        {ItemsTraversed, PathsTraversed} -> {ok, #{dirs_evaluation => ItemsTraversed - PathsTraversed}}
    end.

update_job_progress(ID, Job, Pool, TaskId, Status) ->
    tree_traverse:update_job_progress(ID, Job, Pool, TaskId, Status, ?MODULE).

get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_pool(Config, Size) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, tree_traverse, init, [stress_test_traverse_pool, 5, 30, 10])),

    CachePid = spawn(Worker, fun() -> stress_test_traverse_pool:cache_proc(#{
        check_frequency => timer:minutes(1),
        size => Size
    }) end),

    [{cache_pid, CachePid} | Config].

stop_pool(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, tree_traverse, stop, [stress_test_traverse_pool])),
    CachePid = ?config(cache_pid, Config),
    CachePid ! {finish, self()},
    ok = receive
        finished -> ok
    after
        1000 -> timeout
    end.

%%%===================================================================
%%% Test steps
%%%===================================================================

test_step(Config, Options, MultipathEV) ->
    case get(stress_phase) of
        undefined ->
            case files_stress_test_base:many_files_creation_tree_test_base(Config, Options) of
                [stop | PhaseAns] ->
                    stress_test_traverse_pool:start_traverse(Config,
                        #{mode => undefined, multipath_ev => MultipathEV}, <<"1">>),
                    put(stress_phase, traverse),
                    PhaseAns;
                Other ->
                    Other
            end;
        traverse ->
            {Stop, Ans} = stress_test_traverse_pool:process_task_description(Config, standard, <<"1">>),
            case Stop of
                true ->
                    stress_test_traverse_pool:start_traverse(Config,
                        #{mode => precalculate_dir, multipath_ev => MultipathEV}, <<"2">>),
                    put(stress_phase, traverse2),
                    Ans;
                _ ->
                    Ans
            end;
        traverse2 ->
            {Stop, Ans} = stress_test_traverse_pool:process_task_description(Config, precalculate_dir, <<"2">>),
            case Stop of
                true ->
                    stress_test_traverse_pool:start_traverse(Config,
                        #{mode => undefined, critical_section => parent, multipath_ev => MultipathEV}, <<"3">>),
                    put(stress_phase, traverse3),
                    Ans;
                _ ->
                    Ans
            end;
        traverse3 ->
            {Stop, Ans} = stress_test_traverse_pool:process_task_description(Config, critical_section, <<"3">>),
            case Stop of
                true -> [stop | Ans];
                _ -> Ans
            end
    end.

%%%===================================================================
%%% Helper functions
%%%===================================================================

cache_proc(Options) ->
    bounded_cache:init_cache(?CACHE, Options),
    cache_proc().

cache_proc() ->
    receive
        {bounded_cache_timer, Options} ->
            bounded_cache:check_cache_size(Options),
            cache_proc();
        {finish, Pid} ->
            bounded_cache:terminate_cache(?CACHE),
            Pid ! finished
    end.

process_file([_, undefined, {ItemsTraversed, PathsTraversed}]) ->
    {ok, 0, {ItemsTraversed + 1, PathsTraversed}};
process_file([_, ParentValue, {ItemsTraversed, PathsTraversed}]) ->
    {ok, ParentValue + 1, {ItemsTraversed + 1, PathsTraversed}}.

merge_paths_info(Value1, Value2, {ItemsTraversed1, PathsTraversed1}, {ItemsTraversed2, PathsTraversed2}) ->
    {ok, Value1 + Value2, {ItemsTraversed1 + ItemsTraversed2, PathsTraversed1 + PathsTraversed2}}.

start_traverse(Config, TraverseInfo, ID) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, ?CALL_CACHE(Worker, invalidate, [])),

    ct:print("Start traverse ~p", [ID]),

    User = <<"user1">>,
    SessId = ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    {ok, Guid} = ?assertMatch({ok, _},
        lfm_proxy:resolve_guid(Worker, SessId, <<"/", SpaceName/binary>>)),
    RunOpts = #{task_id => ID, traverse_info => TraverseInfo},
    ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, run, [?MODULE,
        file_ctx:new_by_guid(Guid), RunOpts])).

process_task_description(Config, Type, ID) ->
    timer:sleep(timer:seconds(30)),
    [Worker | _] = ?config(op_worker_nodes, Config),
    DirLevel = ?config(dir_level, Config),
    {ok, #document{value = #traverse_task{description = Description}}} =
        ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, get_task, [?MODULE, ID])),

    DirsDone = maps:get(master_jobs_done, Description, 0),
    Failed = maps:get(master_jobs_failed, Description, 0),
    Failed2 = maps:get(slave_jobs_failed, Description, 0),
    All = maps:get(master_jobs_delegated, Description, 0),
    FilesDone = maps:get(slave_jobs_done, Description, 0),
    Evaluations = maps:get(dirs_evaluation, Description, 0),
    RequiredEvaluations = FilesDone * (DirLevel + 1),

    ct:print("Files done: ~p, dirs done ~p~ndirs evaluations ~p, possible evaluations ~p, cache size ~p, test type ~p",
        [FilesDone, DirsDone, Evaluations, RequiredEvaluations, 500, Type]),

    case Failed + Failed2 > 0 of
        true -> ct:print("Alert! Master jobs failed: ~p, slave jobs failed ~p", [Failed, Failed2]);
        _ -> ok
    end,

    Ans = files_stress_test_base:get_final_ans_tree(Worker, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    {All == DirsDone + Failed, Ans}.

set_ev_options(Job) ->
    TraverseInfo = tree_traverse:get_traverse_info(Job),
    CriticalSection = maps:get(critical_section, TraverseInfo, false),
    Options = #{initial_calculation_info => {0, 1}, in_critical_section => CriticalSection},

    case maps:get(multipath_ev, TraverseInfo, false) of
        true ->
            MergeCallback = fun(NewValue, ValueAcc, NewCalculationInfo, CalculationInfoAcc) ->
                merge_paths_info(NewValue, ValueAcc, NewCalculationInfo, CalculationInfoAcc)
            end,
            Options#{use_referenced_key => true, merge_callback => MergeCallback};
        false ->
            Options
    end.