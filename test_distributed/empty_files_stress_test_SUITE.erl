%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains save stress test for single provider. SUITE tests
%%% creation of large dir by single process and tree of dirs by many processes.
%%% @end
%%%--------------------------------------------------------------------
-module(empty_files_stress_test_SUITE).
-author("Michal Wrzeszcz").

-behaviour(traverse_behaviour).

-include("global_definitions.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([stress_test/1, stress_test_base/1, many_files_creation_tree_test/1,
    many_files_creation_tree_test_base/1]).

%% Pool callbacks
-export([do_master_job/2, do_slave_job/2, update_job_progress/5, get_job/1, get_sync_info/1]).

-define(STRESS_CASES, []).
-define(STRESS_NO_CLEARING_CASES, [
    many_files_creation_tree_test
]).

all() ->
    ?STRESS_ALL(?STRESS_CASES, ?STRESS_NO_CLEARING_CASES).

-define(TIMEOUT, timer:minutes(20)).
-define(CACHE, test_cache).
-define(CALL_CACHE(Worker, Op, Args), rpc:call(Worker, effective_value, Op, [?CACHE | Args])).

%%%===================================================================
%%% Test functions
%%%===================================================================

stress_test(Config) ->
    ?STRESS(Config,[
            {description, "Main stress test function. Links together all cases to be done multiple times as one continous test."},
            {success_rate, 100},
            {config, [{name, stress}, {description, "Basic config for stress test"}]}
        ]
    ).
stress_test_base(Config) ->
    ?STRESS_TEST_BASE(Config).

%%%===================================================================

many_files_creation_tree_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, spawn_beg_level}, {value, 2}, {description, "Level of tree to start spawning processes"}],
            [{name, spawn_end_level}, {value, 3}, {description, "Level of tree to stop spawning processes"}],
            [{name, dir_level}, {value, 3}, {description, "Level of last test directory"}],
            [{name, dirs_per_parent}, {value, 6}, {description, "Child directories in single dir"}],
            [{name, files_per_dir}, {value, 40}, {description, "Number of files in single directory"}]
        ]},
        {description, "Creates directories' and files' tree using multiple process"}
    ]).
many_files_creation_tree_test_base(Config) ->
    case get(stress_phase) of
        undefined ->
            case files_stress_test_base:many_files_creation_tree_test_base(Config, false, true) of
                [stop | PhaseAns] ->
                    start_traverse(Config, undefined, <<"1">>),
                    put(stress_phase, traverse),
                    PhaseAns;
                Other ->
                    Other
            end;
        traverse ->
            {Stop, Ans} = process_traverse_info(Config, standard, <<"1">>),
            case Stop of
                true ->
                    start_traverse(Config, precalculate_dir, <<"2">>),
                    put(stress_phase, traverse2),
                    Ans;
                _ ->
                    Ans
            end;
        traverse2 ->
            {Stop, Ans} = process_traverse_info(Config, precalculate_dir, <<"2">>),
            case Stop of
                true -> [stop | Ans];
                _ -> Ans
            end
    end.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    files_stress_test_base:init_per_suite(Config).

end_per_suite(Config) ->
    files_stress_test_base:end_per_suite(Config).

init_per_testcase(stress_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, tree_traverse, init, [?MODULE, 5, 30, 10])),

    CachePid = spawn(Worker, fun() -> cache_proc(
        #{check_frequency => timer:minutes(1), size => 500}) end),

    files_stress_test_base:init_per_testcase(Case, [{cache_pid, CachePid} | Config]);
init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(stress_test = Case, Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, rpc:call(Worker, tree_traverse, stop, [?MODULE])),
    CachePid = ?config(cache_pid, Config),
    CachePid ! {finish, self()},
    ok = receive
             finished -> ok
         after
             1000 -> timeout
         end,

    files_stress_test_base:end_per_testcase(Case, Config);
end_per_testcase(_Case, Config) ->
    Config.

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

do_master_job(Job, TaskID) ->
    case tree_traverse:get_traverse_info(Job) of
        precalculate_dir ->
            Doc = tree_traverse:get_doc(Job),
            Callback = fun(Args) -> get_file_level(Args) end,
            {ok, _, CalculationInfo} = effective_value:get_or_calculate(?CACHE, Doc, Callback, 0, []),
            case CalculationInfo of
                0 ->
                    tree_traverse:do_master_job(Job, TaskID);
                _ ->
                    {ok, Info} = tree_traverse:do_master_job(Job, TaskID),
                    Info2 = Info#{description => #{dirs_evaluation => CalculationInfo}},
                    {ok, Info2}
            end;
        _ ->
            tree_traverse:do_master_job(Job, TaskID)
    end.

do_slave_job({Doc, _TraverseInfo}, _TaskID) ->
    Callback = fun(Args) -> get_file_level(Args) end,
    {ok, _, CalculationInfo} = effective_value:get_or_calculate(?CACHE, Doc, Callback, 0, []),
    case CalculationInfo of
        1 -> ok;
        _ -> {ok, #{dirs_evaluation => CalculationInfo}}
    end.

update_job_progress(ID, Job, Pool, TaskID, Status) ->
    tree_traverse:update_job_progress(ID, Job, Pool, TaskID, Status, ?MODULE).

get_job(DocOrID) ->
    tree_traverse:get_job(DocOrID).

get_sync_info(Job) ->
    tree_traverse:get_sync_info(Job).

%%%===================================================================
%%% Internal functions
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

get_file_level([_, undefined, CalculationInfo]) ->
    {ok, 0, CalculationInfo + 1};
get_file_level([_, ParentValue, CalculationInfo]) ->
    {ok, ParentValue + 1, CalculationInfo + 1}.

start_traverse(Config, TraverseCache, ID) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?assertEqual(ok, ?CALL_CACHE(Worker, invalidate, [])),

    User = <<"user1">>,
    SessId = ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    {ok, Guid} = ?assertMatch({ok, _},
        lfm_proxy:resolve_guid(Worker, SessId, <<"/", SpaceName/binary>>)),
    RunOpts = #{task_id => ID, traverse_cache => TraverseCache},
    ?assertMatch({ok, _}, rpc:call(Worker, tree_traverse, run, [?MODULE,
        file_ctx:new_by_guid(Guid), RunOpts])).

process_traverse_info(Config, Type, ID) ->
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

    Ans = files_stress_test_base:get_final_ans_tree(Worker, 0, 0, 0, 0, 0, 0, 0, 0),
    {All == DirsDone + Failed, Ans}.