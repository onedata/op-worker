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
-module(save_stress_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/oz/oz_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([stress_test/1, stress_test_base/1, many_files_creation_tree_test/1, many_files_creation_tree_test_base/1,
    single_dir_creation_test/1, single_dir_creation_test_base/1]).
-export([create_single_call/3]).

-define(STRESS_CASES, [single_dir_creation_test]).
-define(STRESS_NO_CLEARING_CASES, [
    many_files_creation_tree_test
]).

-define(TIMEOUT, timer:minutes(20)).

all() ->
    ?STRESS_ALL(?STRESS_CASES, ?STRESS_NO_CLEARING_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

stress_test(Config) ->
    ?STRESS(Config,[
            {description, "Main stress test function. Links together all cases to be done multiple times as one continous test."},
            {success_rate, 90},
            {config, [{name, stress}, {description, "Basic config for stress test"}]}
        ]
    ).
stress_test_base(Config) ->
    ?STRESS_TEST_BASE(Config).

%%%===================================================================

single_dir_creation_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, files_num}, {value, 1000}, {description, "Numer of files in dir"}]
        ]},
        {description, "Creates files in dir using single process"}
    ]).
single_dir_creation_test_base(Config) ->
    FilesNum = ?config(files_num, Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    User = <<"user1">>,

    SessId = ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),

    MainDir = generator:gen_name(),
    Dir = <<"/", SpaceName/binary, "/", MainDir/binary>>,

    case lfm_proxy:mkdir(Worker, SessId, Dir, 8#755) of
        {ok, _} ->
            {SaveOk, SaveTime, SError, SErrorTime} =
                rpc:call(Worker, ?MODULE, create_single_call, [SessId, Dir, FilesNum]),

            {DelOk, DelTime, DError, DErrorTime} = lists:foldl(fun(N, {OkNum, OkTime, ErrorNum, ErrorTime}) ->
                {T, A} = measure_execution_time(fun() ->
                    N2 = integer_to_binary(N),
                    File = <<Dir/binary, "/", N2/binary>>,
                    lfm_proxy:unlink(Worker, SessId, {path, File})
                end),
                case A of
                    ok ->
                        {OkNum+1, OkTime+T, ErrorNum, ErrorTime};
                    _ ->
                        {OkNum, OkTime, ErrorNum+1, ErrorTime+T}
                end
            end, {0,0,0,0}, lists:seq(1,FilesNum)),

            SaveAvgTime = get_avg(SaveOk, SaveTime),
            SErrorAvgTime = get_avg(SError, SErrorTime),
            DelAvgTime = get_avg(DelOk, DelTime),
            DErrorAvgTime = get_avg(DError, DErrorTime),

            ct:print("Save num ~p, del num ~p", [SaveOk, DelOk]),

            get_final_ans(SaveOk, SaveAvgTime, SError, SErrorAvgTime, DelOk, DelAvgTime, DError, DErrorAvgTime, 0);
        _ ->
            timer:sleep(timer:seconds(60)),
            get_final_ans(0,0,0,0,0,0,0,0,1)
    end.

many_files_creation_tree_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, spawn_beg_level}, {value, 4}, {description, "Level of tree to start spawning processes"}],
            [{name, spawn_end_level}, {value, 5}, {description, "Level of tree to stop spawning processes"}],
            [{name, dir_level}, {value, 6}, {description, "Level of last test directory"}],
            [{name, dirs_per_parent}, {value, 6}, {description, "Child directories in single dir"}],
            [{name, files_per_dir}, {value, 40}, {description, "Number of files in single directory"}]
        ]},
        {description, "Creates directories' and files' tree using multiple process"}
    ]).
many_files_creation_tree_test_base(Config) ->
    % Get test and environment description
    SpawnBegLevel = ?config(spawn_beg_level, Config),
    SpawnEndLevel = ?config(spawn_end_level, Config),
    DirLevel = ?config(dir_level, Config),
    DirsPerParent = ?config(dirs_per_parent, Config),
    FilesPerDir = ?config(files_per_dir, Config),

    [Worker | _] = ?config(op_worker_nodes, Config),
    User = <<"user1">>,

    SessId = ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),

    Master = self(),

    StartList = lists:map(fun(N) -> {N, true} end, lists:duplicate(SpawnBegLevel-1, 1)),
    Levels = case get(levels) of
        undefined -> StartList;
        L ->
            L
    end,

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    [{BaseDir, _} | _] = BaseDirsReversed = lists:foldl(fun({N, C}, [{H, _} | _] = Acc) ->
        N2 = integer_to_binary(N),
        NewDir = <<H/binary, "/", N2/binary>>,
        [{NewDir, C} | Acc]
    end, [{<<"/", SpaceName/binary>>, false}], Levels),
    [_ | BaseDirs] = lists:reverse(BaseDirsReversed),

    BaseCreationAns = lists:foldl(fun
        ({D, true}, {ok, _}) ->
            lfm_proxy:mkdir(Worker, SessId, D, 8#755);
        ({D, true}, {error,eexist}) ->
            lfm_proxy:mkdir(Worker, SessId, D, 8#755);
        (_, Acc) ->
            Acc
    end, {ok, ok}, BaseDirs),

    Proceed = case BaseCreationAns of
        {ok, _} ->
            ok;
        {error,eexist} ->
            ok;
        No ->
            No
    end,
    case Proceed of
        ok ->
            Dirs = create_dirs_names(BaseDir, SpawnBegLevel, DirLevel, DirsPerParent),

            Fun = fun(D) ->
                {T, A} = measure_execution_time(fun() ->
                    case lfm_proxy:mkdir(Worker, SessId, D, 8#755) of
                        {ok, _} ->
                            dir_ok;
                        Other ->
                            Other
                    end
                end),
                Master ! {worker_ans, A, T}
            end,

            Fun2 = fun(D) ->
                lists:foreach(fun(N) ->
                    N2 = integer_to_binary(N),
                    F = <<D/binary, "/", N2/binary>>,
                    {T, A} = measure_execution_time(fun() ->
                        case lfm_proxy:create(Worker, SessId, F, 8#755) of
                            {ok, _} ->
                                file_ok;
                            Other ->
                                Other
                        end
                    end),
                    Master ! {worker_ans, A, T}
                end, lists:seq(1, FilesPerDir))
            end,

            spawn_workers(Dirs, Fun, Fun2, SpawnBegLevel, SpawnEndLevel),
            LastLevelDirs = math:pow(DirsPerParent, DirLevel - SpawnBegLevel + 1),
            DirsToDo = DirsPerParent * (1 - LastLevelDirs) / (1 - DirsPerParent),
            FilesToDo = LastLevelDirs * FilesPerDir,
            GatherAns = gather_answers([{file_ok, {0,0}}, {dir_ok, {0,0}}], round(DirsToDo + FilesToDo)),

            NewLevels = lists:foldl(fun
                ({N, _}, []) ->
                    case N of
                        DirsPerParent ->
                            [{1, true}];
                        _ ->
                            [{N+1, true}]
                    end;
                ({N, _}, [H | _] = Acc) ->
                    case H of
                        {1, true} ->
                            case N of
                                DirsPerParent ->
                                    [{1, true} | Acc];
                                _ ->
                                    [{N+1, true} | Acc]
                            end;
                        _ ->
                            [{N, false} | Acc]
                    end
            end, [], lists:reverse(Levels)),
            put(levels, NewLevels),

            {TimeoutCheck, Ans} = GatherAns,
            Timeout = case TimeoutCheck of
                ok -> 0;
                _ -> 1
            end,

            {FilesSaved, FilesTime} = proplists:get_value(file_ok, Ans),
            FilesAvgTime = get_avg(FilesSaved, FilesTime),
            {DirsSaved, DirsTime} = proplists:get_value(dir_ok, Ans),
            DirsAvgTime = get_avg(DirsSaved, DirsTime),
            {OtherAns, OtherTime} = proplists:get_value(other, Ans, {0,0}),
            OtherAvgTime = get_avg(OtherAns, OtherTime),
            FinalAns = get_final_ans_tree(Worker, FilesSaved, FilesAvgTime, DirsSaved,
                DirsAvgTime, OtherAns, OtherAvgTime, 0, Timeout),

            Sum = case get(ok_sum) of
                undefined ->
                    0;
                S ->
                    S
            end,
            NewSum = Sum + FilesSaved + DirsSaved,
            put(ok_sum, NewSum),

            ct:print("Files num ~p, dirs num ~p, agg ~p", [FilesSaved, DirsSaved, NewSum]),

            case NewLevels of
                StartList ->
                    [stop | FinalAns];
                _ ->
                    FinalAns

            end;
        _ ->
            timer:sleep(timer:seconds(60)),
            get_final_ans_tree(Worker, 0, 0, 0, 0, 0,0, 1, 0)
    end.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer]} | Config].

init_per_testcase(stress_test, Config) ->
    application:start(etls),
    hackney:start(),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo);

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(stress_test, Config) ->
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    hackney:stop(),
    application:stop(etls);

end_per_testcase(_Case, Config) ->
    Config.

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_dirs_names(BaseDir, BegLevel, EndLevel, EntriesNumber) ->
    create_dirs_names(BaseDir, 1, BegLevel, EndLevel+1, EntriesNumber+1, []).

create_dirs_names(_CurrentParent, _CurrentNum, LevelLimit, LevelLimit, _EntriesLimit, _TmpAns) ->
    [];
create_dirs_names(_CurrentParent, EntriesLimit, CurrentLevel, LevelLimit, EntriesLimit, TmpAns) ->
    lists:foldl(fun(Dir, Acc) ->
        Children = create_dirs_names(Dir, 1, CurrentLevel + 1, LevelLimit, EntriesLimit, []),
        [{Dir, Children} | Acc]
    end, [], TmpAns);
create_dirs_names(CurrentParent, CurrentNum, CurrentLevel, LevelLimit, EntriesLimit, TmpAns) ->
    CN = integer_to_binary(CurrentNum),
    NewDir = <<CurrentParent/binary, "/", CN/binary>>,
    create_dirs_names(CurrentParent, CurrentNum + 1, CurrentLevel, LevelLimit, EntriesLimit, [NewDir | TmpAns]).

spawn_workers([], _Fun, _Fun2, _Level, _EndSpawnLevel) ->
    ok;
spawn_workers(L, Fun, Fun2, Level, EndSpawnLevel) when is_list(L), Level > EndSpawnLevel ->
    lists:foreach(fun(E) ->
        spawn_workers(E, Fun, Fun2, Level, EndSpawnLevel)
    end, L);
spawn_workers(L, Fun, Fun2, Level, EndSpawnLevel) when is_list(L) ->
    lists:foreach(fun(E) ->
        spawn(fun() -> spawn_workers(E, Fun, Fun2, Level, EndSpawnLevel) end)
    end, L);
spawn_workers({Dir, []}, Fun, Fun2, _Level, _EndSpawnLevel) ->
    Fun(Dir),
    Fun2(Dir);
spawn_workers({Dir, Children}, Fun, Fun2, Level, EndSpawnLevel) ->
    Fun(Dir),
    spawn_workers(Children, Fun, Fun2, Level + 1, EndSpawnLevel).

gather_answers(Answers, 0) ->
    {ok, Answers};
gather_answers(Answers, Num) ->
    case Num rem 1000 of
        0 ->
            ct:print("Gather answers num ~p", [Num]);
        _ ->
            ok
    end,

    receive
        {worker_ans, Ans, ToAddV} ->
            {K, _} = ToAdd = case proplists:lookup(Ans, Answers) of
                {Ans, {V1, V2}} ->
                    {Ans, {V1 + 1, V2 + ToAddV}};
                none ->
                    {V1, V2} = proplists:get_value(other, Answers, {0,0}),
                    {other, {V1 + 1, V2 + ToAddV}}
            end,
            NewAnswers = [ToAdd | proplists:delete(K, Answers)],
            gather_answers(NewAnswers, Num - 1)
    after
        ?TIMEOUT ->
            {timeout, Answers}
    end.

measure_execution_time(Fun) ->
    StartTime = os:timestamp(),
    Ans = Fun(),
    Now = os:timestamp(),
    {timer:now_diff(Now, StartTime), Ans}.

get_avg(Num, Timw) ->
    case Num of
        0 -> Timw;
        _ -> Timw/Num
    end.

get_final_ans_tree(Worker, FilesSaved, FilesTime, DirsSaved, DirsTime, OtherAns, OtherTime, InitFailed, Timeout) ->
    Mem = case rpc:call(Worker, monitoring, get_memory_stats, []) of
        [{<<"mem">>, MemUsage}] ->
            MemUsage;
        _ ->
            -1
    end,
%%    ct:print("Repeat log: ~p", [{FilesSaved, FilesTime, DirsSaved, DirsTime, OtherAns, OtherTime, Mem, InitFailed, Timeout}]),
    [
        #parameter{name = files_saved, value = FilesSaved, description = "Number of files saved"},
        #parameter{name = file_save_avg_time, value = FilesTime, unit = "us",
            description = "Average time of file save operation"},
        #parameter{name = dirs_saved, value = DirsSaved, description = "Number of dirs saved"},
        #parameter{name = dirs_save_avg_time, value = DirsTime, unit = "us",
            description = "Average time of dir save operation"},
        #parameter{name = error_ans_count, value = OtherAns, description = "Number of errors"},
        #parameter{name = error_ans_count_avg_time, value = OtherTime, unit = "us",
            description = "Average time of operation that ended with error"},
        #parameter{name = memory, value = Mem, description = "Memory usage after the test"},
        #parameter{name = init_failed, value = InitFailed, description = "Has first phase of test failed (1 = true)"},
        #parameter{name = timeout, value = Timeout, description = "Has any timeout appeared (1 = true)"}
    ].

get_final_ans(Saved, SaveTime, SErrors, SErrorsTime, Deleted, DelTime, DErrors, DErrorsTime, InitFailed) ->
%%    ct:print("Repeat log: ~p", [{Saved, SaveTime, SErrors, SErrorsTime, Deleted, DelTime, DErrors, DErrorsTime, InitFailed}]),
    [
        #parameter{name = saved, value = Saved, description = "Number of save operations that succed"},
        #parameter{name = save_avg_time, value = SaveTime, unit = "us",
            description = "Average time of successful save operation"},
        #parameter{name = save_error_ans_count, value = SErrors, description = "Number of save errors"},
        #parameter{name = save_error_ans_count_avg_time, value = SErrorsTime, unit = "us",
            description = "Average time of save operation that ended with error"},
        #parameter{name = deleted, value = Deleted, description = "Number of del operations that succed"},
        #parameter{name = del_avg_time, value = DelTime, unit = "us",
            description = "Average time of successful del operation"},
        #parameter{name = del_error_ans_count, value = DErrors, description = "Number of del errors"},
        #parameter{name = del_error_ans_count_avg_time, value = DErrorsTime, unit = "us",
            description = "Average time of del operation that ended with error"},
        #parameter{name = init_failed, value = InitFailed, description = "Has first phase of test failed (1 = true)"}
    ].

create_single_call(SessId, Dir, FilesNum) ->
    lists:foldl(fun(N, {OkNum, OkTime, ErrorNum, ErrorTime}) ->
        {T, A} = measure_execution_time(fun() ->
            N2 = integer_to_binary(N),
            File = <<Dir/binary, "/", N2/binary>>,
            logical_file_manager:create(SessId, File)
        end),
        case A of
            {ok, _} ->
                {OkNum+1, OkTime+T, ErrorNum, ErrorTime};
            _ ->
                {OkNum, OkTime, ErrorNum+1, ErrorTime+T}
        end
    end, {0,0,0,0}, lists:seq(1,FilesNum)).