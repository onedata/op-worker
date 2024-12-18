%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains functions used during the save stress test
%%% for single provider.
%%% @end
%%%--------------------------------------------------------------------
-module(files_stress_test_base).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").
-include_lib("cluster_worker/include/modules/datastore/ha_datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([init_per_suite/1, init_per_testcase/2, end_per_testcase/2, end_per_suite/1]).
-export([many_files_creation_tree_test_base/2, single_dir_creation_test_base/2]).
-export([create_single_call/4, get_final_ans/9, get_final_ans_tree/10, get_param_value/2]).

-define(TIMEOUT, timer:minutes(30)).

%%%===================================================================
%%% Test functions
%%%===================================================================

% Test creates files in a loop. If CreateDeleteLoop is false, number of files grows during the test.
% If CreateDeleteLoop is true, files are deleted after each iteration.
single_dir_creation_test_base(Config, CreateDeleteLoop) ->
    FilesNum = ?config(files_num, Config),
    ProcNum = case CreateDeleteLoop of
        true -> 1;
        false -> ?config(proc_num, Config)
    end,

    [Worker | _] = ?config(op_worker_nodes, Config),
    User = <<"user1">>,

    SessId = ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    RepeatNum = ?config(rep_num, Config),

    % Generate test setup
    {Dir, CheckAns, NameExt} = case {CreateDeleteLoop, RepeatNum} of
        {true, _} ->
            MainDir = generator:gen_name(),
            D = <<"/", SpaceName/binary, "/", MainDir/binary>>,
            MkdirAns = lfm_proxy:mkdir(Worker, SessId, D),
            {D, MkdirAns, 0};
        {_, 1} ->
            case rand:uniform() < 0.5 of
                true ->
                    D = <<"/", SpaceName/binary, "/test_dir">>,
                    MkdirAns = lfm_proxy:mkdir(Worker, SessId, D),
                    put(main_dir, D),
                    put(dirs_created, 1),
                    {D, MkdirAns, 1};
                false ->
                    D = <<"/", SpaceName/binary>>,
                    put(main_dir, D),
                    put(dirs_created, 0),
                    {D, {ok, fslogic_file_id:spaceid_to_space_dir_guid(SpaceId)}, 0}
            end;
        _ ->
            {get(main_dir), {ok, ok}, RepeatNum}
    end,
    NameExtBin = integer_to_binary(NameExt),

    case CheckAns of
        {ok, _} ->
            % Create all
            {SaveOk, SaveTime, SError, SErrorTime} = create_many(Worker, SessId, Dir, FilesNum, ProcNum, NameExtBin),        

            % Delete all is clearing is active
            {DelOk, DelTime, DError, DErrorTime} =
                case CreateDeleteLoop of
                    true ->
                        lists:foldl(fun(N, {OkNum, OkTime, ErrorNum, ErrorTime}) ->
                            {T, A} = measure_execution_time(fun() ->
                                N2 = integer_to_binary(N),
                                File = <<Dir/binary, "/", NameExtBin/binary, "_1_", N2/binary>>,
                                lfm_proxy:unlink(Worker, SessId, {path, File})
                            end),
                            case A of
                                ok ->
                                    {OkNum+1, OkTime+T, ErrorNum, ErrorTime};
                                _ ->
                                    ct:print("Unlink error: ~tp", [A]),
                                    {OkNum, OkTime, ErrorNum+1, ErrorTime+T}
                            end
                        end, {0,0,0,0}, lists:seq(1,FilesNum));
                    _ ->
                        {0,0,0,0}
                end,

            % Gather test results
            SaveAvgTime = get_avg(SaveOk, SaveTime),
            SErrorAvgTime = get_avg(SError, SErrorTime),
            DelAvgTime = get_avg(DelOk, DelTime),
            DErrorAvgTime = get_avg(DError, DErrorTime),

            % Print statistics
            case CreateDeleteLoop of
                true ->
                    ct:print("Save num ~tp, del num ~tp", [SaveOk, DelOk]);
                _ ->
                    Sum = case get(ok_sum) of
                        undefined ->
                            0;
                        S ->
                            S
                    end,
                    NewSum = Sum + SaveOk,
                    put(ok_sum, NewSum),

                    case ?config(test_list, Config) of
                        true ->
                            LastLS = case get(last_ls) of
                                undefined ->
                                    0;
                                LLS ->
                                    LLS
                            end,

                            case NewSum - LastLS >= 20000 of
                                true ->
                                    put(last_ls, NewSum),
                                    LsTime = measure_execution_time(fun() ->
                                        ls(Worker, SessId, Dir, undefined)
                                    end),

                                    ct:print("Save num ~tp, sum ~tp, ls time ~tp",
                                        [SaveOk, NewSum, LsTime]);
                                _ ->
                                    ct:print("Save num ~tp, sum ~tp", [SaveOk, NewSum])
                            end;
                        false ->
                            ct:print("Save num ~tp, sum ~tp", [SaveOk, NewSum])
                    end
            end,

            ?assertEqual(0, SError),
            ?assertEqual(0, DError),
            get_final_ans(SaveOk, SaveAvgTime, SError, SErrorAvgTime, DelOk, DelAvgTime, DError, DErrorAvgTime, 0);
        _ ->
            timer:sleep(timer:seconds(60)),
            ?assertMatch({ok, _}, CheckAns),
            get_final_ans(0,0,0,0,0,0,0,0,1)
    end.

many_files_creation_tree_test_base(Config, Options) ->
    % Get test and environment description
    SpawnBegLevel = ?config(spawn_beg_level, Config),
    SpawnEndLevel = ?config(spawn_end_level, Config),
    DirLevel = ?config(dir_level, Config),
    DirsPerParent = ?config(dirs_per_parent, Config),
    FilesPerDir = ?config(files_per_dir, Config),

    WriteToFile = maps:get(write_to_file, Options, false),
    CacheGUIDS = maps:get(cache_guids, Options, false),
    SetMetadata = maps:get(set_metadata, Options, false),
    HA_Nodes = maps:get(ha_nodes, Options, 1),
    UseHardlinks = maps:get(use_hardlinks, Options, false),

    % Setup test
    [Worker | _] = Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        ?assertEqual(ok, rpc:call(Worker, ha_datastore, change_config, [HA_Nodes, ?HA_CAST_PROPAGATION]))
    end, Workers),

    User = <<"user1">>,

    [SessId | _] = SessIds =
        lists:map(fun(W) -> ?config({session_id, {User, ?GET_DOMAIN(W)}}, Config) end, Workers),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),

    Master = self(),

    StartList = lists:map(fun(N) -> {N, true} end, lists:duplicate(SpawnBegLevel-1, 1)),
    Levels = case get(levels) of
        undefined -> StartList;
        L ->
            L
    end,

    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

    % Create dirs used during main test part (performance not measured)
    [{BaseDir, _} | _] = BaseDirsReversed = lists:foldl(fun({N, C}, [{H, _} | _] = Acc) ->
        N2 = integer_to_binary(N),
        NewDir = <<H/binary, "/", N2/binary>>,
        [{NewDir, C} | Acc]
    end, [{<<"/", SpaceName/binary>>, false}], Levels),
    [_ | BaseDirs] = lists:reverse(BaseDirsReversed),

    {BaseCreationAns, CreatedBaseDirsNum} = lists:foldl(fun
        ({D, true}, {ok, CreatedBaseDirsAcc}) ->
            case lfm_proxy:mkdir(Worker, SessId, D) of
                {ok, _} -> {ok, CreatedBaseDirsAcc + 1};
                {error, eexist} -> {ok, CreatedBaseDirsAcc};
                Other -> {Other, CreatedBaseDirsAcc}
            end;
        (_, Acc) ->
            Acc
    end, {ok, 0}, BaseDirs),

    {ReferencedFilesStatus, ReferencedFilesMap} = case {UseHardlinks, get(referenced_files)} of
        {false, _} ->
            {ok, undefined};
        {true, undefined} ->
            {FoldlStatus, FoldlMap} = FoldlAns = lists:foldl(fun
                (N, {ok, Acc}) ->
                    ReferencedFileName = filename:join(["/", SpaceName, "ref_file" ++ integer_to_list(N)]),
                    case lfm_proxy:create(Worker, SessId, ReferencedFileName) of
                        {ok, ReferencedFileGuid} -> {ok, Acc#{N => ReferencedFileGuid}};
                        Other -> Other
                    end;
                (_, ErrorAcc) ->
                    ErrorAcc
            end, {ok, #{}}, lists:seq(1, FilesPerDir)),

            case FoldlStatus of
                ok -> put(referenced_files, FoldlMap);
                _ -> ok
            end,
            FoldlAns;
        {true, Map} ->
            {ok, Map}
    end,

    case {BaseCreationAns, ReferencedFilesStatus} of
        {ok, ok} ->
            Dirs = create_dirs_names(BaseDir, SpawnBegLevel, DirLevel, DirsPerParent),

            % Function that creates test directories and measures performance
            Fun = fun
                ({{D, DName}, DParent}) ->
                    {W, S} = get_worker_and_session(Workers, SessIds),
                    {T, {A, GUID}} = measure_execution_time(fun() ->
                        MkdirAns = case CacheGUIDS of
                            false ->
                                lfm_proxy:mkdir(W, S, D);
                            _ ->
                                lfm_proxy:mkdir(W, S, DParent, DName, ?DEFAULT_DIR_PERMS)
                        end,
                        case MkdirAns of
                            {ok, DirGuid} ->
                                {dir_ok, DirGuid};
                            Other ->
                                {Other, error}
                        end
                    end),
                    Master ! {worker_ans, A, T},
                    GUID;
                ({D, _}) ->
                    {W, S} = get_worker_and_session(Workers, SessIds),
                    {T, {A, GUID}} = measure_execution_time(fun() ->
                        case lfm_proxy:mkdir(W, S, D) of
                            {ok, DirGuid} ->
                                {dir_ok, DirGuid};
                            Other ->
                                {Other, error}
                        end
                    end),
                    Master ! {worker_ans, A, T},
                    GUID
            end,

            % Function that creates test files and measures performance
            Fun2 = fun(D, GUID) ->
                {W, S} = get_worker_and_session(Workers, SessIds),
                ToSend = lists:foldl(fun(N, Answers) ->
                    N2 = integer_to_binary(N),
                    F = <<D/binary, "/", N2/binary>>,
                    {ToAddV, Ans} = measure_execution_time(fun() ->
                        try
                            case UseHardlinks of
                                true ->
                                    INodeGuid = maps:get(N, ReferencedFilesMap),
                                    {ok, _} = lfm_proxy:make_link(W, S, ?FILE_REF(INodeGuid), ?FILE_REF(GUID), N2),
                                    file_ok;
                                false ->
                                    {ok, FileGUID} = case CacheGUIDS of
                                        false ->
                                            lfm_proxy:create(W, S, F);
                                        _ ->
                                            lfm_proxy:create(W, S, GUID, N2, ?DEFAULT_FILE_PERMS)
                                    end,
                                    % Fill file if needed (depends on test config)
                                    case WriteToFile of
                                        true ->
                                            {ok, Handle} = case CacheGUIDS of
                                                false ->
                                                    lfm_proxy:open(W, S, {path, F}, rdwr);
                                                _ ->
                                                    lfm_proxy:open(W, S, ?FILE_REF(FileGUID), rdwr)
                                            end,
                                            WriteBuf = generator:gen_name(),
                                            WriteSize = size(WriteBuf),
                                            {ok, WriteSize} = lfm_proxy:write(W, Handle, 0, WriteBuf),
                                            ok = lfm_proxy:close(W, Handle),
                                            ok;
                                        _ ->
                                            ok
                                    end,

                                    % set xattr metadata if needed (depends on test config)
                                    case SetMetadata of
                                        true ->
                                            Xattr = #xattr{name = F, value = F},
                                            case CacheGUIDS of
                                                false ->
                                                    lfm_proxy:set_xattr(W, S, {path, F}, Xattr);
                                                _ ->
                                                    lfm_proxy:set_xattr(W, S, ?FILE_REF(FileGUID), Xattr)
                                            end,
                                            file_ok;
                                        _ ->
                                            file_ok
                                    end
                            end
                        catch
                            E1:E2 ->
                                {error, {E1, E2}}
                        end
                    end),
                    process_answer(Answers, Ans, ToAddV)
                end, [{file_ok, {0,0}}], lists:seq(1, FilesPerDir)),
                Master ! {worker_ans, ToSend}
            end,

            % Spawn processes that execute both test functions
            spawn_workers(Dirs, Fun, Fun2, SpawnBegLevel, SpawnEndLevel),
            LastLevelDirs = math:pow(DirsPerParent, DirLevel - SpawnBegLevel + 1),
            DirsToProcess = DirsPerParent * (1 - LastLevelDirs) / (1 - DirsPerParent),
            % Gather test results
            GatherAns = gather_answers([{file_ok, {0,0}}, {dir_ok, {0,0}},
                {other, {0,0}}], round(DirsToProcess + LastLevelDirs)),

            % Calculate and log output
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
                DirsAvgTime, CreatedBaseDirsNum, OtherAns, OtherAvgTime, 0, Timeout),

            Sum = case get(ok_sum) of
                undefined ->
                    0;
                S ->
                    S
            end,
            NewSum = Sum + FilesSaved + DirsSaved,
            put(ok_sum, NewSum),

            ct:print("Files num ~tp, dirs num ~tp, agg ~tp", [FilesSaved, DirsSaved, NewSum]),
            ?assertEqual(ok, TimeoutCheck),
            ?assertEqual(0, OtherAns),

            case NewLevels of
                StartList ->
                    [stop | FinalAns];
                _ ->
                    FinalAns
            end;
        _ ->
            ct:print("Dirs or referenced files not ready"),
            timer:sleep(timer:seconds(60)),
            ?assertEqual(ok, BaseCreationAns),
            get_final_ans_tree(Worker, 0, 0, 0, 0, 0, 0,0, 1, 0)
    end.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    [{?LOAD_MODULES, [initializer, ?MODULE, stress_test_traverse_pool]} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(stress_test, Config) ->
    ssl:start(),
    application:ensure_all_started(hackney),
    initializer:disable_quota_limit(Config),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config, true),

    lfm_proxy:init(ConfigWithSessionInfo);

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(stress_test, Config) ->
    lfm_proxy:teardown(Config),
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:unload_quota_mocks(Config),
    application:stop(hackney),
    ssl:stop();

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
    lists:foldl(fun({DPath, _} = Dir, Acc) ->
        Children = create_dirs_names(DPath, 1, CurrentLevel + 1, LevelLimit, EntriesLimit, []),
        [{Dir, Children} | Acc]
    end, [], TmpAns);
create_dirs_names(CurrentParent, CurrentNum, CurrentLevel, LevelLimit, EntriesLimit, TmpAns) ->
    CN = integer_to_binary(CurrentNum),
    NewDir = <<CurrentParent/binary, "/", CN/binary>>,
    create_dirs_names(CurrentParent, CurrentNum + 1, CurrentLevel, LevelLimit, EntriesLimit, [{NewDir, CN} | TmpAns]).

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
spawn_workers({{{DirPath, _}, _} = Dir, []}, Fun, Fun2, _Level, _EndSpawnLevel) ->
    GUID = Fun(Dir),
    Fun2(DirPath, GUID);
spawn_workers({{DirPath, _} = Dir, []}, Fun, Fun2, _Level, _EndSpawnLevel) ->
    GUID = Fun(Dir),
    Fun2(DirPath, GUID);
spawn_workers({Dir, Children}, Fun, Fun2, Level, EndSpawnLevel) ->
    GUID = Fun(Dir),
    Children2 = lists:map(fun({D2, C2}) ->
        {{D2, GUID}, C2}
    end, Children),
    spawn_workers(Children2, Fun, Fun2, Level + 1, EndSpawnLevel).

gather_answers(Answers, Num) ->
    gather_answers(Answers, Num, 0, 0).

gather_answers(Answers, 0, _, _) ->
    {ok, Answers};
gather_answers(Answers, Num, Gathered, LastReport) ->
    NewLastReport = case (Gathered - LastReport) >= 1000 of
        true ->
            ct:print("Gather answers num ~tp", [Gathered]),
            Gathered;
        _ ->
            LastReport
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
            gather_answers(NewAnswers, Num - 1, Gathered + 1, NewLastReport);
        {worker_ans, AnswersBatch} ->
            {NewAnswers, Sum} = lists:foldl(fun({K, {V1, V2}} = CurrentV, {Acc, TmpSum}) ->
                {NewV, Add} = case proplists:lookup(K, AnswersBatch) of
                    {K, {V1_2, V2_2}} ->
                        {{K, {V1 + V1_2, V2 + V2_2}}, V1_2};
%%                    {K, {V1_2, V2_2, V3_2}} ->
%%                        ct:print("Error ~tp", [V3_2]),
%%                        {{K, {V1 + V1_2, V2 + V2_2}}, V1_2};
                    none ->
                        {CurrentV, 0}
                end,
                {[NewV | Acc], TmpSum + Add}
            end, {[], 0}, Answers),
            gather_answers(NewAnswers, Num - 1, Gathered + Sum, NewLastReport)
    after
        ?TIMEOUT ->
            {timeout, Answers}
    end.

measure_execution_time(Fun) ->
    Stopwatch = stopwatch:start(),
    Ans = Fun(),
    {stopwatch:read_micros(Stopwatch), Ans}.

get_avg(Num, Timw) ->
    case Num of
        0 -> Timw;
        _ -> Timw/Num
    end.

get_final_ans_tree(Worker, FilesSaved, FilesTime, DirsSaved, DirsTime, CreatedBaseDirsNum,
    OtherAns, OtherTime, InitFailed, Timeout
) ->
    Mem = case rpc:call(Worker, monitoring, get_memory_stats, []) of
        [{<<"mem">>, MemUsage}] ->
            MemUsage;
        _ ->
            -1
    end,
%%    ct:print("Repeat log: ~tp", [{FilesSaved, FilesTime, DirsSaved, DirsTime, OtherAns, OtherTime, Mem, InitFailed, Timeout}]),
    [
        #parameter{name = files_saved, value = FilesSaved, description = "Number of files saved"},
        #parameter{name = file_save_avg_time, value = FilesTime, unit = "us",
            description = "Average time of file save operation"},
        #parameter{name = dirs_saved, value = DirsSaved, description = "Number of dirs saved"},
        #parameter{name = dirs_save_avg_time, value = DirsTime, unit = "us",
            description = "Average time of dir save operation"},
        #parameter{name = base_dirs_created, value = CreatedBaseDirsNum, description = "Number of base dirs created"},
        #parameter{name = error_ans_count, value = OtherAns, description = "Number of errors"},
        #parameter{name = error_ans_count_avg_time, value = OtherTime, unit = "us",
            description = "Average time of operation that ended with error"},
        #parameter{name = memory, value = Mem, description = "Memory usage after the test"},
        #parameter{name = init_failed, value = InitFailed, description = "Has first phase of test failed (1 = true)"},
        #parameter{name = timeout, value = Timeout, description = "Has any timeout appeared (1 = true)"}
    ].

get_final_ans(Saved, SaveTime, SErrors, SErrorsTime, Deleted, DelTime, DErrors, DErrorsTime, InitFailed) ->
%%    ct:print("Repeat log: ~tp", [{Saved, SaveTime, SErrors, SErrorsTime, Deleted, DelTime, DErrors, DErrorsTime, InitFailed}]),
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

create_single_call(SessId, Dir, FilesNum, NameExtBin) ->
    lists:foldl(fun(N, {OkNum, OkTime, ErrorNum, ErrorTime}) ->
        {T, A} = measure_execution_time(fun() ->
            N2 = integer_to_binary(N),
            File = <<Dir/binary, "/", NameExtBin/binary, "_", N2/binary>>,
            lfm:create(SessId, File)
        end),
        case A of
            {ok, _} ->
                {OkNum+1, OkTime+T, ErrorNum, ErrorTime};
            _ ->
                ?error("Create error: ~tp", [A]),
                {OkNum, OkTime, ErrorNum+1, ErrorTime+T}
        end
    end, {0,0,0,0}, lists:seq(1,FilesNum)).

process_answer(Answers, Ans, ToAddV) ->
    {K, _} = ToAdd = case proplists:lookup(Ans, Answers) of
        {Ans, {V1, V2}} ->
            {Ans, {V1 + 1, V2 + ToAddV}};
%%        {Ans, {V1, V2, V3}} ->
%%            {Ans, {V1 + 1, V2 + ToAddV, [Ans | V3]}};
        none ->
            {V1, V2} = proplists:get_value(other, Answers, {0,0}),
            {other, {V1 + 1, V2 + ToAddV}}
%%            {V1, V2, V3} = proplists:get_value(other, Answers, {0,0, []}),
%%            {other, {V1 + 1, V2 + ToAddV, [Ans | V3]}}
    end,
    [ToAdd | proplists:delete(K, Answers)].

ls(Worker, SessId, Dir, PaginationToken) ->
    ListOpts = case PaginationToken of
        undefined -> #{tune_for_large_continuous_listing => true};
        _ -> #{pagination_token => PaginationToken}
    end,
    {ok, _, NextListingPaginationToken} = lfm_proxy:get_children(Worker, SessId, {path, Dir}, ListOpts#{limit => 2000}),
    case file_listing:is_finished(NextListingPaginationToken) of
        true -> ok;
        false ->
            ls(Worker, SessId, Dir, NextListingPaginationToken)
    end.

get_param_value(ParamName, ParamsList) ->
    #parameter{value = Value} = lists:keyfind(ParamName, 2, ParamsList),
    Value.

get_worker_and_session([W], [S]) ->
    {W, S};
get_worker_and_session(Workers, Sessions) ->
    Num = rand:uniform(length(Workers)),
    {lists:nth(Num, Workers), lists:nth(Num, Sessions)}.


create_many(Worker, SessId, Dir, FilesNum, ProcNum, NameExtBin) ->
    FilesPerProc = FilesNum div ProcNum,
    Master = self(),
    lists:foreach(fun(N) ->
        spawn(fun() ->
            try
                NBin = integer_to_binary(N),
                RpcAns = rpc:call(Worker, ?MODULE, create_single_call,
                    [SessId, Dir, FilesPerProc, <<NameExtBin/binary, "_", NBin/binary>>]),
                Master ! {create_single_call, RpcAns}
            catch
                Error:Reason:Stacktrace ->
                    ct:print("Error: ~tp:~tp~n~tp", [Error, Reason, Stacktrace]),
                    Master ! {create_single_call, {0, 0, 0, 0}}
            end
        end)
    end, lists:seq(1, ProcNum)),

    lists:foldl(fun(_, {SaveOkAcc, SaveTimeAcc, SErrorAcc, SErrorTimeAcc}) ->
        receive
            {create_single_call, {SaveOk, SaveTime, SError, SErrorTime}} ->
                {SaveOk + SaveOkAcc, SaveTime + SaveTimeAcc, SError + SErrorAcc, SErrorTime + SErrorTimeAcc}
        end
    end, {0, 0, 0, 0}, lists:seq(1, ProcNum)).
