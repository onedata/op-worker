%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains save stress test for single provider with many nodes.
%%% @end
%%%--------------------------------------------------------------------
-module(multinode_stress_test_SUITE).
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
-export([stress_test/1, stress_test_base/1,
    single_dir_creation_test/1, single_dir_creation_test_base/1,
    many_files_creation_tree_test/1, many_files_creation_tree_test_base/1]).
-export([create_single_call/5]).

-define(STRESS_NO_CLEARING_CASES, [
    single_dir_creation_test
%%    many_files_creation_tree_test
]).

-define(TIMEOUT, timer:minutes(1)).

all() ->
    ?STRESS_ALL([], ?STRESS_NO_CLEARING_CASES).

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

single_dir_creation_test(Config) ->
    ?PERFORMANCE(Config, [
        {parameters, [
            [{name, files_num}, {value, 100}, {description, "Numer of files to be created by single process"}],
            [{name, proc_num}, {value, 20}, {description, "Numer of parallel"}]
        ]},
        {description, "Creates files in dir using single process"}
    ]).
single_dir_creation_test_base(Config) ->
    FilesNum = ?config(files_num, Config),
    ProcNum = ?config(proc_num, Config),

%%    [FirstWorker | _] = ?config(op_worker_nodes, Config),
%%    Workers = [rpc:call(FirstWorker, consistent_hashing, get_node, [<<"mmmmmm">>])],
%%    Workers = ?config(op_worker_nodes, Config) -- [rpc:call(FirstWorker, consistent_hashing, get_node, [<<>>])],
    [FirstWorker | _] = Workers = ?config(op_worker_nodes, Config),
    User = <<"user1">>,

    [FirstSessId | _] = SessIds =
        lists:map(fun(Worker) -> ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config) end, Workers),
    [{_SpaceId, SpaceName} | _] = ?config({spaces, User}, Config),
    RepeatNum = ?config(rep_num, Config),

    WorkersWithSessions = create_workers_list(Workers, SessIds, [], 0, ProcNum),

    % Generate test setup
    CheckAns = case RepeatNum of
        1 ->
            Dir = <<"/", SpaceName/binary, "/test_dir">>,
            case lfm_proxy:mkdir(FirstWorker, FirstSessId, Dir, 8#755) of
                {ok, Uuid} = Ans ->
                    put(test_dir_uuid, Uuid),
                    Ans;
                Other ->
                    Other
            end;
        _ ->
            {ok, get(test_dir_uuid)}
    end,
    NameExtBin = integer_to_binary(RepeatNum),

    case CheckAns of
        {ok, DirUuid} ->
            Master = self(),
            StartTime = os:timestamp(),

            lists:foreach(fun({Num, Worker, SessId}) ->
                spawn(fun() ->
                    try
                        {SaveOk, SaveError, ErrorsList} =
                            rpc:call(Worker, ?MODULE, create_single_call,
                                [SessId, DirUuid, 1 + Num*FilesNum, FilesNum, NameExtBin]),
                        case SaveError of
                            0 -> Master ! {slave_ans, SaveOk};
                            _ -> Master ! {slave_ans_error, SaveOk, SaveError, ErrorsList}
                        end
                    catch
                        E1:E2 ->
                            Master ! {slave_error, {E1, E2}}
                    end
                end)
            end, WorkersWithSessions),

            OkSum = lists:foldl(fun(_, Acc) ->
                receive
                    {slave_ans, SaveAns} ->
                        SaveAns + Acc;
                    {slave_ans_error, SaveAns, SaveAnsErrors, ErrorsList} ->
                        ct:print("Slave errors num ~p~nerror list: ~p", [SaveAnsErrors, ErrorsList]),
                        SaveAns + Acc;
                    {slave_error, SlaveError} ->
                        ct:print("Slave error ~p", [SlaveError]),
                        Acc
                after
                    ?TIMEOUT ->
                        ct:print("Slave timeout"),
                        Acc
                end
            end, 0, WorkersWithSessions),

            Time = timer:now_diff(os:timestamp(), StartTime),
            Sum = case get(ok_sum) of
                undefined ->
                    0;
                S ->
                    S
            end,
            NewSum = Sum + OkSum,
            put(ok_sum, NewSum),
            ct:print("Save num ~p, sum ~p, time ~p", [OkSum, NewSum, Time]),
            ok;
        _ ->
            timer:sleep(timer:seconds(60)),
            ?assertMatch({ok, _}, CheckAns)
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
    files_stress_test_base:many_files_creation_tree_test_base(Config, false, true).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    files_stress_test_base:init_per_suite(Config).

init_per_testcase(Case, Config) ->
    files_stress_test_base:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    files_stress_test_base:end_per_testcase(Case, Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_single_call(SessId, DirUuid, StartNum, FilesNum, NameExtBin) ->
    lists:foldl(fun(N, {OkNum, ErrorNum, ErrorsList}) ->
        N2 = integer_to_binary(N),
        File = <<NameExtBin/binary, "_", N2/binary>>,
        case lfm:create(SessId, DirUuid, File, undefined) of
            {ok, _} ->
                {OkNum+1, ErrorNum, ErrorsList};
            ErrorAns ->
                {OkNum, ErrorNum+1, [ErrorAns | ErrorsList]}
        end
    end, {0,0,[]}, lists:seq(StartNum, StartNum + FilesNum - 1)).

create_workers_list(_, _, Ans, FinalLength, FinalLength) ->
    Ans;
create_workers_list([W | Workers], [S | SessIds], Ans, Num, FinalLength) ->
    create_workers_list(Workers ++ [W], SessIds ++ [S], [{Num, W, S} | Ans], Num + 1, FinalLength).