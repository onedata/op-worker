%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of db_sync and proxy
%%% @end
%%%-------------------------------------------------------------------
-module(massive_multi_provider_file_ops2_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    db_sync_basic_opts_test/1,
    db_sync_many_ops_test/1,
    db_sync_many_ops_test_base/1,
    db_sync_distributed_modification_test/1,
    file_consistency_test/1,
    file_consistency_test_base/1,
    multi_space_test/1,
    blocks_suiting_test/1,
    db_sync_basic_opts_with_errors_test/1
]).

-define(TEST_CASES, [
    db_sync_basic_opts_test,
    db_sync_many_ops_test,
    db_sync_distributed_modification_test,
    file_consistency_test,
    multi_space_test,
    blocks_suiting_test,
    db_sync_basic_opts_with_errors_test
]).

-define(PERFORMANCE_TEST_CASES, [
    db_sync_many_ops_test, file_consistency_test
]).

all() ->
    ?ALL(?TEST_CASES, ?PERFORMANCE_TEST_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

-define(performance_description(Desc),
    [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, dirs_num}, {value, 2}, {description, "Numbers of directories used during test."}],
            [{name, files_num}, {value, 5}, {description, "Numbers of files used during test."}]
        ]},
        {description, Desc},
        {config, [{name, large_config},
            {parameters, [
                [{name, dirs_num}, {value, 10}],
                [{name, files_num}, {value, 20}]
            ]},
            {description, ""}
        ]}
    ]).

db_sync_basic_opts_test(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user1">>, {4,2,0}, 120).

db_sync_basic_opts_with_errors_test(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user1">>, {4,2,0}, 120).

db_sync_many_ops_test(Config) ->
    ?PERFORMANCE(Config, ?performance_description("Tests working on dirs and files with db_sync")).
db_sync_many_ops_test_base(Config) ->
    DirsNum = ?config(dirs_num, Config),
    FilesNum = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user1">>, {4,2,0}, 120, DirsNum, FilesNum).

db_sync_distributed_modification_test(Config) ->
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user1">>, {4,2,0}, 120).

file_consistency_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, test_cases}, {value, [1,14]}, {description, "Number of test cases to be executed"}]
        ]},
        {description, "Tests file consistency"},
        {config, [{name, all_cases},
            {parameters, [
                [{name, test_cases}, {value, [1,2,3,4,5,6,7,8,9,10,11,12,13,14]}]
            ]},
            {description, ""}
        ]}
    ]).
file_consistency_test_base(Config) ->
    ConfigsNum = ?config(test_cases, Config),

    Workers = ?config(op_worker_nodes, Config),
    {Worker1, Worker2, Worker3} = lists:foldl(fun(W, {Acc1, Acc2, Acc3}) ->
        Check = fun(Acc, Prov) ->
            case is_atom(Acc) of
                true ->
                    Acc;
                _ ->
                    case string:str(atom_to_list(W), Prov) of
                        0 -> Acc;
                        _ -> W
                    end
            end
        end,
        {Check(Acc1, "p1"), Check(Acc2, "p2"), Check(Acc3, "p3")}
    end, {[], [], []}, Workers),

    multi_provider_file_ops_test_base:file_consistency_test_skeleton(Config, Worker1, Worker2, Worker3, ConfigsNum).

multi_space_test(Config) ->
    User = <<"user1">>,
    Spaces = ?config({spaces, User}, Config),
    Attempts = 120,

    SpaceConfigs = lists:foldl(fun({_, SN}, Acc) ->
        {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider} = case SN of
            <<"space1">> ->
                {4,2,0,1};
            <<"space2">> ->
                {2,4,0,1};
            <<"space9">> ->
                {6,0,0,1};
            _ ->
                {0,6,1,1}
        end,
        EC = multi_provider_file_ops_test_base:extend_config(Config, User,
            {SyncNodes, ProxyNodes, ProxyNodesWritten0, NodesOfProvider}, Attempts),
        [{SN, EC} | Acc]
    end, [], Spaces),

    multi_provider_file_ops_test_base:multi_space_test_base(Config, SpaceConfigs, User).

blocks_suiting_test(Config0) ->
    Attempts = 60,
    User = <<"user1">>,
    Config = multi_provider_file_ops_test_base:extend_config(Config0, User, {6,0,0,1}, Attempts),
    SessId = ?config(session, Config),
    SpaceName = <<"space9">>,
    [Worker1, Worker2, Worker3, Worker4, Worker5, Worker6] = ?config(op_worker_nodes, Config0),

    File = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,
    ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId(Worker1), File, 8#755)),

    multi_provider_file_ops_test_base:verify(Config, fun(W) ->
        ?assertMatch({ok, #file_attr{type = ?REGULAR_FILE_TYPE}},
            lfm_proxy:stat(W, SessId(W), {path, File}), Attempts)
    end),

    write_to_file(Worker2, File, 0, 5, SessId),
    write_to_file(Worker3, File, 5, 10, SessId),
    write_to_file(Worker4, File, 10, 20, SessId),
    write_to_file(Worker5, File, 20, 40, SessId),
    write_to_file(Worker6, File, 40, 65, SessId),
    write_to_file(Worker2, File, 65, 95, SessId),
    write_to_file(Worker3, File, 95, 300, SessId),

    % Verify initial data distribution
    ExpectedDistribution = [[], [[0,5],[65,30]], [[5,5],[95,205]], [[10,10]], [[20,20]], [[40,25]]],
    GetDistFun = fun() ->
        {ok, Distribution} = lfm_proxy:get_file_distribution(Worker1, SessId(Worker1), {path, File}),
        DistBlocks = lists:map(fun(#{<<"blocks">> := Blocks}) -> Blocks end, Distribution),
        lists:sort(DistBlocks)
    end,
    ?assertEqual(ExpectedDistribution, GetDistFun(), Attempts),

    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker1, SessId(Worker1), {path, File}, rdwr)),

    % Request should be splited but blocks should not be enlarged
    check_read(Worker1, SessId, File, Handle, 0, 10, [[0, 5], [5, 5]]),
    % Request should not be enlarged
    check_read(Worker1, SessId, File, Handle, 10, 10, [[10, 10]]),
    % Request should be enlarged - its size should be changed
    check_read(Worker1, SessId, File, Handle, 20, 5, [[20, 10]]),
    % No request should appear because of enlarging last request
    check_read(Worker1, SessId, File, Handle, 25, 5, []),
    % Request should be enlarged and offset should be changed
    check_read(Worker1, SessId, File, Handle, 25, 10, [[30, 10]]),
    check_read(Worker1, SessId, File, Handle, 35, 10, [[40, 10]]),
    % Request should be splited but blocks should not be enlarged
    check_read(Worker1, SessId, File, Handle, 45, 25, [[50, 15], [65, 5]]),
    % Request should be enlarged - its offset and size should be changed
    check_read(Worker1, SessId, File, Handle, 75, 10, [[70, 20]]),
    % Request should be splited and enlarged
    check_read(Worker1, SessId, File, Handle, 95, 20, [[95, 5], [100, 20]]),
    % Large Request should be enlarged
    check_read(Worker1, SessId, File, Handle, 135, 100, [[130, 110]]),

    ?assertEqual(ok, lfm_proxy:close(Worker1, Handle)).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, dbsync_test_utils, multi_provider_file_ops_test_base]},
        {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(blocks_suiting_test, Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),

    {ok, MinReq} = test_utils:get_env(Worker1, ?APP_NAME, minimal_sync_request),
    test_utils:set_env(Worker1, ?APP_NAME, minimal_sync_request, 1),

    Master = self(),
    test_utils:mock_new(Worker1, rtransfer_config, [passthrough]),
    test_utils:mock_expect(Worker1, rtransfer_config, fetch,
        fun(Request, NotifyFun, CompleteFun, TransferId, SpaceId, FileGuid) ->
            #{offset := O, size := S} = Request,
            Master ! {fetch_request, O, S},
            meck:passthrough([Request, NotifyFun, CompleteFun, TransferId, SpaceId, FileGuid])
        end),

    test_utils:mock_new(Worker1, sync_req, [passthrough]),
    test_utils:mock_expect(Worker1, sync_req, synchronize_block,
        fun(UserCtx, FileCtx, Block, _Prefetch, TransferId, Priority) ->
            meck:passthrough([UserCtx, FileCtx, Block, false, TransferId, Priority])
        end),

    test_utils:mock_new(Worker1, helper, [passthrough]),
    test_utils:mock_expect(Worker1, helper, get_block_size,
        fun(_) ->
            10
        end),

    init_per_testcase(?DEFAULT_CASE(blocks_suiting_test), [{minimal_sync_request, MinReq} | Config]);
init_per_testcase(file_consistency_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, file_meta, [passthrough]),
    init_per_testcase(?DEFAULT_CASE(file_consistency_test), Config);
init_per_testcase(db_sync_basic_opts_with_errors_test = Case, Config) ->
    MockedConfig = multi_provider_file_ops_test_base:mock_sync_and_rtransfer_errors(Config),
    init_per_testcase(?DEFAULT_CASE(Case), MockedConfig);
init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 60}),
    lfm_proxy:init(Config).

end_per_testcase(blocks_suiting_test, Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Worker1, [rtransfer_config, sync_req, helper]),
    MinReq = ?config(minimal_sync_request, Config),
    test_utils:set_env(Worker1, ?APP_NAME, minimal_sync_request, MinReq),
    end_per_testcase(?DEFAULT_CASE(blocks_suiting_test), Config);
end_per_testcase(file_consistency_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, file_meta),
    end_per_testcase(?DEFAULT_CASE(file_consistency_test), Config);
end_per_testcase(db_sync_basic_opts_with_errors_test = Case, Config) ->
    multi_provider_file_ops_test_base:unmock_sync_and_rtransfer_errors(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

write_to_file(Worker, File, Offset, End, SessId) ->
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Worker, SessId(Worker), {path, File}, rdwr)),
    Size = End - Offset,
    Data = crypto:strong_rand_bytes(Size),
    ?assertEqual({ok, Size}, lfm_proxy:write(Worker, Handle, Offset, Data)),
    ?assertEqual(ok, lfm_proxy:close(Worker, Handle)).

check_read(Worker, SessId, File, Handle, Offset, Size, ExpectedRequests) ->
    % Perform read
    {ok, Bytes} = ?assertMatch({ok, _}, lfm_proxy:read(Worker, Handle, Offset, Size)),
    ?assertEqual(Size, byte_size(Bytes)),

    % Verify fetch requests
    Requests = gather_fetch_requests(),
    ?assertEqual(ExpectedRequests, lists:sort(Requests)),

    % Verify data distribution
    {ok, Distribution} = ?assertMatch({ok, _}, lfm_proxy:get_file_distribution(Worker, SessId(Worker), {path, File})),
    ProvId = rpc:call(Worker, oneprovider, get_id, []),
    [#{<<"blocks">> := Blocks}] = ?assertMatch([_],
        lists:filter(fun(#{<<"providerId">> := Id}) -> Id =:= ProvId end, Distribution)),
    ExistingBlocks = case get(blocks_cache) of
        undefined -> [];
        Cached -> Cached
    end,
    NewExistingBlocks = merge_blocks(ExistingBlocks, ExpectedRequests),
    put(blocks_cache, NewExistingBlocks),
    ?assertEqual(Blocks, lists:reverse(NewExistingBlocks)).

gather_fetch_requests() ->
    receive
        {fetch_request, O, S} -> [[O, S] | gather_fetch_requests()]
    after
        1000 -> []
    end.

merge_blocks([[ExistingOffset, ExistingSize] | ExistingTail], [[NewOffset, NewSize] | NewTail])
    when ExistingOffset + ExistingSize >= NewOffset ->
    merge_blocks([[ExistingOffset,  NewOffset + NewSize - ExistingOffset] | ExistingTail], NewTail);
merge_blocks(Existing, [New | NewTail]) ->
    merge_blocks([New | Existing], NewTail);
merge_blocks(Existing, []) ->
    Existing.