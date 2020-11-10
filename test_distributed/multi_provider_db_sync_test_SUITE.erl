%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of db_sync in multi provider environment
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_db_sync_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

-export([
    db_sync_basic_opts_test/1,
    db_sync_many_ops_test/1,
    db_sync_many_ops_test_base/1,
    db_sync_distributed_modification_test/1,
    db_sync_with_delays_test/1,
    db_sync_create_after_del_test/1,
    db_sync_create_after_deletion_links_test/1,
    db_sync_basic_opts_with_errors_test/1,
    sparse_files_should_be_created/1
]).

-define(TEST_CASES, [
    db_sync_basic_opts_test,
    db_sync_many_ops_test,
    db_sync_distributed_modification_test,
    db_sync_create_after_del_test,
    db_sync_create_after_deletion_links_test,
    sparse_files_should_be_created,

    % Warning - this test should be executed last as unmocking in cleanup can interfere next tests
    db_sync_basic_opts_with_errors_test
]).

-define(PERFORMANCE_TEST_CASES, [
    db_sync_many_ops_test,
    db_sync_with_delays_test
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
            [{name, dirs_num}, {value, 5}, {description, "Numbers of directories used during test."}],
            [{name, files_num}, {value, 5}, {description, "Numbers of files used during test."}]
        ]},
        {description, Desc},
        {config, [{name, large_config},
            {parameters, [
                [{name, dirs_num}, {value, 200}],
                [{name, files_num}, {value, 300}]
            ]},
            {description, ""}
        ]}
    ]).

create_on_different_providers_test(Config) ->
    multi_provider_file_ops_test_base:create_on_different_providers_test_base(Config).

db_sync_basic_opts_test(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user1">>, {4,0,0,2}, 60).

db_sync_basic_opts_with_errors_test(Config) ->
    multi_provider_file_ops_test_base:basic_opts_test_base(Config, <<"user1">>, {4,0,0,2}, 60, false).

db_sync_create_after_del_test(Config) ->
    multi_provider_file_ops_test_base:create_after_del_test_base(Config, <<"user1">>, {4,0,0,2}, 60).

db_sync_create_after_deletion_links_test(Config) ->
    % The same test as db_sync_create_after_del_test but with mock (see init_per_testcase)
    multi_provider_file_ops_test_base:create_after_del_test_base(Config, <<"user1">>, {4,0,0,2}, 60).

distributed_delete_test(Config) ->
    multi_provider_file_ops_test_base:distributed_delete_test_base(Config, <<"user1">>, {4,0,0,2}, 60).

db_sync_many_ops_test(Config) ->
    ?PERFORMANCE(Config, ?performance_description("Tests working on dirs and files with db_sync")).
db_sync_many_ops_test_base(Config) ->
    DirsNum = ?config(dirs_num, Config),
    FilesNum = ?config(files_num, Config),
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user1">>, {4,0,0,2}, 180, DirsNum, FilesNum).

db_sync_distributed_modification_test(Config) ->
    multi_provider_file_ops_test_base:distributed_modification_test_base(Config, <<"user1">>, {4,0,0,2}, 60).


db_sync_with_delays_test(Config) ->
    multi_provider_file_ops_test_base:many_ops_test_base(Config, <<"user1">>, {4,0,0,2}, 300, 50, 50).

sparse_files_should_be_created(Config0) ->
    Config = multi_provider_file_ops_test_base:extend_config(Config0,
        <<"user1">>, {4, 0, 0, 2}, 30),
    Worker1 = ?config(worker1, Config),
    [Worker2 | _] = ?config(workers2, Config),

    SessId = ?config(session, Config),
    SessId1 = SessId(Worker1),
    SessId2 = SessId(Worker2),
    Provider1Id = rpc:call(Worker1, oneprovider, get_id, []),
    Provider2Id = rpc:call(Worker2, oneprovider, get_id, []),


    % Hole between not empty blocks created by other provider
    {ok, FileGuid1} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId1,
        <<"/space1/", (generator:gen_name())/binary>>, 8#755)),
    file_ops_test_utils:write_byte_to_file(Worker1, SessId1, FileGuid1, 0),
    verify_sparse_file(Worker2, SessId2, FileGuid1, 1, {Provider1Id, [[0, 1]]}, false),
    file_ops_test_utils:write_byte_to_file(Worker1, SessId1, FileGuid1, 10),
    verify_sparse_file(Worker2, SessId2, FileGuid1, 11, {Provider1Id, [[0, 1], [10, 1]]}),

    % Hole before single empty created by other provider
    {ok, FileGuid2} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId1,
        <<"/space1/", (generator:gen_name())/binary>>, 8#755)),
    file_ops_test_utils:write_byte_to_file(Worker1, SessId1, FileGuid2, 10),
    verify_sparse_file(Worker2, SessId2, FileGuid2, 11, {Provider1Id, [[10, 1]]}),

    % Empty block write on other provider to not empty file
    {ok, FileGuid3} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId1,
        <<"/space1/", (generator:gen_name())/binary>>, 8#755)),
    file_ops_test_utils:write_byte_to_file(Worker1, SessId1, FileGuid3, 0),
    verify_sparse_file(Worker2, SessId2, FileGuid3, 1, {Provider1Id, [[0, 1]]}, false),
    file_ops_test_utils:empty_write_to_file(Worker1, SessId1, FileGuid3, 10),
    verify_sparse_file(Worker2, SessId2, FileGuid3, 10, {Provider1Id, [[0, 1]]}),

    % Empty block write on other provider to empty file
    {ok, FileGuid4} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId1,
        <<"/space1/", (generator:gen_name())/binary>>, 8#755)),
    file_ops_test_utils:empty_write_to_file(Worker1, SessId1, FileGuid4, 10),
    verify_sparse_file(Worker2, SessId2, FileGuid4, 10, {Provider1Id, []}),

    % Creation of hole using truncate on other provider on not empty file
    {ok, FileGuid5} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId1,
        <<"/space1/", (generator:gen_name())/binary>>, 8#755)),
    file_ops_test_utils:write_byte_to_file(Worker1, SessId1, FileGuid5, 0),
    verify_sparse_file(Worker2, SessId2, FileGuid5, 1, {Provider1Id, [[0, 1]]}, false),
    ?assertEqual(ok, lfm_proxy:truncate(Worker1, SessId1, {guid, FileGuid5}, 10)),
    ?assertEqual(ok, lfm_proxy:fsync(Worker1, SessId1, {guid, FileGuid5}, Provider1Id)),
    verify_sparse_file(Worker2, SessId2, FileGuid5, 10, {Provider1Id, [[0, 1]]}),

    % Creation of hole using truncate on other provider on empty file
    {ok, FileGuid6} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId1,
        <<"/space1/", (generator:gen_name())/binary>>, 8#755)),
    ?assertEqual(ok, lfm_proxy:truncate(Worker1, SessId1, {guid, FileGuid6}, 10)),
    ?assertEqual(ok, lfm_proxy:fsync(Worker1, SessId1, {guid, FileGuid5}, Provider1Id)),
    verify_sparse_file(Worker2, SessId2, FileGuid6, 10, {Provider1Id, []}),

    % Truncate on empty file and read by other provider
    {ok, FileGuid7} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId1,
        <<"/space1/", (generator:gen_name())/binary>>, 8#755)),
    verify_sparse_file(Worker2, SessId2, FileGuid7, 0, {Provider1Id, []}, false),
    ?assertEqual(ok, lfm_proxy:truncate(Worker2, SessId2, {guid, FileGuid7}, 10)),
    ?assertEqual(ok, lfm_proxy:fsync(Worker2, SessId2, {guid, FileGuid7}, Provider2Id)),
    verify_sparse_file(Worker1, SessId1, FileGuid7, 10, {Provider2Id, []}),

    % Truncate on not empty file and read by other provider
    {ok, FileGuid8} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId1,
        <<"/space1/", (generator:gen_name())/binary>>, 8#755)),
    file_ops_test_utils:write_byte_to_file(Worker1, SessId1, FileGuid8, 0),
    verify_sparse_file(Worker2, SessId2, FileGuid8, 1, {Provider1Id, [[0, 1]]}, false),
    ?assertEqual(ok, lfm_proxy:truncate(Worker2, SessId2, {guid, FileGuid8}, 10)),
    ?assertEqual(ok, lfm_proxy:fsync(Worker2, SessId2, {guid, FileGuid8}, Provider2Id)),
    verify_sparse_file(Worker1, SessId1, FileGuid8, 10, {Provider1Id, [[0, 1]]}),

    % Write to empty file and read by other provider
    {ok, FileGuid9} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId1,
        <<"/space1/", (generator:gen_name())/binary>>, 8#755)),
    verify_sparse_file(Worker2, SessId2, FileGuid9, 0, {Provider1Id, []}, false),
    file_ops_test_utils:write_byte_to_file(Worker2, SessId2, FileGuid9, 10),
    verify_sparse_file(Worker1, SessId1, FileGuid9, 11, {Provider2Id, [[10, 1]]}),

    % Write to not empty file and read by other provider
    {ok, FileGuid10} = ?assertMatch({ok, _}, lfm_proxy:create(Worker1, SessId1,
        <<"/space1/", (generator:gen_name())/binary>>, 8#755)),
    file_ops_test_utils:write_byte_to_file(Worker1, SessId1, FileGuid10, 0),
    verify_sparse_file(Worker2, SessId2, FileGuid10, 1, {Provider1Id, [[0, 1]]}, false),
    file_ops_test_utils:write_byte_to_file(Worker2, SessId2, FileGuid10, 10),
    verify_sparse_file(Worker1, SessId1, FileGuid10, 11, [{Provider1Id, [[0, 1]]}, {Provider2Id, [[10, 1]]}]),

    ok.

verify_sparse_file(W, SessId, FileGuid, FileSize, ExpectedBlocks) ->
    verify_sparse_file(W, SessId, FileGuid, FileSize, ExpectedBlocks, true).

verify_sparse_file(W, SessId, FileGuid, FileSize, ExpectedBlocks, ReadFile) when is_list(ExpectedBlocks) ->
    lists:foreach(fun({ProviderId, Blocks}) ->
        BlocksSize = lists:foldl(fun([_, Size], Acc) -> Acc + Size end, 0, Blocks),
        GetProviderBlocks = fun() ->
            case lfm_proxy:get_file_distribution(W, SessId, {guid, FileGuid}) of
                {ok, Distribution} ->
                    lists:filter(fun(#{<<"providerId">> := Id}) -> ProviderId =:= Id end, Distribution);
                Other ->
                    Other
            end
        end,

        ?assertMatch([#{<<"blocks">> := Blocks, <<"totalBlocksSize">> := BlocksSize}], GetProviderBlocks(), 30)
    end, ExpectedBlocks),

    ?assertMatch({ok, #file_attr{size = FileSize}}, lfm_proxy:stat(W, SessId, {guid, FileGuid}), 30),

    case ReadFile of
        true ->
            ExpectedFileContent = file_ops_test_utils:get_sparse_file_content(ExpectedBlocks, FileSize),
            {ok, Handle} = lfm_proxy:open(W, SessId, {guid, FileGuid}, rdwr),
            ?assertMatch({ok, ExpectedFileContent}, lfm_proxy:read(W, Handle, 0, 100)),
            ?assertEqual(ok, lfm_proxy:close(W, Handle));
        false ->
            ok
    end;
verify_sparse_file(W, SessId, FileGuid, FileSize, ExpectedBlocks, ReadFile) ->
    verify_sparse_file(W, SessId, FileGuid, FileSize, [ExpectedBlocks], ReadFile).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(db_sync_create_after_deletion_links_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, fslogic_delete, [passthrough]),
    test_utils:mock_expect(Workers, fslogic_delete, get_open_file_handling_method,
        fun(Ctx) -> {deletion_link, Ctx} end),
    init_per_testcase(?DEFAULT_CASE(db_sync_create_after_deletion_links_test), Config);
init_per_testcase(db_sync_with_delays_test, Config) ->
    ct:timetrap({hours, 3}),
    Config2 = init_per_testcase(?DEFAULT_CASE(db_sync_with_delays_test), Config),

    Workers = ?config(op_worker_nodes, Config),
    {_Workers1, WorkersNot1} = lists:foldl(fun(W, {Acc2, Acc3}) ->
        case string:str(atom_to_list(W), "p1") of
            0 -> {Acc2, [W | Acc3]};
            _ -> {[W | Acc2], Acc3}
        end
    end, {[], []}, Workers),

    test_utils:mock_new(WorkersNot1, [
        datastore_throttling
    ]),
    test_utils:mock_expect(WorkersNot1, datastore_throttling, configure_throttling,
        fun() ->
            ok
        end
    ),
    test_utils:mock_expect(WorkersNot1, datastore_throttling, throttle_model, fun
        (file_meta) ->
            timer:sleep(50),
            ok;
        (_) ->
            ok
    end
    ),

    Config2;
init_per_testcase(db_sync_basic_opts_with_errors_test = Case, Config) ->
    MockedConfig = multi_provider_file_ops_test_base:mock_sync_and_rtransfer_errors(Config),
    init_per_testcase(?DEFAULT_CASE(Case), MockedConfig);
init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 60}),
    lfm_proxy:init(Config).

end_per_testcase(db_sync_create_after_deletion_links_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, fslogic_delete),
    end_per_testcase(?DEFAULT_CASE(db_sync_create_after_deletion_links_test), Config);
end_per_testcase(db_sync_with_delays_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [
        datastore_throttling
    ]),

    end_per_testcase(?DEFAULT_CASE(db_sync_with_delays_test), Config);
end_per_testcase(db_sync_basic_opts_with_errors_test = Case, Config) ->
    multi_provider_file_ops_test_base:unmock_sync_and_rtransfer_errors(Config),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).