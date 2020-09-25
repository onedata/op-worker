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
    db_sync_basic_opts_with_errors_test/1
]).

-define(TEST_CASES, [
    db_sync_basic_opts_test,
    db_sync_many_ops_test,
    db_sync_distributed_modification_test,
    db_sync_create_after_del_test,
    db_sync_create_after_deletion_links_test,

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
    MockedConfig = multi_provider_file_ops_test_base:mock_sync_errors(Config),
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
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [dbsync_in_stream_worker, dbsync_communicator]),
    RequestDelay = ?config(request_delay, Config),
    test_utils:set_env(Workers, ?APP_NAME, dbsync_changes_request_delay, RequestDelay),
    end_per_testcase(?DEFAULT_CASE(Case), Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).