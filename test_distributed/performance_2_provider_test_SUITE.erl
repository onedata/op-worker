%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains performance test for 2 provider environment.
%%% @end
%%%--------------------------------------------------------------------
-module(performance_2_provider_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([
    all/0, init_per_suite/1, end_per_suite/1, 
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    synchronizer_test/1, synchronizer_test_base/1,
    cancel_synchronizations_for_session_with_mocked_rtransfer_test/1,
    cancel_synchronizations_for_session_with_mocked_rtransfer_test_base/1,
    cancel_synchronizations_for_session_test/1, 
    cancel_synchronizations_for_session_test_base/1,
    transfer_files_to_source_provider_test/1,
    transfer_files_to_source_provider_test_base/1
]).

-define(TEST_CASES, [
    synchronizer_test,
    cancel_synchronizations_for_session_with_mocked_rtransfer_test,
    cancel_synchronizations_for_session_test,
    transfer_files_to_source_provider_test
]).

all() ->
    ?ALL(?TEST_CASES, ?TEST_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

synchronizer_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, file_size_mb}, {value, 10}, {description, "File size in MB"}],
            [{name, block_size}, {value, 1}, {description, "Block size in bytes"}],
            [{name, block_count}, {value, 20000},
                {description, "Number of blocks read from each file"}],
            [{name, random_read}, {value, false}, {description, "Random read"}],
            [{name, separate_blocks}, {value, true},
                {description, "Separate blocks in non-random read"}],
            [{name, threads_num}, {value, 1}, {description, "Number of threads/files"}]
        ]},
        {description, "Tests performance of synchronizer"},
        {config, [{name, basic},
            {parameters, [
            ]},
            {description, ""}
        ]},
        {config, [{name, random},
            {parameters, [
                [{name, random_read}, {value, true}]
            ]},
            {description, ""}
        ]},
        {config, [{name, simple},
            {parameters, [
                [{name, separate_blocks}, {value, false}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many1},
            {parameters, [
                [{name, threads_num}, {value, 5}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many2},
            {parameters, [
                [{name, threads_num}, {value, 10}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many3},
            {parameters, [
                [{name, threads_num}, {value, 20}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many1_random},
            {parameters, [
                [{name, threads_num}, {value, 5}],
                [{name, random_read}, {value, true}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many2_random},
            {parameters, [
                [{name, threads_num}, {value, 10}],
                [{name, random_read}, {value, true}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many3_random},
            {parameters, [
                [{name, threads_num}, {value, 20}],
                [{name, random_read}, {value, true}]
            ]},
            {description, ""}
        ]}
    ]).
synchronizer_test_base(Config) ->
    multi_provider_file_ops_test_base:synchronizer_test_base(Config).

cancel_synchronizations_for_session_with_mocked_rtransfer_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, block_size}, {value, 80}, {description, "Block size in MB"}],
            [{name, block_count}, {value, 20000},
                {description, "Total number of blocks to synchronize"}],
            % note: users have to be defined in env_desc.json and have form like <<"user1">>
            [{name, user_count}, {value, 1}, {description, "Number of users used in test"}]
        ]},
        {description, "Test performance of transfer cancelation"},
        {config, [{name, basic},
            {parameters, [
                [{name, block_count}, {value, 120000}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many_users},
            {parameters, [
                [{name, user_count}, {value, 9}]
            ]},
            {description, ""}
        ]}
    ]).
cancel_synchronizations_for_session_with_mocked_rtransfer_test_base(Config) ->
    multi_provider_file_ops_test_base:cancel_synchronizations_for_session_with_mocked_rtransfer_test_base(
        Config
    ).

cancel_synchronizations_for_session_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, block_size}, {value, 80}, {description, "Block size in MB"}],
            [{name, block_count}, {value, 10000},
                {description, "Total number of blocks to synchronize"}],
            % note: users have to be defined in env_desc.json and have form like <<"user1">>
            [{name, user_count}, {value, 1}, {description, "Number of users used in test"}]
        ]},
        {description, "Test performance of transfer cancelation"},
        {config, [{name, basic},
            {parameters, [
                [{name, block_count}, {value, 60000}]
            ]},
            {description, ""}
        ]},
        {config, [{name, many_users},
            {parameters, [
                [{name, user_count}, {value, 9}]
            ]},
            {description, ""}
        ]}
    ]).
cancel_synchronizations_for_session_test_base(Config) ->
    multi_provider_file_ops_test_base:cancel_synchronizations_for_session_test_base(
        Config
    ).


transfer_files_to_source_provider_test(Config) ->
    ?PERFORMANCE(Config, [
        {repeats, 1},
        {success_rate, 100},
        {parameters, [
            [{name, file_size}, {value, 100}, {description, "File size in bytes"}],
            [{name, files_num}, {value, 2000}, {description, "Number of files"}]
        ]},
        {description, "Many transfers to the same provider"},
        {config, [{name, basic},
            {parameters, [
            ]},
            {description, ""}
        ]}
    ]).
transfer_files_to_source_provider_test_base(Config) ->
    multi_provider_file_ops_test_base:transfer_files_to_source_provider(Config).
    

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> multi_provider_file_ops_test_base:init_env(NewConfig) end,
    [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]}, {?ENV_UP_POSTHOOK, Posthook} | Config].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(synchronizer_test, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(Worker) ->
        test_utils:set_env(Worker, ?APP_NAME, minimal_sync_request, 1)
                  end, Workers),

    ok = test_utils:mock_new(Workers, rtransfer_config),
    test_utils:mock_expect(Workers, rtransfer_config, fetch,
        fun(#{offset := O, size := S} = _Request, NotifyFun, CompleteFun,
            TransferId, SpaceId, FileGuid) ->
            % below call is intentional and must not be deleted
            % this is to pretend behaviour of the mocked function except of call to rtransfer
            (fun() -> _TransferData = erlang:term_to_binary({TransferId, SpaceId, FileGuid}) end)(),
            Ref = make_ref(),
            NotifyFun(Ref, O, S),
            CompleteFun(Ref, {ok, ok}),
            {ok, Ref}
        end),
    init_per_testcase(?DEFAULT_CASE(synchronizer_test), Config);

init_per_testcase(cancel_synchronizations_for_session_with_mocked_rtransfer_test, Config) ->
    [Worker1 | _] = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Worker1, rtransfer_config),
    ok = test_utils:mock_expect(Worker1, rtransfer_config, fetch,
        fun(Request, NotifyFun, CompleteFun, _, _, _) ->
            #{offset := O, size := S} = Request,
            Ref = make_ref(),
            spawn(fun() ->
                timer:sleep(timer:seconds(60)),
                NotifyFun(Ref, O, S),
                CompleteFun(Ref, {ok, ok})
            end),
            {ok, Ref}
        end
    ),
    init_per_testcase(?DEFAULT_CASE(cancel_synchronizations_for_session_with_mocked_rtransfer_test), Config);
init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).

end_per_testcase(Case, Config) when
    Case =:= synchronizer_test;
    Case =:= cancel_synchronizations_for_session_with_mocked_rtransfer_test ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, rtransfer_config),
    end_per_testcase(?DEFAULT_CASE(synchronizer_test), Config);

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).

