%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests for QoS management on single provider.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_test_SUITE).
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("qos_tests_utils.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, init_per_testcase/2,
    end_per_suite/1, end_per_testcase/2
]).

%% test functions
-export([
    % QoS bounded cache tests
    qos_bounded_cache_should_be_periodically_cleaned_if_overfilled/1,
    qos_bounded_cache_should_not_be_cleaned_if_not_overfilled/1,

    % Invalid QoS expression tests
    key_without_value/1,
    two_keys_without_value_connected_with_operand/1,
    operator_without_second_operand/1,
    operator_without_first_operand/1,
    two_operators_in_row/1,
    closing_paren_without_matching_opening_one/1,
    opening_paren_without_matching_closing_one/1,
    mismatching_nested_parens/1,

    % Single QoS expression tests
    simple_key_val_qos/1,
    qos_with_intersection/1,
    qos_with_complement/1,
    qos_with_union/1,
    qos_with_multiple_replicas/1,
    qos_with_multiple_replicas_and_union/1,
    qos_with_intersection_and_union/1,
    qos_with_union_and_complement/1,
    qos_with_intersection_and_complement/1,
    key_val_qos_that_cannot_be_fulfilled/1,
    qos_that_cannot_be_fulfilled/1,
    qos_with_parens/1,

    % Multi QoS tests
    multi_qos_resulting_in_the_same_storages/1,
    same_qos_multiple_times/1,
    contrary_qos/1,
    multi_qos_where_one_cannot_be_satisfied/1,
    multi_qos_that_overlaps/1,
    multi_qos_resulting_in_different_storages/1,

    % Effective QoS tests
    effective_qos_for_file_in_directory/1,
    effective_qos_for_file_in_nested_directories/1,
    effective_qos_for_files_in_different_directories_of_tree_structure/1
]).

all() -> [
    % QoS bounded cache tests
    qos_bounded_cache_should_be_periodically_cleaned_if_overfilled,
    qos_bounded_cache_should_not_be_cleaned_if_not_overfilled,

    % TODO: VFS-5569 uncomment below tests after implementing proper validation
    % Invalid QoS expression tests
%%    key_without_value,
%%    two_keys_without_value_connected_with_operand,
%%    operator_without_second_operand,
%%    operator_without_first_operand,
%%    two_operators_in_row,
%%    closing_paren_without_matching_opening_one,
%%    opening_paren_without_matching_closing_one,
%%    mismatching_nested_parens,

    % Single QoS expression tests
    simple_key_val_qos,
    qos_with_intersection,
    qos_with_complement,
    qos_with_union,
    qos_with_multiple_replicas,
    qos_with_multiple_replicas_and_union,
    qos_with_intersection_and_union,
    qos_with_union_and_complement,
    qos_with_intersection_and_complement,
    key_val_qos_that_cannot_be_fulfilled,
    qos_that_cannot_be_fulfilled,
    qos_with_parens,

    % Multi QoS tests
    multi_qos_resulting_in_the_same_storages,
    same_qos_multiple_times,
    contrary_qos,
    multi_qos_where_one_cannot_be_satisfied,
    multi_qos_that_overlaps,
    multi_qos_resulting_in_different_storages,

    % Effective QoS tests
    effective_qos_for_file_in_directory,
    effective_qos_for_file_in_nested_directories,
    effective_qos_for_files_in_different_directories_of_tree_structure
].


-define(SPACE1_ID, <<"space_id1">>).
-define(SPACE1, <<"/space_name1">>).
-define(TEST_DATA, <<"test_data">>).
-define(TEST_FILE_PATH, filename:join(?SPACE1, <<"file1">>)).
-define(TEST_DIR_PATH, filename:join(?SPACE1, <<"dir1">>)).


% record for specification of tests that adds QoS expression and checks QoS docs
% all fields are associated with matching records defined in qos_tests_utils.hrl
-record(add_qos_and_check_docs_test_spec, {
    qos_to_add,
    expected_qos_entries,
    expected_file_qos
}).


% mock qos for test storages
-define(P1_TEST_QOS, #{
    <<"country">> => <<"PL">>,
    <<"city">> => <<"Krakow">>,
    <<"type">> => <<"disk">>,
    <<"tier">> => <<"t3">>
}).

-define(P2_TEST_QOS, #{
    <<"country">> => <<"FR">>,
    <<"type">> => <<"tape">>,
    <<"tier">> => <<"t2">>
}).

-define(P3_TEST_QOS, #{
    <<"country">> => <<"PT">>,
    <<"type">> => <<"disk">>,
    <<"tier">> => <<"t2">>
}).

-define(P4_TEST_QOS, #{
    <<"country">> => <<"GB">>,
    <<"type">> => <<"disk">>,
    <<"tier">> => <<"t3">>
}).

-define(TEST_PROVIDERS_QOS, #{
    ?P1 => ?P1_TEST_QOS,
    ?P2 => ?P2_TEST_QOS,
    ?P3 => ?P3_TEST_QOS,
    ?P4 => ?P4_TEST_QOS
}).


-define(GET_CACHE_TABLE_SIZE(SPACE_ID),
    element(2, lists:keyfind(size, 1, rpc:call(Worker, ets, info, [?CACHE_TABLE_NAME(SPACE_ID)])))
).

-define(QOS_CACHE_TEST_OPTIONS(Size),
    #{
        size => Size,
        group => true,
        name => ?QOS_BOUNDED_CACHE_GROUP,
        check_frequency => timer:seconds(300)
    }
).


-define(NESTED_DIR_STRUCTURE, {?SPACE1, [
    {<<"dir1">>, [
        {<<"dir2">>, [
            {<<"dir3">>, [
                {<<"file31">>, <<"data">>},
                {<<"dir4">>, [
                    {<<"file41">>, <<"data">>}
                ]}
            ]}
        ]}
    ]}
]}).


%%%===================================================================
%%% Test functions
%%%===================================================================

%%%===================================================================
%%% QoS bounded cache tests.
%%%===================================================================

qos_bounded_cache_should_be_periodically_cleaned_if_overfilled(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    Dir1Path = filename:join([?SPACE1, <<"dir1">>]),
    FilePath = filename:join([?SPACE1, <<"dir1">>, <<"dir2">>, <<"dir3">>, <<"dir4">>, <<"file41">>]),

    EffQosTestSpec = #effective_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = ?NESTED_DIR_STRUCTURE
        },
        qos_to_add = [
            #qos_to_add{
                path = Dir1Path,
                qos_name = ?QOS1,
                expression = <<"country=FR">>
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                path = FilePath,
                qos_entries = [?QOS1],
                target_storages = #{?P2 => [?QOS1]}
            }
        ]
    },

    % add QoS and calculate effective QoS to fill in cache
    add_qos_for_dir_and_check_effective_qos(Config, EffQosTestSpec),

    % check that QoS cache is overfilled
    SizeBeforeCleaning = ?GET_CACHE_TABLE_SIZE(?SPACE1_ID),
    ?assertEqual(6, SizeBeforeCleaning),

    % send message that checks cache size and cleans it if necessary
    ?assertMatch(ok, rpc:call(Worker, bounded_cache, check_cache_size, [?QOS_CACHE_TEST_OPTIONS(1)])),

    % check that cache has been cleaned
    SizeAfterCleaning = ?GET_CACHE_TABLE_SIZE(?SPACE1_ID),
    ?assertEqual(0, SizeAfterCleaning).


qos_bounded_cache_should_not_be_cleaned_if_not_overfilled(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    Dir1Path = filename:join([?SPACE1, <<"dir1">>]),
    FilePath = filename:join([?SPACE1, <<"dir1">>, <<"dir2">>, <<"dir3">>, <<"dir4">>, <<"file41">>]),

    EffQosTestSpec = #effective_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = ?NESTED_DIR_STRUCTURE
        },
        qos_to_add = [
            #qos_to_add{
                path = Dir1Path,
                qos_name = ?QOS1,
                expression = <<"country=FR">>
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                path = FilePath,
                qos_entries = [?QOS1],
                target_storages = #{?P2 => [?QOS1]}
            }
        ]
    },

    % add QoS and calculate effective QoS to fill in cache
    add_qos_for_dir_and_check_effective_qos(Config, EffQosTestSpec),

    % check that QoS cache is not empty
    SizeBeforeCleaning = ?GET_CACHE_TABLE_SIZE(?SPACE1_ID),
    ?assertEqual(6, SizeBeforeCleaning),

    % send message that checks cache size and cleans it if necessary
    ?assertMatch(ok, rpc:call(Worker, bounded_cache, check_cache_size, [?QOS_CACHE_TEST_OPTIONS(6)])),

    SizeAfterCleaning = ?GET_CACHE_TABLE_SIZE(?SPACE1_ID),
    ?assertEqual(6, SizeAfterCleaning).


%%%===================================================================
%%% Invalid QoS expression tests.
%%%===================================================================

key_without_value(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        ?ERROR_INVALID_QOS_EXPRESSION,
        lfm_proxy:add_qos(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"country">>, 1)
    ).


two_keys_without_value_connected_with_operand(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        ?ERROR_INVALID_QOS_EXPRESSION,
        lfm_proxy:add_qos(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"country|type">>, 1)
    ).


operator_without_second_operand(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        ?ERROR_INVALID_QOS_EXPRESSION,
        lfm_proxy:add_qos(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"country=PL&">>, 1)
    ).


operator_without_first_operand(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        ?ERROR_INVALID_QOS_EXPRESSION,
        lfm_proxy:add_qos(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"|country=PL">>, 1)
    ).


two_operators_in_row(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        ?ERROR_INVALID_QOS_EXPRESSION,
        lfm_proxy:add_qos(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"country=PL&-type-disk">>, 1)
    ).


closing_paren_without_matching_opening_one(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        ?ERROR_INVALID_QOS_EXPRESSION,
        lfm_proxy:add_qos(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"country=PL)">>, 1)
    ).


opening_paren_without_matching_closing_one(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        ?ERROR_INVALID_QOS_EXPRESSION,
        lfm_proxy:add_qos(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"(country=PL">>, 1)
    ).


mismatching_nested_parens(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    create_test_file(Config),

    ?assertMatch(
        ?ERROR_INVALID_QOS_EXPRESSION,
        lfm_proxy:add_qos(Worker, SessId, {path, ?TEST_FILE_PATH}, <<"(type=disk|tier=t2&(country=PL)">>, 1)
    ).


%%%===================================================================
%%% Group of tests that adds single QoS expression for file or directory
%%% and checks QoS docs.
%%% Each test case is executed once for file and once for directory.
%%%===================================================================

simple_key_val_qos(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"country=FR">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"country=FR">>],
                    replicas_num = 1
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1],
                    target_storages = #{?P2 => [?QOS1]}
                }
            ]
        }
    end).


qos_with_intersection(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"type=disk&tier=t2">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"type=disk">>, <<"tier=t2">>, <<"&">>],
                    replicas_num = 1
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1],
                    target_storages = #{?P3 => [?QOS1]}
                }
            ]
        }
    end).


qos_with_complement(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"tier=t3-country=GB">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"tier=t3">>, <<"country=GB">>, <<"-">>],
                    replicas_num = 1
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1],
                    target_storages = #{?P1 => [?QOS1]}
                }
            ]
        }
    end).


qos_with_union(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression =  <<"country=PL|city=Krakow">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"country=PL">>, <<"city=Krakow">>, <<"|">>],
                    replicas_num = 1
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1],
                    target_storages = #{?P1 => [?QOS1]}
                }
            ]
        }
    end).


qos_with_multiple_replicas(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"type=disk">>,
                    replicas_num = 3
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"type=disk">>],
                    replicas_num = 3
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1],
                    target_storages = #{
                        ?P1 => [?QOS1],
                        ?P3 => [?QOS1],
                        ?P4 => [?QOS1]
                    }
                }
            ]
        }
    end).


qos_with_intersection_and_union(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"type=disk&tier=t2|country=FR">>,
                    replicas_num = 2
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"type=disk">>, <<"tier=t2">>, <<"&">>, <<"country=FR">>, <<"|">>],
                    replicas_num = 2
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1],
                    target_storages = #{
                        ?P2 => [?QOS1],
                        ?P3 => [?QOS1]
                    }
                }
            ]
        }
    end).


qos_with_union_and_complement(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"country=PL|country=FR-type=tape">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"country=PL">>, <<"country=FR">>, <<"|">>, <<"type=tape">>, <<"-">>],
                    replicas_num = 1
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1],
                    target_storages = #{
                        ?P1 => [?QOS1]
                    }
                }
            ]
        }
    end).


qos_with_intersection_and_complement(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"type=disk&tier=t3-country=PL">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"type=disk">>, <<"tier=t3">>, <<"&">>, <<"country=PL">>, <<"-">>],
                    replicas_num = 1
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1],
                    target_storages = #{
                        ?P4 => [?QOS1]
                    }
                }
            ]
        }
    end).


qos_with_multiple_replicas_and_union(Config) -> 
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"country=PL|country=FR|country=PT">>,
                    replicas_num = 3
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"country=PL">>, <<"country=FR">>, <<"|">>, <<"country=PT">>, <<"|">>],
                    replicas_num = 3
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1],
                    target_storages = #{
                        ?P1 => [?QOS1],
                        ?P2 => [?QOS1],
                        ?P3 => [?QOS1]
                    }
                }
            ]
        }
    end).


key_val_qos_that_cannot_be_fulfilled(Config) -> 
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"country=IT">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"country=IT">>],
                    replicas_num = 1,
                    is_possible = false
    
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1],
                    target_storages = #{}
                }
            ]
        }
    end).


qos_that_cannot_be_fulfilled(Config) -> 
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"country=PL|country=PT-type=disk">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"country=PL">>, <<"country=PT">>, <<"|">>, <<"type=disk">>, <<"-">>],
                    replicas_num = 1,
                    is_possible = false
    
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1],
                    target_storages = #{}
                }
            ]
        }
    end).


qos_with_parens(Config) -> 
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"country=PL|(country=PT-type=disk)">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"country=PL">>, <<"country=PT">>, <<"type=disk">>, <<"-">>, <<"|">>],
                    replicas_num = 1
    
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1],
                    target_storages = #{?P1 => [?QOS1]}
                }
            ]
        }
    end).


%%%===================================================================
%%% Group of tests that adds multiple QoS expression for single file or
%%% directory and checks QoS docs.
%%% Each test case is executed once for file and once for directory.
%%%===================================================================

multi_qos_resulting_in_different_storages(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"type=disk&tier=t2">>,
                    replicas_num = 1
                },
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS2,
                    expression = <<"country=FR">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"type=disk">>, <<"tier=t2">>, <<"&">>],
                    replicas_num = 1
                },
                #expected_qos_entry{
                    qos_name = ?QOS2,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"country=FR">>],
                    replicas_num = 1
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1, ?QOS2],
                    target_storages = #{
                        ?P2 => [?QOS2],
                        ?P3 => [?QOS1]
                    }
                }
            ]
        }
    end).


multi_qos_resulting_in_the_same_storages(Config) -> 
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"type=tape">>,
                    replicas_num = 1
                },
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS2,
                    expression = <<"country=FR">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"type=tape">>],
                    replicas_num = 1
                },
                #expected_qos_entry{
                    qos_name = ?QOS2,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"country=FR">>],
                    replicas_num = 1
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1, ?QOS2],
                    target_storages = #{
                        ?P2 => [?QOS1, ?QOS2]
                    }
                }
            ]
        }
    end).


same_qos_multiple_times(Config) -> 
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"type=tape">>,
                    replicas_num = 1
                },
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS2,
                    expression = <<"type=tape">>,
                    replicas_num = 1
                },
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS3,
                    expression = <<"type=tape">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"type=tape">>],
                    replicas_num = 1
                },
                #expected_qos_entry{
                    qos_name = ?QOS2,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"type=tape">>],
                    replicas_num = 1
                },
                #expected_qos_entry{
                    qos_name = ?QOS3,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"type=tape">>],
                    replicas_num = 1
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1, ?QOS2, ?QOS3],
                    target_storages = #{?P2 => [?QOS1, ?QOS2, ?QOS3]}
                }
            ]
        }
    end).


contrary_qos(Config) -> 
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"country=PL">>,
                    replicas_num = 1
                },
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS2,
                    expression = <<"type=tape-country=PL">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"country=PL">>],
                    replicas_num = 1
                },
                #expected_qos_entry{
                    qos_name = ?QOS2,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"type=tape">>, <<"country=PL">>, <<"-">>],
                    replicas_num = 1
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1, ?QOS2],
                    target_storages = #{
                        ?P1 => [?QOS1],
                        ?P2 => [?QOS2]
                    }
                }
            ]
        }
    end).


multi_qos_where_one_cannot_be_satisfied(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"country=FR">>,
                    replicas_num = 1
                },
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS2,
                    expression =  <<"country=IT">>,
                    replicas_num = 1
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"country=FR">>],
                    replicas_num = 1
                },
                #expected_qos_entry{
                    qos_name = ?QOS2,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"country=IT">>],
                    replicas_num = 1,
                    is_possible = false
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1, ?QOS2],
                    target_storages = #{?P2 => [?QOS1]}
                }
            ]
        }
    end).


multi_qos_that_overlaps(Config) ->
    run_tests(Config, [file, dir], fun(Path) ->
        #add_qos_and_check_docs_test_spec{
            qos_to_add = [
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS1,
                    expression = <<"type=disk">>,
                    replicas_num = 3
                },
                #qos_to_add{
                    path = Path,
                    qos_name = ?QOS2,
                    expression = <<"tier=t3">>,
                    replicas_num = 2
                }
            ],
            expected_qos_entries = [
                #expected_qos_entry{
                    qos_name = ?QOS1,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"type=disk">>],
                    replicas_num = 3
                },
                #expected_qos_entry{
                    qos_name = ?QOS2,
                    file_key = {path, Path},
                    qos_expression_in_rpn = [<<"tier=t3">>],
                    replicas_num = 2
                }
            ],
            expected_file_qos = [
                #expected_file_qos{
                    path = Path,
                    qos_entries = [?QOS1, ?QOS2],
                    target_storages = #{
                        ?P1 => [?QOS1, ?QOS2],
                        ?P3 => [?QOS1],
                        ?P4 => [?QOS1, ?QOS2]
                    }
                }
            ]
        }
    end).


%%%===================================================================
%%% Group of tests that creates directory structure, adds QoS on different
%%% levels of created structure and checks effective QoS and QoS docs.
%%%===================================================================

effective_qos_for_file_in_directory(Config) ->
    DirPath = filename:join(?SPACE1, <<"dir1">>),
    FilePath = filename:join(DirPath, <<"file1">>),

    TestSpec = #effective_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = {?SPACE1, [
                {<<"dir1">>, [
                    {<<"file1">>, ?TEST_DATA}
                ]}
            ]}
        },
        qos_to_add = [
            #qos_to_add{
                path = DirPath,
                qos_name = ?QOS1,
                expression = <<"country=FR">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                qos_name = ?QOS1,
                qos_expression_in_rpn = [<<"country=FR">>],
                replicas_num = 1,
                file_key = {path, DirPath}
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                path = FilePath,
                qos_entries = [?QOS1],
                target_storages = #{?P2 => [?QOS1]}
            }
        ]
    },

    add_qos_for_dir_and_check_effective_qos(Config, TestSpec).


effective_qos_for_file_in_nested_directories(Config) ->
    Dir2Path = filename:join(<<"/space_name1/dir1">>, <<"dir2">>),
    Dir3Path = filename:join(Dir2Path, <<"dir3">>),
    Dir4Path = filename:join(Dir3Path, <<"dir4">>),
    File31Path = filename:join(Dir3Path, <<"file31">>),
    File41Path = filename:join(Dir4Path, <<"file41">>),

    TestSpec = #effective_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = ?NESTED_DIR_STRUCTURE
        },
        qos_to_add = [
            #qos_to_add{
                path = Dir2Path,
                qos_name = ?QOS1,
                expression = <<"country=PL">>,
                replicas_num = 1
            },
            #qos_to_add{
                path = Dir3Path,
                qos_name = ?QOS2,
                expression = <<"country=FR">>,
                replicas_num = 1
            },
            #qos_to_add{
                path = Dir4Path,
                qos_name = ?QOS3,
                expression = <<"country=PT">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                qos_name = ?QOS1,
                qos_expression_in_rpn = [<<"country=PL">>],
                replicas_num = 1,
                file_key = {path, Dir2Path}
            },
            #expected_qos_entry{
                qos_name = ?QOS2,
                qos_expression_in_rpn = [<<"country=FR">>],
                replicas_num = 1,
                file_key = {path, Dir3Path}
            },
            #expected_qos_entry{
                qos_name = ?QOS3,
                qos_expression_in_rpn = [<<"country=PT">>],
                replicas_num = 1,
                file_key = {path, Dir4Path}
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                path = File41Path,
                qos_entries = [?QOS1, ?QOS2, ?QOS3],
                target_storages = #{
                    ?P1 => [?QOS1],
                    ?P2 => [?QOS2],
                    ?P3 => [?QOS3]
                }
            },
            #expected_file_qos{
                path = File31Path,
                qos_entries = [?QOS1, ?QOS2],
                target_storages = #{
                    ?P1 => [?QOS1],
                    ?P2 => [?QOS2]
                }
            }
        ]
    },

    add_qos_for_dir_and_check_effective_qos(Config, TestSpec).


effective_qos_for_files_in_different_directories_of_tree_structure(Config) ->
    Dir1Path = filename:join(<<"/space_name1">>, <<"dir1">>),
    Dir2Path = filename:join(Dir1Path, <<"dir2">>),
    Dir3Path = filename:join(Dir1Path, <<"dir3">>),
    File21Path = filename:join(Dir2Path, <<"file21">>),
    File31Path = filename:join(Dir3Path, <<"file31">>),

    TestSpec = #effective_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = {?SPACE1, [
                {<<"dir1">>, [
                    {<<"dir2">>, [{<<"file21">>, ?TEST_DATA}]},
                    {<<"dir3">>, [{<<"file31">>, ?TEST_DATA}]}
                ]}
            ]}
        },
        qos_to_add = [
            #qos_to_add{
                path = Dir1Path,
                qos_name = ?QOS1,
                expression = <<"country=PL">>,
                replicas_num = 1
            },
            #qos_to_add{
                path = Dir2Path,
                qos_name = ?QOS2,
                expression = <<"country=FR">>,
                replicas_num = 1
            },
            #qos_to_add{
                path = Dir3Path,
                qos_name = ?QOS3,
                expression = <<"country=PT">>,
                replicas_num = 1
            }
        ],
        expected_qos_entries = [
            #expected_qos_entry{
                qos_name = ?QOS1,
                qos_expression_in_rpn = [<<"country=PL">>],
                replicas_num = 1,
                file_key = {path, Dir1Path}
            },
            #expected_qos_entry{
                qos_name = ?QOS2,
                qos_expression_in_rpn = [<<"country=FR">>],
                replicas_num = 1,
                file_key = {path, Dir2Path}
            },
            #expected_qos_entry{
                qos_name = ?QOS3,
                qos_expression_in_rpn = [<<"country=PT">>],
                replicas_num = 1,
                file_key = {path, Dir3Path}
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                path = File31Path,
                qos_entries = [?QOS1, ?QOS3],
                target_storages = #{
                    ?P1 => [?QOS1],
                    ?P3 => [?QOS3]
                }
            },
            #expected_file_qos{
                path = File21Path,
                qos_entries = [?QOS1, ?QOS2],
                target_storages = #{
                    ?P1 => [?QOS1],
                    ?P2 => [?QOS2]
                }
            }
        ]
    },

    add_qos_for_dir_and_check_effective_qos(Config, TestSpec).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer, qos_tests_utils]} | Config].


end_per_suite(Config) ->
    initializer:teardown_storage(Config).


init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    qos_tests_utils:mock_providers_qos(ConfigWithSessionInfo, ?TEST_PROVIDERS_QOS),
    % do not start file synchronization
    qos_tests_utils:mock_synchronize_transfers(ConfigWithSessionInfo),
    qos_tests_utils:mock_space_storages(ConfigWithSessionInfo, maps:keys(?TEST_PROVIDERS_QOS)),
    qos_tests_utils:init_qos_bounded_cache(ConfigWithSessionInfo),
    mock_start_traverse(ConfigWithSessionInfo),
    lfm_proxy:init(ConfigWithSessionInfo).


end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    test_utils:mock_unload(Workers),
    initializer:clean_test_users_and_spaces_no_validate(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================

run_tests(Config, FileTypes, TestSpecFun) ->
    lists:foreach(fun(FileType) ->
        TestSpec = case FileType of
            file ->
                ct:pal("Starting for file"),
                create_test_file(Config),
                TestSpecFun(?TEST_FILE_PATH);
            dir ->
                ct:pal("Starting for dir"),
                create_test_dir_with_file(Config),
                TestSpecFun(?TEST_DIR_PATH)
        end,
        add_qos_and_check_qos_docs(Config, TestSpec)
    end, FileTypes).


add_qos_and_check_qos_docs(Config, TestSpec) ->
    #add_qos_and_check_docs_test_spec{
        qos_to_add = QosToAddList,
        expected_qos_entries = ExpectedQosEntries,
        expected_file_qos = ExpectedFileQos
    } = TestSpec,

    % add QoS for file and w8 for appropriate QoS status
    QosNameIdMapping = qos_tests_utils:add_multiple_qos_in_parallel(Config, QosToAddList),
    qos_tests_utils:wait_for_qos_fulfilment_in_parallel(Config, undefined, QosNameIdMapping, ExpectedQosEntries),

    % check qos documents
    qos_tests_utils:assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping),
    qos_tests_utils:assert_file_qos_documents(Config, ExpectedFileQos, QosNameIdMapping, false).


add_qos_for_dir_and_check_effective_qos(Config, TestSpec) ->
    #effective_qos_test_spec{
        initial_dir_structure = InitialDirStructure,
        qos_to_add = QosToAddList,
        expected_qos_entries = ExpectedQosEntries,
        expected_effective_qos = ExpectedEffectiveQos
    } = TestSpec,

    % create initial dir structure
    qos_tests_utils:create_dir_structure(Config, InitialDirStructure),

    % add QoS and w8 for appropriate QoS status
    QosNameIdMapping = qos_tests_utils:add_multiple_qos_in_parallel(Config, QosToAddList),
    qos_tests_utils:wait_for_qos_fulfilment_in_parallel(Config, undefined, QosNameIdMapping, ExpectedQosEntries),

    % check qos_entry documents and effective QoS
    qos_tests_utils:assert_qos_entry_documents(Config, ExpectedQosEntries, QosNameIdMapping),
    qos_tests_utils:assert_effective_qos(Config, ExpectedEffectiveQos, QosNameIdMapping, false).


create_test_file(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    qos_tests_utils:create_file(Worker, SessId, ?TEST_FILE_PATH, ?TEST_DATA).


create_test_dir_with_file(Config) ->
    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),
    DirGuid = qos_tests_utils:create_directory(Worker, SessId, ?TEST_DIR_PATH),
    FilePath = filename:join(?TEST_DIR_PATH, <<"file1">>),
    _FileGuid = qos_tests_utils:create_file(Worker, SessId, FilePath, ?TEST_DATA),
    DirGuid.


mock_start_traverse(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, qos_hooks, [passthrough]),
    ok = test_utils:mock_expect(Workers, qos_hooks, maybe_start_traverse,
        fun(FileCtx, QosId, Storage, TaskId) ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            ok = file_qos:add_qos(FileUuid, SpaceId, QosId, [Storage]),
            ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
            ok = qos_traverse:start_initial_traverse(FileCtx, QosId, Storage, TaskId),
            {ok, _} = qos_entry:mark_traverse_started(QosId, TaskId),
            ok
        end).
