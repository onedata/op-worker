%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests for basic QoS management. The main goal of those
%%% tests is to perform given action related to QoS and then check whether
%%% appropriate qos documents have been created and appropriately updated .
%%% @end
%%%--------------------------------------------------------------------
-module(single_qos_test_SUITE).
-author("Michal Cwiertnia").

%% API
-export([
    all/0,
    init_per_suite/1, init_per_testcase/2,
    end_per_suite/1, end_per_testcase/2
]).

%% test functions
-export([
    add_single_key_val_qos_for_file_and_check_qos_docs/1,
    add_qos_with_intersection_for_file_and_check_qos_docs/1,
    add_qos_with_complement_for_file_and_check_qos_docs/1,
    add_qos_with_union_for_file_and_check_qos_docs/1,
    add_qos_with_multiple_replicas_for_file_and_check_qos_docs/1,
    add_qos_with_multiple_replicas_and_union_for_file_and_check_qos_docs/1,
    add_qos_intersection_and_union_for_file_and_check_qos_docs/1,
    add_qos_with_union_and_complement_for_file_and_check_qos_docs/1,
    add_qos_with_intersection_and_complement_for_file_and_check_qos_docs/1,
    add_key_val_qos_that_cannot_be_fulfilled_for_file_and_check_qos_docs/1,
    add_qos_that_cannot_be_fulfilled_for_file_and_check_qos_docs/1,
    add_qos_with_parens_for_file_and_check_qos_docs/1,
    add_multi_qos_resulting_in_the_same_storages_for_file_and_check_qos_docs/1,
    add_the_same_qos_multiple_time_for_file_and_check_qos_docs/1,
    add_contrary_qos_for_file_and_check_qos_docs/1,
    add_multi_qos_where_one_cannot_be_satisfied_for_file_and_check_qos_docs/1,
    add_multi_qos_that_overlaps_for_file_and_check_qos_docs/1,
    add_multi_qos_resulting_in_different_storages_for_file_and_check_qos_docs/1,

    add_single_key_val_qos_for_dir_and_check_qos_docs/1,
    add_qos_with_intersection_for_dir_and_check_qos_docs/1,
    add_qos_with_complement_for_dir_and_check_qos_docs/1,
    add_qos_with_union_for_dir_and_check_qos_docs/1,
    add_qos_with_multiple_replicas_for_dir_and_check_qos_docs/1,
    add_qos_with_multiple_replicas_and_union_for_dir_and_check_qos_docs/1,
    add_qos_intersection_and_union_for_dir_and_check_qos_docs/1,
    add_qos_with_union_and_complement_for_dir_and_check_qos_docs/1,
    add_qos_with_intersection_and_complement_for_dir_and_check_qos_docs/1,
    add_key_val_qos_that_cannot_be_fulfilled_for_dir_and_check_qos_docs/1,
    add_qos_that_cannot_be_fulfilled_for_dir_and_check_qos_docs/1,
    add_qos_with_parens_for_dir_and_check_qos_docs/1,
    add_multi_qos_resulting_in_the_same_storages_for_dir_and_check_qos_docs/1,
    add_the_same_qos_multiple_time_for_dir_and_check_qos_docs/1,
    add_contrary_qos_for_dir_and_check_qos_docs/1,
    add_multi_qos_where_one_cannot_be_satisfied_for_dir_and_check_qos_docs/1,
    add_multi_qos_that_overlaps_for_dir_and_check_qos_docs/1,
    add_multi_qos_resulting_in_different_storages_for_dir_and_check_qos_docs/1,  
    
    effective_qos_for_file_in_directory/1,
    effective_qos_for_file_in_nested_directories/1,
    effective_qos_for_file_in_directory_tree/1
]).

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

all() -> [
    add_single_key_val_qos_for_file_and_check_qos_docs,
    add_qos_with_intersection_for_file_and_check_qos_docs,
    add_qos_with_complement_for_file_and_check_qos_docs,
    add_qos_with_union_for_file_and_check_qos_docs,
    add_qos_with_multiple_replicas_for_file_and_check_qos_docs,
    add_qos_with_multiple_replicas_and_union_for_file_and_check_qos_docs,
    add_qos_intersection_and_union_for_file_and_check_qos_docs,
    add_qos_with_union_and_complement_for_file_and_check_qos_docs,
    add_qos_with_intersection_and_complement_for_file_and_check_qos_docs,
    add_key_val_qos_that_cannot_be_fulfilled_for_file_and_check_qos_docs,
    add_qos_that_cannot_be_fulfilled_for_file_and_check_qos_docs,
    add_qos_with_parens_for_file_and_check_qos_docs,
    add_multi_qos_resulting_in_the_same_storages_for_file_and_check_qos_docs,
    add_the_same_qos_multiple_time_for_file_and_check_qos_docs,
    add_contrary_qos_for_file_and_check_qos_docs,
    add_multi_qos_where_one_cannot_be_satisfied_for_file_and_check_qos_docs,
    add_multi_qos_that_overlaps_for_file_and_check_qos_docs,
    add_multi_qos_resulting_in_different_storages_for_file_and_check_qos_docs,

    add_single_key_val_qos_for_dir_and_check_qos_docs,
    add_qos_with_intersection_for_dir_and_check_qos_docs,
    add_qos_with_complement_for_dir_and_check_qos_docs,
    add_qos_with_union_for_dir_and_check_qos_docs,
    add_qos_with_multiple_replicas_for_dir_and_check_qos_docs,
    add_qos_with_multiple_replicas_and_union_for_dir_and_check_qos_docs,
    add_qos_intersection_and_union_for_dir_and_check_qos_docs,
    add_qos_with_union_and_complement_for_dir_and_check_qos_docs,
    add_qos_with_intersection_and_complement_for_dir_and_check_qos_docs,
    add_key_val_qos_that_cannot_be_fulfilled_for_dir_and_check_qos_docs,
    add_qos_that_cannot_be_fulfilled_for_dir_and_check_qos_docs,
    add_qos_with_parens_for_dir_and_check_qos_docs,
    add_multi_qos_resulting_in_the_same_storages_for_dir_and_check_qos_docs,
    add_the_same_qos_multiple_time_for_dir_and_check_qos_docs,
    add_contrary_qos_for_dir_and_check_qos_docs,
    add_multi_qos_where_one_cannot_be_satisfied_for_dir_and_check_qos_docs,
    add_multi_qos_that_overlaps_for_dir_and_check_qos_docs,
    add_multi_qos_resulting_in_different_storages_for_dir_and_check_qos_docs,

    effective_qos_for_file_in_directory,
    effective_qos_for_file_in_nested_directories,
    effective_qos_for_file_in_directory_tree
].


% mock qos for test storages
-define(P1_TEST_QOS, #{
    <<"country">> => <<"PL">>,
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

-define(P1, <<"p1">>).
-define(P2, <<"p2">>).
-define(P3, <<"p3">>).
-define(P4, <<"p4">>).

-define(TEST_PROVIDERS_QOS, #{
    ?P1 => ?P1_TEST_QOS,
    ?P2 => ?P2_TEST_QOS,
    ?P3 => ?P3_TEST_QOS,
    ?P4 => ?P4_TEST_QOS
}).

-define(SPACE1, <<"/space_name1">>).
-define(TEST_DATA, <<"test_data">>).
-define(ATTEMPTS, 60).

-define(QOS1, <<"Qos1">>).
-define(QOS2, <<"Qos2">>).
-define(QOS3, <<"Qos3">>).


%%%===================================================================
%%% Test functions
%%%===================================================================

%%%===================================================================
%%% Single QoS expression test. Each test case is executed once for file
%%% and once for directory
%%%===================================================================

add_single_key_val_qos_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"country=FR">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=FR">>]
            }
        ],
        qos_list => [?QOS1],
        target_storages => #{?P2 => [?QOS1]}
    }.

add_single_key_val_qos_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_single_key_val_qos_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_single_key_val_qos_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_single_key_val_qos_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_qos_with_intersection_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"type=disk&tier=t2">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"type=disk">>, <<"tier=t2">>, <<"&">>]
            }
        ],
        qos_list => [?QOS1],
        target_storages => #{?P3 => [?QOS1]}
    }.

add_qos_with_intersection_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_intersection_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_qos_with_intersection_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_intersection_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_qos_with_complement_test_case() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"type=disk-tier=t2">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"type=disk">>, <<"tier=t2">>, <<"-">>]
            }
        ],
        qos_list => [?QOS1],
        target_storages => #{?P1 => [?QOS1]}
    }.

add_qos_with_complement_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_complement_test_case(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_qos_with_complement_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_complement_test_case(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_qos_with_union_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"country=PL|type=tape">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=PL">>, <<"type=tape">>, <<"|">>]
            }
        ],
        qos_list => [?QOS1],
        target_storages => #{?P1 => [?QOS1]}
    }.

add_qos_with_union_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_union_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_qos_with_union_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_union_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_qos_with_multiple_replicas_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"type=disk">>,
                replicas_num => 2,
                qos_expression_in_rpn => [<<"type=disk">>]
            }
        ],
        qos_list => [?QOS1],
        target_storages => #{
            ?P1 => [?QOS1],
            ?P3 => [?QOS1]
        }
    }.

add_qos_with_multiple_replicas_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_multiple_replicas_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_qos_with_multiple_replicas_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_multiple_replicas_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_qos_with_intersection_and_union_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"type=disk&tier=t2|country=FR">>,
                replicas_num => 2,
                qos_expression_in_rpn => [<<"type=disk">>, <<"tier=t2">>, <<"&">>, <<"country=FR">>, <<"|">>]
            }
        ],
        qos_list => [?QOS1],
        target_storages => #{
            ?P2 => [?QOS1],
            ?P3 => [?QOS1]
        }
    }.

add_qos_intersection_and_union_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_intersection_and_union_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_qos_intersection_and_union_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_intersection_and_union_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_qos_with_union_and_complement_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"country=PL|country=FR-type=tape">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=PL">>, <<"country=FR">>, <<"|">>, <<"type=tape">>, <<"-">>]
            }
        ],
        qos_list => [?QOS1],
        target_storages => #{
            ?P1 => [?QOS1]
        }
    }.

add_qos_with_union_and_complement_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_union_and_complement_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_qos_with_union_and_complement_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_union_and_complement_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_qos_with_intersection_and_complement_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"type=disk&tier=t3-country=PL">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"type=disk">>, <<"tier=t3">>, <<"&">>, <<"country=PL">>, <<"-">>]
            }
        ],
        qos_list => [?QOS1],
        target_storages => #{
            ?P4 => [?QOS1]
        }
    }.

add_qos_with_intersection_and_complement_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_intersection_and_complement_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_qos_with_intersection_and_complement_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_intersection_and_complement_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_qos_with_multiple_replicas_and_union_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"country=PL|country=FR|country=PT">>,
                replicas_num => 3,
                qos_expression_in_rpn => [<<"country=PL">>, <<"country=FR">>, <<"|">>, <<"country=PT">>, <<"|">>]
            }
        ],
        qos_list => [?QOS1],
        target_storages => #{
            ?P1 => [?QOS1],
            ?P2 => [?QOS1],
            ?P3 => [?QOS1]
        }
    }.

add_qos_with_multiple_replicas_and_union_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_multiple_replicas_and_union_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).

add_qos_with_multiple_replicas_and_union_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_multiple_replicas_and_union_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).


add_key_val_qos_that_cannot_be_fulfilled_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"country=IT">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=IT">>],
                is_possible => false
            }
        ],
        qos_list => [?QOS1],
        target_storages => #{}
    }.

add_key_val_qos_that_cannot_be_fulfilled_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_key_val_qos_that_cannot_be_fulfilled_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_key_val_qos_that_cannot_be_fulfilled_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_key_val_qos_that_cannot_be_fulfilled_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_qos_that_cannot_be_fulfilled_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"country=PL|country=PT-type=disk">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=PL">>, <<"country=PT">>, <<"|">>, <<"type=disk">>, <<"-">>],
                is_possible => false
            }
        ],
        qos_list => [?QOS1],
        target_storages => #{}
    }.

add_qos_that_cannot_be_fulfilled_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_qos_that_cannot_be_fulfilled_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).

add_qos_that_cannot_be_fulfilled_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_qos_that_cannot_be_fulfilled_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).


add_qos_with_parens_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"country=PL|(country=PT-type=disk)">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=PL">>, <<"country=PT">>, <<"type=disk">>, <<"-">>, <<"|">>]
            }
        ],
        qos_list => [?QOS1],
        target_storages => #{
            ?P1 => [?QOS1]
        }
    }.

add_qos_with_parens_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_parens_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).

add_qos_with_parens_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_qos_with_parens_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).


%%%===================================================================
%%% Multi QoS test. Each test case is executed once for file
%%% and once for directory
%%%===================================================================

add_multi_qos_resulting_in_different_storages_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"type=disk&tier=t2">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"type=disk">>, <<"tier=t2">>, <<"&">>]
            },
            #{
                qos_name => ?QOS2,
                qos_expression => <<"country=FR">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=FR">>]
            }
        ],
        qos_list => [?QOS1, ?QOS2],
        target_storages => #{
            ?P2 => [?QOS2],
            ?P3 => [?QOS1]
        }
    }.

add_multi_qos_resulting_in_different_storages_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_multi_qos_resulting_in_different_storages_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_multi_qos_resulting_in_different_storages_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_multi_qos_resulting_in_different_storages_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_multi_qos_resulting_in_the_same_storages_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"type=tape">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"type=tape">>]
            },
            #{
                qos_name => ?QOS2,
                qos_expression => <<"country=FR">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=FR">>]
            }
        ],
        qos_list => [?QOS1, ?QOS2],
        target_storages => #{
            ?P2 => [?QOS1, ?QOS2]
        }
    }.

add_multi_qos_resulting_in_the_same_storages_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_multi_qos_resulting_in_the_same_storages_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_multi_qos_resulting_in_the_same_storages_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_multi_qos_resulting_in_the_same_storages_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_the_same_qos_multiple_time_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"type=tape">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"type=tape">>]
            },
            #{
                qos_name => ?QOS2,
                qos_expression => <<"type=tape">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"type=tape">>]
            },
            #{
                qos_name => ?QOS3,
                qos_expression => <<"type=tape">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"type=tape">>]
            }
        ],
        qos_list => [?QOS1, ?QOS2, ?QOS3],
        target_storages => #{
            ?P2 => [?QOS1, ?QOS2, ?QOS3]
        }
    }.

add_the_same_qos_multiple_time_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_the_same_qos_multiple_time_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_the_same_qos_multiple_time_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_the_same_qos_multiple_time_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_contrary_qos_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"country=PL">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=PL">>]
            },
            #{
                qos_name => ?QOS2,
                qos_expression => <<"type=tape-country=PL">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"type=tape">>, <<"country=PL">>, <<"-">>]
            }
        ],
        qos_list => [?QOS1, ?QOS2],
        target_storages => #{
            ?P1 => [?QOS1],
            ?P2 => [?QOS2]
        }
    }.

add_contrary_qos_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_contrary_qos_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_contrary_qos_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_contrary_qos_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_multi_qos_where_one_cannot_be_satisfied_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"country=FR">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=FR">>]
            },
            #{
                qos_name => ?QOS2,
                qos_expression => <<"country=IT">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=IT">>],
                is_possible => false
            }
        ],
        qos_list => [?QOS1, ?QOS2],
        target_storages => #{
            ?P2 => [?QOS1]
        }
    }.

add_multi_qos_where_one_cannot_be_satisfied_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_multi_qos_where_one_cannot_be_satisfied_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_multi_qos_where_one_cannot_be_satisfied_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_multi_qos_where_one_cannot_be_satisfied_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


add_multi_qos_that_overlaps_test_spec() ->
    #{
        qos => [
            #{
                qos_name => ?QOS1,
                qos_expression => <<"type=disk">>,
                replicas_num => 3,
                qos_expression_in_rpn => [<<"type=disk">>]
            },
            #{
                qos_name => ?QOS2,
                qos_expression => <<"tier=t3">>,
                replicas_num => 2,
                qos_expression_in_rpn => [<<"tier=t3">>]
            }
        ],
        qos_list => [?QOS1, ?QOS2],
        target_storages => #{
            ?P1 => [?QOS1, ?QOS2],
            ?P3 => [?QOS1],
            ?P4 => [?QOS1, ?QOS2]
        }
    }.

add_multi_qos_that_overlaps_for_file_and_check_qos_docs(Config) ->
    TestSpec = add_multi_qos_that_overlaps_test_spec(),
    add_qos_for_file_and_check_qos_docs(Config, TestSpec).

add_multi_qos_that_overlaps_for_dir_and_check_qos_docs(Config) ->
    TestSpec = add_multi_qos_that_overlaps_test_spec(),
    add_qos_for_dir_and_check_qos_docs(Config, TestSpec).


%%%===================================================================
%%% Effective QoS tests
%%%===================================================================

effective_qos_for_file_in_directory(Config) ->
    DirPath = filename:join(?SPACE1, <<"dir1">>),
    FilePath = filename:join(DirPath, <<"file1">>),

    TestSpec = #{
        dir_structure => {
            <<"dir1">>, [
                {<<"file1">>, ?TEST_DATA}
            ]
        },
        qos => [
            #{
                dir_path => DirPath,
                qos_name => ?QOS1,
                qos_expression => <<"country=FR">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=FR">>]
            }
        ],
        effective_qos => [
            #{
                file_path => FilePath,
                qos_list => [?QOS1],
                target_storages => #{?P2 => [?QOS1]}
            }
        ]
    },

    add_qos_for_dir_and_check_effective_qos(Config, TestSpec).


effective_qos_for_file_in_nested_directories(Config) ->
    Dir1Path = filename:join(<<"/space_name1">>, <<"dir1">>),
    Dir2Path = filename:join(Dir1Path, <<"dir2">>),
    File21Path = filename:join(Dir2Path, <<"file21">>),
    Dir3Path = filename:join(Dir2Path, <<"dir3">>),
    File31Path = filename:join(Dir3Path, <<"file31">>),

    TestSpec = #{
        dir_structure => {
            <<"dir1">>, [
                {<<"dir2">>, [
                    {<<"file21">>, ?TEST_DATA},
                    {<<"dir3">>, [
                        {<<"file31">>, ?TEST_DATA}
                    ]}
                ]}
            ]
        },
        qos => [
            #{
                dir_path => Dir1Path,
                qos_name => ?QOS1,
                qos_expression => <<"country=PL">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=PL">>]
            },
            #{
                dir_path => Dir2Path,
                qos_name => ?QOS2,
                qos_expression => <<"country=FR">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=FR">>]
            },
            #{
                dir_path => Dir3Path,
                qos_name => ?QOS3,
                qos_expression => <<"country=PT">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=PT">>]
            }
        ],
        effective_qos => [
            #{
                file_path => File31Path,
                qos_list => [?QOS1, ?QOS2, ?QOS3],
                target_storages => #{
                    ?P1 => [?QOS1],
                    ?P2 => [?QOS2],
                    ?P3 => [?QOS3]
                }
            },
            #{
                file_path => File21Path,
                qos_list => [?QOS1, ?QOS2],
                target_storages => #{
                    ?P1 => [?QOS1],
                    ?P2 => [?QOS2]
                }
            }
        ]
    },

    add_qos_for_dir_and_check_effective_qos(Config, TestSpec).


effective_qos_for_file_in_directory_tree(Config) ->
    Dir1Path = filename:join(<<"/space_name1">>, <<"dir1">>),
    Dir2Path = filename:join(Dir1Path, <<"dir2">>),
    File21Path = filename:join(Dir2Path, <<"file21">>),
    Dir3Path = filename:join(Dir1Path, <<"dir3">>),
    File31Path = filename:join(Dir3Path, <<"file31">>),

    TestSpec = #{
        dir_structure => {
            <<"dir1">>, [
                {<<"dir2">>, [{<<"file21">>, ?TEST_DATA}]},
                {<<"dir3">>, [{<<"file31">>, ?TEST_DATA}]}
            ]
        },
        qos => [
            #{
                dir_path => Dir1Path,
                qos_name => ?QOS1,
                qos_expression => <<"country=PL">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=PL">>]
            },
            #{
                dir_path => Dir2Path,
                qos_name => ?QOS2,
                qos_expression => <<"country=FR">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=FR">>]
            },
            #{
                dir_path => Dir3Path,
                qos_name => ?QOS3,
                qos_expression => <<"country=PT">>,
                replicas_num => 1,
                qos_expression_in_rpn => [<<"country=PT">>]
            }
        ],
        effective_qos => [
            #{
                file_path => File31Path,
                qos_list => [?QOS1, ?QOS3],
                target_storages => #{
                    ?P1 => [?QOS1],
                    ?P3 => [?QOS3]
                }
            },
            #{
                file_path => File21Path,
                qos_list => [?QOS1, ?QOS2],
                target_storages => #{
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
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    Workers = ?config(op_worker_nodes, ConfigWithSessionInfo),
    rpc:multicall(Workers, fslogic_worker, init_qos_cache_for_all_spaces, []),
    mock_providers_qos(Config),
    mock_synchronize_file(Config),
    mock_space_storages(Config, maps:keys(?TEST_PROVIDERS_QOS)),
    mock_start_traverse(Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    test_utils:mock_unload(Workers),
    initializer:clean_test_users_and_spaces_no_validate(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

mock_providers_qos(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, providers_qos),
    test_utils:mock_expect(Workers, providers_qos, get_storage_qos,
        fun(StorageId, _StorageSet) ->
            % names of test providers start with p1, p2 etc.
            maps:get(binary:part(StorageId, 0, 2), ?TEST_PROVIDERS_QOS)
        end).


mock_synchronize_file(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, qos_traverse, [passthrough]),
    ok = test_utils:mock_expect(Workers, qos_traverse, synchronize_file,
        fun(_, _, _) ->
            ok
        end).


mock_space_storages(Config, StorageList) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, qos_req, [passthrough]),
    ok = test_utils:mock_expect(Workers, qos_req, get_space_storages,
        fun(_, _) ->
            StorageList
        end).


mock_start_traverse(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, qos_hooks, [passthrough]),
    ok = test_utils:mock_expect(Workers, qos_hooks, maybe_start_traverse,
        fun(FileCtx, QosId, OriginFileGuid, Storage, TaskId) ->
            SpaceId = file_ctx:get_space_id_const(FileCtx),
            FileUuid = file_ctx:get_uuid_const(FileCtx),
            ok = file_qos:add_qos(FileUuid, SpaceId, QosId, [Storage]),
            ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId),
            ok = qos_traverse:fulfill_qos(FileCtx, QosId, OriginFileGuid, Storage, traverse, TaskId),
            ok = qos_entry:add_traverse(SpaceId, QosId, TaskId),
            case qos_entry:remove_traverse_req(QosId, TaskId) of
                {ok, _} -> ok;
                % request is from the same provider and qos_entry is not yet created
                {error, not_found} -> ok;
                {error, _} = Error -> throw(Error)
            end,
            true
        end).


wait_for_qos_fulfillment(Worker, SessId, QosId) ->
    ?assertMatch(true, lfm_proxy:check_qos_fulfilled(Worker, SessId, QosId), ?ATTEMPTS).


add_qos_for_file_and_check_qos_docs(Config, TestSpec) ->
    #{
        qos := Qos,
        qos_list := ExpectedQosEntries,
        target_storages := ExpectedTargetStorages
    } = TestSpec,

    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    % create file
    FilePath = filename:join(?SPACE1, <<"file1">>),
    FileGuid = qos_tests_utils:create_file(Worker, SessId, FilePath, ?TEST_DATA),
    FileUuid = file_id:guid_to_uuid(FileGuid),

    % add QoS for file
    QosNameIdMapping = lists:foldl(fun(QosCfg, QosNameIdMapping) ->
        #{
            qos_name := QosName,
            qos_expression := QosExpression,
            replicas_num := ReplicasNum,
            qos_expression_in_rpn := QosExpressionRPN
        } = QosCfg,
        IsPossible = maps:get(is_possible, QosCfg, true),

        {ok, QosId} = ?assertMatch(
            {ok, _QosId},
            lfm_proxy:add_qos(Worker, SessId, {guid, FileGuid}, QosExpression, ReplicasNum)
        ),

        case IsPossible == false of
            true -> ok;
            false -> wait_for_qos_fulfillment(Worker, SessId, QosId)
        end,

        % check qos_entry document
        qos_tests_utils:assert_qos_entry_document(
            Worker, QosId, FileUuid, QosExpressionRPN, ReplicasNum, IsPossible
        ),

        QosNameIdMapping#{QosName => QosId}
    end, #{}, Qos),

    ExpectedQosEntriesId = map_qos_names_to_ids(ExpectedQosEntries, QosNameIdMapping),

    qos_tests_utils:assert_qos_list(Worker, FileUuid, ExpectedQosEntriesId).


add_qos_for_dir_and_check_qos_docs(Config, TestSpec) ->
    #{
        qos := Qos,
        qos_list := ExpectedQosEntries,
        target_storages := ExpectedTargetStorages
    } = TestSpec,

    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    % create directory with file
    DirPath = filename:join(?SPACE1, <<"dir1">>),
    FilePath = filename:join(DirPath, <<"file1">>),
    DirGuid = qos_tests_utils:create_directory(Worker, SessId, DirPath),
    DirUuid = file_id:guid_to_uuid(DirGuid),
    FileGuid = qos_tests_utils:create_file(Worker, SessId, FilePath, ?TEST_DATA),

    % add QoS for directory
    QosNameIdMapping = lists:foldl(fun(QosCfg, QosNameIdMapping) ->
        #{
            qos_name := QosName,
            qos_expression := QosExpression,
            replicas_num := ReplicasNum,
            qos_expression_in_rpn := QosExpressionRPN
        } = QosCfg,
        IsPossible = maps:get(is_possible, QosCfg, true),

        {ok, QosId} = ?assertMatch(
            {ok, _QosId},
            lfm_proxy:add_qos(Worker, SessId, {guid, DirGuid}, QosExpression, ReplicasNum)
        ),

          case IsPossible == false of
            true -> ok;
            false -> wait_for_qos_fulfillment(Worker, SessId, QosId)
        end,

        % check qos_entry document
        qos_tests_utils:assert_qos_entry_document(
            Worker, QosId, DirUuid, QosExpressionRPN, ReplicasNum, IsPossible
        ),

        QosNameIdMapping#{QosName => QosId}
    end, #{}, Qos),

    ExpectedQosEntriesId = map_qos_names_to_ids(ExpectedQosEntries, QosNameIdMapping),

    qos_tests_utils:assert_qos_list(Worker, DirUuid, ExpectedQosEntriesId),
    
    % check that for file document file_qos has not been created
    ?assertMatch({error, not_found}, rpc:call(Worker, file_qos, get, [FileGuid])).


add_qos_for_dir_and_check_effective_qos(Config, TestSpec) ->
    #{
        dir_structure := DirStructure,
        qos := Qos,
        effective_qos := ExpectedEffQosEntries
    } = TestSpec,

    [Worker] = ?config(op_worker_nodes, Config),
    SessId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(Worker)}}, Config),

    % create directory structure
    qos_tests_utils:create_dir_structure(Worker, SessId, DirStructure, ?SPACE1),

    % add QoS according to test specification
    QosNameIdMapping = lists:foldl(fun(QosCfg, QosNameIdMapping) ->
        #{
            dir_path := DirPath,
            qos_name := QosName,
            qos_expression := QosExpression,
            replicas_num := ReplicasNum,
            qos_expression_in_rpn := QosExpressionRPN
        } = QosCfg,
        IsPossible = maps:get(is_possible, QosCfg, true),

        DirGuid = qos_tests_utils:get_guid(Worker, SessId, DirPath),
        DirUuid = file_id:guid_to_uuid(DirGuid),
        {ok, QosId} = ?assertMatch(
            {ok, _QosId},
            lfm_proxy:add_qos(Worker, SessId, {guid, DirGuid}, QosExpression, ReplicasNum)
        ),

        case IsPossible == false of
            true -> ok;
            false -> wait_for_qos_fulfillment(Worker, SessId, QosId)
        end,

        % check qos_entry document
        qos_tests_utils:assert_qos_entry_document(
            Worker, QosId, DirUuid, QosExpressionRPN, ReplicasNum, IsPossible
        ),

        QosNameIdMapping#{QosName => QosId}
    end, #{}, Qos),

    % check effective QoS according to test specification
    lists:foreach(fun(ExpectedEffQos) ->
        #{
            qos_list := ExpectedQosEntries,
            target_storages := ExpectedTargetStorages,
            file_path := FilePath
        } = ExpectedEffQos,

        ExpectedQosEntriesId = map_qos_names_to_ids(ExpectedQosEntries, QosNameIdMapping),
        ExpectedTargetStoragesId = maps:map(fun(_, QosNamesList) ->
            map_qos_names_to_ids(QosNamesList, QosNameIdMapping)
        end, ExpectedTargetStorages),
        FileGuid = qos_tests_utils:get_guid(Worker, SessId, FilePath),
        FileUuid = file_id:guid_to_uuid(FileGuid),
        qos_tests_utils:assert_effective_qos(Worker, FileUuid, ExpectedQosEntriesId, ExpectedTargetStoragesId),

        % check that for file document file_qos has not been created
        ?assertMatch({error, not_found}, rpc:call(Worker, file_qos, get, [FileUuid]))
    end, ExpectedEffQosEntries).


map_qos_names_to_ids(QosNamesList, QosNameIdMapping) ->
    [maps:get(QosName, QosNameIdMapping) || QosName <- QosNamesList].