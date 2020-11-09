%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests which check whether files are created on
%%% storage with proper permissions and ownership.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_files_test_SUITE).
-author("Jakub Kudzia").

-include("storage_files_test_SUITE.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    space_directory_mode_and_owner_test/1,
    regular_file_mode_and_owner_test/1,
    regular_file_custom_mode_and_owner_test/1,
    regular_file_unknown_owner_test/1,
    directory_mode_and_owner_test/1,
    directory_custom_mode_and_owner_test/1,
    directory_with_unknown_owner_test/1,
    rename_file_test/1,
    creating_file_should_result_in_eacces_when_mapping_is_not_found/1
]).

-define(RUN(TestSpec), run_for_each_setup(TestSpec#test_spec{test_name = ?FUNCTION_NAME})).
-record(test_spec, {
    config :: list(),
    test_fun :: function(),
    test_name :: atom(),
    params = [] :: list(map()),
    custom_test_setups :: #{od_space:id() => map() | [map()]},
    generic_test_args = #{} :: map()
}).

%%%===================================================================
%%% API
%%%===================================================================

all() -> [
    space_directory_mode_and_owner_test,
    regular_file_mode_and_owner_test,
    regular_file_custom_mode_and_owner_test,
    regular_file_unknown_owner_test,
    directory_mode_and_owner_test,
    directory_custom_mode_and_owner_test,
    directory_with_unknown_owner_test,
    rename_file_test,
    creating_file_should_result_in_eacces_when_mapping_is_not_found
].

%%%===================================================================
%%% Test functions
%%%===================================================================

space_directory_mode_and_owner_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?RUN(#test_spec{
        config = Config,
        test_fun = fun space_directory_mode_and_owner_test_base/4,
        custom_test_setups = #{
            ?SPACE_ID1 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1),
                    expected_display_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1),
                    expected_display_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1),
                    expected_display_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1)
                }
            ],
            ?SPACE_ID2 => [
                #{
                    user => ?USER1,
                    expected_owner => ?OWNER(2000, 2000),
                    expected_display_owner => ?OWNER(2222, 2222)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?OWNER(2000, 2000),
                    expected_display_owner => ?OWNER(2222, 2222)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?OWNER(2000, 2000),
                    expected_display_owner => ?OWNER(2222, 2222)
                }
            ],
            ?SPACE_ID3 => [
                #{
                    user => ?USER1,
                    expected_owner => ?OWNER(3000, 3000),
                    expected_display_owner => ?OWNER(3333, 3333)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?OWNER(3000, 3000),
                    expected_display_owner => ?OWNER(3333, 3333)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?OWNER(3000, 3000),
                    expected_display_owner => ?OWNER(3333, 3333)
                }
            ],
            ?SPACE_ID4 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID4),
                    expected_display_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID4)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID4),
                    expected_display_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID4)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID4),
                    expected_display_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID4)
                }
            ],
            ?SPACE_ID5 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5),
                    expected_display_owner => ?OWNER(5555, 5555)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5),
                    expected_display_owner => ?OWNER(5555, 5555)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5),
                    expected_display_owner => ?OWNER(5555, 5555)
                }
            ],
            ?SPACE_ID6 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6),
                    expected_display_owner => ?OWNER(6666, 6666)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6),
                    expected_display_owner => ?OWNER(6666, 6666)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6),
                    expected_display_owner => ?OWNER(6666, 6666)
                }
            ],
            ?SPACE_ID7 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?GEN_SPACE_OWNER(?SPACE_ID7)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?GEN_SPACE_OWNER(?SPACE_ID7)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?GEN_SPACE_OWNER(?SPACE_ID7)
                }
            ],
            ?SPACE_ID8 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?OWNER(8888, 8888)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?OWNER(8888, 8888)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?OWNER(8888, 8888)
                }
            ],
            ?SPACE_ID9 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?OWNER(9999, 9999)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?OWNER(9999, 9999)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?OWNER(9999, 9999)
                }
            ]

        }}).

regular_file_mode_and_owner_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?RUN(#test_spec{
        config = Config,
        test_fun = fun regular_file_mode_and_owner_test_base/4,
        custom_test_setups = #{
            ?SPACE_ID1 => [
                #{
                    user => ?USER1,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER1),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER1)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER2),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER2)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID2 => [
                #{
                    user => ?USER1,
                    expected_owner => ?OWNER(2001, 2000),
                    expected_display_owner => ?OWNER(2221, 2222)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?OWNER(2002, 2000),
                    expected_display_owner => ?OWNER(2002, 2222)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID3 => [
                #{
                    user => ?USER1,
                    expected_owner => ?OWNER(3001, 3000),
                    expected_display_owner => ?OWNER(3331, 3333)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?OWNER(3002, 3000),
                    expected_display_owner => ?OWNER(3002, 3333)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID4 => [
                #{
                    user => ?USER1,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER1),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER1)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER2),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER2)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID5 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5, 5001),
                    expected_display_owner => ?OWNER(5551, 5555)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5, 5002),
                    expected_display_owner => ?OWNER(5002, 5555)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID6 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6, 6001),
                    expected_display_owner => ?OWNER(6661, 6666)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6, 6002),
                    expected_display_owner => ?OWNER(6002, 6666)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID7 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?GEN_OWNER(?USER1, ?SPACE_ID7)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?GEN_OWNER(?USER2, ?SPACE_ID7)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID8 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?OWNER(8881, 8888)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?OWNER(?UID(?USER2), 8888)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID9 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?OWNER(9991, 9999)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?OWNER(?UID(?USER2), 9999)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?ROOT_OWNER
                }
            ]

        },
        generic_test_args = #{file_perms => ?DEFAULT_FILE_PERMS}
    }).

regular_file_custom_mode_and_owner_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?RUN(#test_spec{
        config = Config,
        test_fun = fun regular_file_mode_and_owner_test_base/4,
        custom_test_setups = #{
            ?SPACE_ID1 => [
                #{
                    user => ?USER1,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER1),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER1)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER2),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER2)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID2 => [
                #{
                    user => ?USER1,
                    expected_owner => ?OWNER(2001, 2000),
                    expected_display_owner => ?OWNER(2221, 2222)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?OWNER(2002, 2000),
                    expected_display_owner => ?OWNER(2002, 2222)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID3 => [
                #{
                    user => ?USER1,
                    expected_owner => ?OWNER(3001, 3000),
                    expected_display_owner => ?OWNER(3331, 3333)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?OWNER(3002, 3000),
                    expected_display_owner => ?OWNER(3002, 3333)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID4 => [
                #{
                    user => ?USER1,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER1),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER1)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER2),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER2)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID5 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5, 5001),
                    expected_display_owner => ?OWNER(5551, 5555)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5, 5002),
                    expected_display_owner => ?OWNER(5002, 5555)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID6 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6, 6001),
                    expected_display_owner => ?OWNER(6661, 6666)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6, 6002),
                    expected_display_owner => ?OWNER(6002, 6666)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID7 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?GEN_OWNER(?USER1, ?SPACE_ID7)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?GEN_OWNER(?USER2, ?SPACE_ID7)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID8 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?OWNER(8881, 8888)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?OWNER(?UID(?USER2), 8888)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID9 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?OWNER(9991, 9999)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?OWNER(?UID(?USER2), 9999)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?ROOT_OWNER
                }
            ]

        },
        generic_test_args = #{file_perms => 8#777}
    }).

regular_file_unknown_owner_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?RUN(#test_spec{
        config = Config,
        test_fun = fun regular_file_unknown_owner_test_base/4,
        custom_test_setups = #{
            ?SPACE_ID1 => [
                #{
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1),
                    expected_owner2 => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1, ?UID(?UNKNOWN_USER)),
                    expected_display_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1, ?UID(?UNKNOWN_USER))
                }
            ],
            ?SPACE_ID4 => [
                #{
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1),
                    expected_owner2 => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID4, ?UID(?UNKNOWN_USER)),
                    expected_display_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID4, ?UID(?UNKNOWN_USER))
                }
            ],
            ?SPACE_ID7 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?GEN_OWNER(?UNKNOWN_USER, ?SPACE_ID7)
                }
            ]

        },
        generic_test_args = #{file_perms => ?DEFAULT_FILE_PERMS}
    }).

directory_mode_and_owner_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?RUN(#test_spec{
        config = Config,
        test_fun = fun directory_mode_and_owner_test_base/4,
        custom_test_setups = #{
            ?SPACE_ID1 => [
                #{
                    user => ?USER1,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER1),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER1)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER2),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER2)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID2 => [
                #{
                    user => ?USER1,
                    expected_owner => ?OWNER(2001, 2000),
                    expected_display_owner => ?OWNER(2221, 2222)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?OWNER(2002, 2000),
                    expected_display_owner => ?OWNER(2002, 2222)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID3 => [
                #{
                    user => ?USER1,
                    expected_owner => ?OWNER(3001, 3000),
                    expected_display_owner => ?OWNER(3331, 3333)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?OWNER(3002, 3000),
                    expected_display_owner => ?OWNER(3002, 3333)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID4 => [
                #{
                    user => ?USER1,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER1),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER1)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER2),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER2)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID5 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5, 5001),
                    expected_display_owner => ?OWNER(5551, 5555)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5, 5002),
                    expected_display_owner => ?OWNER(5002, 5555)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID6 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6, 6001),
                    expected_display_owner => ?OWNER(6661, 6666)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6, 6002),
                    expected_display_owner => ?OWNER(6002, 6666)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID7 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?GEN_OWNER(?USER1, ?SPACE_ID7)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?GEN_OWNER(?USER2, ?SPACE_ID7)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID8 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?OWNER(8881, 8888)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?OWNER(?UID(?USER2), 8888)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID9 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?OWNER(9991, 9999)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?OWNER(?UID(?USER2), 9999)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?ROOT_OWNER
                }
            ]


        },
        generic_test_args = #{dir_perms => ?DEFAULT_DIR_PERMS}
    }).

directory_custom_mode_and_owner_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?RUN(#test_spec{
        config = Config,
        test_fun = fun directory_mode_and_owner_test_base/4,
        custom_test_setups = #{
            ?SPACE_ID1 => [
                #{
                    user => ?USER1,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER1),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER1)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER2),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER2)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID2 => [
                #{
                    user => ?USER1,
                    expected_owner => ?OWNER(2001, 2000),
                    expected_display_owner => ?OWNER(2221, 2222)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?OWNER(2002, 2000),
                    expected_display_owner => ?OWNER(2002, 2222)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID3 => [
                #{
                    user => ?USER1,
                    expected_owner => ?OWNER(3001, 3000),
                    expected_display_owner => ?OWNER(3331, 3333)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?OWNER(3002, 3000),
                    expected_display_owner => ?OWNER(3002, 3333)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID4 => [
                #{
                    user => ?USER1,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER1),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER1)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER2),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER2)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID5 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5, 5001),
                    expected_display_owner => ?OWNER(5551, 5555)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5, 5002),
                    expected_display_owner => ?OWNER(5002, 5555)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID6 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6, 6001),
                    expected_display_owner => ?OWNER(6661, 6666)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6, 6002),
                    expected_display_owner => ?OWNER(6002, 6666)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_owner => ?ROOT_OWNER,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID7 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?GEN_OWNER(?USER1, ?SPACE_ID7)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?GEN_OWNER(?USER2, ?SPACE_ID7)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID8 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?OWNER(8881, 8888)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?OWNER(?UID(?USER2), 8888)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?ROOT_OWNER
                }
            ],
            ?SPACE_ID9 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?OWNER(9991, 9999)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?OWNER(?UID(?USER2), 9999)
                },
                #{
                    user => ?ROOT_USER_ID,
                    expected_display_owner => ?ROOT_OWNER
                }
            ]
        },
        generic_test_args = #{dir_perms => 8#777}
    }).

directory_with_unknown_owner_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    ?RUN(#test_spec{
        config = Config,
        test_fun = fun directory_with_unknown_owner_test_base/4,
        custom_test_setups = #{
            ?SPACE_ID1 => [
                #{
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1),
                    expected_owner2 => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1, ?UID(?UNKNOWN_USER)),
                    expected_display_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID1, ?UID(?UNKNOWN_USER))
                }
            ],
            ?SPACE_ID4 => [
                #{
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID4),
                    expected_owner2 => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID4, ?UID(?UNKNOWN_USER)),
                    expected_display_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID4, ?UID(?UNKNOWN_USER))
                }
            ],
            ?SPACE_ID7 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?GEN_OWNER(?UNKNOWN_USER, ?SPACE_ID7)
                }
            ]

        },
        generic_test_args = #{dir_perms => ?DEFAULT_DIR_PERMS}
    }).

rename_file_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    % this test case is not run for user=?ROOT_USER_ID because
    % we cannot perform mv in context of ROOT as path cannot be resolve in context of special session
    ?RUN(#test_spec{
        config = Config,
        test_fun = fun rename_file_test_base/4,
        custom_test_setups = #{
            ?SPACE_ID1 => [
                #{
                    user => ?USER1,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER1),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER1)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER2),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID1, ?USER2)
                }
            ],
            ?SPACE_ID2 => [
                #{
                    user => ?USER1,
                    expected_owner => ?OWNER(2001, 2000),
                    expected_display_owner => ?OWNER(2221, 2222)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?OWNER(2002, 2000),
                    expected_display_owner => ?OWNER(2002, 2222)
                }
            ],
            ?SPACE_ID3 => [
                #{
                    user => ?USER1,
                    expected_owner => ?OWNER(3001, 3000),
                    expected_display_owner => ?OWNER(3331, 3333)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?OWNER(3002, 3000),
                    expected_display_owner => ?OWNER(3002, 3333)
                }
            ],
            ?SPACE_ID4 => [
                #{
                    user => ?USER1,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER1),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER1)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER2),
                    expected_display_owner => ?GEN_OWNER(Worker, ?STORAGE_ID4, ?USER2)
                }
            ],
            ?SPACE_ID5 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5, 5001),
                    expected_display_owner => ?OWNER(5551, 5555)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID5, 5002),
                    expected_display_owner => ?OWNER(5002, 5555)
                }
            ],
            ?SPACE_ID6 => [
                #{
                    user => ?USER1,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6, 6001),
                    expected_display_owner => ?OWNER(6661, 6666)
                },
                #{
                    user => ?USER2,
                    expected_owner => ?MOUNT_DIR_OWNER(Worker, ?STORAGE_ID6, 6002),
                    expected_display_owner => ?OWNER(6002, 6666)
                }
            ],
            ?SPACE_ID7 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?GEN_OWNER(?USER1, ?SPACE_ID7)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?GEN_OWNER(?USER2, ?SPACE_ID7)
                }
            ],
            ?SPACE_ID8 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?OWNER(8881, 8888)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?OWNER(?UID(?USER2), 8888)
                }
            ],
            ?SPACE_ID9 => [
                #{
                    user => ?USER1,
                    expected_display_owner => ?OWNER(9991, 9999)
                },
                #{
                    user => ?USER2,
                    expected_display_owner => ?OWNER(?UID(?USER2), 9999)
                }
            ]
        },
        generic_test_args = #{file_perms => ?DEFAULT_FILE_PERMS, dir_perms => ?DEFAULT_DIR_PERMS}
    }).

creating_file_should_result_in_eacces_when_mapping_is_not_found(Config) ->
    ?RUN(#test_spec{
        config = Config,
        test_fun = fun mapping_not_found_test_base/4,
        custom_test_setups = #{
            ?SPACE_ID2 => [#{user => ?USER3}, #{user => ?USER4}],
            ?SPACE_ID3 => [#{user => ?USER3}],
            ?SPACE_ID5 => [#{user => ?USER3}, #{user => ?USER4}],
            ?SPACE_ID6 => [#{user => ?USER3}],
            ?SPACE_ID8 => [#{user => ?USER3}, #{user => ?USER4}],
            ?SPACE_ID9 => [#{user => ?USER3}],
            ?SPACE_ID10 => [#{user => ?USER1}]
        }
    }).

%%%===================================================================
%%% Test bases
%%%===================================================================

space_directory_mode_and_owner_test_base(TestName, Config, SpaceId, TestArgs) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    User = maps:get(user, TestArgs),
    SessId = ?SESS_ID(Worker, Config, User),
    FileName = ?FILE_NAME(TestName),
    ExpectedDisplayOwner = maps:get(expected_display_owner, TestArgs),
    ExpectedDisplayUid = maps:get(uid, ExpectedDisplayOwner),
    ExpectedDisplayGid = maps:get(gid, ExpectedDisplayOwner),
    SpaceGuid = ?SPACE_GUID(SpaceId),

    % when
    {ok, _} = lfm_proxy:create_and_open(Worker, SessId, SpaceGuid, FileName, ?DEFAULT_FILE_PERMS),

    % then
    ?assertMatch({ok, #file_attr{
        uid = ExpectedDisplayUid,
        gid = ExpectedDisplayGid,
        mode = ?DEFAULT_DIR_PERMS
    }}, lfm_proxy:stat(Worker, SessId, {guid, SpaceGuid})),

    ?EXEC_IF_SUPPORTED_BY_POSIX(Worker, SpaceId, fun() ->
        SpacePath = storage_test_utils:space_path(Worker, SpaceId),
        ExpectedOwner = maps:get(expected_owner, TestArgs),
        ?assertFileInfo(maps:merge(#{mode => ?DEFAULT_DIR_MODE}, ExpectedOwner), Worker, SpacePath)
    end).

regular_file_mode_and_owner_test_base(TestName, Config, SpaceId, TestArgs) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    User = maps:get(user, TestArgs),
    SessId = ?SESS_ID(Worker, Config, User),
    FileName = ?FILE_NAME(TestName),
    ExpectedDisplayOwner = maps:get(expected_display_owner, TestArgs),
    ExpectedDisplayUid = maps:get(uid, ExpectedDisplayOwner),
    ExpectedDisplayGid = maps:get(gid, ExpectedDisplayOwner),
    FilePerms = maps:get(file_perms, TestArgs),

    % when
    {ok, {FileGuid, _}} = lfm_proxy:create_and_open(Worker, SessId, ?SPACE_GUID(SpaceId), FileName, FilePerms),

    % then
    ?assertMatch({ok, #file_attr{
        uid = ExpectedDisplayUid,
        gid = ExpectedDisplayGid,
        mode = FilePerms
    }}, lfm_proxy:stat(Worker, SessId, {guid, FileGuid})),

    ?EXEC_IF_SUPPORTED_BY_POSIX(Worker, SpaceId, fun() ->
        StorageFilePath = storage_test_utils:file_path(Worker, SpaceId, FileName),
        ExpectedOwner = maps:get(expected_owner, TestArgs),
        ?assertFileInfo(maps:merge(#{mode => ?FILE_MODE(FilePerms)}, ExpectedOwner), Worker, StorageFilePath)
    end).

regular_file_unknown_owner_test_base(TestName, Config, SpaceId, TestArgs) ->
    % this test is only run on spaces without LUMA because mapping is not defined for ?UNKNOWN_USER
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker, Config, ?USER1),
    FileName = ?FILE_NAME(TestName),
    ExpectedDisplayOwner = maps:get(expected_display_owner, TestArgs),
    ExpectedDisplayUid = maps:get(uid, ExpectedDisplayOwner),
    ExpectedDisplayGid = maps:get(gid, ExpectedDisplayOwner),
    FilePerms = maps:get(file_perms, TestArgs),

    % when
    {ok, FileGuid} = lfm_proxy:create(Worker, SessId, ?SPACE_GUID(SpaceId), FileName, FilePerms),
    {FileUuid, _} = file_id:unpack_guid(FileGuid),

    % pretend that files belongs to an unknown user (not yet logged to Onezone)
    % such file may occur when it was synced from storage in remote provider with reverse LUMA
    {ok, _} = rpc:call(Worker, file_meta, update, [FileUuid, fun(FM) ->
        {ok, FM#file_meta{owner = ?UNKNOWN_USER}}
    end]),

    {ok, _} = lfm_proxy:open(Worker, SessId, {guid, FileGuid}, read),

    % then
    ?assertMatch({ok, #file_attr{
        uid = ExpectedDisplayUid,
        gid = ExpectedDisplayGid,
        mode = FilePerms
    }}, lfm_proxy:stat(Worker, SessId, {guid, FileGuid})),

    ?EXEC_IF_SUPPORTED_BY_POSIX(Worker, SpaceId, fun() ->
        StorageFilePath = storage_test_utils:file_path(Worker, SpaceId, FileName),
        ExpectedOwner = maps:get(expected_owner, TestArgs),
        ?assertFileInfo(maps:merge(#{mode => ?FILE_MODE(FilePerms)}, ExpectedOwner), Worker, StorageFilePath),

        % pretend that ?UNKNOWN_USER logged to Onezone
        ok = rpc:call(Worker, files_to_chown, chown_deferred_files, [?UNKNOWN_USER]),
        ExpectedOwner2 = maps:get(expected_owner2, TestArgs),
        ?assertFileInfo(maps:merge(#{mode => ?FILE_MODE(FilePerms)}, ExpectedOwner2), Worker, StorageFilePath)
    end).

directory_mode_and_owner_test_base(TestName, Config, SpaceId, TestArgs) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    User = maps:get(user, TestArgs),
    SessId = ?SESS_ID(Worker, Config, User),
    DirName = ?DIR_NAME(TestName),
    FileName = ?FILE_NAME(TestName),
    ExpectedDisplayOwner = maps:get(expected_display_owner, TestArgs),
    ExpectedDisplayUid = maps:get(uid, ExpectedDisplayOwner),
    ExpectedDisplayGid = maps:get(gid, ExpectedDisplayOwner),
    DirPerms = maps:get(dir_perms, TestArgs),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, SessId, ?SPACE_GUID(SpaceId), DirName, DirPerms),

    % directory is created on storage when its child is created on storage
    {ok, _} = lfm_proxy:create_and_open(Worker, SessId, DirGuid, FileName, ?DEFAULT_FILE_PERMS),

    % then
    ?assertMatch({ok, #file_attr{
        uid = ExpectedDisplayUid,
        gid = ExpectedDisplayGid,
        mode = DirPerms
    }}, lfm_proxy:stat(Worker, SessId, {guid, DirGuid})),

    ?EXEC_IF_SUPPORTED_BY_POSIX(Worker, SpaceId, fun() ->
        StorageDirPath = storage_test_utils:file_path(Worker, SpaceId, DirName),
        ExpectedOwner = maps:get(expected_owner, TestArgs),
        ?assertFileInfo(maps:merge(#{mode => ?DIR_MODE(DirPerms)}, ExpectedOwner), Worker, StorageDirPath)
    end).

directory_with_unknown_owner_test_base(TestName, Config, SpaceId, TestArgs) ->
    % this test is only run on spaces without LUMA because mapping is not defined for ?UNKNOWN_USER
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker, Config, ?USER1),
    DirName = ?DIR_NAME(TestName),
    FileName = ?FILE_NAME(TestName),
    ExpectedDisplayOwner = maps:get(expected_display_owner, TestArgs),
    ExpectedDisplayUid = maps:get(uid, ExpectedDisplayOwner),
    ExpectedDisplayGid = maps:get(gid, ExpectedDisplayOwner),
    DirPerms = maps:get(dir_perms, TestArgs),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, SessId, ?SPACE_GUID(SpaceId), DirName, DirPerms),
    {DirUuid, _} = file_id:unpack_guid(DirGuid),

    % pretend that files belongs to an unknown user (not yet logged to Onezone)
    % such file may occur when it was synced from storage in remote provider with reverse LUMA
    {ok, _} = rpc:call(Worker, file_meta, update, [DirUuid, fun(FM) ->
        {ok, FM#file_meta{owner = ?UNKNOWN_USER}}
    end]),

    % directory is created on storage when its child is created on storage
    {ok, _} = lfm_proxy:create_and_open(Worker, SessId, DirGuid, FileName, ?DEFAULT_FILE_PERMS),

    % then
    ?assertMatch({ok, #file_attr{
        uid = ExpectedDisplayUid,
        gid = ExpectedDisplayGid,
        mode = DirPerms
    }}, lfm_proxy:stat(Worker, SessId, {guid, DirGuid})),

    ?EXEC_IF_SUPPORTED_BY_POSIX(Worker, SpaceId, fun() ->
        StorageDirPath = storage_test_utils:file_path(Worker, SpaceId, DirName),
        ExpectedOwner = maps:get(expected_owner, TestArgs),
        ?assertFileInfo(maps:merge(#{mode => ?DIR_MODE(DirPerms)}, ExpectedOwner), Worker, StorageDirPath),

        % pretend that ?UNKNOWN_USER logged to Onezone
        ok = rpc:call(Worker, files_to_chown, chown_deferred_files, [?UNKNOWN_USER]),
        ExpectedOwner2 = maps:get(expected_owner2, TestArgs),
        ?assertFileInfo(maps:merge(#{mode => ?DIR_MODE(DirPerms)}, ExpectedOwner2), Worker, StorageDirPath)
    end).

rename_file_test_base(TestName, Config, SpaceId, TestArgs) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    User = maps:get(user, TestArgs),
    SessId = ?SESS_ID(Worker, Config, User),
    DirName = ?DIR_NAME(TestName),
    FileName = ?FILE_NAME(TestName),
    TargetFilePath = filename:join([<<"/">>, ?SPACE_NAME(SpaceId, Config), DirName, FileName]),
    FilePerms = maps:get(file_perms, TestArgs),
    DirPerms = maps:get(dir_perms, TestArgs),
    ExpectedDisplayOwner = maps:get(expected_display_owner, TestArgs),
    ExpectedDisplayUid = maps:get(uid, ExpectedDisplayOwner),
    ExpectedDisplayGid = maps:get(gid, ExpectedDisplayOwner),

    % when
    {ok, DirGuid} = lfm_proxy:mkdir(Worker, SessId, ?SPACE_GUID(SpaceId), DirName, DirPerms),
    {ok, {FileGuid, Handle}} = lfm_proxy:create_and_open(Worker, SessId, DirGuid, FileName, FilePerms),
    ok = lfm_proxy:close(Worker, Handle),

    % and
    {ok, _} = lfm_proxy:mv(Worker, SessId, {guid, FileGuid}, TargetFilePath),

    % then
    ?assertMatch({ok, #file_attr{
        uid = ExpectedDisplayUid,
        gid = ExpectedDisplayGid,
        mode = FilePerms
    }}, lfm_proxy:stat(Worker, SessId, {guid, FileGuid})),

    % and
    ?assertMatch({ok, #file_attr{
        uid = ExpectedDisplayUid,
        gid = ExpectedDisplayGid,
        mode = DirPerms
    }}, lfm_proxy:stat(Worker, SessId, {guid, DirGuid})),

    % and
    ?EXEC_IF_SUPPORTED_BY_POSIX(Worker, SpaceId, fun() ->
        TargetStorageDirPath = storage_test_utils:file_path(Worker, SpaceId, DirName),
        TargetStorageFilePath = filename:join(TargetStorageDirPath, FileName),
        ExpectedOwner = maps:get(expected_owner, TestArgs),
        ?assertFileInfo(maps:merge(#{mode => ?DIR_MODE(DirPerms)}, ExpectedOwner), Worker, TargetStorageDirPath),
        ?assertFileInfo(maps:merge(#{mode => ?DEFAULT_FILE_MODE}, ExpectedOwner), Worker, TargetStorageFilePath)
    end).

mapping_not_found_test_base(TestName, Config, SpaceId, TestArgs) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    User = maps:get(user, TestArgs),
    SessId = ?SESS_ID(Worker, Config, User),
    FileName = ?FILE_NAME(TestName),

    % when
    ?assertMatch({error, ?EACCES}, lfm_proxy:create_and_open(Worker, SessId, ?SPACE_GUID(SpaceId), FileName, 8#664)).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        initializer:mock_provider_ids(NewConfig),
        NewConfig2 = multi_provider_file_ops_test_base:init_env(NewConfig),
        sort_workers(NewConfig2)
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer]}
        | Config
    ].

end_per_suite(Config) ->
    multi_provider_file_ops_test_base:teardown_env(Config).

init_per_testcase(Config) ->
    init_per_testcase(default, Config).

init_per_testcase(default, Config) ->
    Config;
init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).

end_per_testcase(Config) ->
    end_per_testcase(default, Config).

end_per_testcase(default, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:foreach(fun(W) -> lfm_proxy:close_all(W) end, Workers),
    clean_spaces(Workers),
    lists:foreach(fun(W) -> clean_posix_storage_mountpoints(W) end, Workers),
    clear_luma_db(hd(Workers));
end_per_testcase(_Case, _Config) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_supported_spaces(Worker) ->
    rpc:call(Worker, provider_logic, get_spaces, []).

clean_spaces(Workers = [W1 | _]) ->
    {ok, SpaceIds} = rpc:call(W1, provider_logic, get_spaces, []),
    lists:foreach(fun(SpaceId) ->
        clean_space(W1, SpaceId),
        check_if_space_cleaned(Workers, SpaceId)
    end, SpaceIds).

check_if_space_cleaned(Workers, SpaceId) ->
    lists:foreach(fun(W) ->
        ?assertMatch({ok, []}, lfm_proxy:get_children(W, ?ROOT_SESS_ID, {guid, ?SPACE_GUID(SpaceId)}, 0, 1))
    end, Workers).

clean_space(Worker, SpaceId) ->
    BatchSize = 1000,
    {ok, Children} = lfm_proxy:get_children(Worker, ?ROOT_SESS_ID, {guid, ?SPACE_GUID(SpaceId)}, 0, BatchSize),
    lists:foreach(fun({G, _}) ->
        ok = lfm_proxy:rm_recursive(Worker, ?ROOT_SESS_ID, {guid, G})
    end, Children),
    case length(Children) < BatchSize of
        true ->
            ?assertMatch({ok, []}, lfm_proxy:get_children(Worker, ?ROOT_SESS_ID, {guid, ?SPACE_GUID(SpaceId)}, 0, 1));
        false ->
            clean_space(Worker, SpaceId)
    end.

clean_posix_storage_mountpoints(Worker) ->
    {ok, SpaceIds} = get_supported_spaces(Worker),
    SpacesAndSupportingPosixStorageIds = lists:filtermap(fun(SpaceId) ->
        {ok, StorageId} = storage_test_utils:get_supporting_storage_id(Worker, SpaceId),
        case storage_test_utils:is_posix_compatible_storage(Worker, StorageId) of
            true -> {true, {SpaceId, StorageId}};
            false -> false
        end
    end, SpaceIds),
    clean_posix_storage_mountpoints(Worker, SpacesAndSupportingPosixStorageIds).

clear_luma_db(Worker) ->
    lists:foreach(fun(StorageId) ->
        ok = rpc:call(Worker, luma, clear_db, [StorageId])
    end, ?AUTO_FEED_LUMA_STORAGES ++ ?EXTERNAL_FEED_LUMA_STORAGES).

clean_posix_storage_mountpoints(Worker, SpacesAndSupportingPosixStorageIds) ->
    lists:foreach(fun({SpaceId, StorageId}) ->
        clean_posix_storage_mountpoint(Worker, SpaceId, StorageId)
    end, SpacesAndSupportingPosixStorageIds).

clean_posix_storage_mountpoint(Worker, SpaceId, StorageId) ->
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    ok = rpc:call(Worker, dir_location, delete, [SpaceUuid]),
    SDHandle = sd_test_utils:new_handle(Worker, SpaceId, <<"/">>, StorageId),
    sd_test_utils:recursive_rm(Worker, SDHandle, true),
    ?assertMatch({ok, []}, sd_test_utils:ls(Worker, SDHandle, 0, 1)).


sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).

run_for_each_setup(#test_spec{
    config = Config,
    test_fun = TestBaseFun,
    test_name = TestName,
    custom_test_setups = CustomTestSetups,
    generic_test_args = GenericTestArgs
}) ->

    TestConfigs = lists:flatmap(fun
        ({SpaceId, TestArgs}) when is_map(TestArgs) ->
            [{SpaceId, TestArgs, 1}];
        ({SpaceId, TestArgsList}) when is_list(TestArgsList) ->
            [{SpaceId, TestArgs, I} || {TestArgs, I} <- lists:zip(TestArgsList, lists:seq(1, length(TestArgsList)))]
    end, maps:to_list(CustomTestSetups)),

    AllSucceeded = lists:foldl(fun({SpaceId, TestArgs, TestNo}, Acc) ->
        Acc and run(TestName, TestBaseFun, TestNo, Config, SpaceId, maps:merge(TestArgs, GenericTestArgs))
    end, true, TestConfigs),

    case AllSucceeded of
        true -> ok;
        false -> ct:fail("Not all cases suceeded")
    end.

run(TestName, TestBaseFun, TestNo, Config, SpaceId, TestArgs) ->
    Config2 = init_per_testcase(Config),
    TestResult = run_test(TestName, TestBaseFun, TestNo, Config2, SpaceId, TestArgs),
    end_per_testcase(Config2),
    TestResult.

run_test(TestName, TestBaseFun, TestNo, Config, SpaceId, TestArgs) ->
    try
        ct:pal("Starting test \"~p\" for space ~p and setup no. ~p", [TestName, SpaceId, TestNo]),
        TestBaseFun(TestName, Config, SpaceId, TestArgs),
        ct:pal("Test \"~p\" for space ~p and setup no. ~p PASSED.", [TestName, SpaceId, TestNo]),
        true
    catch
        Error:Reason ->
            ct:pal("Test ~p for space ~p and setup no. ~p FAILED.~nError: ~p.~n"
            "Stacktrace:~n~p", [TestName, SpaceId, TestNo, {Error, Reason}, erlang:get_stacktrace()]),
            false
    end.
