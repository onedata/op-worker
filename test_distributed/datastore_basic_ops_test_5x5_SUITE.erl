%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Tests datastore basic operations at all levels.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_basic_ops_test_5x5_SUITE).
-author("Michal Wrzeszcz").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("annotations/include/annotations.hrl").
-include("datastore_basic_ops_utils.hrl").
-include("cluster/worker/modules/datastore/datastore_common_internal.hrl").
-include("cluster/worker/modules/datastore/datastore_models_def.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([create_delete_db_test/1, save_db_test/1, update_db_test/1, get_db_test/1, exists_db_test/1,
    create_delete_global_store_test/1, no_transactions_create_delete_global_store_test/1,
    save_global_store_test/1, no_transactions_save_global_store_test/1, update_global_store_test/1,
    no_transactions_update_global_store_test/1, get_global_store_test/1, exists_global_store_test/1,
    create_delete_local_store_test/1, save_local_store_test/1, update_local_store_test/1,
    get_local_store_test/1, exists_local_store_test/1,
    create_delete_global_cache_test/1, save_global_cache_test/1, update_global_cache_test/1,
    create_sync_delete_global_cache_test/1, save_sync_global_cache_test/1, update_sync_global_cache_test/1,
    get_global_cache_test/1, exists_global_cache_test/1,
    create_delete_local_cache_test/1, save_local_cache_test/1, update_local_cache_test/1,
    create_sync_delete_local_cache_test/1, save_sync_local_cache_test/1, update_sync_local_cache_test/1,
    get_local_cache_test/1, exists_local_cache_test/1,
    mixed_db_test/1, mixed_global_store_test/1, mixed_local_store_test/1,
    mixed_global_cache_test/1, mixed_local_cache_test/1
]).

-performance({test_cases,
    [create_delete_db_test, save_db_test, update_db_test, get_db_test, exists_db_test,
        create_delete_global_store_test, no_transactions_create_delete_global_store_test,
        save_global_store_test, no_transactions_save_global_store_test, update_global_store_test,
        no_transactions_update_global_store_test, get_global_store_test, exists_global_store_test,
        create_delete_local_store_test, save_local_store_test, update_local_store_test,
        get_local_store_test, exists_local_store_test,
        create_delete_global_cache_test, save_global_cache_test, update_global_cache_test,
        create_sync_delete_global_cache_test, save_sync_global_cache_test, update_sync_global_cache_test,
        get_global_cache_test, exists_global_cache_test,
        create_delete_local_cache_test, save_local_cache_test, update_local_cache_test,
        create_sync_delete_local_cache_test, save_sync_local_cache_test, update_sync_local_cache_test,
        get_local_cache_test, exists_local_cache_test,
        mixed_db_test, mixed_global_store_test, mixed_local_store_test,
        mixed_global_cache_test, mixed_local_cache_test
    ]
}).
all() ->
    [create_delete_db_test, save_db_test, update_db_test, get_db_test, exists_db_test,
        create_delete_global_store_test, no_transactions_create_delete_global_store_test,
        save_global_store_test, no_transactions_save_global_store_test, update_global_store_test,
        no_transactions_update_global_store_test, get_global_store_test, exists_global_store_test,
        create_delete_local_store_test, save_local_store_test, update_local_store_test,
        get_local_store_test, exists_local_store_test,
        create_delete_global_cache_test, save_global_cache_test, update_global_cache_test,
        create_sync_delete_global_cache_test, save_sync_global_cache_test, update_sync_global_cache_test,
        get_global_cache_test, exists_global_cache_test,
        create_delete_local_cache_test, save_local_cache_test, update_local_cache_test,
        create_sync_delete_local_cache_test, save_sync_local_cache_test, update_sync_local_cache_test,
        get_local_cache_test, exists_local_cache_test
    ].

%%%===================================================================
%%% Test functions
%%%===================================================================

-performance(?create_delete_test_def).
create_delete_db_test(Config) ->
    datastore_basic_ops_utils:create_delete_test(Config, disk_only).

-performance(?save_test_def).
save_db_test(Config) ->
    datastore_basic_ops_utils:save_test(Config, disk_only).

-performance(?update_test_def).
update_db_test(Config) ->
    datastore_basic_ops_utils:update_test(Config, disk_only).

-performance(?get_test_def).
get_db_test(Config) ->
    datastore_basic_ops_utils:get_test(Config, disk_only).

-performance(?exists_test_def).
exists_db_test(Config) ->
    datastore_basic_ops_utils:exists_test(Config, disk_only).

%% ====================================================================

-performance(?create_delete_test_def).
create_delete_global_store_test(Config) ->
    datastore_basic_ops_utils:create_delete_test(Config, global_only).

-performance(?no_transactions_create_delete_test_def).
no_transactions_create_delete_global_store_test(Config) ->
    datastore_basic_ops_utils:create_delete_test(Config, global_only).

-performance(?save_test_def).
save_global_store_test(Config) ->
    datastore_basic_ops_utils:save_test(Config, global_only).

-performance(?no_transactions_save_test_def).
no_transactions_save_global_store_test(Config) ->
    datastore_basic_ops_utils:save_test(Config, global_only).

-performance(?update_test_def).
update_global_store_test(Config) ->
    datastore_basic_ops_utils:update_test(Config, global_only).

-performance(?no_transactions_update_test_def).
no_transactions_update_global_store_test(Config) ->
    datastore_basic_ops_utils:update_test(Config, global_only).

-performance(?get_test_def).
get_global_store_test(Config) ->
    datastore_basic_ops_utils:get_test(Config, global_only).

-performance(?exists_test_def).
exists_global_store_test(Config) ->
    datastore_basic_ops_utils:exists_test(Config, global_only).

%% ====================================================================

-performance(?create_delete_test_def).
create_delete_local_store_test(Config) ->
    datastore_basic_ops_utils:create_delete_test(Config, local_only).

-performance(?save_test_def).
save_local_store_test(Config) ->
    datastore_basic_ops_utils:save_test(Config, local_only).

-performance(?update_test_def).
update_local_store_test(Config) ->
    datastore_basic_ops_utils:update_test(Config, local_only).

-performance(?get_test_def).
get_local_store_test(Config) ->
    datastore_basic_ops_utils:get_test(Config, local_only).

-performance(?exists_test_def).
exists_local_store_test(Config) ->
    datastore_basic_ops_utils:exists_test(Config, local_only).

%% ====================================================================

-performance(?create_delete_test_def).
create_delete_global_cache_test(Config) ->
    datastore_basic_ops_utils:create_delete_test(Config, globally_cached).

-performance(?save_test_def).
save_global_cache_test(Config) ->
    datastore_basic_ops_utils:save_test(Config, globally_cached).

-performance(?update_test_def).
update_global_cache_test(Config) ->
    datastore_basic_ops_utils:update_test(Config, globally_cached).

-performance(?create_sync_delete_test_def).
create_sync_delete_global_cache_test(Config) ->
    datastore_basic_ops_utils:create_sync_delete_test(Config, globally_cached).

-performance(?save_sync_test_def).
save_sync_global_cache_test(Config) ->
    datastore_basic_ops_utils:save_sync_test(Config, globally_cached).

-performance(?update_sync_test_def).
update_sync_global_cache_test(Config) ->
    datastore_basic_ops_utils:update_sync_test(Config, globally_cached).

-performance(?get_test_def).
get_global_cache_test(Config) ->
    datastore_basic_ops_utils:get_test(Config, globally_cached).

-performance(?exists_test_def).
exists_global_cache_test(Config) ->
    datastore_basic_ops_utils:exists_test(Config, globally_cached).

%% ====================================================================

-performance(?create_delete_test_def).
create_delete_local_cache_test(Config) ->
    datastore_basic_ops_utils:create_delete_test(Config, locally_cached).

-performance(?save_test_def).
save_local_cache_test(Config) ->
    datastore_basic_ops_utils:save_test(Config, locally_cached).

-performance(?update_test_def).
update_local_cache_test(Config) ->
    datastore_basic_ops_utils:update_test(Config, locally_cached).

-performance(?create_sync_delete_test_def).
create_sync_delete_local_cache_test(Config) ->
    datastore_basic_ops_utils:create_sync_delete_test(Config, locally_cached).

-performance(?save_sync_test_def).
save_sync_local_cache_test(Config) ->
    datastore_basic_ops_utils:save_sync_test(Config, locally_cached).

-performance(?update_sync_test_def).
update_sync_local_cache_test(Config) ->
    datastore_basic_ops_utils:update_sync_test(Config, locally_cached).

-performance(?get_test_def).
get_local_cache_test(Config) ->
    datastore_basic_ops_utils:get_test(Config, locally_cached).

-performance(?exists_test_def).
exists_local_cache_test(Config) ->
    datastore_basic_ops_utils:exists_test(Config, locally_cached).

%% ====================================================================

-performance(?long_test_def).
mixed_db_test(Config) ->
    datastore_basic_ops_utils:mixed_test(Config, disk_only).

-performance(?long_test_def).
mixed_global_store_test(Config) ->
    datastore_basic_ops_utils:mixed_test(Config, global_only).

-performance(?long_test_def).
mixed_local_store_test(Config) ->
    datastore_basic_ops_utils:mixed_test(Config, local_only).

-performance(?long_test_def).
mixed_global_cache_test(Config) ->
    datastore_basic_ops_utils:mixed_test(Config, globally_cached).

-performance(?long_test_def).
mixed_local_cache_test(Config) ->
    datastore_basic_ops_utils:mixed_test(Config, locally_cached).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")).

end_per_suite(Config) ->
    test_node_starter:clean_environment(Config).

init_per_testcase(Case, Config) when
    Case =:= no_transactions_create_delete_global_store_test;
    Case =:= no_transactions_save_global_store_test;
    Case =:= no_transactions_update_global_store_test  ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, some_record, [passthrough]),
    test_utils:mock_expect(Workers, some_record, model_init, fun() ->
        #model_config{name = some_record,
            size = record_info(size, some_record),
            fields = record_info(fields, some_record),
            defaults = #some_record{},
            bucket = test_bucket,
            hooks = [{some_record, update}],
            store_level = globally_cached, % use of macro results in test errors
            link_store_level = globally_cached,
            transactional_global_cache = false
        }
    end),
    datastore_basic_ops_utils:set_hooks(Case, Config);

init_per_testcase(Case, Config) ->
    datastore_basic_ops_utils:set_hooks(Case, Config).

end_per_testcase(Case, Config) when
    Case =:= no_transactions_create_delete_global_store_test;
    Case =:= no_transactions_save_global_store_test;
    Case =:= no_transactions_update_global_store_test  ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, [some_record]),
    datastore_basic_ops_utils:unset_hooks(Case, Config);

end_per_testcase(Case, Config) ->
    datastore_basic_ops_utils:unset_hooks(Case, Config).