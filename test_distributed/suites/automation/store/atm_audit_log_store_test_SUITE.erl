%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation audit_log store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_audit_log_store_test_SUITE).
-author("Lukasz Opiola").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").

-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("onenv_ct/include/test_rpc.hrl").


%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_test/1,
    apply_operation_test/1,
    iterator_test/1,
    browse_by_index_test/1,
    browse_by_offset_test/1,
    browse_by_timestamp_test/1
]).

groups() -> [
    {infinite_log_based_stores_common_tests, [parallel], [
        create_test,
        apply_operation_test,
        iterator_test,
        browse_by_index_test,
        browse_by_offset_test
    ]},
    {audit_log_specific_tests, [sequential], [
        browse_by_timestamp_test
    ]}
].

all() -> [
    {group, infinite_log_based_stores_common_tests},
    {group, audit_log_specific_tests}
].

-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?opw_test_rpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% API functions
%%%===================================================================


create_test(_Config) ->
    atm_infinite_log_based_stores_test_base:create_test_base(#{
        store_configs => example_configs(),
        get_item_initializer_data_spec_fun => fun get_log_content_data_spec/1,
        prepare_item_initializer_fun => fun prepare_item_initializer/1
    }).


apply_operation_test(_Config) ->
    atm_infinite_log_based_stores_test_base:apply_operation_test_base(#{
        store_configs => example_configs(),
        get_item_initializer_data_spec_fun => fun get_log_content_data_spec/1,
        prepare_item_initializer_fun => fun prepare_item_initializer/1,
        prepare_item_fun => fun prepare_item/3
    }).


iterator_test(_Config) ->
    atm_infinite_log_based_stores_test_base:iterator_test_base(#{
        store_configs => example_configs(),
        get_item_initializer_data_spec_fun => fun get_log_content_data_spec/1,
        prepare_item_initializer_fun => fun prepare_item_initializer/1,
        prepare_item_fun => fun prepare_item/3,
        randomly_remove_item_fun => fun randomly_remove_item/3
    }).


browse_by_index_test(_Config) ->
    atm_infinite_log_based_stores_test_base:browse_content_test_base(index, #{
        store_configs => example_configs(),
        get_item_initializer_data_spec_fun => fun get_log_content_data_spec/1,
        prepare_item_initializer_fun => fun prepare_item_initializer/1,
        prepare_item_fun => fun prepare_item/3,
        randomly_remove_item_fun => fun randomly_remove_item/3
    }).


browse_by_offset_test(_Config) ->
    atm_infinite_log_based_stores_test_base:browse_content_test_base(offset, #{
        store_configs => example_configs(),
        get_item_initializer_data_spec_fun => fun get_log_content_data_spec/1,
        prepare_item_initializer_fun => fun prepare_item_initializer/1,
        prepare_item_fun => fun prepare_item/3,
        randomly_remove_item_fun => fun randomly_remove_item/3
    }).


browse_by_timestamp_test(_Config) ->
    ok = time_test_utils:set_current_time_millis(123),

    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        ?PROVIDER_SELECTOR, user1, space_krk
    ),

    AtmStoreSchema = atm_store_test_utils:build_store_schema(#atm_audit_log_store_config{
        log_content_data_spec = #atm_data_spec{type = atm_object_type}
    }),
    {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
        AtmWorkflowExecutionAuth, undefined, AtmStoreSchema
    ))),

    ItemsNum = rand:uniform(1000),
    [FirstTimestamp | _] = lists:map(fun(Index) ->
        Timestamp = time_test_utils:get_frozen_time_millis(),
        LogContent = #{<<"value">> => Index},
        ItemToAdd = case rand:uniform(4) of
            1 -> LogContent;
            2 -> #{<<"content">> => LogContent, <<"severity">> => <<"info">>};
            3 -> #{<<"content">> => LogContent};
            4 -> LogContent#{<<"severity">> => <<"info">>}
        end,
        ?assertEqual(ok, ?rpc(atm_store_api:apply_operation(
            AtmWorkflowExecutionAuth, append, ItemToAdd, #{}, AtmStoreId
        ))),
        time_test_utils:simulate_millis_passing(1),
        Timestamp
    end, lists:seq(1, ItemsNum)),

    lists:foreach(fun(_) ->
        StartIndex = rand:uniform(ItemsNum),
        Limit = rand:uniform(ItemsNum),
        BrowseOpts = #{
            start_timestamp => StartIndex + FirstTimestamp,
            limit => Limit
        },
        Expected = lists:map(fun(Index) ->
            {
                integer_to_binary(Index),
                {ok, #{
                    <<"content">> => #{<<"value">> => Index + 1},
                    <<"timestamp">> => FirstTimestamp + Index,
                    <<"severity">> => <<"info">>}
                }
            }
        end, lists:seq(StartIndex, min(StartIndex + Limit - 1, ItemsNum - 1))),

        ?assertEqual(
            {Expected, StartIndex + Limit >= ItemsNum},
            ?rpc(atm_store_api:browse_content(AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId))
        )
    end, lists:seq(1, 8)).


%===================================================================
% Helper functions
%===================================================================


%% @private
-spec example_configs() -> [atm_single_value_store_config:record()].
example_configs() ->
    lists:map(fun(ItemDataSpec) ->
        #atm_audit_log_store_config{log_content_data_spec = ItemDataSpec}
    end, [
        %% TODO VFS-8686 enable after implementing compress/expand for array
%%        #atm_data_spec{type = atm_array_type},
        #atm_data_spec{type = atm_dataset_type},
        #atm_data_spec{type = atm_file_type},
        #atm_data_spec{type = atm_integer_type},
        #atm_data_spec{type = atm_object_type},
        #atm_data_spec{type = atm_string_type}
        #atm_data_spec{type = atm_time_series_measurements_type}
    ]).


%% @private
-spec get_log_content_data_spec(atm_list_store_config:record()) ->
    atm_data_spec:record().
get_log_content_data_spec(#atm_audit_log_store_config{log_content_data_spec = ItemDataSpec}) ->
    ItemDataSpec.


%% @private
-spec prepare_item_initializer(atm_value:expanded()) -> atm_value:expanded().
prepare_item_initializer(LogContent) ->
    Severity = lists_utils:random_element([str_utils:rand_hex(16) | ?LOGGER_SEVERITIES]),

    case rand:uniform(5) of
        1 when is_map(LogContent) -> LogContent#{<<"severity">> => Severity};
        2 -> #{<<"content">> => LogContent};
        3 -> #{<<"content">> => LogContent, <<"severity">> => Severity};
        _ -> LogContent
    end.


%% @private
-spec prepare_item(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_store:id()
) ->
    atm_value:expanded().
prepare_item(AtmWorkflowExecutionAuth, ItemInitializer, ItemDataSpec) ->
    LogContent = case ItemInitializer of
        #{<<"content">> := LC} -> LC;
        #{<<"severity">> := _} = Object -> maps:without([<<"severity">>], Object);
        _ -> ItemInitializer
    end,
    Severity = case is_map(ItemInitializer) of
        true -> maps:get(<<"severity">>, ItemInitializer, ?LOGGER_INFO);
        false -> ?LOGGER_INFO
    end,

    #{
        <<"content">> => atm_store_test_utils:ensure_fully_expanded_data(
            ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, LogContent, ItemDataSpec
        ),
        <<"severity">> => case lists:member(Severity, ?LOGGER_SEVERITIES) of
            true -> Severity;
            false -> ?LOGGER_INFO
        end,
        <<"timestamp">> => time_test_utils:get_frozen_time_millis()
    }.


%% @private
-spec randomly_remove_item(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_spec:record()
) ->
    false | {true, errors:error()}.
randomly_remove_item(AtmWorkflowExecutionAuth, #{<<"content">> := LogContent}, LogContentDataSpec) ->
    atm_store_test_utils:randomly_remove_item(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, LogContent, LogContentDataSpec
    ).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE | atm_infinite_log_based_stores_test_base:modules_to_load()],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(infinite_log_based_stores_common_tests, Config) ->
    atm_infinite_log_based_stores_test_base:init_per_group(Config);
init_per_group(audit_log_specific_tests, Config) ->
    time_test_utils:freeze_time(Config),
    Config.


end_per_group(infinite_log_based_stores_common_tests, Config) ->
    atm_infinite_log_based_stores_test_base:end_per_group(Config);
end_per_group(audit_log_specific_tests, Config) ->
    time_test_utils:unfreeze_time(Config).


init_per_testcase(browse_by_timestamp_test, Config) ->
    ct:timetrap({minutes, 5}),
    Config;
init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
