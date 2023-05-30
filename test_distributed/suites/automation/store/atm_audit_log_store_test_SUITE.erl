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
-include("onenv_test_utils.hrl").
-include_lib("cluster_worker/include/audit_log.hrl").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


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
    update_content_test/1,
    iterator_test/1,
    browse_by_index_test/1,
    browse_by_offset_test/1,
    browse_by_timestamp_test/1,
    expiration_test/1,
    logging_level_test/1
]).

groups() -> [
    {infinite_log_based_stores_common_tests, [parallel], [
        create_test,
        update_content_test,
        iterator_test,
        browse_by_index_test,
        browse_by_offset_test
    ]},
    {audit_log_specific_tests, [sequential], [
        browse_by_timestamp_test,
        expiration_test,
        logging_level_test
    ]}
].

all() -> [
    {group, infinite_log_based_stores_common_tests},
    {group, audit_log_specific_tests}
].

-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).
-define(erpc(Expr), ?erpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% API functions
%%%===================================================================


create_test(_Config) ->
    atm_infinite_log_based_stores_test_base:create_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1
    }).


update_content_test(_Config) ->
    atm_infinite_log_based_stores_test_base:update_content_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_store_item => fun input_item_to_exp_store_item/4,
        build_content_update_options => fun build_content_update_options/1,
        get_content => fun get_content/2
    }).


iterator_test(_Config) ->
    atm_infinite_log_based_stores_test_base:iterator_test_base(#{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_store_item => fun input_item_to_exp_store_item/4,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3
    }).


browse_by_index_test(_Config) ->
    atm_infinite_log_based_stores_test_base:browse_content_test_base(index, #{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_store_item => fun input_item_to_exp_store_item/4,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3,
        build_content_browse_options => fun build_content_browse_options/1,
        build_content_browse_result => fun build_content_browse_result/2
    }).


browse_by_offset_test(_Config) ->
    atm_infinite_log_based_stores_test_base:browse_content_test_base(offset, #{
        store_configs => example_configs(),
        get_input_item_generator_seed_data_spec => fun get_input_item_generator_seed_data_spec/1,
        input_item_formatter => fun input_item_formatter/1,
        input_item_to_exp_store_item => fun input_item_to_exp_store_item/4,
        randomly_remove_entity_referenced_by_item => fun randomly_remove_entity_referenced_by_item/3,
        build_content_browse_options => fun build_content_browse_options/1,
        build_content_browse_result => fun build_content_browse_result/2
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
        AtmWorkflowExecutionAuth, ?LOGGER_DEBUG_LEVEL, undefined, AtmStoreSchema
    ))),

%%    ItemsNum = rand:uniform(1000),  @TODO VFS-10429
    ItemsNum = rand:uniform(10),
    ContentUpdateOpts = build_content_update_options(append),
    [FirstTimestamp | _] = lists:map(fun(Index) ->
        Timestamp = time_test_utils:get_frozen_time_millis(),
        LogContent = #{<<"value">> => Index},
        ItemToAdd = case rand:uniform(4) of
            1 -> LogContent;
            2 -> #{<<"content">> => LogContent, <<"severity">> => <<"info">>};
            3 -> #{<<"content">> => LogContent};
            4 -> LogContent#{<<"severity">> => <<"info">>}
        end,
        ?assertEqual(ok, ?rpc(atm_store_api:update_content(
            AtmWorkflowExecutionAuth, ItemToAdd, ContentUpdateOpts, AtmStoreId
        ))),
        time_test_utils:simulate_millis_passing(1),
        Timestamp
    end, lists:seq(1, ItemsNum)),

    lists:foreach(fun(_) ->
        StartIndex = rand:uniform(ItemsNum),
        Limit = rand:uniform(ItemsNum),
        BrowseOpts = build_content_browse_options(#{
            <<"timestamp">> => StartIndex + FirstTimestamp,
            <<"limit">> => Limit
        }),

        ExpEntries = lists:map(fun(Index) ->
            {
                integer_to_binary(Index),
                {ok, #{
                    <<"content">> => #{<<"value">> => Index + 1},
                    <<"timestamp">> => FirstTimestamp + Index,
                    <<"severity">> => <<"info">>,
                    <<"source">> => ?USER_AUDIT_LOG_ENTRY_SOURCE
                }}
            }
        end, lists:seq(StartIndex, min(StartIndex + Limit - 1, ItemsNum - 1))),

        ExpContentBrowseResult = build_content_browse_result(
            ExpEntries,
            StartIndex + Limit >= ItemsNum
        ),

        ?assertEqual(
            ExpContentBrowseResult,
            ?rpc(atm_store_api:browse_content(AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId))
        )
    end, lists:seq(1, 8)).


expiration_test(_Config) ->
    ok = time_test_utils:set_current_time_millis(123),

    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        ?PROVIDER_SELECTOR, user1, space_krk
    ),

    AtmStoreSchema = atm_store_test_utils:build_store_schema(#atm_audit_log_store_config{
        log_content_data_spec = #atm_data_spec{type = atm_object_type}
    }),
    {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
        AtmWorkflowExecutionAuth, ?LOGGER_DEBUG_LEVEL, undefined, AtmStoreSchema
    ))),

    CallBrowseContent = fun() ->
        catch ?erpc(atm_store_api:browse_content(
            AtmWorkflowExecutionAuth, build_content_browse_options(#{}), AtmStoreId
        ))
    end,

    % the underlying log should not be created until the first log is appended
    ?assertEqual(?ERROR_NOT_FOUND, CallBrowseContent()),
    ?assertEqual(ok, ?rpc(atm_store_api:update_content(
        AtmWorkflowExecutionAuth, #{<<"value">> => ?RAND_STR()}, build_content_update_options(append), AtmStoreId
    ))),
    ?assertMatch(#atm_audit_log_store_content_browse_result{}, CallBrowseContent()),
    % simulate expiration of the audit log
    {ok, #atm_store{container = {_, _, _, BackendId}}} = ?rpc(atm_store_api:get(AtmStoreId)),
    ?rpc(audit_log:delete(BackendId)),
    ?assertEqual(?ERROR_NOT_FOUND, CallBrowseContent()),
    % any append should cause log recreation
    ?assertEqual(ok, ?rpc(atm_store_api:update_content(
        AtmWorkflowExecutionAuth, #{<<"value">> => ?RAND_STR()}, build_content_update_options(append), AtmStoreId
    ))),
    ?assertMatch(#atm_audit_log_store_content_browse_result{}, CallBrowseContent()).


logging_level_test(_Config) ->
    ok = time_test_utils:set_current_time_millis(123),
    Timestamp = time_test_utils:get_frozen_time_millis(),

    AtmWorkflowExecutionAuth = atm_store_test_utils:create_workflow_execution_auth(
        ?PROVIDER_SELECTOR, user1, space_krk
    ),

    LoggingSeverity = ?RAND_ELEMENT(?LOGGER_SEVERITY_LEVELS),
    LoggingLevel = atm_audit_log_store_container:severity_to_logging_level(LoggingSeverity),

    AtmStoreSchema = atm_store_test_utils:build_store_schema(#atm_audit_log_store_config{
        log_content_data_spec = #atm_data_spec{type = atm_object_type}
    }),
    {ok, AtmStoreId} = ?extract_key(?rpc(atm_store_api:create(
        AtmWorkflowExecutionAuth, LoggingLevel, undefined, AtmStoreSchema
    ))),

    ItemsNum = rand:uniform(100),

    {ItemsToAdd, ExpEntries} = lists:mapfoldl(fun(Index, Acc) ->
        LogContent = #{<<"value">> => Index},
        ItemToAdd = case rand:uniform(4) of
            1 -> LogContent;
            2 -> #{<<"content">> => LogContent, <<"severity">> => ?RAND_ELEMENT(?LOGGER_SEVERITY_LEVELS)};
            3 -> #{<<"content">> => LogContent};
            4 -> LogContent#{<<"severity">> => ?RAND_ELEMENT(?LOGGER_SEVERITY_LEVELS)}
        end,
        LogSeverity = maps:get(<<"severity">>, ItemToAdd, ?LOGGER_INFO),
        case atm_audit_log_store_container:severity_to_logging_level(LogSeverity) =< LoggingLevel of
            true ->
                ExpAddedItem = #{
                    <<"content">> => LogContent,
                    <<"timestamp">> => Timestamp,
                    <<"severity">> => LogSeverity,
                    <<"source">> => ?USER_AUDIT_LOG_ENTRY_SOURCE
                },
                {ItemToAdd, Acc ++ [{integer_to_binary(length(Acc)), {ok, ExpAddedItem}}]};
            false ->
                {ItemToAdd, Acc}
        end
    end, [], lists:seq(1, ItemsNum)),

    ContentUpdateOpts = build_content_update_options(extend),
    ?assertEqual(ok, ?rpc(atm_store_api:update_content(
        AtmWorkflowExecutionAuth, ItemsToAdd, ContentUpdateOpts, AtmStoreId
    ))),
    ExpContentBrowseResult = build_content_browse_result(ExpEntries, true),

    BrowseOpts = build_content_browse_options(#{
        <<"timestamp">> => Timestamp,
        <<"limit">> => ItemsNum
    }),
    ?assertEqual(
        ExpContentBrowseResult,
        ?rpc(atm_store_api:browse_content(AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId))
    ).


%===================================================================
% Helper functions
%===================================================================


%% @private
-spec example_configs() -> [atm_single_value_store_config:record()].
example_configs() ->
    SupportedStaticBasicTypes = [
        atm_boolean_type,
        atm_number_type,
        atm_string_type,
        atm_object_type,
        atm_range_type,
        atm_time_series_measurement_type
    ],

    lists:flatten([
        #atm_audit_log_store_config{log_content_data_spec = #atm_data_spec{
            type = atm_array_type,
            value_constraints = #{
                item_data_spec => atm_store_test_utils:example_data_spec(?RAND_ELEMENT(
                    SupportedStaticBasicTypes
                ))
            }
        }},

        lists:map(fun(LogContentDataType) ->
            #atm_audit_log_store_config{log_content_data_spec = atm_store_test_utils:example_data_spec(
                LogContentDataType
            )}
        end, SupportedStaticBasicTypes)
    ]).


%% @private
-spec get_input_item_generator_seed_data_spec(atm_list_store_config:record()) ->
    atm_data_spec:record().
get_input_item_generator_seed_data_spec(#atm_audit_log_store_config{
    log_content_data_spec = LogContentDataSpec
}) ->
    LogContentDataSpec.


%% @private
-spec input_item_formatter(atm_value:expanded()) -> atm_value:expanded().
input_item_formatter(LogContent) ->
    Severity = lists_utils:random_element([str_utils:rand_hex(16) | ?AUDIT_LOG_SEVERITY_LEVELS]),

    case rand:uniform(5) of
        1 when is_map(LogContent) -> LogContent#{<<"severity">> => Severity};
        2 -> #{<<"content">> => LogContent};
        3 -> #{<<"content">> => LogContent, <<"severity">> => Severity};
        _ -> LogContent
    end.


%% @private
-spec input_item_to_exp_store_item(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_store:id(),
    Index :: non_neg_integer()
) ->
    atm_value:expanded().
input_item_to_exp_store_item(_AtmWorkflowExecutionAuth, InputItem, _ItemDataSpec, Index) ->
    LogContent = case InputItem of
        #{<<"content">> := LC} -> LC;
        #{<<"severity">> := _} = Object -> maps:without([<<"severity">>], Object);
        _ -> InputItem
    end,
    Severity = case is_map(InputItem) of
        true -> maps:get(<<"severity">>, InputItem, ?INFO_AUDIT_LOG_SEVERITY);
        false -> ?INFO_AUDIT_LOG_SEVERITY
    end,

    #{
        <<"content">> => LogContent,
        <<"severity">> => case lists:member(Severity, ?AUDIT_LOG_SEVERITY_LEVELS) of
            true -> Severity;
            false -> ?INFO_AUDIT_LOG_SEVERITY
        end,
        <<"timestamp">> => time_test_utils:get_frozen_time_millis(),
        <<"index">> => integer_to_binary(Index),
        <<"source">> => ?USER_AUDIT_LOG_ENTRY_SOURCE
    }.


%% @private
-spec randomly_remove_entity_referenced_by_item(
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_spec:record()
) ->
    false | {true, errors:error()}.
randomly_remove_entity_referenced_by_item(
    AtmWorkflowExecutionAuth,
    #{<<"content">> := LogContent},
    LogContentDataSpec
) ->
    atm_store_test_utils:randomly_remove_entity_referenced_by_item(
        ?PROVIDER_SELECTOR, AtmWorkflowExecutionAuth, LogContent, LogContentDataSpec
    ).


%% @private
-spec build_content_update_options(atm_list_store_content_update_options:update_function()) ->
    atm_audit_log_store_content_update_options:record().
build_content_update_options(UpdateFun) ->
    #atm_audit_log_store_content_update_options{function = UpdateFun}.


%% @private
-spec get_content(atm_workflow_execution_auth:record(), atm_store:id()) ->
    [atm_value:expanded()].
get_content(AtmWorkflowExecutionAuth, AtmStoreId) ->
    BrowseOpts = build_content_browse_options(#{<<"limit">> => 1000}),
    try
        #atm_audit_log_store_content_browse_result{result = #{
            <<"logEntries">> := Logs,
            <<"isLast">> := true
        }} = ?erpc(atm_store_api:browse_content(AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId)),
        Logs
    catch throw:?ERROR_NOT_FOUND ->
        % possible when the underlying log hasn't been created yet (no appends were done)
        []
    end.


%% @private
-spec build_content_browse_options(json_utils:json_map()) ->
    atm_audit_log_store_content_browse_options:record().
build_content_browse_options(OptsJson) ->
    atm_audit_log_store_content_browse_options:sanitize(OptsJson#{
        <<"type">> => <<"auditLogStoreContentBrowseOptions">>,
        <<"direction">> => str_utils:to_binary(?FORWARD)
    }).


%% @private
-spec build_content_browse_result([audit_log:entry()], boolean()) ->
    atm_audit_log_store_content_browse_result:record().
build_content_browse_result(Entries, IsLast) ->
    #atm_audit_log_store_content_browse_result{result = #{
        <<"logEntries">> => lists:map(fun({Index, {ok, Entry}}) ->
            Entry#{<<"index">> => Index}
        end, Entries),
        <<"isLast">> => IsLast
    }}.


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
