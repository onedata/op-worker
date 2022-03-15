%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation time series store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_time_series_store_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("onenv_test_utils.hrl").
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
    create_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_test
    ]}
].

all() -> [
    {group, all_tests}
].


-define(MAX_FILE_SIZE_TS_NAME, <<"max_file_size">>).

-define(MAX_FILE_SIZE_TS_SCHEMA, #atm_time_series_schema{
    name_generator_type = exact,
    name_generator = ?MAX_FILE_SIZE_TS_NAME,
    unit = bytes,
    metrics = [
        #metric_config{
            label = ?MAX_FILE_SIZE_TS_NAME,
            resolution = ?MONTH_RESOLUTION,
            retention = 1,
            aggregator = max
        }
    ]
}).

-define(MINUTE_METRIC_NAME, <<"minute">>).
-define(HOUR_METRIC_NAME, <<"hour">>).
-define(DAY_METRIC_NAME, <<"day">>).

-define(COUNT_TS_SCHEMA, #atm_time_series_schema{
    name_generator_type = add_prefix,
    name_generator = <<"count_">>,
    unit = counts_per_sec,
    metrics = [
        #metric_config{
            label = ?MINUTE_METRIC_NAME,
            resolution = ?MINUTE_RESOLUTION,
            retention = 120,
            aggregator = sum
        },
        #metric_config{
            label = ?HOUR_METRIC_NAME,
            resolution = ?HOUR_RESOLUTION,
            retention = 48,
            aggregator = sum
        },
        #metric_config{
            label = ?DAY_METRIC_NAME,
            resolution = ?DAY_RESOLUTION,
            retention = 60,
            aggregator = sum
        }
    ]
}).

-define(ATM_STORE_CONFIG, #atm_time_series_store_config{schemas = [
    ?MAX_FILE_SIZE_TS_SCHEMA,
    ?COUNT_TS_SCHEMA
]}).


-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).
-define(erpc(Expr), ?erpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% Test functions
%%%===================================================================


create_test(_Config) ->
    AtmWorkflowExecutionAuth = create_workflow_execution_auth(),
    AtmStoreSchema = build_store_schema(?ATM_STORE_CONFIG),

    ?assertThrow(
        ?ERROR_BAD_DATA(<<"initialContent">>, <<"Time series store does not accept initial content">>),
        ?erpc(atm_store_api:create(AtmWorkflowExecutionAuth, [], AtmStoreSchema))
    ),

    {ok, #document{key = AtmStoreId}} = ?assertMatch(
        {ok, #document{value = #atm_store{initial_content = undefined, frozen = false}}},
        ?rpc(atm_store_api:create(AtmWorkflowExecutionAuth, undefined, AtmStoreSchema))
    ),

    % Assert only ts for exact generators are initiated
    ExpLayout = #{?MAX_FILE_SIZE_TS_NAME => [?MAX_FILE_SIZE_TS_NAME]},
    ?assertEqual(ExpLayout, get_layout(AtmWorkflowExecutionAuth, AtmStoreId)),
    ?assertEqual(
        #{?MAX_FILE_SIZE_TS_NAME => #{?MAX_FILE_SIZE_TS_NAME => []}},
        get_windows(AtmWorkflowExecutionAuth, AtmStoreId, ExpLayout)
    ).


%===================================================================
% Helper functions
%===================================================================


%% @private
-spec create_workflow_execution_auth() -> atm_workflow_execution_auth:record().
create_workflow_execution_auth() ->
    atm_store_test_utils:create_workflow_execution_auth(
        ?PROVIDER_SELECTOR, user1, space_krk
    ).


%% @private
-spec build_store_schema(atm_store_config:record()) -> atm_store_schema:record().
build_store_schema(AtmStoreConfig) ->
    % time_series store does not allow initial content and as such 'requires_initial_content'
    % is always set as 'false' (it is enforced by oz)
    atm_store_test_utils:build_store_schema(AtmStoreConfig, false).


%% @private
-spec get_layout(atm_workflow_execution_auth:record(), atm_store:id()) ->
    time_series_collection:metrics_by_time_series().
get_layout(AtmWorkflowExecutionAuth, AtmStoreId) ->
    BrowseOpts = #atm_time_series_store_content_browse_options{
        request = #get_atm_time_series_store_content_layout{}
    },

    #atm_time_series_store_content_browse_result{
        result = #atm_time_series_store_content_layout{
            layout = Layout
        }
    } = ?rpc(atm_store_api:browse_content(
        AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId
    )),

    Layout.


%% @private
-spec get_windows(
    atm_workflow_execution_auth:record(),
    atm_store:id(),
    time_series_collection:metrics_by_time_series()
) ->
    #{time_series_collection:time_series_id() => #{ts_metric:id() => [ts_windows:value()]}}.
get_windows(AtmWorkflowExecutionAuth, AtmStoreId, Layout) ->
    BrowseOpts = #atm_time_series_store_content_browse_options{
        request = #get_atm_time_series_store_content_slice{
            layout = Layout,
            start_timestamp = undefined,
            windows_limit = 10000000000
        }
    },

    #atm_time_series_store_content_browse_result{
        result = #atm_time_series_store_content_slice{
            slice = Slice
        }
    } = ?rpc(atm_store_api:browse_content(
        AtmWorkflowExecutionAuth, BrowseOpts, AtmStoreId
    )),

    Slice.


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    ModulesToLoad = [?MODULE, atm_store_test_utils],
    oct_background:init_per_suite([{?LOAD_MODULES, ModulesToLoad} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_, Config) ->
    Config.


end_per_group(_, _Config) ->
    ok.


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
