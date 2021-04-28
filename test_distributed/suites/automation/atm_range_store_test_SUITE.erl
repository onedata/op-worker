%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation range store.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_store_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_tmp.hrl").

-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
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
    iterate_one_by_one_with_end_100/1,
    iterate_one_by_one_with_start_25_end_100/1,
    iterate_one_by_one_with_start_25_end_100_step_4/1,
    iterate_one_by_one_with_start_50_end_minus_50_step_minus_2/1,
    iterate_in_chunks_10_with_start_minus_50_end_50_step_4/1
]).

groups() -> [
    {all_tests, [parallel], [
        iterate_one_by_one_with_end_100,
        iterate_one_by_one_with_start_25_end_100,
        iterate_one_by_one_with_start_25_end_100_step_4,
        iterate_one_by_one_with_start_50_end_minus_50_step_minus_2,
        iterate_in_chunks_10_with_start_minus_50_end_50_step_4
    ]}
].

all() -> [
    {group, all_tests}
].


-define(ATM_RANGE_STORE_SCHEMA, #atm_store_schema{
    name = <<"range_store">>,
    summary = <<"summary">>,
    description = <<"description">>,
    is_input_store = true,
    store_type = range,
    data_spec = #atm_data_spec{type = atm_integer_type}
}).

-type item() :: integer().

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API functions
%%%===================================================================


iterate_one_by_one_with_end_100(_Config) ->
    iterate_test_base(
        #{<<"end">> => 100},
        #atm_store_stream_schema{mode = #serial_mode{}},
        lists:seq(0, 100)
    ).


iterate_one_by_one_with_start_25_end_100(_Config) ->
    iterate_test_base(
        #{<<"start">> => 25, <<"end">> => 100},
        #atm_store_stream_schema{mode = #serial_mode{}},
        lists:seq(25, 100)
    ).


iterate_one_by_one_with_start_25_end_100_step_4(_Config) ->
    iterate_test_base(
        #{<<"start">> => 25, <<"end">> => 100, <<"step">> => 4},
        #atm_store_stream_schema{mode = #serial_mode{}},
        lists:seq(25, 100, 4)
    ).


iterate_one_by_one_with_start_50_end_minus_50_step_minus_2(_Config) ->
    iterate_test_base(
        #{<<"start">> => 50, <<"end">> => -50, <<"step">> => -2},
        #atm_store_stream_schema{mode = #serial_mode{}},
        lists:seq(50, -50, -2)
    ).


iterate_in_chunks_10_with_start_minus_50_end_50_step_4(_Config) ->
    iterate_test_base(
        #{<<"start">> => -50, <<"end">> => 50, <<"step">> => 4},
        #atm_store_stream_schema{mode = #bulk_mode{size = 10}},
        split_into_chunks(10, [], lists:seq(-50, 50, 4))
    ).


%% @private
-spec iterate_test_base(
    atm_store_api:init_args(),
    atm_store_stream_schema(),
    [item()] | [[item()]]
) ->
    ok | no_return().
iterate_test_base(AtmRangeStoreInitArgs, AtmStoreStreamSchema, ExpItems) ->
    Node = oct_background:get_random_provider_node(krakow),

    {ok, AtmRangeStoreId} = create_store(Node, ?ATM_RANGE_STORE_SCHEMA, AtmRangeStoreInitArgs),
    AtmStoreStream = create_store_stream(Node, AtmStoreStreamSchema, AtmRangeStoreId),

    assert_all_items_listed(AtmStoreStream, ExpItems).


%% @private
-spec assert_all_items_listed(atm_store_stream:stream(), [item()] | [[item()]]) ->
    ok | no_return().
assert_all_items_listed(AtmStoreStream, []) ->
    ?assertEqual(stop, iterator:get_next(AtmStoreStream)),
    ok;
assert_all_items_listed(AtmStoreStream0, [ExpItem | RestItems]) ->
    {ok, _, _, AtmStoreStream1} = ?assertMatch(
        {ok, ExpItem, _, _}, iterator:get_next(AtmStoreStream0)
    ),
    assert_all_items_listed(AtmStoreStream1, RestItems).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec split_into_chunks(pos_integer(), [[item()]], [item()]) ->
    [[item()]].
split_into_chunks(_Size, Acc, []) ->
    lists:reverse(Acc);
split_into_chunks(Size, Acc, [_ | _] = Items) ->
    Chunk = lists:sublist(Items, 1, Size),
    split_into_chunks(Size, [Chunk | Acc], Items -- Chunk).


%% @private
-spec create_store(node(), atm_store_schema(), atm_store_api:init_args()) ->
    {ok, atm_store:id()} | {error, term()}.
create_store(Node, AtmStoreSchema, InitArgs) ->
    rpc:call(Node, atm_store_api, create, [AtmStoreSchema, InitArgs]).


%% @private
-spec create_store_stream(node(), atm_store_stream_schema(), atm_store:id()) ->
    atm_store_stream:stream().
create_store_stream(Node, AtmStoreStreamSchema, AtmStoreId) ->
    rpc:call(Node, atm_store_api, init_stream, [AtmStoreStreamSchema, AtmStoreId]).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
