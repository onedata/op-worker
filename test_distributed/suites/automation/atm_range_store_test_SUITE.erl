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
    create_store_with_invalid_args_test/1,

    stream_one_by_one_with_end_100_test/1,
    stream_one_by_one_with_start_25_end_100_test/1,
    stream_one_by_one_with_start_25_end_100_step_4_test/1,
    stream_one_by_one_with_start_50_end_minus_50_step_minus_2_test/1,
    stream_one_by_one_with_start_10_end_10_step_1_test/1,

    stream_in_chunks_5_with_start_10_end_50_step_2_test/1,
    stream_in_chunks_10_with_start_1_end_2_step_10_test/1,
    stream_in_chunks_10_with_start_minus_50_end_50_step_4_test/1,
    stream_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test/1,
    stream_in_chunks_3_with_start_10_end_10_step_2_test/1,

    stream_cursor_test_base/1
]).

groups() -> [
    {all_tests, [parallel], [
        create_store_with_invalid_args_test,

        stream_one_by_one_with_end_100_test,
        stream_one_by_one_with_start_25_end_100_test,
        stream_one_by_one_with_start_25_end_100_step_4_test,
        stream_one_by_one_with_start_50_end_minus_50_step_minus_2_test,
        stream_one_by_one_with_start_10_end_10_step_1_test,

        stream_in_chunks_5_with_start_10_end_50_step_2_test,
        stream_in_chunks_10_with_start_1_end_2_step_10_test,
        stream_in_chunks_10_with_start_minus_50_end_50_step_4_test,
        stream_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test,
        stream_in_chunks_3_with_start_10_end_10_step_2_test,

        stream_cursor_test_base
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
    data_spec = #atm_data_spec2{type = atm_integer_type}
}).

-type item() :: integer().

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API functions
%%%===================================================================


create_store_with_invalid_args_test(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),

    lists:foreach(fun(InvalidInitArgs) ->
        ?assertEqual(?EINVAL, create_store(Node, ?ATM_RANGE_STORE_SCHEMA, InvalidInitArgs))
    end, [
        undefined,
        #{<<"end">> => <<"NaN">>},
        #{<<"start">> => <<"NaN">>, <<"end">> => 10},
        #{<<"start">> => 5, <<"end">> => 10, <<"step">> => <<"NaN">>},
        #{<<"start">> => 5, <<"end">> => 10, <<"step">> => 0},
        #{<<"start">> => 15, <<"end">> => 10, <<"step">> => 1},
        #{<<"start">> => -15, <<"end">> => -10, <<"step">> => -1},
        #{<<"start">> => 10, <<"end">> => 15, <<"step">> => -1}
    ]).


stream_one_by_one_with_end_100_test(_Config) ->
    stream_one_by_one_test_base(#{<<"end">> => 100}).


stream_one_by_one_with_start_25_end_100_test(_Config) ->
    stream_one_by_one_test_base(#{<<"start">> => 25, <<"end">> => 100}).


stream_one_by_one_with_start_25_end_100_step_4_test(_Config) ->
    stream_one_by_one_test_base(#{<<"start">> => 25, <<"end">> => 100, <<"step">> => 4}).


stream_one_by_one_with_start_50_end_minus_50_step_minus_2_test(_Config) ->
    stream_one_by_one_test_base(#{<<"start">> => 50, <<"end">> => -50, <<"step">> => -2}).


stream_one_by_one_with_start_10_end_10_step_1_test(_Config) ->
    stream_one_by_one_test_base(#{<<"start">> => 10, <<"end">> => 10, <<"step">> => 1}).


%% @private
-spec stream_one_by_one_test_base(atm_store_api:init_args()) -> ok | no_return().
stream_one_by_one_test_base(#{<<"end">> := End} = InitArgs) ->
    Start = maps:get(<<"start">>, InitArgs, 0),
    Step = maps:get(<<"step">>, InitArgs, 1),

    AtmStreamSchema = #atm_stream_schema{mode = #serial_mode{}},
    stream_test_base(InitArgs, AtmStreamSchema, lists:seq(Start, End, Step)).


stream_in_chunks_5_with_start_10_end_50_step_2_test(_Config) ->
    stream_in_chunks_test_base(5, #{<<"start">> => 10, <<"end">> => 50, <<"step">> => 2}).


stream_in_chunks_10_with_start_1_end_2_step_10_test(_Config) ->
    stream_in_chunks_test_base(10, #{<<"start">> => 1, <<"end">> => 2, <<"step">> => 10}).


stream_in_chunks_10_with_start_minus_50_end_50_step_4_test(_Config) ->
    stream_in_chunks_test_base(10, #{<<"start">> => -50, <<"end">> => 50, <<"step">> => 4}).


stream_in_chunks_7_with_start_50_end_minus_50_step_minus_3_test(_Config) ->
    stream_in_chunks_test_base(7, #{<<"start">> => 50, <<"end">> => -50, <<"step">> => -3}).


stream_in_chunks_3_with_start_10_end_10_step_2_test(_Config) ->
    stream_in_chunks_test_base(3, #{<<"start">> => 10, <<"end">> => 10, <<"step">> => 2}).


%% @private
-spec stream_in_chunks_test_base(pos_integer(), atm_store_api:init_args()) ->
    ok | no_return().
stream_in_chunks_test_base(ChunkSize, #{<<"end">> := End} = InitArgs) ->
    Start = maps:get(<<"start">>, InitArgs, 0),
    Step = maps:get(<<"step">>, InitArgs, 1),

    stream_test_base(
        InitArgs,
        #atm_stream_schema{mode = #bulk_mode{size = ChunkSize}},
        split_into_chunks(ChunkSize, [], lists:seq(Start, End, Step))
    ).


%% @private
-spec stream_test_base(atm_store_api:init_args(), atm_stream_schema(), [item()] | [[item()]]) ->
    ok | no_return().
stream_test_base(AtmRangeStoreInitArgs, AtmStreamSchema, ExpItems) ->
    Node = oct_background:get_random_provider_node(krakow),

    {ok, AtmRangeStoreId} = create_store(Node, ?ATM_RANGE_STORE_SCHEMA, AtmRangeStoreInitArgs),
    AtmStream = create_store_stream(Node, AtmStreamSchema, AtmRangeStoreId),

    assert_all_items_listed(Node, AtmStream, ExpItems).


%% @private
-spec assert_all_items_listed(node(), atm_stream:stream(), [item()] | [[item()]]) ->
    ok | no_return().
assert_all_items_listed(Node, AtmStream, []) ->
    ?assertEqual(stop, iterator_get_next(Node, AtmStream)),
    ok;
assert_all_items_listed(Node, AtmStream0, [ExpItem | RestItems]) ->
    {ok, _, _, AtmStream1} = ?assertMatch(
        {ok, ExpItem, _, _}, iterator_get_next(Node, AtmStream0)
    ),
    assert_all_items_listed(Node, AtmStream1, RestItems).


stream_cursor_test_base(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),

    InitArgs = #{<<"start">> => 2, <<"end">> => 16, <<"step">> => 3},
    {ok, AtmRangeStoreId} = create_store(Node, ?ATM_RANGE_STORE_SCHEMA, InitArgs),

    AtmSerialStreamSchema = #atm_stream_schema{mode = #serial_mode{}},
    AtmSerialStream0 = create_store_stream(Node, AtmSerialStreamSchema, AtmRangeStoreId),

    {ok, _, Cursor1, AtmSerialStream1} = ?assertMatch({ok, 2, _, _}, iterator_get_next(Node, AtmSerialStream0)),
    {ok, _, _Cursor2, AtmSerialStream2} = ?assertMatch({ok, 5, _, _}, iterator_get_next(Node, AtmSerialStream1)),
    {ok, _, Cursor3, AtmSerialStream3} = ?assertMatch({ok, 8, _, _}, iterator_get_next(Node, AtmSerialStream2)),
    {ok, _, _Cursor4, AtmSerialStream4} = ?assertMatch({ok, 11, _, _}, iterator_get_next(Node, AtmSerialStream3)),
    {ok, _, _Cursor5, AtmSerialStream5} = ?assertMatch({ok, 14, _, _}, iterator_get_next(Node, AtmSerialStream4)),
    ?assertMatch(stop, iterator_get_next(Node, AtmSerialStream5)),

    % Assert cursor shifts iterator to the beginning
    AtmSerialStream6 = iterator_jump_to(Node, Cursor3, AtmSerialStream5),
    {ok, _, _Cursor7, AtmSerialStream7} = ?assertMatch({ok, 11, _, _}, iterator_get_next(Node, AtmSerialStream6)),
    ?assertMatch({ok, 14, _, _}, iterator_get_next(Node, AtmSerialStream7)),

    AtmSerialStream8 = iterator_jump_to(Node, Cursor1, AtmSerialStream7),
    {ok, _, _Cursor9, AtmSerialStream9} = ?assertMatch({ok, 5, _, _}, iterator_get_next(Node, AtmSerialStream8)),
    ?assertMatch({ok, 8, _, _}, iterator_get_next(Node, AtmSerialStream9)),

    % Assert <<>> cursor shifts iterator to the beginning
    AtmSerialStream10 = iterator_jump_to(Node, <<>>, AtmSerialStream9),
    ?assertMatch({ok, 2, _, _}, iterator_get_next(Node, AtmSerialStream10)),

    % Invalid cursors should be rejected
    ?assertMatch(?EINVAL, iterator_jump_to(Node, <<"dummy">>, AtmSerialStream9)),
    ?assertMatch(?EINVAL, iterator_jump_to(Node, <<"-2">>, AtmSerialStream9)),
    ?assertMatch(?EINVAL, iterator_jump_to(Node, <<"3">>, AtmSerialStream9)),
    ?assertMatch(?EINVAL, iterator_jump_to(Node, <<"20">>, AtmSerialStream9)).


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
-spec create_store(node(), atm_stream_schema(), atm_store_api:init_args()) ->
    {ok, atm_store:id()} | {error, term()}.
create_store(Node, AtmStoreSchema, InitArgs) ->
    rpc:call(Node, atm_store_api, create, [AtmStoreSchema, InitArgs]).


%% @private
-spec create_store_stream(node(), atm_stream_schema(), atm_store:id()) ->
    atm_stream:stream().
create_store_stream(Node, AtmStreamSchema, AtmStoreId) ->
    rpc:call(Node, atm_store_api, init_stream, [AtmStreamSchema, AtmStoreId]).


%% @private
-spec iterator_get_next(node(), iterator:iterator()) ->
    {ok, iterator:item(), iterator:cursor(), iterator:iterato()} | stop.
iterator_get_next(Node, Iterator) ->
    rpc:call(Node, iterator, get_next, [Iterator]).


%% @private
-spec iterator_jump_to(node(), iterator:cursor(), iterator:iterator()) ->
    iterator:iterato().
iterator_jump_to(Node, Cursor, Iterator) ->
    rpc:call(Node, iterator, jump_to, [Cursor, Iterator]).


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
