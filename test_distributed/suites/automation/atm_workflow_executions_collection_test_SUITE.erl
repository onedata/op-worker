%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of automation workflow executions collections: waiting, ongoing and ended.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_executions_collection_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    add_to_waiting_test/1,
    add_to_ongoing_test/1,
    add_to_ended_test/1,

    delete_from_waiting_test/1,
    delete_from_ongoing_test/1,
    delete_from_ended_test/1,

    list_with_invalid_listing_opts_test/1,
    list_with_negative_offset_test/1,

    iterate_over_100_waiting_atm_workflow_executions_using_offset_and_limit_1_test/1,
    iterate_over_100_waiting_atm_workflow_executions_using_offset_and_limit_10_test/1,
    iterate_over_100_waiting_atm_workflow_executions_using_offset_and_limit_100_test/1,
    iterate_over_100_waiting_atm_workflow_executions_using_start_index_and_limit_1_test/1,
    iterate_over_100_waiting_atm_workflow_executions_using_start_index_and_limit_10_test/1,
    iterate_over_100_waiting_atm_workflow_executions_using_start_index_and_limit_100_test/1,

    iterate_over_100_ongoing_atm_workflow_executions_using_offset_and_limit_1_test/1,
    iterate_over_100_ongoing_atm_workflow_executions_using_offset_and_limit_10_test/1,
    iterate_over_100_ongoing_atm_workflow_executions_using_offset_and_limit_100_test/1,
    iterate_over_100_ongoing_atm_workflow_executions_using_start_index_and_limit_1_test/1,
    iterate_over_100_ongoing_atm_workflow_executions_using_start_index_and_limit_10_test/1,
    iterate_over_100_ongoing_atm_workflow_executions_using_start_index_and_limit_100_test/1,

    iterate_over_100_ended_atm_workflow_executions_using_offset_and_limit_1_test/1,
    iterate_over_100_ended_atm_workflow_executions_using_offset_and_limit_10_test/1,
    iterate_over_100_ended_atm_workflow_executions_using_offset_and_limit_100_test/1,
    iterate_over_100_ended_atm_workflow_executions_using_start_index_and_limit_1_test/1,
    iterate_over_100_ended_atm_workflow_executions_using_start_index_and_limit_10_test/1,
    iterate_over_100_ended_atm_workflow_executions_using_start_index_and_limit_100_test/1
]).

groups() -> [
    {all_tests, [parallel], [
        add_to_waiting_test,
        add_to_ongoing_test,
        add_to_ended_test,

        delete_from_waiting_test,
        delete_from_ongoing_test,
        delete_from_ended_test,

        list_with_invalid_listing_opts_test,
        list_with_negative_offset_test,

        iterate_over_100_waiting_atm_workflow_executions_using_offset_and_limit_1_test,
        iterate_over_100_waiting_atm_workflow_executions_using_offset_and_limit_10_test,
        iterate_over_100_waiting_atm_workflow_executions_using_offset_and_limit_100_test,
        iterate_over_100_waiting_atm_workflow_executions_using_start_index_and_limit_1_test,
        iterate_over_100_waiting_atm_workflow_executions_using_start_index_and_limit_10_test,
        iterate_over_100_waiting_atm_workflow_executions_using_start_index_and_limit_100_test,

        iterate_over_100_ongoing_atm_workflow_executions_using_offset_and_limit_1_test,
        iterate_over_100_ongoing_atm_workflow_executions_using_offset_and_limit_10_test,
        iterate_over_100_ongoing_atm_workflow_executions_using_offset_and_limit_100_test,
        iterate_over_100_ongoing_atm_workflow_executions_using_start_index_and_limit_1_test,
        iterate_over_100_ongoing_atm_workflow_executions_using_start_index_and_limit_10_test,
        iterate_over_100_ongoing_atm_workflow_executions_using_start_index_and_limit_100_test,

        iterate_over_100_ended_atm_workflow_executions_using_offset_and_limit_1_test,
        iterate_over_100_ended_atm_workflow_executions_using_offset_and_limit_10_test,
        iterate_over_100_ended_atm_workflow_executions_using_offset_and_limit_100_test,
        iterate_over_100_ended_atm_workflow_executions_using_start_index_and_limit_1_test,
        iterate_over_100_ended_atm_workflow_executions_using_start_index_and_limit_10_test,
        iterate_over_100_ended_atm_workflow_executions_using_start_index_and_limit_100_test
    ]}
].

all() -> [
    {group, all_tests}
].


-type listing_method() :: offset | start_index.

-define(ATM_WORKFLOW_EXECUTIONS_PHASES, [
    ?WAITING_PHASE, ?ONGOING_PHASE, ?ENDED_PHASE
]).

-define(DUMMY_ATM_INVENTORY_ID, <<"dummyId">>).

-define(DEFAULT_PROVIDER_SYNC_TIME_SEC, 15).
-define(ATTEMPTS, 30).


%%%===================================================================
%%% API functions
%%%===================================================================


add_to_waiting_test(_Config) ->
    add_links_test_base(?WAITING_PHASE).


add_to_ongoing_test(_Config) ->
    add_links_test_base(?ONGOING_PHASE).


add_to_ended_test(_Config) ->
    add_links_test_base(?ENDED_PHASE).


%% @private
-spec add_links_test_base(atm_workflow_execution:phase()) -> ok.
add_links_test_base(Phase) ->
    SpaceId = oct_background:get_space_id(space_krk_par_p),
    KrkNode = oct_background:get_random_provider_node(krakow),
    ParNode = oct_background:get_random_provider_node(paris),

    AtmInventoryId = str_utils:rand_hex(32),
    AtmWorkflowExecutionDoc = gen_rand_workflow(SpaceId, AtmInventoryId),
    AtmWorkflowExecutionId = AtmWorkflowExecutionDoc#document.key,

    add_link(KrkNode, Phase, AtmWorkflowExecutionDoc),

    ?assertEqual(true, is_member(KrkNode, SpaceId, Phase, all, AtmWorkflowExecutionId)),
    ?assertEqual(true, is_member(KrkNode, SpaceId, Phase, AtmInventoryId, AtmWorkflowExecutionId)),
    ?assertEqual(false, is_member(KrkNode, SpaceId, Phase, ?DUMMY_ATM_INVENTORY_ID, AtmWorkflowExecutionId)),
    ?assertEqual(false, is_member(ParNode, SpaceId, Phase, all, AtmWorkflowExecutionId)),

    % Assert links are not synchronized between providers
    % (wait some time to give time for potential synchronization)
    timer:sleep(timer:seconds(?DEFAULT_PROVIDER_SYNC_TIME_SEC)),

    ?assertEqual(true, is_member(KrkNode, SpaceId, Phase, AtmWorkflowExecutionId)),
    ?assertEqual(false, is_member(ParNode, SpaceId, Phase, AtmWorkflowExecutionId)),

    ok.


delete_from_waiting_test(_Config) ->
    delete_links_test_base(?WAITING_PHASE).


delete_from_ongoing_test(_Config) ->
    delete_links_test_base(?ONGOING_PHASE).


delete_from_ended_test(_Config) ->
    delete_links_test_base(?ENDED_PHASE).


%% @private
-spec delete_links_test_base(atm_workflow_execution:phase()) -> ok.
delete_links_test_base(Phase) ->
    SpaceId = oct_background:get_space_id(space_krk_par_p),
    KrkNode = oct_background:get_random_provider_node(krakow),

    AtmInventoryId = str_utils:rand_hex(32),
    AtmWorkflowExecutionDoc = gen_rand_workflow(SpaceId, AtmInventoryId),
    AtmWorkflowExecutionId = AtmWorkflowExecutionDoc#document.key,

    add_link(KrkNode, Phase, AtmWorkflowExecutionDoc),
    ?assertEqual(true, is_member(KrkNode, SpaceId, Phase, AtmWorkflowExecutionId)),

    % Assert links are not synchronized between providers
    % (wait some time to give time for potential synchronization)
    timer:sleep(timer:seconds(?DEFAULT_PROVIDER_SYNC_TIME_SEC)),

    ?assertEqual(true, is_member(KrkNode, SpaceId, Phase, AtmWorkflowExecutionId)),
    delete_link(KrkNode, Phase, AtmWorkflowExecutionDoc),
    ?assertEqual(false, is_member(KrkNode, SpaceId, Phase, AtmWorkflowExecutionId)),

    ok.


list_with_invalid_listing_opts_test(_Config) ->
    SpaceId = oct_background:get_space_id(space_krk_par_p),
    KrkNode = oct_background:get_random_provider_node(krakow),
    Phase = lists_utils:random_element(?ATM_WORKFLOW_EXECUTIONS_PHASES),

    lists:foreach(fun(InvalidListingOpts) ->
        ?assertEqual(?EINVAL, list_links(KrkNode, SpaceId, Phase, all, InvalidListingOpts))
    end, [
        % Either offset or start_index must be specified
        #{}, #{limit => 10},
        % Limit lower than 1 is not allowed
        #{offset => 0, limit => -10}, #{offset => 0, limit => 0}, #{offset => 0, limit => all},
        % Offset must be proper integer
        #{offset => <<>>}, #{offset => -2.5},
        % Start index must be proper binary
        #{start_index => 10}
    ]).


list_with_negative_offset_test(_Config) ->
    % Use random SpaceId so that it would be possible to run test concurrently
    SpaceId = str_utils:rand_hex(32),
    Node = oct_background:get_random_provider_node(paris),
    Phase = lists_utils:random_element(?ATM_WORKFLOW_EXECUTIONS_PHASES),

    AtmInventoryIds = [str_utils:rand_hex(32), str_utils:rand_hex(32)],
    AllLinks = populate_links(Node, SpaceId, Phase, AtmInventoryIds, AtmInventoryIds, 30),

    ?assertEqual(AllLinks, list_links(Node, SpaceId, Phase, all, #{offset => -10})),
    ?assertEqual(AllLinks, list_links(Node, SpaceId, Phase, all, #{offset => -10, limit => 30})),

    StartIndex = element(2, lists:nth(20, AllLinks)),
    ExpLinks = lists:sublist(AllLinks, 15, 10),
    ?assertEqual(
        ExpLinks,
        list_links(Node, SpaceId, Phase, all, #{start_index => StartIndex, offset => -5, limit => 10})
    ).


iterate_over_100_waiting_atm_workflow_executions_using_offset_and_limit_1_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?WAITING_PHASE, 200, offset, 1).


iterate_over_100_waiting_atm_workflow_executions_using_offset_and_limit_10_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?WAITING_PHASE, 200, offset, 10).


iterate_over_100_waiting_atm_workflow_executions_using_offset_and_limit_100_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?WAITING_PHASE, 200, offset, 100).


iterate_over_100_waiting_atm_workflow_executions_using_start_index_and_limit_1_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?WAITING_PHASE, 200, start_index, 1).


iterate_over_100_waiting_atm_workflow_executions_using_start_index_and_limit_10_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?WAITING_PHASE, 200, start_index, 10).


iterate_over_100_waiting_atm_workflow_executions_using_start_index_and_limit_100_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?WAITING_PHASE, 200, start_index, 100).


iterate_over_100_ongoing_atm_workflow_executions_using_offset_and_limit_1_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?ONGOING_PHASE, 200, offset, 1).


iterate_over_100_ongoing_atm_workflow_executions_using_offset_and_limit_10_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?ONGOING_PHASE, 200, offset, 10).


iterate_over_100_ongoing_atm_workflow_executions_using_offset_and_limit_100_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?ONGOING_PHASE, 200, offset, 100).


iterate_over_100_ongoing_atm_workflow_executions_using_start_index_and_limit_1_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?ONGOING_PHASE, 200, start_index, 1).


iterate_over_100_ongoing_atm_workflow_executions_using_start_index_and_limit_10_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?ONGOING_PHASE, 200, start_index, 10).


iterate_over_100_ongoing_atm_workflow_executions_using_start_index_and_limit_100_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?ONGOING_PHASE, 200, start_index, 100).


iterate_over_100_ended_atm_workflow_executions_using_offset_and_limit_1_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?ENDED_PHASE, 200, offset, 1).


iterate_over_100_ended_atm_workflow_executions_using_offset_and_limit_10_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?ENDED_PHASE, 200, offset, 10).


iterate_over_100_ended_atm_workflow_executions_using_offset_and_limit_100_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?ENDED_PHASE, 200, offset, 100).


iterate_over_100_ended_atm_workflow_executions_using_start_index_and_limit_1_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?ENDED_PHASE, 200, start_index, 1).


iterate_over_100_ended_atm_workflow_executions_using_start_index_and_limit_10_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?ENDED_PHASE, 200, start_index, 10).


iterate_over_100_ended_atm_workflow_executions_using_start_index_and_limit_100_test(_Config) ->
    iterate_over_atm_workflow_executions_test_base(?ENDED_PHASE, 200, start_index, 100).


%% @private
-spec iterate_over_atm_workflow_executions_test_base(
    atm_workflow_execution:phase(),
    pos_integer(),
    listing_method(),
    atm_workflow_executions_forest:limit()
) ->
    ok.
iterate_over_atm_workflow_executions_test_base(Phase, LinksNum, ListingMethod, Limit) ->
    % Use random SpaceId so that it would be possible to run test concurrently
    SpaceId = str_utils:rand_hex(32),
    Node = oct_background:get_random_provider_node(paris),

    AllAtmInventoryIds = [str_utils:rand_hex(32) || _ <- lists:seq(1, 5)],
    ListedAtmInventoryIds = lists_utils:random_element([
        all,
        lists_utils:random_element(AllAtmInventoryIds),
        lists_utils:random_sublist(AllAtmInventoryIds)
    ]),
    ExpLinks = populate_links(Node, SpaceId, Phase, AllAtmInventoryIds, ListedAtmInventoryIds, LinksNum),

    ListingOpts = case ListingMethod of
        offset -> #{offset => 0, limit => Limit};
        start_index -> #{start_index => <<>>, limit => Limit}
    end,
    ListedLinks = list_all_links_by_chunk(
        Node, ListingMethod, SpaceId, Phase, ListedAtmInventoryIds, ListingOpts, []
    ),
    ?assertEqual(ExpLinks, ListedLinks).


%% @private
-spec list_all_links_by_chunk(
    node(),
    listing_method(),
    od_space:id(),
    atm_workflow_execution:phase(),
    atm_workflow_executions_forest:tree_ids(),
    atm_workflow_executions_forest:listing_opts(),
    [{atm_workflow_execution:id(), atm_workflow_executions_forest:index()}]
) ->
    [{atm_workflow_execution:id(), atm_workflow_executions_forest:index()}].
list_all_links_by_chunk(Node, ListingMethod, SpaceId, Phase, ListedAtmInventoryIds, ListingOpts, LinksAcc) ->
    case list_links(Node, SpaceId, Phase, ListedAtmInventoryIds, ListingOpts) of
        [] ->
            LinksAcc;
        ListedLinks ->
            list_all_links_by_chunk(
                Node, ListingMethod, SpaceId, Phase, ListedAtmInventoryIds,
                update_listing_opts(ListingMethod, ListingOpts, ListedLinks),
                LinksAcc ++ ListedLinks
            )
    end.


%% @private
-spec update_listing_opts(
    listing_method(),
    atm_workflow_executions_forest:listing_opts(),
    [{atm_workflow_execution:id(), atm_workflow_executions_forest:index()}]
) ->
    atm_workflow_executions_forest:listing_opts().
update_listing_opts(offset, ListingOpts, ListedLinks) ->
    maps:update_with(offset, fun(Offset) -> Offset + length(ListedLinks) end, ListingOpts);
update_listing_opts(start_index, ListingOpts, ListedLinks) ->
    maps:update(start_index, element(2, lists:last(ListedLinks)), ListingOpts#{offset => 1}).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec populate_links(
    node(),
    od_space:id(),
    atm_workflow_execution:phase(),
    [od_atm_inventory:id()],
    atm_workflow_executions_forest:tree_ids(),
    pos_integer()
) ->
    atm_workflow_execution:listing().
populate_links(Node, SpaceId, Phase, AllAtmInventoryIds, all, LinksNum) ->
    populate_links(Node, SpaceId, Phase, AllAtmInventoryIds, AllAtmInventoryIds, LinksNum);

populate_links(Node, SpaceId, Phase, AllAtmInventoryIds, ListedAtmInventoryId, LinksNum) when
    is_binary(ListedAtmInventoryId)
->
    populate_links(Node, SpaceId, Phase, AllAtmInventoryIds, [ListedAtmInventoryId], LinksNum);

populate_links(Node, SpaceId, Phase, AllAtmInventoryIds, ListedAtmInventoryIds, LinksNum) ->
    lists:keysort(2, lists:filtermap(fun(_) ->
        AtmInventoryId = lists_utils:random_element(AllAtmInventoryIds),
        AtmWorkflowExecutionDoc = gen_rand_workflow(SpaceId, AtmInventoryId),
        AtmWorkflowExecutionId = AtmWorkflowExecutionDoc#document.key,
        add_link(Node, Phase, AtmWorkflowExecutionDoc),

        case lists:member(AtmInventoryId, ListedAtmInventoryIds) of
            true ->
                {true, {AtmWorkflowExecutionId, index(Phase, AtmWorkflowExecutionDoc)}};
            false ->
                false
        end
    end, lists:seq(1, LinksNum))).


%% @private
-spec gen_rand_workflow(od_space:id(), od_atm_inventory:id()) ->
    atm_workflow_execution:record().
gen_rand_workflow(SpaceId, AtmInventoryId) ->
    #document{key = str_utils:rand_hex(32), value = #atm_workflow_execution{
        space_id = SpaceId,
        atm_inventory_id = AtmInventoryId,
        schedule_time = rand:uniform(9999999),
        start_time = rand:uniform(9999999),
        finish_time = rand:uniform(9999999)
    }}.


%% @private
-spec index(atm_workflow_execution:phase(), atm_workflow_execution:doc()) ->
    atm_workflow_executions_forest:index().
index(?WAITING_PHASE, #document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    schedule_time = ScheduleTime
}}) ->
    <<(integer_to_binary(?EPOCH_INFINITY - ScheduleTime))/binary, AtmWorkflowExecutionId/binary>>;
index(?ONGOING_PHASE, #document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    start_time = StartTime
}}) ->
    <<(integer_to_binary(?EPOCH_INFINITY - StartTime))/binary, AtmWorkflowExecutionId/binary>>;
index(?ENDED_PHASE, #document{key = AtmWorkflowExecutionId, value = #atm_workflow_execution{
    finish_time = FinishTime
}}) ->
    <<(integer_to_binary(?EPOCH_INFINITY - FinishTime))/binary, AtmWorkflowExecutionId/binary>>.


%% @private
-spec add_link(node(), atm_workflow_execution:phase(), atm_workflow_execution:doc()) -> ok.
add_link(Node, ?WAITING_PHASE, AtmWorkflowExecutionDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_waiting_workflow_executions, add, [AtmWorkflowExecutionDoc]));
add_link(Node, ?ONGOING_PHASE, AtmWorkflowExecutionDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_ongoing_workflow_executions, add, [AtmWorkflowExecutionDoc]));
add_link(Node, ?ENDED_PHASE, AtmWorkflowExecutionDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_ended_workflow_executions, add, [AtmWorkflowExecutionDoc])).


%% @private
-spec delete_link(node(), atm_workflow_execution:phase(), atm_workflow_execution:doc()) -> ok.
delete_link(Node, ?WAITING_PHASE, AtmWorkflowExecutionDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_waiting_workflow_executions, delete, [AtmWorkflowExecutionDoc]));
delete_link(Node, ?ONGOING_PHASE, AtmWorkflowExecutionDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_ongoing_workflow_executions, delete, [AtmWorkflowExecutionDoc]));
delete_link(Node, ?ENDED_PHASE, AtmWorkflowExecutionDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_ended_workflow_executions, delete, [AtmWorkflowExecutionDoc])).


%% @private
-spec is_member(node(), od_space:id(), atm_workflow_execution:phase(), atm_workflow_execution:id()) ->
    boolean().
is_member(Node, SpaceId, Phase, AtmWorkflowExecutionId) ->
    is_member(Node, SpaceId, Phase, all, AtmWorkflowExecutionId).


%% @private
-spec is_member(
    node(),
    od_space:id(),
    atm_workflow_execution:phase(),
    atm_workflow_executions_forest:tree_ids(),
    atm_workflow_execution:id()
) ->
    boolean().
is_member(Node, SpaceId, Phase, TreeIds, AtmWorkflowExecutionId) ->
    lists:keymember(AtmWorkflowExecutionId, 1, list_all_links(Node, SpaceId, Phase, TreeIds)).


%% @private
-spec list_all_links(
    node(),
    od_space:id(),
    atm_workflow_execution:phase(),
    atm_workflow_executions_forest:tree_ids()
) ->
    atm_workflow_execution:listing().
list_all_links(Node, SpaceId, Phase, TreeIds) ->
    list_links(Node, SpaceId, Phase, TreeIds, #{offset => 0}).


%% @private
-spec list_links(
    node(),
    od_space:id(),
    atm_workflow_execution:phase(),
    atm_workflow_executions_forest:tree_ids(),
    atm_workflow_executions_forest:listing_opts()
) ->
    atm_workflow_execution:listing().
list_links(Node, SpaceId, ?WAITING_PHASE, TreeIds, ListingOpts) ->
    rpc:call(Node, atm_waiting_workflow_executions, list, [SpaceId, TreeIds, ListingOpts]);
list_links(Node, SpaceId, ?ONGOING_PHASE, TreeIds, ListingOpts) ->
    rpc:call(Node, atm_ongoing_workflow_executions, list, [SpaceId, TreeIds, ListingOpts]);
list_links(Node, SpaceId, ?ENDED_PHASE, TreeIds, ListingOpts) ->
    rpc:call(Node, atm_ended_workflow_executions, list, [SpaceId, TreeIds, ListingOpts]).


%===================================================================
% SetUp and TearDown functions
%===================================================================


init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    Config.


end_per_testcase(_Case, _Config) ->
    ok.
