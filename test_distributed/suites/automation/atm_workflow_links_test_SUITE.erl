%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of workflows link trees.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_links_test_SUITE).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_wokflow.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([
    all/0,
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

    move_from_waiting_to_ongoing_test/1,
    move_from_ongoing_to_ended/1
]).

all() -> [
    add_to_waiting_test,
    add_to_ongoing_test,
    add_to_ended_test,

    delete_from_waiting_test,
    delete_from_ongoing_test,
    delete_from_ended_test,

    move_from_waiting_to_ongoing_test,
    move_from_ongoing_to_ended
].


-define(ATTEMPTS, 30).


%%%===================================================================
%%% API functions
%%%===================================================================


add_to_waiting_test(_Config) ->
    add_links_test_base(?WAITING_WORKFLOWS_KEY).


add_to_ongoing_test(_Config) ->
    add_links_test_base(?ONGOING_WORKFLOWS_KEY).


add_to_ended_test(_Config) ->
    add_links_test_base(?ENDED_WORKFLOWS_KEY).


%% @private
-spec add_links_test_base(atm_workflow_links:virtual_list_id()) -> ok.
add_links_test_base(State) ->
    SpaceId = oct_background:get_space_id(space_krk_par_p),
    KrkNode = oct_background:get_random_provider_node(krakow),
    ParNode = oct_background:get_random_provider_node(paris),

    WorkflowId = str_utils:rand_hex(32),
    WorkflowDoc = #document{key = WorkflowId, value = gen_rand_workflow(SpaceId)},

    add_link(KrkNode, State, WorkflowDoc),

    ?assertEqual(true, is_member(KrkNode, SpaceId, State, WorkflowId)),
    ?assertEqual(false, is_member(ParNode, SpaceId, State, WorkflowId)),

    timer:sleep(timer:seconds(15)),

    ?assertEqual(true, is_member(KrkNode, SpaceId, State, WorkflowId)),
    ?assertEqual(false, is_member(ParNode, SpaceId, State, WorkflowId)),

    ok.


delete_from_waiting_test(_Config) ->
    delete_links_test_base(?WAITING_WORKFLOWS_KEY).


delete_from_ongoing_test(_Config) ->
    delete_links_test_base(?ONGOING_WORKFLOWS_KEY).


delete_from_ended_test(_Config) ->
    delete_links_test_base(?ENDED_WORKFLOWS_KEY).


%% @private
-spec delete_links_test_base(atm_workflow_links:virtual_list_id()) -> ok.
delete_links_test_base(State) ->
    SpaceId = oct_background:get_space_id(space_krk_par_p),
    KrkNode = oct_background:get_random_provider_node(krakow),

    WorkflowId = str_utils:rand_hex(32),
    WorkflowDoc = #document{key = WorkflowId, value = gen_rand_workflow(SpaceId)},

    add_link(KrkNode, State, WorkflowDoc),
    ?assertEqual(true, is_member(KrkNode, SpaceId, State, WorkflowId)),

    timer:sleep(timer:seconds(5)),

    ?assertEqual(true, is_member(KrkNode, SpaceId, State, WorkflowId)),
    delete_link(KrkNode, State, WorkflowDoc),
    ?assertEqual(false, is_member(KrkNode, SpaceId, State, WorkflowId)),

    ok.


move_from_waiting_to_ongoing_test(_Config) ->
    move_links_test_base(?WAITING_WORKFLOWS_KEY, ?ONGOING_WORKFLOWS_KEY).


move_from_ongoing_to_ended(_Config) ->
    move_links_test_base(?ONGOING_WORKFLOWS_KEY, ?ENDED_WORKFLOWS_KEY).


%% @private
-spec move_links_test_base(
    atm_workflow_links:virtual_list_id(),
    atm_workflow_links:virtual_list_id()
) ->
    ok.
move_links_test_base(OriginalState, NewState) ->
    SpaceId = oct_background:get_space_id(space_krk_par_p),
    KrkNode = oct_background:get_random_provider_node(krakow),
    ParNode = oct_background:get_random_provider_node(paris),

    WorkflowId = str_utils:rand_hex(32),
    WorkflowDoc = #document{key = WorkflowId, value = gen_rand_workflow(SpaceId)},

    add_link(KrkNode, OriginalState, WorkflowDoc),
    ?assertEqual(true, is_member(KrkNode, SpaceId, OriginalState, WorkflowId)),

    case {OriginalState, NewState} of
        {?WAITING_WORKFLOWS_KEY, ?ONGOING_WORKFLOWS_KEY} ->
            move_link_from_waiting_to_ongoing(KrkNode, WorkflowDoc);
        {?ONGOING_WORKFLOWS_KEY, ?ENDED_WORKFLOWS_KEY} ->
            move_link_from_ongoing_to_ended(KrkNode, WorkflowDoc)
    end,

    ?assertEqual(false, is_member(KrkNode, SpaceId, OriginalState, WorkflowId)),
    ?assertEqual(true, is_member(KrkNode, SpaceId, NewState, WorkflowId)),

    timer:sleep(timer:seconds(15)),

    ?assertEqual(true, is_member(KrkNode, SpaceId, NewState, WorkflowId)),

    ?assertEqual(false, is_member(ParNode, SpaceId, OriginalState, WorkflowId)),
    ?assertEqual(false, is_member(ParNode, SpaceId, NewState, WorkflowId)),

    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec gen_rand_workflow(od_space:id()) -> atm_workflow:record().
gen_rand_workflow(SpaceId) ->
    #atm_workflow{
        space_id = SpaceId,
        schedule_time = rand:uniform(9999999),
        start_time = rand:uniform(9999999),
        finish_time = rand:uniform(9999999)
    }.


%% @private
-spec add_link(node(), atm_workflow_links:virtual_list_id(), atm_workflow:doc()) -> ok.
add_link(Node, ?WAITING_WORKFLOWS_KEY, WorkflowDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_workflow_links, add_waiting, [WorkflowDoc]));
add_link(Node, ?ONGOING_WORKFLOWS_KEY, WorkflowDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_workflow_links, add_ongoing, [WorkflowDoc]));
add_link(Node, ?ENDED_WORKFLOWS_KEY, WorkflowDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_workflow_links, add_ended, [WorkflowDoc])).


%% @private
-spec delete_link(node(), atm_workflow_links:virtual_list_id(), atm_workflow:doc()) -> ok.
delete_link(Node, ?WAITING_WORKFLOWS_KEY, WorkflowDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_workflow_links, delete_waiting, [WorkflowDoc]));
delete_link(Node, ?ONGOING_WORKFLOWS_KEY, WorkflowDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_workflow_links, delete_ongoing, [WorkflowDoc]));
delete_link(Node, ?ENDED_WORKFLOWS_KEY, WorkflowDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_workflow_links, delete_ended, [WorkflowDoc])).


%% @private
-spec move_link_from_waiting_to_ongoing(node(), atm_workflow:doc()) -> ok.
move_link_from_waiting_to_ongoing(Node, WorkflowDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_workflow_links, move_from_waiting_to_ongoing, [WorkflowDoc])).


%% @private
-spec move_link_from_ongoing_to_ended(node(), atm_workflow:doc()) -> ok.
move_link_from_ongoing_to_ended(Node, WorkflowDoc) ->
    ?assertEqual(ok, rpc:call(Node, atm_workflow_links, move_from_ongoing_to_ended, [WorkflowDoc])).


%% @private
-spec is_member(node(), od_space:id(), atm_workflow_links:virtual_list_id(), atm_workflow:id()) ->
    boolean().
is_member(Node, SpaceId, State, WorkflowId) ->
    lists:keymember(WorkflowId, 1, list_all_links(Node, SpaceId, State)).


%% @private
-spec list_all_links(node(), od_space:id(), atm_workflow_links:virtual_list_id()) ->
    [{atm_workflow:id(), atm_workflow_links:link_key()}].
list_all_links(Node, SpaceId, State) ->
    rpc:call(Node, atm_workflow_links, list, [SpaceId, State, undefined, 0, 1000]).


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
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
