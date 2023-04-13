%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Test of file_links_reconciliation_traverse.
%%% @end
%%%-------------------------------------------------------------------
-module(file_links_reconciliation_traverse_test_SUITE).
-author("Michal Stanisz").

-include("onenv_test_utils.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).

-export([
    file_links_reconciliation_traverse_test/1
]).

all() -> [
    file_links_reconciliation_traverse_test
].

-define(ATTEMPTS, 60).

%%%===================================================================
%%% API
%%%===================================================================

file_links_reconciliation_traverse_test(Config) ->
    #object{guid = DirGuid, children = ChildrenObjects} = onenv_file_test_utils:create_file_tree(
        oct_background:get_user_id(user1),
        fslogic_file_id:spaceid_to_space_dir_guid(oct_background:get_space_id(space1)),
        oct_background:get_provider_id(paris),
        #dir_spec{children = lists:duplicate(100, #file_spec{})}
    ),
    timer:sleep(timer:seconds(10)), % wait for all other docs to sync (links_node documents are ignored)
    stop_op_worker(Config, paris),
    [KrakowNode | _] = oct_background:get_provider_nodes(krakow),
    ExpectedChildren = lists:sort(lists:map(fun(#object{guid = Guid}) -> Guid end, ChildrenObjects)),
    
    ListChildrenFun = fun() ->
        {ok, Listed, _} = lfm_proxy:get_children(
            KrakowNode,
            oct_background:get_user_session_id(user1, krakow),
            ?FILE_REF(DirGuid),
            #{limit => 100, offset => 0, tune_for_large_continuous_listing => false}
        ),
        lists:sort(lists:map(fun({Guid, _Name}) -> Guid end, Listed))
    end,
    
    ?assertEqual(ok, rpc:call(KrakowNode, file_links_reconciliation_traverse, start, [])),
    
    timer:sleep(timer:seconds(5)),
    
    start_op_worker(Config, paris),
    wait_for_traverse_finish(space1),
    stop_op_worker(Config, paris),
    
    ?assertEqual(ExpectedChildren, ListChildrenFun(), ?ATTEMPTS),
    ok.


%%%===================================================================
%%% Helper functions
%%%===================================================================

stop_op_worker(Config, ProviderSelector) ->
    op_worker_service_operation(Config, ProviderSelector, "stop").


start_op_worker(Config, ProviderSelector) ->
    op_worker_service_operation(Config, ProviderSelector, "start").


op_worker_service_operation(Config, ProviderSelector, ServiceOperation) ->
    OnenvScript = test_config:get_onenv_script_path(Config),
    lists:foreach(fun(Node) ->
        Pod = test_config:get_custom(Config, [pods, Node]),
        Res = utils:cmd([OnenvScript, "service", ServiceOperation, Pod, "-s", "worker"]),
        ct:print("~s", [Res])
    end, oct_background:get_provider_nodes(ProviderSelector)).


wait_for_traverse_finish(SpaceSelector) ->
    SpaceId = oct_background:get_space_id(SpaceSelector),
    receive
        {traverse_finished, SpaceId} ->
            ok
    after timer:seconds(?ATTEMPTS) ->
        throw(traverse_not_finished)
    end.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {file_links_reconciliation_traverse_max_retry_sleep, timer:seconds(8)}
        ]}]
    }).

init_per_testcase(_Case, Config) ->
    % mock dbsync so it does not save link_node documents
    KrakowNodes = oct_background:get_provider_nodes(krakow),
    test_utils:mock_new(KrakowNodes, dbsync_changes, [passthrough]),
    ok = test_utils:mock_expect(KrakowNodes, dbsync_changes, apply,
        fun (#document{value = #links_node{model = file_meta}}) ->
                ok;
            (Doc) -> meck:passthrough([Doc])
        end
    ),
    test_utils:mock_new(KrakowNodes, file_links_reconciliation_traverse, [passthrough]),
    test_utils:mock_new(KrakowNodes, fslogic_event_emitter, [passthrough]),
    TestPid = self(),
    ok = test_utils:mock_expect(KrakowNodes, file_links_reconciliation_traverse, task_finished,
        fun (TaskId, PoolName) ->
            TestPid ! {traverse_finished, TaskId},
            meck:passthrough([TaskId, PoolName])
        end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_file_attr_changed,
        fun (_, _) -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_file_attr_changed,
        fun (_, _, _) -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_file_attr_changed_with_replication_status,
        fun (_, _, _) -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_sizeless_file_attrs_changed,
        fun (_) -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_file_location_changed,
        fun (_, _) -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_file_location_changed,
        fun (_, _, _) -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_file_location_changed,
        fun (_, _, _, _) -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_file_locations_changed,
        fun (_, _) -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_file_perm_changed,
        fun (_) -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_file_removed,
        fun (_,_) -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_file_renamed_to_client,
        fun (_,_,_,_,_) -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_file_renamed_no_exclude,
        fun (_,_,_,_,_) -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_quota_exceeded,
        fun () -> ok end
    ),
    ok = test_utils:mock_expect(KrakowNodes, fslogic_event_emitter, emit_helper_params_changed,
        fun (_) -> ok end
    ),
    % disable op_worker healthcheck in onepanel, so nodes are not started up automatically
    oct_environment:disable_panel_healthcheck(Config),
    lfm_proxy:init(Config, false).


end_per_testcase(_Case, Config) ->
    % unload only in krakow as paris is stopped
    test_utils:mock_unload(oct_background:get_provider_nodes(krakow)),
    lfm_proxy:teardown(Config).

end_per_suite(_Config) ->
    ok.
