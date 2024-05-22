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
-include_lib("ctool/include/logging.hrl").
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

-define(ATTEMPTS, 120).

%%%===================================================================
%%% API
%%%===================================================================

file_links_reconciliation_traverse_test(Config) ->
    ChildrenCount = 100,
    #object{guid = DirGuid, children = ChildrenObjects} = onenv_file_test_utils:create_file_tree(
        oct_background:get_user_id(user1),
        fslogic_file_id:spaceid_to_space_dir_guid(oct_background:get_space_id(space1)),
        oct_background:get_provider_id(paris),
        #dir_spec{children = lists:duplicate(ChildrenCount, #file_spec{})}
    ),
    wait_for_dbsynced_docs(file_meta, synced, ChildrenCount + 1), % +1 from parent dir doc (space doc was synced before test started)
    wait_for_dbsynced_docs(links_forest, synced, 2), % space and parent dir docs
    wait_for_dbsynced_docs(links_node, synced, 1), % space dir doc
    LinkNodeIds = get_dbsynced_docs(links_node, ignored, 1), % parent dir docs
    
    stop_op_worker(Config, paris),
    
    [KrakowNode | _] = oct_background:get_provider_nodes(krakow),
    lists:foreach(fun(LinkNodeId) ->
        ?assertEqual({error, not_found}, get_doc_from_db(KrakowNode, LinkNodeId))
    end, LinkNodeIds),
    
    ChildrenGuids = [Guid || #object{guid = Guid} <- ChildrenObjects],
    ExpectedChildren = lists:sort(ChildrenGuids),
    
    % eagain is expected when fetching remote doc fails
    ?assertEqual({error, ?EAGAIN}, get_children(KrakowNode, DirGuid)),
    
    ?assertEqual(ok, opw_test_rpc:call(KrakowNode, file_links_reconciliation_traverse, start_for_space,
        [oct_background:get_space_id(space1)])),
    
    % check that traverse started and hangs
    ?assertEqual({ok, 1}, get_number_of_ongoing_traverses(KrakowNode), ?ATTEMPTS),
    timer:sleep(timer:seconds(8)),
    ?assertEqual({ok, 1}, get_number_of_ongoing_traverses(KrakowNode)),
    
    start_op_worker(Config, paris),
    ?assertEqual({ok, 0}, get_number_of_ongoing_traverses(KrakowNode), ?ATTEMPTS),
    stop_op_worker(Config, paris),
    
    lists:foreach(fun(LinkNodeId) ->
        % try ?ATTEMPTS times as document could be not flushed from datastore to db yet
        ?assertMatch({ok, _, _}, get_doc_from_db(KrakowNode, LinkNodeId), ?ATTEMPTS)
    end, LinkNodeIds),
    
    ListChildrenFun = fun() ->
        {ok, Listed, _} = get_children(KrakowNode, DirGuid),
        lists:sort(lists:map(fun({Guid, _Name}) -> Guid end, Listed))
    end,
    ?assertEqual(ExpectedChildren, ListChildrenFun()),
    ok.


%%%===================================================================
%%% Helper functions
%%%===================================================================

wait_for_dbsynced_docs(Model, Type, ExpectedNum) ->
    get_dbsynced_docs(Model, Type, ExpectedNum),
    ok.


get_dbsynced_docs(Model, Type, ExpectedNum) ->
    get_dbsynced_docs(Model, Type, ExpectedNum, []).


get_dbsynced_docs(_Model, _Type, 0, Acc) ->
    Acc;
get_dbsynced_docs(Model, Type, ExpectedNum, Acc) ->
    receive
        {file_links_reconciliation_traverse_test, Type, Model, Key} ->
            get_dbsynced_docs(Model, Type, ExpectedNum - 1, [Key | Acc])
    after timer:seconds(?ATTEMPTS) ->
        throw({documents_not_synced, Model, ExpectedNum})
    end.


get_doc_from_db(Node, Key) ->
    opw_test_rpc:call(Node, couchbase_driver, get, [#{bucket => <<"onedata">>}, Key]).


stop_op_worker(Config, ProviderSelector) ->
    op_worker_service_operation(Config, ProviderSelector, "stop").


start_op_worker(Config, ProviderSelector) ->
    op_worker_service_operation(Config, ProviderSelector, "start").


op_worker_service_operation(Config, ProviderSelector, ServiceOperation) ->
    OnenvScript = test_config:get_onenv_script_path(Config),
    lists:foreach(fun(Node) ->
        Pod = test_config:get_custom(Config, [pods, Node]),
        case shell_utils:execute([OnenvScript, "service", ServiceOperation, Pod, "-s", "worker"]) of
            {0, Stdout, _} ->
                % propagate output of onenv service command
                ct:pal("~ts", [Stdout]);
            {ExitCode, Stdout, Stderr} ->
                ?dump(?autoformat_with_msg("Error in op worker operation ",
                    [ServiceOperation, ExitCode, Stdout, Stderr])),
                throw(service_operation_failed)
        end
    end, oct_background:get_provider_nodes(ProviderSelector)).


get_number_of_ongoing_traverses(Node) ->
    opw_test_rpc:call(Node, fun() ->
        datastore_model:fold_links(traverse_task:get_ctx(), <<"ONGOING_qos_traverse">>, all, fun(_, Acc) -> {ok, Acc + 1} end, 0, #{size => 100})
    end).


get_children(Node, DirGuid) ->
    lfm_proxy:get_children(
        Node,
        oct_background:get_user_session_id(user1, krakow),
        ?FILE_REF(DirGuid),
        #{limit => 100, offset => 0, tune_for_large_continuous_listing => false}
    ).


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
    TestPid = self(),
    ok = test_utils:mock_expect(KrakowNodes, dbsync_changes, apply,
        fun (#document{key = Key, value = #links_node{model = file_meta, key = RelatedKey}} = Doc) ->
                case fslogic_file_id:is_space_dir_uuid(RelatedKey) of
                    true ->
                        % do not ignore links_node for space dir as it already exists on this provider (created alongside
                        % archives root dir on space support), so it will never be fetched from remote provider)
                        TestPid ! {file_links_reconciliation_traverse_test, synced, links_node, Key},
                        meck:passthrough([Doc]);
                    false ->
                        TestPid ! {file_links_reconciliation_traverse_test, ignored, links_node, Key}
                end,
                ok;
            (#document{key = Key, value = Value} = Doc) ->
                TestPid ! {file_links_reconciliation_traverse_test, synced, element(1, Value), Key},
                meck:passthrough([Doc])
        end
    ),
    test_utils:mock_new(KrakowNodes, fslogic_event_emitter, [passthrough]),
    % mock events as they can fetch links under the hood
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
    MessageQueueCleanFun = fun F() ->
        receive
            _ -> F()
        after 0 ->
            ok
        end
    end,
    MessageQueueCleanFun(),
    lfm_proxy:teardown(Config).

end_per_suite(_Config) ->
    ok.
