%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of synchronization of file_meta links between providers: links that
%%% must never be synced (the tmp dir's subtree), moving files out of and into
%%% the tmp dir, and reconciliation of missing links by file_links_reconciliation_traverse.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_links_sync_test_SUITE).
-author("Michal Stanisz").

-include("env/space_setup_utils.hrl").
-include("file/file_tree_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([all/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).

-export([
    tmp_dir_links_are_not_synced_test/1,
    tmp_dir_ignores_remote_trees_and_merge_keeps_flag_test/1,
    tmp_dir_works_with_remote_provider_unreachable_test/1,
    tmp_dir_links_are_not_synced_after_deleting_opened_file_test/1,
    tmp_dir_links_are_not_synced_after_recursive_deletion_test/1,
    moving_out_of_tmp_dir_makes_subtree_visible_to_remote_provider_test/1,
    moving_into_tmp_dir_is_rejected_test/1,
    newly_supported_space_is_marked_as_not_needing_traverse_test/1,
    file_links_reconciliation_traverse_test/1
]).

all() -> [
    tmp_dir_links_are_not_synced_test,
    tmp_dir_ignores_remote_trees_and_merge_keeps_flag_test,
    tmp_dir_works_with_remote_provider_unreachable_test,
    moving_out_of_tmp_dir_makes_subtree_visible_to_remote_provider_test,
    moving_into_tmp_dir_is_rejected_test,
    tmp_dir_links_are_not_synced_after_deleting_opened_file_test,
    tmp_dir_links_are_not_synced_after_recursive_deletion_test,
    newly_supported_space_is_marked_as_not_needing_traverse_test,
    % leaves paris's op-worker stopped, so it must run last
    file_links_reconciliation_traverse_test
].

-define(ATTEMPTS, 120).
-define(SPACE, space_krk_par_p).
-define(SYMLINK_VALUE, <<"some/symlink/target">>).
-define(REMOTE_GET_OF_TMP_DIR_LINKS, remote_get_of_tmp_dir_links).
-define(LINKS_DOC_RECEIVED, links_doc_received).

%%%===================================================================
%%% Test functions
%%%===================================================================

tmp_dir_links_are_not_synced_test(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    KrakowSessId = oct_background:get_user_session_id(user1, krakow),
    KrakowId = oct_background:get_provider_id(krakow),
    TestDirName = gen_test_dir_name(?FUNCTION_NAME),
    spy_file_meta_links_docs_received(paris),

    #object{guid = TestDirGuid, children = [
        #object{guid = D1Guid, children = [#object{guid = F1Guid, name = F1Name}]},
        #object{guid = D2Guid},
        #object{guid = F2Guid},
        #object{guid = SymlinkGuid}
    ]} = create_file_tree_in_tmp_dir(krakow, SpaceId, #dir_spec{name = TestDirName, children = [
        #dir_spec{children = [#file_spec{}]},
        #dir_spec{},
        #file_spec{},
        #symlink_spec{symlink_value = ?SYMLINK_VALUE}
    ]}),
    % D2 is empty, so its links forest is created by the rename
    ?assertMatch({ok, _}, lfm_proxy:mv(KrakowNode, KrakowSessId, ?FILE_REF(F1Guid), ?FILE_REF(D2Guid), F1Name)),
    ?assertEqual(ok, lfm_proxy:unlink(KrakowNode, KrakowSessId, ?FILE_REF(F2Guid))),

    await_sync_barrier(SpaceId, ?FUNCTION_NAME, krakow, paris),

    lists:foreach(fun(Guid) ->
        assert_links_tree_absent(paris, file_id:guid_to_uuid(Guid), KrakowId)
    end, [TestDirGuid, D1Guid, D2Guid]),
    assert_no_links_docs_received(paris, [tmp_dir:uuid(SpaceId) | [file_id:guid_to_uuid(G) || G <- [TestDirGuid, D1Guid, D2Guid]]]),

    assert_file_metas_absent(paris, [
        file_id:guid_to_uuid(Guid) || Guid <- [TestDirGuid, SymlinkGuid, D1Guid, D2Guid, F1Guid, F2Guid]
    ]),

    ?assertEqual({error, not_found}, get_links(paris, tmp_dir:uuid(SpaceId), KrakowId, TestDirName)).


tmp_dir_ignores_remote_trees_and_merge_keeps_flag_test(_Config) ->
    SpaceId = set_up_space_supported_by_both_providers(?FUNCTION_NAME),
    KrakowId = oct_background:get_provider_id(krakow),
    ParisId = oct_background:get_provider_id(paris),
    ParisNode = oct_background:get_random_provider_node(paris),
    ParisSessId = oct_background:get_user_session_id(user1, paris),
    TmpGuid = tmp_dir:guid(SpaceId),
    TmpUuid = tmp_dir:uuid(SpaceId),

    StaleName = inject_stale_synced_tmp_dir_link_on_krakow(SpaceId),
    ?assertMatch({ok, [_]}, get_links(paris, TmpUuid, KrakowId, StaleName), ?ATTEMPTS),

    StaleNameWithSuffix = ?CONFLICTING_LOGICAL_FILE_NAME(StaleName, (binary:part(KrakowId, 0, 4))),
    TmpDirChildrenNames = [Name || {_, Name} <- list_tmp_dir_children(paris, SpaceId)],
    lists:foreach(fun(Name) ->
        ?assertNot(lists:member(Name, TmpDirChildrenNames)),
        ?assertEqual({error, ?ENOENT}, lfm_proxy:get_child_attr(ParisNode, ParisSessId, TmpGuid, Name))
    end, [StaleName, StaleNameWithSuffix]),
    ?assertEqual({ok, [ParisId]}, opw_test_rpc:call(paris, file_meta_forest, get_trees, [TmpUuid])),
    % the stale remote name must not block creating a local file with the same name
    #object{guid = SameNameFileGuid} = create_file_tree_in_tmp_dir(paris, SpaceId, #file_spec{name = StaleName}),
    ?assertMatch({ok, #file_attr{guid = SameNameFileGuid, name = StaleName}},
        lfm_proxy:get_child_attr(ParisNode, ParisSessId, TmpGuid, StaleName)),
    ?assertEqual(
        [{SameNameFileGuid, StaleName}],
        [Child || {_, Name} = Child <- list_tmp_dir_children(paris, SpaceId),
            Name =:= StaleName orelse Name =:= StaleNameWithSuffix]
    ),

    % without keeping the local flag on the forest merge, paris's merged tmp forest would be
    % broadcast as synced and krakow would see paris's tree
    create_file_tree_in_tmp_dir(paris, SpaceId, #file_spec{}),
    await_sync_barrier(SpaceId, ?FUNCTION_NAME, paris, krakow),
    assert_links_tree_absent(krakow, TmpUuid, ParisId).


tmp_dir_works_with_remote_provider_unreachable_test(_Config) ->
    % a space of its own: krakow's tmp dir links_node docs must not be on paris yet - reading
    % a tree whose nodes are present locally never calls the remote driver, so the final
    % assertion could not fail
    SpaceId = set_up_space_supported_by_both_providers(?FUNCTION_NAME),
    mock_tmp_dir_links_nodes_never_reaching_paris(SpaceId),
    KrakowId = oct_background:get_provider_id(krakow),
    ParisNode = oct_background:get_random_provider_node(paris),
    ParisSessId = oct_background:get_user_session_id(user1, paris),
    TmpGuid = tmp_dir:guid(SpaceId),
    TmpUuid = tmp_dir:uuid(SpaceId),

    inject_stale_synced_tmp_dir_link_on_krakow(SpaceId),
    % links_node docs of tmp are dropped by the dbsync mock, so only the forest arrives
    ?assertEqual(true, has_links_tree(paris, TmpUuid, KrakowId), ?ATTEMPTS),
    #object{guid = ParisFileGuid, name = ParisFileName} = create_file_tree_in_tmp_dir(paris, SpaceId, #file_spec{}),

    ?assertEqual(
        expected_paris_own_tmp_children(SpaceId, [{ParisFileGuid, ParisFileName}]),
        list_tmp_dir_children(paris, SpaceId)
    ),
    ?assertMatch({ok, #file_attr{guid = ParisFileGuid}},
        lfm_proxy:get_child_attr(ParisNode, ParisSessId, TmpGuid, ParisFileName)),
    ?assertNotReceivedMatch({?REMOTE_GET_OF_TMP_DIR_LINKS, _}, timer:seconds(1)).


tmp_dir_links_are_not_synced_after_deleting_opened_file_test(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    KrakowSessId = oct_background:get_user_session_id(user1, krakow),
    OpenedDeletedFilesDirUuid = file_id:guid_to_uuid(opened_deleted_files_dir:guid(SpaceId)),
    spy_file_meta_links_docs_received(paris),

    #object{guid = TestDirGuid, children = [#object{guid = FileGuid}]} = create_file_tree_in_tmp_dir(
        krakow, SpaceId, #dir_spec{name = gen_test_dir_name(?FUNCTION_NAME), children = [#file_spec{}]}
    ),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    HiddenLinkUuid = opw_test_rpc:call(krakow, fslogic_file_id, gen_deleted_opened_file_link_uuid, [FileUuid]),
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(KrakowNode, KrakowSessId, ?FILE_REF(FileGuid), rdwr)),
    ?assertEqual(ok, lfm_proxy:unlink(KrakowNode, KrakowSessId, ?FILE_REF(FileGuid))),
    assert_dir_children(krakow, TestDirGuid, []),
    % until closed, the file is kept under a hidden hardlink named after its uuid
    ?assertMatch({ok, [_]}, get_file_meta_forest_link(krakow, OpenedDeletedFilesDirUuid, FileUuid), ?ATTEMPTS),
    ?assertEqual(ok, lfm_proxy:close(KrakowNode, Handle)),
    ?assertEqual({error, not_found}, get_file_meta_forest_link(krakow, OpenedDeletedFilesDirUuid, FileUuid), ?ATTEMPTS),

    await_sync_barrier(SpaceId, ?FUNCTION_NAME, krakow, paris),

    assert_no_links_docs_received(paris, [file_id:guid_to_uuid(TestDirGuid), OpenedDeletedFilesDirUuid]),
    assert_file_metas_absent(paris, [file_id:guid_to_uuid(TestDirGuid), FileUuid, HiddenLinkUuid]).


tmp_dir_links_are_not_synced_after_recursive_deletion_test(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    KrakowSessId = oct_background:get_user_session_id(user1, krakow),
    TestDirName = gen_test_dir_name(?FUNCTION_NAME),
    spy_file_meta_links_docs_received(paris),

    #object{guid = TestDirGuid, children = [#object{guid = SubDirGuid}, _]} = TestDir = create_file_tree_in_tmp_dir(
        krakow, SpaceId, #dir_spec{name = TestDirName, children = [
            #dir_spec{children = [#file_spec{}]},
            #file_spec{}
        ]}
    ),
    ?assertEqual(ok, lfm_proxy:rm_recursive(KrakowNode, KrakowSessId, ?FILE_REF(TestDirGuid))),
    % the root is deleted last, so its absence means the whole subtree is gone
    ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(KrakowNode, KrakowSessId, ?FILE_REF(TestDirGuid)), ?ATTEMPTS),
    ?assertNot(lists:keymember(TestDirName, 2, list_tmp_dir_children(krakow, SpaceId))),

    await_sync_barrier(SpaceId, ?FUNCTION_NAME, krakow, paris),

    assert_no_links_docs_received(paris, [
        tmp_dir:uuid(SpaceId), trash_dir:uuid(SpaceId),
        file_id:guid_to_uuid(TestDirGuid), file_id:guid_to_uuid(SubDirGuid)
    ]),
    assert_file_metas_absent(paris, [file_id:guid_to_uuid(Guid) || Guid <- list_guids(TestDir)]).


moving_out_of_tmp_dir_makes_subtree_visible_to_remote_provider_test(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    UserId = oct_background:get_user_id(user1),
    KrakowId = oct_background:get_provider_id(krakow),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    KrakowSessId = oct_background:get_user_session_id(user1, krakow),

    #object{guid = TmpTestDirGuid, children = [MovedDir, MovedFile]} = create_file_tree_in_tmp_dir(
        krakow, SpaceId, #dir_spec{name = gen_test_dir_name(?FUNCTION_NAME), children = [
            #dir_spec{children = [
                #dir_spec{children = [#file_spec{content = ?RAND_STR()}]},
                #file_spec{content = ?RAND_STR()},
                #symlink_spec{symlink_value = ?SYMLINK_VALUE}
            ]},
            #file_spec{content = ?RAND_STR()}
        ]}
    ),
    #object{guid = TargetDirGuid} = TargetDir = file_tree_test_utils:create_file_tree(
        UserId, space_dir:guid(SpaceId), krakow, #dir_spec{name = gen_test_dir_name(?FUNCTION_NAME)}
    ),

    lists:foreach(fun(#object{guid = Guid, name = Name}) ->
        ?assertMatch({ok, _}, lfm_proxy:mv(KrakowNode, KrakowSessId, ?FILE_REF(Guid), ?FILE_REF(TargetDirGuid), Name))
    end, [MovedDir, MovedFile]),

    ExpectedTargetDir = TargetDir#object{children = [MovedDir, MovedFile]},
    assert_dir_children(krakow, TmpTestDirGuid, []),
    assert_dir_children(krakow, TargetDirGuid, children_names_and_guids(ExpectedTargetDir)),
    % checks attrs, links and distribution of the whole moved subtree
    file_tree_test_utils:await_sync(KrakowId, [oct_background:get_provider_id(paris)], UserId, ExpectedTargetDir),
    ParisNode = oct_background:get_random_provider_node(paris),
    ParisSessId = oct_background:get_user_session_id(user1, paris),
    lists:foreach(fun(#object{guid = Guid, content = Content}) ->
        ?assertEqual(Content, lfm_test_utils:read_file(ParisNode, ParisSessId, Guid, byte_size(Content)))
    end, list_regular_files(ExpectedTargetDir)).


moving_into_tmp_dir_is_rejected_test(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    #object{guid = SourceDirGuid, name = SourceDirName, children = SourceDirChildren} = SourceDir =
        file_tree_test_utils:create_and_sync_file_tree(user1, ?SPACE, #dir_spec{name = gen_test_dir_name(?FUNCTION_NAME), children = [
            #dir_spec{children = [#file_spec{}]},
            #file_spec{},
            #symlink_spec{symlink_value = ?SYMLINK_VALUE}
        ]}, krakow),

    lists:foreach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessId = oct_background:get_user_session_id(user1, Provider),
        #object{guid = TmpTestDirGuid} = create_file_tree_in_tmp_dir(
            Provider, SpaceId, #dir_spec{name = gen_test_dir_name(?FUNCTION_NAME)}
        ),
        lists:foreach(fun({#object{guid = Guid, name = Name}, TargetParentGuid}) ->
            ?assertEqual({error, ?EINVAL},
                lfm_proxy:mv(Node, SessId, ?FILE_REF(Guid), ?FILE_REF(TargetParentGuid), Name))
        end, [{Moved, TargetParentGuid} || Moved <- [SourceDir | SourceDirChildren],
            TargetParentGuid <- [tmp_dir:guid(SpaceId), TmpTestDirGuid]]),
        assert_dir_children(Provider, TmpTestDirGuid, [])
    end, [krakow, paris]),

    lists:foreach(fun(Provider) ->
        Node = oct_background:get_random_provider_node(Provider),
        SessId = oct_background:get_user_session_id(user1, Provider),
        ?assertMatch({ok, #file_attr{guid = SourceDirGuid}},
            lfm_proxy:get_child_attr(Node, SessId, space_dir:guid(SpaceId), SourceDirName)),
        assert_dir_children(Provider, SourceDirGuid, children_names_and_guids(SourceDir))
    end, [krakow, paris]).


newly_supported_space_is_marked_as_not_needing_traverse_test(_Config) ->
    SpaceId = space_setup_utils:set_up_space(#space_spec{supports = [
        #support_spec{provider = paris}
    ]}),
    opw_test_rpc:call(paris, file_links_reconciliation_traverse, start_for_space, [SpaceId]),
    timer:sleep(timer:seconds(1)),
    test_utils:mock_assert_num_calls_sum(oct_background:get_provider_nodes(paris),
        file_links_reconciliation_traverse, task_started, 2, 0),
    ok = opw_test_rpc:call(paris, file_links_reconciliation_traverse, mark_traverse_needed_for_space, [SpaceId]),
    ok = opw_test_rpc:call(paris, file_links_reconciliation_traverse, start_for_space, [SpaceId]),
    test_utils:mock_assert_num_calls_sum(oct_background:get_provider_nodes(paris),
        file_links_reconciliation_traverse, task_started, 2, 1, ?ATTEMPTS).


file_links_reconciliation_traverse_test(Config) ->
    ChildrenCount = 100,
    #object{guid = DirGuid, children = ChildrenObjects} = file_tree_test_utils:create_file_tree(
        oct_background:get_user_id(user1),
        space_dir:guid(oct_background:get_space_id(space1)),
        oct_background:get_provider_id(paris),
        #dir_spec{children = lists:duplicate(ChildrenCount, #file_spec{})}
    ),
    wait_for_dbsynced_docs(file_meta, synced, ChildrenCount + 1), % +1 from parent dir doc (space doc was synced before test started)
    % only the parent dir's forest is created - paris's tree in the space dir's forest exists since
    % the support, so adding a link there does not resend that forest
    wait_for_dbsynced_doc(links_forest, get_file_meta_forest_doc_key(krakow, file_id:guid_to_uuid(DirGuid))),
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
    
    ?assertEqual(ok, opw_test_rpc:call(KrakowNode, file_links_reconciliation_traverse, mark_traverse_needed_for_space,
        [oct_background:get_space_id(space1)])), % space was marked as not needing traverse after being supported
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

set_up_space_supported_by_both_providers(TestCase) ->
    ScenarioSpaceId = oct_background:get_space_id(?SPACE),
    space_setup_utils:set_up_space(#space_spec{
        name = TestCase,
        owner = user1,
        supports = [
            #support_spec{provider = krakow, storage_spec = opt_spaces:get_storage_id(krakow, ScenarioSpaceId)},
            #support_spec{provider = paris, storage_spec = opt_spaces:get_storage_id(paris, ScenarioSpaceId)}
        ]
    }).


%% @private
gen_test_dir_name(TestCase) ->
    <<(atom_to_binary(TestCase))/binary, "_", (?RAND_STR())/binary>>.


%% @private
create_file_tree_in_tmp_dir(CreationProvider, SpaceId, ObjectSpec) ->
    file_tree_test_utils:create_file_tree(
        oct_background:get_user_id(user1), tmp_dir:guid(SpaceId), CreationProvider, ObjectSpec
    ).


%% @private
%% @doc
%% Waits until TargetProvider has applied every change SourceProvider made in the space before
%% the call: first until TargetProvider sees a file created after them (so they were broadcast),
%% then until TargetProvider's dbsync sequence of SourceProvider equals SourceProvider's own
%% (a batch is applied in parallel, so seeing its last document does not mean the earlier ones
%% were applied).
%% @end
await_sync_barrier(SpaceId, TestCase, SourceProvider, TargetProvider) ->
    #object{children = [#object{guid = BarrierGuid}]} = file_tree_test_utils:create_file_tree(
        oct_background:get_user_id(user1), space_dir:guid(SpaceId), SourceProvider,
        #dir_spec{name = gen_test_dir_name(TestCase), children = [#file_spec{}]}
    ),
    TargetNode = oct_background:get_random_provider_node(TargetProvider),
    TargetSessId = oct_background:get_user_session_id(user1, TargetProvider),
    ?assertMatch({ok, _}, lfm_proxy:stat(TargetNode, TargetSessId, ?FILE_REF(BarrierGuid)), ?ATTEMPTS),
    ?assertEqual(true, are_dbsync_seqs_equal(SpaceId, SourceProvider, TargetProvider), ?ATTEMPTS),
    ok.


%% @private
are_dbsync_seqs_equal(SpaceId, SourceProvider, TargetProvider) ->
    SourceProviderId = oct_background:get_provider_id(SourceProvider),
    GetSeq = fun(Provider) ->
        opw_test_rpc:call(Provider, dbsync_state, get_seq, [SpaceId, SourceProviderId])
    end,
    GetSeq(SourceProvider) =:= GetSeq(TargetProvider).


%% @private
%% @doc
%% Simulates a provider on an old version, that writes the tmp dir's links as synced.
%% The forest is forced into changes as well, because krakow's tmp dir forest already
%% exists (created not synced at space support) and adding a link to an existing tree does
%% not save the forest again.
%% @end
inject_stale_synced_tmp_dir_link_on_krakow(SpaceId) ->
    KrakowId = oct_background:get_provider_id(krakow),
    TmpUuid = tmp_dir:uuid(SpaceId),
    StaleName = <<"stale_", (?RAND_STR())/binary>>,
    StaleTargetUuid = ?RAND_STR(),
    ?assertMatch({ok, _}, opw_test_rpc:call(krakow, fun() ->
        SyncedCtx = (file_meta:get_ctx())#{scope => SpaceId, ignore_in_changes => false},
        {ok, _} = datastore_model:add_links(SyncedCtx, TmpUuid, KrakowId, {StaleName, StaleTargetUuid}),
        ok = datastore_model:ensure_forest_in_changes(SyncedCtx, TmpUuid, KrakowId),
        {ok, StaleName}
    end)),
    StaleName.


%% @private
expected_paris_own_tmp_children(SpaceId, CreatedChildren) ->
    OpenedDeletedFilesDir = {opened_deleted_files_dir:guid(SpaceId), ?OPENED_DELETED_FILES_DIR_DIR_NAME},
    lists:sort([OpenedDeletedFilesDir | CreatedChildren]).


%% @private
list_tmp_dir_children(ProviderSelector, SpaceId) ->
    {ok, Children} = ?assertMatch({ok, _}, list_children(ProviderSelector, tmp_dir:guid(SpaceId))),
    Children.


%% @private
assert_dir_children(ProviderSelector, DirGuid, ExpectedChildren) ->
    ?assertEqual({ok, lists:sort(ExpectedChildren)}, list_children(ProviderSelector, DirGuid), ?ATTEMPTS).


%% @private
list_children(ProviderSelector, DirGuid) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    SessId = oct_background:get_user_session_id(user1, ProviderSelector),
    case lfm_proxy:get_children(Node, SessId, ?FILE_REF(DirGuid), #{limit => 100, tune_for_large_continuous_listing => false}) of
        {ok, Children, _} -> {ok, lists:sort(Children)};
        {error, _} = Error -> Error
    end.


%% @private
children_names_and_guids(#object{children = Children}) ->
    [{Guid, Name} || #object{guid = Guid, name = Name} <- Children].


%% @private
list_regular_files(#object{type = ?REGULAR_FILE_TYPE} = File) ->
    [File];
list_regular_files(#object{type = ?DIRECTORY_TYPE, children = Children}) ->
    lists:flatmap(fun list_regular_files/1, Children);
list_regular_files(#object{}) ->
    [].


%% @private
assert_links_tree_absent(ProviderSelector, ForestUuid, TreeId) ->
    case get_links_trees(ProviderSelector, ForestUuid) of
        {ok, TreeIds} -> ?assertNot(lists:member(TreeId, TreeIds));
        Result -> ?assertEqual({error, not_found}, Result)
    end.


%% @private
has_links_tree(ProviderSelector, ForestUuid, TreeId) ->
    case get_links_trees(ProviderSelector, ForestUuid) of
        {ok, TreeIds} -> lists:member(TreeId, TreeIds);
        _ -> false
    end.


%% @private
get_links_trees(ProviderSelector, ForestUuid) ->
    opw_test_rpc:call(ProviderSelector, fun() ->
        datastore_model:get_links_trees(file_meta:get_ctx(), ForestUuid)
    end).


%% @private
get_links(ProviderSelector, ForestUuid, TreeId, LinkName) ->
    opw_test_rpc:call(ProviderSelector, fun() ->
        datastore_model:get_links(file_meta:get_ctx(), ForestUuid, TreeId, LinkName)
    end).


%% @private
mock_tmp_dir_links_nodes_never_reaching_paris(SpaceId) ->
    ParisNodes = oct_background:get_provider_nodes(paris),
    TmpDirLinksKey = get_file_meta_links_key(paris, tmp_dir:uuid(SpaceId)),
    TestPid = self(),
    ok = test_utils:mock_new(ParisNodes, [dbsync_changes, datastore_remote_driver], [passthrough]),
    ok = test_utils:mock_expect(ParisNodes, dbsync_changes, apply, fun
        (#document{value = #links_node{model = file_meta, key = LinksKey}}) when LinksKey =:= TmpDirLinksKey ->
            ok;
        (Doc) ->
            meck:passthrough([Doc])
    end),
    ok = test_utils:mock_expect(ParisNodes, datastore_remote_driver, get_async, fun
        (#{routing_key := RoutingKey} = Ctx, Key) when RoutingKey =:= TmpDirLinksKey ->
            TestPid ! {?REMOTE_GET_OF_TMP_DIR_LINKS, Key},
            meck:passthrough([Ctx, Key]);
        (Ctx, Key) ->
            meck:passthrough([Ctx, Key])
    end),
    ok = test_utils:mock_expect(ParisNodes, datastore_remote_driver, wait, fun(_Futures) ->
        {error, interrupted_call}
    end).


%% @private
%% @doc
%% Returns the key carried by the file_meta links documents (forest, node, mask) of the
%% forest - its unique key, which is also the routing key of the forest's operations.
%% @end
get_file_meta_links_key(ProviderSelector, ForestUuid) ->
    opw_test_rpc:call(ProviderSelector, datastore_model, get_unique_key, [file_meta, ForestUuid]).


%% @private
get_file_meta_forest_doc_key(ProviderSelector, ForestUuid) ->
    opw_test_rpc:call(ProviderSelector, datastore_links, get_forest_id,
        [get_file_meta_links_key(ProviderSelector, ForestUuid)]).


%% @private
%% @doc
%% Reports to the test process the key of every file_meta links document
%% (forest, node, mask) that the provider receives through dbsync. Documents are applied as usual.
%% @end
spy_file_meta_links_docs_received(ProviderSelector) ->
    Nodes = oct_background:get_provider_nodes(ProviderSelector),
    TestPid = self(),
    ok = test_utils:mock_new(Nodes, dbsync_changes, [passthrough]),
    ok = test_utils:mock_expect(Nodes, dbsync_changes, apply, fun(#document{value = Value} = Doc) ->
        case Value of
            #links_forest{model = file_meta, key = LinksKey} -> TestPid ! {?LINKS_DOC_RECEIVED, LinksKey};
            #links_node{model = file_meta, key = LinksKey} -> TestPid ! {?LINKS_DOC_RECEIVED, LinksKey};
            #links_mask{model = file_meta, key = LinksKey} -> TestPid ! {?LINKS_DOC_RECEIVED, LinksKey};
            _ -> ok
        end,
        meck:passthrough([Doc])
    end).


%% @private
assert_no_links_docs_received(ProviderSelector, ForestUuids) ->
    WatchedLinksKeys = [get_file_meta_links_key(ProviderSelector, Uuid) || Uuid <- ForestUuids],
    ReceivedLinksKeys = flush_received_links_docs_keys([]),
    ?assertEqual([], lists:usort([Key || Key <- ReceivedLinksKeys, lists:member(Key, WatchedLinksKeys)])).


%% @private
flush_received_links_docs_keys(Acc) ->
    receive
        {?LINKS_DOC_RECEIVED, LinksKey} -> flush_received_links_docs_keys([LinksKey | Acc])
    after 0 ->
        Acc
    end.


%% @private
assert_file_metas_absent(ProviderSelector, Uuids) ->
    lists:foreach(fun(Uuid) ->
        ?assertEqual({error, not_found}, opw_test_rpc:call(ProviderSelector, file_meta, get_including_deleted, [Uuid]))
    end, Uuids).


%% @private
get_file_meta_forest_link(ProviderSelector, ForestUuid, LinkName) ->
    opw_test_rpc:call(ProviderSelector, file_meta_forest, get, [ForestUuid, all, LinkName]).


%% @private
list_guids(#object{guid = Guid, children = Children}) ->
    [Guid | lists:flatmap(fun list_guids/1, utils:ensure_defined(Children, []))].


%% @private
wait_for_dbsynced_docs(Model, Type, ExpectedNum) ->
    get_dbsynced_docs(Model, Type, ExpectedNum),
    ok.


%% @private
wait_for_dbsynced_doc(Model, Key) ->
    receive
        {file_links_reconciliation_traverse_test, synced, Model, Key} -> ok
    after timer:seconds(?ATTEMPTS) ->
        throw({document_not_synced, Model, Key})
    end.


%% @private
get_dbsynced_docs(Model, Type, ExpectedNum) ->
    get_dbsynced_docs(Model, Type, ExpectedNum, []).


%% @private
get_dbsynced_docs(_Model, _Type, 0, Acc) ->
    Acc;
get_dbsynced_docs(Model, Type, ExpectedNum, Acc) ->
    receive
        {file_links_reconciliation_traverse_test, Type, Model, Key} ->
            get_dbsynced_docs(Model, Type, ExpectedNum - 1, [Key | Acc])
    after timer:seconds(?ATTEMPTS) ->
        throw({documents_not_synced, Model, ExpectedNum})
    end.


%% @private
get_doc_from_db(Node, Key) ->
    opw_test_rpc:call(Node, couchbase_driver, get, [#{bucket => <<"onedata">>}, Key]).


%% @private
stop_op_worker(Config, ProviderSelector) ->
    op_worker_service_operation(Config, ProviderSelector, "stop").


%% @private
start_op_worker(Config, ProviderSelector) ->
    op_worker_service_operation(Config, ProviderSelector, "start").


%% @private
op_worker_service_operation(Config, ProviderSelector, ServiceOperation) ->
    OnenvScript = test_config:get_onenv_script_path(Config),
    lists:foreach(fun(Node) ->
        Pod = test_config:get_custom(Config, [pods, Node]),
        case shell_utils:execute([OnenvScript, "service", ServiceOperation, Pod, "-s", "worker"]) of
            {0, Stdout, _} ->
                % propagate output of onenv service command
                ct:pal("~ts", [Stdout]);
            {ExitCode, Stdout, Stderr} ->
                ct:pal(
                    "Error in op worker operation ~n~ts ~n~tp ~n~ts ~n~ts",
                    [ServiceOperation, ExitCode, Stdout, Stderr]
                ),
                throw(service_operation_failed)
        end
    end, oct_background:get_provider_nodes(ProviderSelector)).


%% @private
get_number_of_ongoing_traverses(Node) ->
    opw_test_rpc:call(Node, fun() ->
        datastore_model:fold_links(traverse_task:get_ctx(), <<"ONGOING_qos_traverse">>, all, fun(_, Acc) -> {ok, Acc + 1} end, 0, #{size => 100})
    end).


%% @private
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
    opt:init_per_suite([{?LOAD_MODULES, [?MODULE]} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {file_links_reconciliation_traverse_max_retry_sleep, timer:seconds(8)}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(newly_supported_space_is_marked_as_not_needing_traverse_test, Config) ->
    test_utils:mock_new(oct_background:get_provider_nodes(paris), file_links_reconciliation_traverse, [passthrough]),
    lfm_proxy:init(Config, false);
init_per_testcase(file_links_reconciliation_traverse_test, Config) ->
    % mock dbsync so it does not save link_node documents
    KrakowNodes = oct_background:get_provider_nodes(krakow),
    test_utils:mock_new(KrakowNodes, dbsync_changes, [passthrough]),
    TestPid = self(),
    ok = test_utils:mock_expect(KrakowNodes, dbsync_changes, apply,
        fun (#document{key = Key, value = #links_node{model = file_meta, key = RelatedKey}} = Doc) ->
                case space_dir:is_special(uuid, RelatedKey) of
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
    % the hook looks up the file's link in its parent, which fetches the ignored links_node from paris
    test_utils:mock_new(KrakowNodes, dbsync_file_meta_handler, [passthrough]),
    ok = test_utils:mock_expect(KrakowNodes, dbsync_file_meta_handler, file_meta_change_replicated,
        fun(_, _) -> ok end
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
    lfm_proxy:init(Config, false);
init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).


end_per_testcase(newly_supported_space_is_marked_as_not_needing_traverse_test, Config) ->
    test_utils:mock_unload(oct_background:get_provider_nodes(paris)),
    end_per_testcase(default, Config);
end_per_testcase(file_links_reconciliation_traverse_test, Config) ->
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
    end_per_testcase(default, Config);
end_per_testcase(Case, Config) when
    Case =:= tmp_dir_links_are_not_synced_test;
    Case =:= tmp_dir_works_with_remote_provider_unreachable_test;
    Case =:= tmp_dir_links_are_not_synced_after_deleting_opened_file_test;
    Case =:= tmp_dir_links_are_not_synced_after_recursive_deletion_test
->
    test_utils:mock_unload(oct_background:get_provider_nodes(paris)),
    end_per_testcase(default, Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
