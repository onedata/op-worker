%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests checking that links of the tmp dir's subtree are not synced
%%% between providers, that stale remote trees under the tmp dir
%%% are ignored and that files can be moved out of, but not into, the tmp dir.
%%% @end
%%%-------------------------------------------------------------------
-module(tmp_dir_test_SUITE).
-author("Michal Stanisz").

-include("env/space_setup_utils.hrl").
-include("file/file_tree_test.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/errors.hrl").
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
    moving_out_of_tmp_dir_makes_subtree_visible_to_remote_provider_test/1,
    moving_into_tmp_dir_is_rejected_test/1
]).

all() -> [
    tmp_dir_links_are_not_synced_test,
    tmp_dir_ignores_remote_trees_and_merge_keeps_flag_test,
    tmp_dir_works_with_remote_provider_unreachable_test,
    moving_out_of_tmp_dir_makes_subtree_visible_to_remote_provider_test,
    moving_into_tmp_dir_is_rejected_test
].

-define(ATTEMPTS, 60).
-define(SPACE, space_krk_par_p).
-define(SYMLINK_VALUE, <<"some/symlink/target">>).
-define(REMOTE_DRIVER_WAIT_CALLED, remote_driver_wait_called).

%%%===================================================================
%%% Test functions
%%%===================================================================

tmp_dir_links_are_not_synced_test(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    KrakowNode = oct_background:get_random_provider_node(krakow),
    KrakowSessId = oct_background:get_user_session_id(user1, krakow),
    KrakowId = oct_background:get_provider_id(krakow),
    TestDirName = gen_test_dir_name(?FUNCTION_NAME),

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

    lists:foreach(fun(Guid) ->
        Uuid = file_id:guid_to_uuid(Guid),
        ?assertEqual({error, not_found}, opw_test_rpc:call(paris, file_meta, get_including_deleted, [Uuid]))
    end, [TestDirGuid, SymlinkGuid, D1Guid, D2Guid, F1Guid, F2Guid]),

    ?assertNot(lists:keymember(TestDirName, 2, list_tmp_dir_children(paris, SpaceId))).


tmp_dir_ignores_remote_trees_and_merge_keeps_flag_test(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
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
    % the links_node docs of tmp must never have reached paris, which no longer holds in ?SPACE
    % once tmp_dir_ignores_remote_trees_and_merge_keeps_flag_test has run there
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
    ?assertNotReceivedMatch({?REMOTE_DRIVER_WAIT_CALLED, _}, timer:seconds(1)).


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
%% Creates a regular file in a dir of its own in the space dir on SourceProvider and waits
%% until TargetProvider sees it. As dbsync streams changes of a provider in order,
%% everything SourceProvider did before has either arrived or been skipped.
%% @end
await_sync_barrier(SpaceId, TestCase, SourceProvider, TargetProvider) ->
    #object{children = [#object{guid = BarrierGuid}]} = file_tree_test_utils:create_file_tree(
        oct_background:get_user_id(user1), space_dir:guid(SpaceId), SourceProvider,
        #dir_spec{name = gen_test_dir_name(TestCase), children = [#file_spec{}]}
    ),
    TargetNode = oct_background:get_random_provider_node(TargetProvider),
    TargetSessId = oct_background:get_user_session_id(user1, TargetProvider),
    ?assertMatch({ok, _}, lfm_proxy:stat(TargetNode, TargetSessId, ?FILE_REF(BarrierGuid)), ?ATTEMPTS),
    ok.


%% @private
%% @doc
%% Simulates a provider on an old version, that writes the tmp dir's links as synced.
%% The forest is forced into changes as well, because krakow's tmp dir forest already
%% exists (created unsynced at space support) and adding a link to an existing tree does
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
    TmpUuid = tmp_dir:uuid(SpaceId),
    TestPid = self(),
    ok = test_utils:mock_new(ParisNodes, [dbsync_changes, datastore_remote_driver], [passthrough]),
    ok = test_utils:mock_expect(ParisNodes, dbsync_changes, apply, fun
        (#document{value = #links_node{model = file_meta, key = ForestUuid}}) when ForestUuid =:= TmpUuid ->
            ok;
        (Doc) ->
            meck:passthrough([Doc])
    end),
    ok = test_utils:mock_expect(ParisNodes, datastore_remote_driver, wait, fun(Futures) ->
        TestPid ! {?REMOTE_DRIVER_WAIT_CALLED, Futures},
        {error, interrupted_call}
    end).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    opt:init_per_suite([{?LOAD_MODULES, [?MODULE]} | Config], #onenv_test_config{
        onenv_scenario = "2op"
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).


end_per_testcase(tmp_dir_works_with_remote_provider_unreachable_test, Config) ->
    test_utils:mock_unload(oct_background:get_provider_nodes(paris), [dbsync_changes, datastore_remote_driver]),
    end_per_testcase(default, Config);
end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).
