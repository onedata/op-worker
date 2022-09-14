%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of archivisation cancellation mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(archivisation_cancellation_test_SUITE).
-author("Michał Stanisz").


-include("onenv_test_utils.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/dataset/archive.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([
    all/0, groups/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    cancel_plain_local_archivisation_master_job/1,
    cancel_plain_remote_archivisation_master_job/1,
    cancel_plain_local_archivisation_slave_job/1,
    cancel_plain_remote_archivisation_slave_job/1,
    cancel_bagit_archivisation_master_job/1,
    cancel_bagit_archivisation_slave_job/1,
    cancel_plain_archivisation_master_job_cancel_dip/1,
    cancel_plain_archivisation_slave_job_cancel_dip/1,
    cancel_plain_archivisation_master_job_cancel_aip/1,
    cancel_plain_archivisation_slave_job_cancel_aip/1,
    
    cancel_local_verification_master_job/1,
    cancel_local_verification_slave_job/1,
    cancel_remote_verification_master_job/1,
    cancel_remote_verification_slave_job/1,
    cancel_verification_cancel_aip/1,
    cancel_verification_cancel_dip/1,
    
    nested_cancel_archivisation_cancel_parent/1,
    nested_cancel_archivisation_cancel_nested/1,
    nested_cancel_archivisation_cancel_parent_cancel_aip/1,
    nested_cancel_archivisation_cancel_nested_cancel_aip/1,
    nested_cancel_archivisation_cancel_parent_cancel_dip/1,
    nested_cancel_archivisation_cancel_nested_cancel_dip/1,
    
    nested_cancel_verification_cancel_parent/1,
    nested_cancel_verification_cancel_nested/1,
    nested_cancel_verification_cancel_parent_cancel_aip/1,
    nested_cancel_verification_cancel_nested_cancel_aip/1,
    nested_cancel_verification_cancel_parent_cancel_dip/1,
    nested_cancel_verification_cancel_nested_cancel_dip/1,
    
    cancel_preserved_archive_test/1
]).

groups() -> [
    {cancel_tests, [
        cancel_plain_local_archivisation_master_job,
        cancel_plain_remote_archivisation_master_job,
        cancel_plain_local_archivisation_slave_job,
        cancel_plain_remote_archivisation_slave_job,
        cancel_bagit_archivisation_master_job,
        cancel_bagit_archivisation_slave_job,
        cancel_plain_archivisation_master_job_cancel_dip,
        cancel_plain_archivisation_slave_job_cancel_dip,
        cancel_plain_archivisation_master_job_cancel_aip,
        cancel_plain_archivisation_slave_job_cancel_aip,

        cancel_local_verification_master_job,
        cancel_local_verification_slave_job,
        cancel_remote_verification_master_job,
        cancel_remote_verification_slave_job,
        cancel_verification_cancel_aip,
        cancel_verification_cancel_dip,
    
        nested_cancel_archivisation_cancel_parent,
        nested_cancel_archivisation_cancel_nested,
        nested_cancel_archivisation_cancel_parent_cancel_aip,
        nested_cancel_archivisation_cancel_nested_cancel_aip,
        nested_cancel_archivisation_cancel_parent_cancel_dip,
        nested_cancel_archivisation_cancel_nested_cancel_dip,
    
        nested_cancel_verification_cancel_parent,
        nested_cancel_verification_cancel_nested,
        nested_cancel_verification_cancel_parent_cancel_aip,
        nested_cancel_verification_cancel_nested_cancel_aip,
        nested_cancel_verification_cancel_parent_cancel_dip,
        nested_cancel_verification_cancel_nested_cancel_dip,
        
        cancel_preserved_archive_test
    ]}
].


all() -> [
    {group, cancel_tests}
].

-define(ATTEMPTS, 60).

-define(SPACE, space_krk_par_p).
-define(USER1, user1).
-define(TEST_DATA, <<"testdata">>).

% NOTE: setting cancelled_job_type = master_job with nested strategy other than no_nested 
% makes no sense, as nested archive will not be created.
-record(test_config, {
    cancelling_provider = krakow :: oct_background:entity_placeholder(),
    layout = random :: ?ARCHIVE_PLAIN_LAYOUT | ?ARCHIVE_BAGIT_LAYOUT | random,
    dip_strategy = no_dip :: no_dip | cancel_aip | cancel_dip,
    nested_strategy = no_nested :: no_nested | cancel_parent | cancel_nested,
    cancelled_traverse :: archivisation_traverse | archive_verification_traverse,
    cancelled_job_type = slave_job :: slave_job | master_job
}).


%===================================================================
% Test functions
%===================================================================

cancel_plain_local_archivisation_master_job(_Config) ->
    cancel_test_base(#test_config{
        cancelling_provider = krakow,
        layout = ?ARCHIVE_PLAIN_LAYOUT,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = master_job
    }).

cancel_plain_remote_archivisation_master_job(_Config) ->
    cancel_test_base(#test_config{
        cancelling_provider = paris,
        layout = ?ARCHIVE_PLAIN_LAYOUT,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = master_job
    }).

cancel_plain_local_archivisation_slave_job(_Config) ->
    cancel_test_base(#test_config{
        cancelling_provider = krakow,
        layout = ?ARCHIVE_PLAIN_LAYOUT,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = slave_job
    }).

cancel_plain_remote_archivisation_slave_job(_Config) ->
    cancel_test_base(#test_config{
        cancelling_provider = paris,
        layout = ?ARCHIVE_PLAIN_LAYOUT,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = slave_job
    }).

cancel_bagit_archivisation_master_job(_Config) ->
    cancel_test_base(#test_config{
        layout = ?ARCHIVE_BAGIT_LAYOUT,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = master_job
    }).

cancel_bagit_archivisation_slave_job(_Config) ->
    cancel_test_base(#test_config{
        layout = ?ARCHIVE_BAGIT_LAYOUT,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = slave_job
    }).

cancel_plain_archivisation_master_job_cancel_dip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_dip,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = master_job
    }).

cancel_plain_archivisation_slave_job_cancel_dip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_dip,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = slave_job
    }).

cancel_plain_archivisation_master_job_cancel_aip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_aip,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = master_job
    }).

cancel_plain_archivisation_slave_job_cancel_aip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_aip,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = slave_job
    }).

cancel_local_verification_master_job(_Config) ->
    cancel_test_base(#test_config{
        cancelling_provider = krakow,
        cancelled_traverse = archive_verification_traverse,
        cancelled_job_type = master_job
    }).

cancel_local_verification_slave_job(_Config) ->
    cancel_test_base(#test_config{
        cancelling_provider = krakow,
        cancelled_traverse = archive_verification_traverse,
        cancelled_job_type = slave_job
    }).

cancel_remote_verification_master_job(_Config) ->
    cancel_test_base(#test_config{
        cancelling_provider = paris,
        cancelled_traverse = archive_verification_traverse,
        cancelled_job_type = master_job
    }).

cancel_remote_verification_slave_job(_Config) ->
    cancel_test_base(#test_config{
        cancelling_provider = paris,
        cancelled_traverse = archive_verification_traverse,
        cancelled_job_type = slave_job
    }).

cancel_verification_cancel_aip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_aip,
        cancelled_traverse = archive_verification_traverse
    }).

cancel_verification_cancel_dip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_dip,
        cancelled_traverse = archive_verification_traverse
    }).

nested_cancel_archivisation_cancel_parent(_Config) ->
    cancel_test_base(#test_config{
        nested_strategy = cancel_parent,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = slave_job
    }).

nested_cancel_archivisation_cancel_nested(_Config) ->
    cancel_test_base(#test_config{
        nested_strategy = cancel_nested,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = slave_job
    }).

nested_cancel_archivisation_cancel_parent_cancel_aip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_aip,
        nested_strategy = cancel_parent,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = slave_job
    }).

nested_cancel_archivisation_cancel_nested_cancel_aip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_aip,
        nested_strategy = cancel_nested,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = slave_job
    }).

nested_cancel_archivisation_cancel_parent_cancel_dip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_dip,
        nested_strategy = cancel_parent,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = slave_job
    }).

nested_cancel_archivisation_cancel_nested_cancel_dip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_dip,
        nested_strategy = cancel_nested,
        cancelled_traverse = archivisation_traverse,
        cancelled_job_type = slave_job
    }).

nested_cancel_verification_cancel_parent(_Config) ->
    cancel_test_base(#test_config{
        nested_strategy = cancel_parent,
        cancelled_traverse = archive_verification_traverse,
        cancelled_job_type = slave_job
    }).

nested_cancel_verification_cancel_nested(_Config) ->
    cancel_test_base(#test_config{
        nested_strategy = cancel_nested,
        cancelled_traverse = archive_verification_traverse,
        cancelled_job_type = slave_job
    }).

nested_cancel_verification_cancel_parent_cancel_aip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_aip,
        nested_strategy = cancel_parent,
        cancelled_traverse = archive_verification_traverse,
        cancelled_job_type = slave_job
    }).

nested_cancel_verification_cancel_nested_cancel_aip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_aip,
        nested_strategy = cancel_nested,
        cancelled_traverse = archive_verification_traverse,
        cancelled_job_type = slave_job
    }).

nested_cancel_verification_cancel_parent_cancel_dip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_dip,
        nested_strategy = cancel_parent,
        cancelled_traverse = archive_verification_traverse,
        cancelled_job_type = slave_job
    }).

nested_cancel_verification_cancel_nested_cancel_dip(_Config) ->
    cancel_test_base(#test_config{
        dip_strategy = cancel_dip,
        nested_strategy = cancel_nested,
        cancelled_traverse = archive_verification_traverse,
        cancelled_job_type = slave_job
    }).


cancel_preserved_archive_test(_Config) ->
    #object{dataset = #dataset_object{archives = [#archive_object{id = ArchiveId}]}} = 
        setup_initial_environment(#test_config{}),
    archive_tests_utils:assert_archive_state(ArchiveId, ?ARCHIVE_PRESERVED, krakow, ?ATTEMPTS),
    ?assertEqual(ok, cancel_archivisation(paris, ArchiveId)),
    archive_tests_utils:assert_archive_state(ArchiveId, ?ARCHIVE_PRESERVED, [paris, krakow], ?ATTEMPTS).

%===================================================================
% Test bases
%===================================================================

cancel_test_base( #test_config{
    cancelling_provider = CancellingProvider,
    cancelled_traverse = CancelledTraverse
} = TestConfig) ->
    
    mock_required_functions(TestConfig),
    FileTreeObject = setup_initial_environment(TestConfig),
    
    {ArchiveToCancelId, CancelledArchivesToCheck, NotCancelledArchivesToCheck} =
        archives_to_check(TestConfig, FileTreeObject),
    
    InitialExpectedState = case CancelledTraverse of
        archivisation_traverse -> ?ARCHIVE_BUILDING;
        archive_verification_traverse -> ?ARCHIVE_VERIFYING
    end,
    archive_tests_utils:assert_archive_state(ArchiveToCancelId, InitialExpectedState, ?ATTEMPTS),
    
    ?assertEqual(ok, cancel_archivisation(CancellingProvider, ArchiveToCancelId)),
    
    archive_tests_utils:assert_archive_state(CancelledArchivesToCheck, ?ARCHIVE_CANCELLING, ?ATTEMPTS),
    
    continue_mocked_jobs(),
    
    archive_tests_utils:assert_archive_state(CancelledArchivesToCheck, ?ARCHIVE_CANCELLED, ?ATTEMPTS),
    archive_tests_utils:assert_archive_state(NotCancelledArchivesToCheck, ?ARCHIVE_PRESERVED, ?ATTEMPTS),
    
    case CancelledTraverse of
        archivisation_traverse ->
            assert_stats_not_full(CancelledArchivesToCheck),
            assert_archive_not_preserved(TestConfig, FileTreeObject);
        _ ->
            ok
    end.

%===================================================================
% Assertion functions
%===================================================================

% NOTE: this function assumes dir structure created in setup_initial_environment/1
assert_archive_not_preserved(#test_config{cancelled_job_type = master_job}, FileTreeObject) ->
    #object{dataset = #dataset_object{archives = [#archive_object{id = ArchiveId}]}} = FileTreeObject,
    ?assertMatch({ok, [], _}, list_children(get_data_dir_guid(ArchiveId)));
assert_archive_not_preserved(#test_config{cancelled_job_type = slave_job, nested_strategy = cancel_parent}, FileTreeObject) ->
    #object{dataset = #dataset_object{archives = [#archive_object{id = ArchiveId}]}} = FileTreeObject,
    {ok, [{DirGuid1, _}], _} = ?assertMatch({ok, [_], _}, list_children(get_data_dir_guid(ArchiveId))),
    {ok, [{DirGuid2, _}], _} = ?assertMatch({ok, [_], _}, list_children(DirGuid1)),
    % file was created in a nested archive, which was not cancelled and is symlinked in parent archive
    ?assertMatch({ok, [_], _}, list_children(DirGuid2));
assert_archive_not_preserved(#test_config{cancelled_job_type = slave_job, nested_strategy = cancel_nested}, FileTreeObject) ->
    #object{dataset = #dataset_object{archives = [#archive_object{id = ArchiveId}]}} = FileTreeObject,
    % creation of parent archive was successful, so the regular file was copied
    {ok, [{DirGuid1, _}], _} = ?assertMatch({ok, [_], _}, list_children(get_data_dir_guid(ArchiveId))),
    {ok, [{Guid1, _}, {Guid2, _}], _} = ?assertMatch({ok, [_, _], _}, list_children(DirGuid1)),
    DirGuid2 = case is_dir(Guid1) of
        true -> Guid1;
        false -> Guid2
    end,
    ?assertMatch({ok, [], _}, list_children(DirGuid2));
assert_archive_not_preserved(#test_config{cancelled_job_type = slave_job, nested_strategy = _}, FileTreeObject) ->
    #object{dataset = #dataset_object{archives = [#archive_object{id = ArchiveId}]}} = FileTreeObject,
    {ok, [{DirGuid1, _}], _} = ?assertMatch({ok, [_], _}, list_children(get_data_dir_guid(ArchiveId))),
    {ok, [{DirGuid2, _}], _} = ?assertMatch({ok, [_], _}, list_children(DirGuid1)),
    ?assertMatch({ok, [], _}, list_children(DirGuid2)).


assert_stats_not_full(ArchiveList) ->
    lists_utils:pforeach(fun(ArchiveId) ->
        {ok, #archive_info{stats = Stats}} = opt_archives:get_info(
            krakow, oct_background:get_user_session_id(?USER1, krakow), ArchiveId),
        DataSize = 2 * byte_size(?TEST_DATA),
        ?assertNotMatch(#archive_stats{files_archived = 2, bytes_archived = DataSize}, Stats)
    end, ArchiveList).


%===================================================================
% Helper functions
%===================================================================

setup_initial_environment(#test_config{
    layout = Layout,
    dip_strategy = DipStrategy,
    nested_strategy = NestedStrategy
}) ->
    onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{
            layout = case Layout of
                random -> lists_utils:random_element(?ARCHIVE_LAYOUTS);
                _ -> Layout
            end,
            include_dip = DipStrategy =/= no_dip,
            create_nested_archives = NestedStrategy =/= no_nested
        }}]},
        children = [
            #dir_spec{dataset = #dataset_spec{}, children = [#file_spec{content = ?TEST_DATA}]},
            #file_spec{content = ?TEST_DATA}
        ]
    }, krakow).


% NOTE: this function assumes dir structure created in setup_initial_environment/1
archives_to_check(#test_config{nested_strategy = no_nested} = TestConfig, FileTreeObject) ->
    #object{dataset = #dataset_object{archives = [#archive_object{id = ArchiveId}]}} = FileTreeObject,
    DipArchiveId = get_dip_archive_id(ArchiveId),
    case TestConfig#test_config.dip_strategy of
        no_dip -> {ArchiveId, [ArchiveId], []};
        cancel_aip -> {ArchiveId, [ArchiveId, DipArchiveId], []};
        cancel_dip -> {DipArchiveId, [ArchiveId, DipArchiveId], []}
    end;
archives_to_check(#test_config{nested_strategy = cancel_parent} = TestConfig, FileTreeObject) ->
    #object{dataset = #dataset_object{archives = [#archive_object{id = ArchiveId}]}} = FileTreeObject,
    DipArchiveId = get_dip_archive_id(ArchiveId),
    NestedArchiveId = get_nested_archive_id(FileTreeObject),
    NestedDipArchiveId = get_dip_archive_id(NestedArchiveId),
    case TestConfig#test_config.dip_strategy of
        no_dip -> {ArchiveId, [ArchiveId], [NestedArchiveId]};
        cancel_aip -> {ArchiveId, [ArchiveId, DipArchiveId], [NestedArchiveId, NestedDipArchiveId]};
        cancel_dip -> {DipArchiveId, [ArchiveId, DipArchiveId], [NestedArchiveId, NestedDipArchiveId]}
    end;
archives_to_check(#test_config{nested_strategy = cancel_nested} = TestConfig, FileTreeObject) ->
    #object{dataset = #dataset_object{archives = [#archive_object{id = ArchiveId}]}} = FileTreeObject,
    DipArchiveId = get_dip_archive_id(ArchiveId),
    NestedArchiveId = get_nested_archive_id(FileTreeObject),
    NestedDipArchiveId = get_dip_archive_id(NestedArchiveId),
    case TestConfig#test_config.dip_strategy of
        no_dip -> {NestedArchiveId, [NestedArchiveId], [ArchiveId]};
        cancel_aip -> {NestedArchiveId, [NestedArchiveId, NestedDipArchiveId], [ArchiveId, DipArchiveId]};
        cancel_dip -> {NestedDipArchiveId, [NestedArchiveId, NestedDipArchiveId], [ArchiveId, DipArchiveId]}
    end.


get_dip_archive_id(ArchiveId) ->
    {ok, DipArchiveId} = opw_test_rpc:call(oct_background:get_random_provider_node(krakow), archive, get_related_dip_id, [ArchiveId]),
    DipArchiveId.


list_children(Guid) ->
    lfm_proxy:get_children(
        oct_background:get_random_provider_node(krakow),
        oct_background:get_user_session_id(?USER1, krakow),
        #file_ref{guid = Guid, follow_symlink = true},
        #{tune_for_large_continuous_listing  => false}
    ).

get_data_dir_guid(ArchiveId) ->
    {ok, #document{value = #archive{
        data_dir_guid = ArchiveDataDirGuid
    }}} = opw_test_rpc:call(krakow, archive, get, [ArchiveId]),
    ArchiveDataDirGuid.


is_dir(Guid) ->
    lfm_proxy:is_dir(oct_background:get_random_provider_node(krakow), oct_background:get_user_session_id(?USER1, krakow), #file_ref{guid = Guid, follow_symlink = true}).

get_nested_archive_id(#object{children = [#object{dataset = #dataset_object{id = NestedDatasetId}}, _]}) ->
    {ok, {[{_, NestedArchiveId}], _}} = opt_archives:list(
        krakow, oct_background:get_user_session_id(?USER1, krakow), NestedDatasetId, #{offset => 0, limit => 1}),
    NestedArchiveId.

cancel_archivisation(ProviderId, ArchiveId) ->
    opt_archives:cancel_archivisation(
        ProviderId, oct_background:get_user_session_id(?USER1, ProviderId), ArchiveId).

%===================================================================
% Mock related functions
%===================================================================

mock_required_functions(#test_config{cancelled_traverse = TraverseModule, cancelled_job_type = master_job}) ->
    mock_job_function(TraverseModule, do_dir_master_job_unsafe, 2);
mock_required_functions(#test_config{cancelled_traverse = archivisation_traverse, cancelled_job_type = slave_job}) ->
    mock_job_function(archivisation_traverse, do_slave_job_unsafe, 3);
mock_required_functions(#test_config{cancelled_traverse = archive_verification_traverse, cancelled_job_type = slave_job}) ->
    mock_job_function(archive_verification_traverse, do_slave_job_unsafe, 2).


mock_job_function(Module, FunName, Arity) ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_new(Nodes, Module),
    TestProcess = self(),
    MockFunBase = fun(Args) ->
        Ref = make_ref(),
        TestProcess ! {stopped, Ref, self()},
        receive {continue, Ref} ->
            meck:passthrough(Args)
        end
    end,
    MockFun = case Arity of
        2 -> fun(Job, Arg2) -> MockFunBase([Job, Arg2]) end;
        3 -> fun(Job, Arg2, Arg3) -> MockFunBase([Job, Arg2, Arg3]) end
    end,
    test_utils:mock_expect(Nodes, Module, FunName, MockFun).


continue_mocked_jobs() ->
    receive {stopped, Ref, Pid} ->
        Pid ! {continue, Ref},
        continue_mocked_jobs()
    after 0 ->
        ok
    end.

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [?MODULE, archive_tests_utils]} | Config],
        #onenv_test_config{
            onenv_scenario = "2op",
            envs = [{op_worker, op_worker, [
                {fuse_session_grace_period_seconds, 24 * 60 * 60},
                {provider_token_ttl_sec, 24 * 60 * 60}
            ]}]
        }).


end_per_suite(Config) ->
    oct_background:end_per_suite(),
    dir_stats_test_utils:enable_stats_counting(Config).


init_per_group(_Group, Config) ->
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    test_utils:mock_unload(oct_background:get_all_providers_nodes(), archivisation_traverse),
    test_utils:mock_unload(oct_background:get_all_providers_nodes(), archive_verification_traverse), 
    ok.
