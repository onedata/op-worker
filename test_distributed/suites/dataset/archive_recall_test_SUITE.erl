%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of archives recall mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_recall_test_SUITE).
-author("Michal Stanisz").


-include("modules/dataset/archive.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").


%% exported for CT
-export([
    all/0, groups/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/2
]).

%% @TODO VFS-7617 Test more complex symlink examples - symlinks loops, symlinks in archive and nested archives
%% @TODO VFS-7617 Test external and internal symlinks
%% @TODO VFS-7617 Test archive without follow symlinks and recall
%% @TODO VFS-7617 Test recall options (name)
%% @TODO VFS-7617 Test recall block in already recalling dir
%% @TODO VFS-7617 Test rest api
%% @TODO VFS-7617 Test already recalling block
%% @TODO VFS-7617 Test stats with nested archives


%% tests
-export([
    recall_plain_simple_archive_test/1,
    recall_plain_simple_archive_dip_test/1,
    recall_bagit_simple_archive_test/1,
    recall_bagit_simple_archive_dip_test/1,
    recall_plain_empty_dir_archive_test/1,
    recall_plain_empty_dir_archive_dip_test/1,
    recall_bagit_empty_dir_archive_test/1,
    recall_bagit_empty_dir_archive_dip_test/1,
    recall_plain_single_file_archive_test/1,
    recall_plain_single_file_archive_dip_test/1,
    recall_bagit_single_file_archive_test/1,
    recall_bagit_single_file_archive_dip_test/1,
    recall_plain_nested_archive_test/1,
    recall_plain_nested_archive_dip_test/1,
    recall_bagit_nested_archive_test/1,
    recall_bagit_nested_archive_dip_test/1,
    recall_plain_containing_symlink_archive_test/1,
    recall_plain_containing_symlink_archive_dip_test/1,
    recall_bagit_containing_symlink_archive_test/1,
    recall_bagit_containing_symlink_archive_dip_test/1,
    
    recall_details_test/1
]).

groups() -> [
    {parallel_tests, [parallel], [
        recall_plain_simple_archive_test,
        recall_plain_simple_archive_dip_test,
        recall_bagit_simple_archive_test,
        recall_bagit_simple_archive_dip_test,
        recall_plain_empty_dir_archive_test,
        recall_plain_empty_dir_archive_dip_test,
        recall_bagit_empty_dir_archive_test,
        recall_bagit_empty_dir_archive_dip_test,
        recall_plain_single_file_archive_test,
        recall_plain_single_file_archive_dip_test,
        recall_bagit_single_file_archive_test,
        recall_bagit_single_file_archive_dip_test,
        recall_plain_nested_archive_test,
        recall_plain_nested_archive_dip_test,
        recall_bagit_nested_archive_test,
        recall_bagit_nested_archive_dip_test
%%      @TODO VFS-8851 Uncomment after archives with not resolved symlinks are properly recalled
%%        recall_plain_containing_symlink_archive_test,
%%        recall_plain_containing_symlink_archive_dip_test,
%%        recall_bagit_containing_symlink_archive_test,
%%        recall_bagit_containing_symlink_archive_dip_test
    ]},
    {sequential_tests, [
        recall_details_test
    ]}
].


all() -> [
    {group, parallel_tests},
    {group, sequential_tests}
].

-define(ATTEMPTS, 60).

-define(SPACE, space_krk_par_p).
-define(USER1, user1).


-define(RAND_NAME(), str_utils:rand_hex(20)).
-define(RAND_SIZE(), rand:uniform(50)).
-define(RAND_CONTENT(), crypto:strong_rand_bytes(?RAND_SIZE())).
-define(RAND_CONTENT(Size), crypto:strong_rand_bytes(Size)).

-define(RAND_JSON_METADATA(), begin
    lists:foldl(fun(_, AccIn) ->
        AccIn#{?RAND_NAME() => ?RAND_NAME()}
    end, #{}, lists:seq(1, rand:uniform(10)))
end).

%===================================================================
% Parallel tests - tests which can be safely run in parallel
% as they do not interfere with any other test.
%===================================================================

recall_plain_simple_archive_test(_Config) ->
    recall_simple_archive_base(?ARCHIVE_PLAIN_LAYOUT, false).

recall_bagit_simple_archive_test(_Config) ->
    recall_simple_archive_base(?ARCHIVE_BAGIT_LAYOUT, false).

recall_plain_simple_archive_dip_test(_Config) ->
    recall_simple_archive_base(?ARCHIVE_PLAIN_LAYOUT, true).

recall_bagit_simple_archive_dip_test(_Config) ->
    recall_simple_archive_base(?ARCHIVE_BAGIT_LAYOUT, true).

recall_plain_empty_dir_archive_test(_Config) ->
    recall_empty_dir_archive_base(?ARCHIVE_PLAIN_LAYOUT, false).

recall_bagit_empty_dir_archive_test(_Config) ->
    recall_empty_dir_archive_base(?ARCHIVE_BAGIT_LAYOUT, false).

recall_plain_empty_dir_archive_dip_test(_Config) ->
    recall_empty_dir_archive_base(?ARCHIVE_PLAIN_LAYOUT, true).

recall_bagit_empty_dir_archive_dip_test(_Config) ->
    recall_empty_dir_archive_base(?ARCHIVE_BAGIT_LAYOUT, true).

recall_plain_single_file_archive_test(_Config) ->
    recall_single_file_archive_base(?ARCHIVE_PLAIN_LAYOUT, false).

recall_bagit_single_file_archive_test(_Config) ->
    recall_single_file_archive_base(?ARCHIVE_BAGIT_LAYOUT, false).

recall_plain_single_file_archive_dip_test(_Config) ->
    recall_single_file_archive_base(?ARCHIVE_PLAIN_LAYOUT, true).

recall_bagit_single_file_archive_dip_test(_Config) ->
    recall_single_file_archive_base(?ARCHIVE_BAGIT_LAYOUT, true).

recall_plain_nested_archive_test(_Config) ->
    recall_nested_archive_base(?ARCHIVE_PLAIN_LAYOUT, false).

recall_bagit_nested_archive_test(_Config) ->
    recall_nested_archive_base(?ARCHIVE_BAGIT_LAYOUT, false).

recall_plain_nested_archive_dip_test(_Config) ->
    recall_nested_archive_base(?ARCHIVE_PLAIN_LAYOUT, true).

recall_bagit_nested_archive_dip_test(_Config) ->
    recall_nested_archive_base(?ARCHIVE_BAGIT_LAYOUT, true).

recall_plain_containing_symlink_archive_test(_Config) ->
    recall_containing_symlink_archive_base(?ARCHIVE_PLAIN_LAYOUT, false).

recall_bagit_containing_symlink_archive_test(_Config) ->
    recall_containing_symlink_archive_base(?ARCHIVE_BAGIT_LAYOUT, false).

recall_plain_containing_symlink_archive_dip_test(_Config) ->
    recall_containing_symlink_archive_base(?ARCHIVE_PLAIN_LAYOUT, true).

recall_bagit_containing_symlink_archive_dip_test(_Config) ->
    recall_containing_symlink_archive_base(?ARCHIVE_BAGIT_LAYOUT, true).


%===================================================================
% Sequential tests - tests which must be run sequentially because 
% of used mocks.
%===================================================================

recall_details_test(_Config) ->
    FileSize1 = rand:uniform(20),
    % file with size extending copy buffer so progress is reported multiple times
    FileSize2 = rand:uniform(200) + 100 * 1024 * 1024, 
    {ArchiveId, _TargetParentGuid} = recall_test_setup(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = ?ARCHIVE_PLAIN_LAYOUT}}]},
        children = [
            #file_spec{content = ?RAND_CONTENT(FileSize1)},
            #file_spec{content = ?RAND_CONTENT(FileSize2)}
            
        ]
    }),
    {ok, #document{value = #archive{recalls = [RecallId]}}} =
        ?assertMatch({ok, #document{value = #archive{recalls = [_]}}},
            opw_test_rpc:call(krakow, archive, get, [ArchiveId]), ?ATTEMPTS),
    
    % wait for archive recall traverse to finish (mocked in init_per_testcase)
    Pid = receive
        {recall_traverse_finished, P} -> P
    after timer:seconds(?ATTEMPTS) ->
        throw({error, recall_traverse_did_not_finish})
    end,
    
    % check archive_recall document
    Providers = oct_background:get_space_supporting_providers(?SPACE),
    Timestamp = time_test_utils:get_frozen_time_millis(),
    TotalBytes = FileSize1 + FileSize2,
    lists:foreach(fun(Provider) ->
        ?assertMatch({ok, #archive_recall{
            source_archive = ArchiveId,
            start_timestamp = Timestamp,
            finish_timestamp = undefined,
            target_files = 2,
            target_bytes = TotalBytes
        }},
            opw_test_rpc:call(Provider, archive_recall, get_details, [RecallId]), ?ATTEMPTS)
    end, Providers),
    
    % check recall stats (stats are only stored on provider performing recall)
    ?assertMatch({ok, #{
          {<<"currentBytes">>,<<"hour">>} := [{_,{_, TotalBytes}}],
          {<<"currentBytes">>,<<"minute">>} := [{_,{_, TotalBytes}}],
          {<<"currentFiles">>,<<"hour">>} := [{_,{2,2}}],
          {<<"currentFiles">>,<<"minute">>} := [{_,{2,2}}]
    }}, opw_test_rpc:call(krakow, archive_recall, get_stats, [RecallId]), ?ATTEMPTS),
    ?assertEqual(?ERROR_NOT_FOUND, opw_test_rpc:call(paris, archive_recall, get_stats, [RecallId])),
    
    % run archive_recall_traverse:task_finished
    Pid ! continue,

    %% @TODO VFS-7617 check finish timestamp etc
    ok.

%===================================================================
% Test bases
%===================================================================

recall_simple_archive_base(Layout, IncludeDip) ->
    recall_test_base(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip}}]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
        children = [
            #file_spec{metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}},
            #file_spec{metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}},
            #file_spec{metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}}
        ]
    }).

recall_empty_dir_archive_base(Layout, IncludeDip) ->
    recall_test_base(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip}}]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}
    }).

recall_single_file_archive_base(Layout, IncludeDip) ->
    recall_test_base(#file_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip}}]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}
    }).

recall_nested_archive_base(Layout, IncludeDip) ->
    recall_test_base(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip, create_nested_archives = true}}]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
        children = [#dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip}}]},
            metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}
        }]
    }).

recall_containing_symlink_archive_base(Layout, IncludeDip) ->
    recall_test_base(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip, follow_symlinks = false}}]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
        children = [#symlink_spec{symlink_value = <<"some/dummy/value">>}]
    }).


recall_test_base(StructureSpec) ->
    recall_test_base(StructureSpec, follow_symlinks).

recall_test_base(StructureSpec, SymlinkMode) ->
    SessionId = oct_background:get_user_session_id(?USER1, krakow),
    {ArchiveId, TargetParentGuid} = recall_test_setup(StructureSpec),
    {ok, ArchiveDataDirGuid} = opw_test_rpc:call(krakow, archive, get_data_dir_guid, [ArchiveId]),
    archive_tests_utils:assert_copied(oct_background:get_random_provider_node(krakow), SessionId, 
        get_direct_child(ArchiveDataDirGuid), get_direct_child(TargetParentGuid), SymlinkMode == follow_symlinks, ?ATTEMPTS),
    ?assertThrow(?ERROR_ALREADY_EXISTS, opw_test_rpc:call(krakow, mi_archives, init_recall, % @TODO VFS-7617 opt_archives
        [oct_background:get_user_session_id(?USER1, krakow), ArchiveId, TargetParentGuid, default])).


recall_test_setup(StructureSpec) ->
    SessionId = oct_background:get_user_session_id(?USER1, krakow),
    #object{
        dataset = #dataset_object{archives = [#archive_object{id = ArchiveId}]
        }} = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, StructureSpec),
    #object{guid = TargetParentGuid} = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #dir_spec{}),
    opw_test_rpc:call(krakow, mi_archives, init_recall, [SessionId, ArchiveId, TargetParentGuid, default]), % @TODO VFS-7617 opt_archives
    {ArchiveId, TargetParentGuid}.


%===================================================================
% Helper functions
%===================================================================

get_direct_child(Guid) ->
    SessionId = oct_background:get_user_session_id(?USER1, krakow),
    {ok, [{ChildGuid, _}], _} = ?assertMatch({ok, [_], #{is_last := true}}, lfm_proxy:get_children(
        oct_background:get_random_provider_node(krakow), SessionId, #file_ref{guid = Guid}, #{offset => 0}), ?ATTEMPTS),
    ChildGuid.

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [?MODULE, archive_tests_utils]} | Config], #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {provider_token_ttl_sec, 24 * 60 * 60}
        ]}]
    }).

end_per_suite(_Config) ->
    oct_background:end_per_suite().

init_per_group(_Group, Config) ->
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2, false).

end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).

init_per_testcase(recall_details_test, Config) ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_new(Nodes, archive_recall_traverse),
    Self = self(),
    test_utils:mock_expect(Nodes, archive_recall_traverse, task_finished, fun(TaskId, Pool) ->
        Self ! {recall_traverse_finished, self()},
        receive continue ->
            meck:passthrough([TaskId, Pool])
        end
    end),
    time_test_utils:freeze_time(Config),
    Config;
init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(recall_details_test, Config) ->
    time_test_utils:unfreeze_time(Config),
    test_utils:mock_unload(oct_background:get_all_providers_nodes()),
    ok;
end_per_testcase(_Case, _Config) ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_unload(Nodes),
    ok.
