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
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_time_series.hrl").
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

%% @TODO VFS-8851 Test symlinks loops

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
    recall_plain_archive_containing_internal_symlink_test/1,
    recall_bagit_archive_containing_internal_symlink_test/1,
    recall_plain_archive_containing_internal_symlink_dip_test/1,
    recall_bagit_archive_containing_internal_symlink_dip_test/1,
    recall_plain_archive_containing_external_symlink_test/1,
    recall_bagit_archive_containing_external_symlink_test/1,
    recall_plain_archive_containing_external_symlink_dip_test/1,
    recall_bagit_archive_containing_external_symlink_dip_test/1,
    recall_plain_archive_containing_nested_internal_symlink_test/1,
    recall_bagit_archive_containing_nested_internal_symlink_test/1,
    recall_plain_archive_containing_nested_internal_symlink_dip_test/1,
    recall_bagit_archive_containing_nested_internal_symlink_dip_test/1,
    recall_plain_archive_containing_nested_external_symlink_test/1,
    recall_bagit_archive_containing_nested_external_symlink_test/1,
    recall_plain_archive_containing_nested_external_symlink_dip_test/1,
    recall_bagit_archive_containing_nested_external_symlink_dip_test/1,
    recall_plain_archive_containing_nested_parent_symlink_test/1,
    recall_bagit_archive_containing_nested_parent_symlink_test/1,
    recall_plain_archive_containing_nested_parent_symlink_dip_test/1,
    recall_bagit_archive_containing_nested_parent_symlink_dip_test/1,
    recall_plain_archive_containing_nested_child_symlink_test/1,
    recall_bagit_archive_containing_nested_child_symlink_test/1,
    recall_plain_archive_containing_nested_child_symlink_dip_test/1,
    recall_bagit_archive_containing_nested_child_symlink_dip_test/1,
    recall_custom_name_test/1,
    
    recall_details_test/1,
    recall_details_nested_test/1,
    recall_to_recalling_dir_test/1,
    recall_to_recalling_dir_by_symlink_test/1,
    recall_dir_error_test/1,
    recall_file_error_test/1,
    recall_effective_cache_test/1,
    cancel_local_recall_test/1,
    cancel_remote_recall_test/1,
    cancel_finish_race_test/1,
    cancel_cancel_race_test/1
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
        recall_bagit_nested_archive_dip_test,
%%      @TODO VFS-8851 Uncomment after archives with not resolved symlinks are properly recalled
%%        recall_plain_containing_invalid_symlink_archive_test,
%%        recall_plain_containing_invalid_symlink_archive_dip_test,
%%        recall_bagit_containing_invalid_symlink_archive_test,
%%        recall_bagit_containing_invalid_symlink_archive_dip_test,

        recall_plain_archive_containing_internal_symlink_test,
        recall_bagit_archive_containing_internal_symlink_test,
        recall_plain_archive_containing_internal_symlink_dip_test,
        recall_bagit_archive_containing_internal_symlink_dip_test,
        recall_plain_archive_containing_external_symlink_test,
        recall_bagit_archive_containing_external_symlink_test,
        recall_plain_archive_containing_external_symlink_dip_test,
        recall_bagit_archive_containing_external_symlink_dip_test,
        recall_plain_archive_containing_nested_internal_symlink_test,
        recall_bagit_archive_containing_nested_internal_symlink_test,
        recall_plain_archive_containing_nested_internal_symlink_dip_test,
        recall_bagit_archive_containing_nested_internal_symlink_dip_test,
        recall_plain_archive_containing_nested_external_symlink_test,
        recall_bagit_archive_containing_nested_external_symlink_test,
        recall_plain_archive_containing_nested_external_symlink_dip_test,
        recall_bagit_archive_containing_nested_external_symlink_dip_test,
        recall_plain_archive_containing_nested_parent_symlink_test,
        recall_bagit_archive_containing_nested_parent_symlink_test,
        recall_plain_archive_containing_nested_parent_symlink_dip_test,
        recall_bagit_archive_containing_nested_parent_symlink_dip_test,
        recall_plain_archive_containing_nested_child_symlink_test,
        recall_bagit_archive_containing_nested_child_symlink_test,
        recall_plain_archive_containing_nested_child_symlink_dip_test,
        recall_bagit_archive_containing_nested_child_symlink_dip_test,

        recall_custom_name_test
    ]},
    {sequential_tests, [
        recall_details_test,
        recall_details_nested_test,
        recall_to_recalling_dir_test,
        recall_to_recalling_dir_by_symlink_test,
        recall_dir_error_test,
        recall_file_error_test,
        recall_effective_cache_test,
        cancel_local_recall_test,
        cancel_remote_recall_test,
        cancel_finish_race_test,
        cancel_cancel_race_test
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
    recall_simple_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => false}).

recall_bagit_simple_archive_test(_Config) ->
    recall_simple_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => false}).

recall_plain_simple_archive_dip_test(_Config) ->
    recall_simple_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => true}).

recall_bagit_simple_archive_dip_test(_Config) ->
    recall_simple_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => true}).

recall_plain_empty_dir_archive_test(_Config) ->
    recall_empty_dir_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => false}).

recall_bagit_empty_dir_archive_test(_Config) ->
    recall_empty_dir_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => false}).

recall_plain_empty_dir_archive_dip_test(_Config) ->
    recall_empty_dir_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => true}).

recall_bagit_empty_dir_archive_dip_test(_Config) ->
    recall_empty_dir_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => true}).

recall_plain_single_file_archive_test(_Config) ->
    recall_single_file_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => false}).

recall_bagit_single_file_archive_test(_Config) ->
    recall_single_file_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => false}).

recall_plain_single_file_archive_dip_test(_Config) ->
    recall_single_file_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => true}).

recall_bagit_single_file_archive_dip_test(_Config) ->
    recall_single_file_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => true}).

recall_plain_nested_archive_test(_Config) ->
    recall_nested_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => false}).

recall_bagit_nested_archive_test(_Config) ->
    recall_nested_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => false}).

recall_plain_nested_archive_dip_test(_Config) ->
    recall_nested_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => true}).

recall_bagit_nested_archive_dip_test(_Config) ->
    recall_nested_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => true}).

recall_plain_containing_symlink_archive_test(_Config) ->
    recall_containing_invalid_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => false}).

recall_bagit_containing_symlink_archive_test(_Config) ->
    recall_containing_invalid_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => false}).

recall_plain_containing_symlink_archive_dip_test(_Config) ->
    recall_containing_invalid_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => true}).

recall_bagit_containing_symlink_archive_dip_test(_Config) ->
    recall_containing_invalid_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => true}).

recall_plain_archive_containing_internal_symlink_test(_Config) ->
    recall_containing_internal_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => false}).

recall_bagit_archive_containing_internal_symlink_test(_Config) ->
    recall_containing_internal_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => false}).

recall_plain_archive_containing_internal_symlink_dip_test(_Config) ->
    recall_containing_internal_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => true}).

recall_bagit_archive_containing_internal_symlink_dip_test(_Config) ->
    recall_containing_internal_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => true}).

recall_plain_archive_containing_external_symlink_test(_Config) ->
    recall_containing_external_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => false}).

recall_bagit_archive_containing_external_symlink_test(_Config) ->
    recall_containing_external_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => false}).

recall_plain_archive_containing_external_symlink_dip_test(_Config) ->
    recall_containing_external_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => true}).

recall_bagit_archive_containing_external_symlink_dip_test(_Config) ->
    recall_containing_external_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => true}).

recall_plain_archive_containing_nested_internal_symlink_test(_Config) ->
    recall_containing_nested_internal_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => false}).

recall_bagit_archive_containing_nested_internal_symlink_test(_Config) ->
    recall_containing_nested_internal_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => false}).

recall_plain_archive_containing_nested_internal_symlink_dip_test(_Config) ->
    recall_containing_nested_internal_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => true}).

recall_bagit_archive_containing_nested_internal_symlink_dip_test(_Config) ->
    recall_containing_nested_internal_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => true}).

recall_plain_archive_containing_nested_external_symlink_test(_Config) ->
    recall_containing_nested_external_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => false}).

recall_bagit_archive_containing_nested_external_symlink_test(_Config) ->
    recall_containing_nested_external_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => false}).

recall_plain_archive_containing_nested_external_symlink_dip_test(_Config) ->
    recall_containing_nested_external_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => true}).

recall_bagit_archive_containing_nested_external_symlink_dip_test(_Config) ->
    recall_containing_nested_external_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => true}).

recall_plain_archive_containing_nested_parent_symlink_test(_Config) ->
    recall_containing_nested_parent_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => false}).

recall_bagit_archive_containing_nested_parent_symlink_test(_Config) ->
    recall_containing_nested_parent_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => false}).

recall_plain_archive_containing_nested_parent_symlink_dip_test(_Config) ->
    recall_containing_nested_parent_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => true}).

recall_bagit_archive_containing_nested_parent_symlink_dip_test(_Config) ->
    recall_containing_nested_parent_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => true}).

recall_plain_archive_containing_nested_child_symlink_test(_Config) ->
    recall_containing_nested_child_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => false}).

recall_bagit_archive_containing_nested_child_symlink_test(_Config) ->
    recall_containing_nested_child_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => false}).

recall_plain_archive_containing_nested_child_symlink_dip_test(_Config) ->
    recall_containing_nested_child_symlink_archive_base(#{layout => ?ARCHIVE_PLAIN_LAYOUT, include_dip => true}).

recall_bagit_archive_containing_nested_child_symlink_dip_test(_Config) ->
    recall_containing_nested_child_symlink_archive_base(#{layout => ?ARCHIVE_BAGIT_LAYOUT, include_dip => true}).


%===================================================================
% Sequential tests - tests which must be run sequentially because 
% of used mocks.
%===================================================================

recall_details_test(_Config) ->
    FileSize1 = rand:uniform(20),
    % file with size extending copy buffer so progress is reported multiple times
    FileSize2 = rand:uniform(200) + 100 * 1024 * 1024, 
    Spec = #dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = ?ARCHIVE_PLAIN_LAYOUT}}]},
        children = [
            #file_spec{content = ?RAND_CONTENT(FileSize1)},
            #file_spec{content = ?RAND_CONTENT(FileSize2)}
        ]
    },
    recall_details_test_base(Spec, 2, FileSize1 + FileSize2).

recall_details_nested_test(_Config) ->
    FileSize = rand:uniform(2000),
    Spec = #dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = ?ARCHIVE_PLAIN_LAYOUT, create_nested_archives = true}}]},
        children = [
            #dir_spec{
                dataset = #dataset_spec{},
                children = [
                    #file_spec{content = ?RAND_CONTENT(FileSize)},
                    #file_spec{content = ?RAND_CONTENT(FileSize)},
                    #file_spec{content = ?RAND_CONTENT(FileSize)}
                ]
            },
            #file_spec{content = ?RAND_CONTENT(FileSize)},
            #file_spec{content = ?RAND_CONTENT(FileSize)}
        ]
    },
    recall_details_test_base(Spec, 5, 5 * FileSize).


recall_custom_name_test(_Config) ->
    Filename = str_utils:rand_hex(20),
    {_ArchiveId, _TargetParentGuid, RootFileGuid} = recall_test_setup(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = ?ARCHIVE_PLAIN_LAYOUT}}]},
        children = [
            #file_spec{metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}}
        ]
    }, Filename),
    Node = oct_background:get_random_provider_node(krakow),
    SessionId = oct_background:get_user_session_id(?USER1, krakow),
    ?assertMatch({ok, #file_attr{name = Filename}}, lfm_proxy:stat(Node, SessionId, #file_ref{guid = RootFileGuid})).


recall_to_recalling_dir_test(_Config) ->
    recall_to_recalling_dir_test_base(standard).


recall_to_recalling_dir_by_symlink_test(_Config) ->
    recall_to_recalling_dir_test_base(symlink).


recall_dir_error_test(_Config) ->
    Spec = #dir_spec{ dataset = #dataset_spec{archives = 1}},
    recall_error_test_base(Spec, do_dir_master_job_unsafe).


recall_file_error_test(_Config) ->
    Spec = #file_spec{ dataset = #dataset_spec{archives = 1}},
    recall_error_test_base(Spec, do_slave_job_unsafe).


recall_effective_cache_test(_Config) ->
    {ArchiveId, _TargetParentGuid, RecallRootFileGuid1} = recall_test_setup(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = ?ARCHIVE_PLAIN_LAYOUT}}]},
        children = [
            #file_spec{metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}}
        ]
    }),
    SessId = oct_background:get_user_session_id(?USER1, krakow),
    
    check_effective_cache_values(RecallRootFileGuid1),
    {ok, RecallRootFileGuid2} = ?assertMatch({ok, _}, opt_archives:recall(krakow, SessId, ArchiveId, RecallRootFileGuid1, default)),
    check_effective_cache_values(RecallRootFileGuid2).


cancel_local_recall_test(_Config) ->
    cancel_recall_test_base(krakow).


cancel_remote_recall_test(_Config) ->
    cancel_recall_test_base(paris).


cancel_finish_race_test(_Config) ->
    SessId = fun(P) -> oct_background:get_user_session_id(?USER1, P) end,
    Providers = oct_background:get_space_supporting_providers(?SPACE),
    
    {Pid, RecallRootFileGuid} = cancel_race_test_setup(),
    CancelTimestamp = time_test_utils:get_frozen_time_millis(),
    
    opt_archives:cancel_recall(paris, SessId(paris), RecallRootFileGuid),
    time_test_utils:simulate_millis_passing(1),
    
    finish_recall(Pid),
    FinishTimestamp = time_test_utils:get_frozen_time_millis(),
    
    lists:foreach(fun(Provider) ->
        ?assertMatch({ok, #archive_recall_details{
            cancel_timestamp = CancelTimestamp,
            finish_timestamp = FinishTimestamp
        }}, opt_archives:get_recall_details(Provider, SessId(Provider), RecallRootFileGuid), ?ATTEMPTS)
    end, Providers).


cancel_cancel_race_test(_Config) ->
    SessId = fun(P) -> oct_background:get_user_session_id(?USER1, P) end,
    Providers = oct_background:get_space_supporting_providers(?SPACE),
    
    {Pid, RecallRootFileGuid} = cancel_race_test_setup(),
    CancelTimestamp = time_test_utils:get_frozen_time_millis(),
    
    opt_archives:cancel_recall(krakow, SessId(krakow), RecallRootFileGuid),
    time_test_utils:simulate_millis_passing(1),
    opt_archives:cancel_recall(paris, SessId(paris), RecallRootFileGuid),
    
    time_test_utils:simulate_millis_passing(1),
    finish_recall(Pid),
    FinishTimestamp = time_test_utils:get_frozen_time_millis(),
    
    lists:foreach(fun(Provider) ->
        ?assertMatch({ok, #archive_recall_details{
            cancel_timestamp = CancelTimestamp,
            finish_timestamp = FinishTimestamp
        }}, opt_archives:get_recall_details(Provider, SessId(Provider), RecallRootFileGuid), ?ATTEMPTS)
    end, Providers).


%===================================================================
% Test bases
%===================================================================

recall_simple_archive_base(#{layout := Layout, include_dip := IncludeDip}) ->
    recall_test_base(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip}}]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
        children = [
            #file_spec{metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}},
            #file_spec{metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}},
            #file_spec{metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}}
        ]
    }).

recall_empty_dir_archive_base(#{layout := Layout, include_dip := IncludeDip}) ->
    recall_test_base(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip}}]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}
    }).

recall_single_file_archive_base(#{layout := Layout, include_dip := IncludeDip}) ->
    recall_test_base(#file_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip}}]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}
    }).

recall_nested_archive_base(#{layout := Layout, include_dip := IncludeDip}) ->
    recall_test_base(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip, create_nested_archives = true}}]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
        children = [#dir_spec{
            dataset = #dataset_spec{},
            metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}
        }]
    }).

recall_containing_invalid_symlink_archive_base(#{layout := Layout, include_dip := IncludeDip}) ->
    recall_test_base(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip, follow_symlinks = false}}]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
        children = [#symlink_spec{symlink_value = <<"some/dummy/value">>}]
    }, false).

recall_containing_internal_symlink_archive_base(#{layout := Layout, include_dip := IncludeDip}) ->
    recall_test_base(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip}}]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
        children = [
            #file_spec{custom_label = <<"target">>}, 
            #symlink_spec{symlink_value = {custom_label, <<"target">>}}
        ]
    }, false).

recall_containing_nested_internal_symlink_archive_base(#{layout := Layout, include_dip := IncludeDip}) ->
    recall_test_base(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip, create_nested_archives = true}}]},
        metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
        children = [#dir_spec{
            dataset = #dataset_spec{},
            children = [
                #file_spec{custom_label = <<"target">>},
                #symlink_spec{symlink_value = {custom_label, <<"target">>}}
            ]}
        ]
    }, nested_archive_only ).

recall_containing_external_symlink_archive_base(#{layout := Layout, include_dip := IncludeDip}) ->
    recall_test_base([
        #file_spec{custom_label = <<"target">>},
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip}}]},
            metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
            children = [#dir_spec{
                metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
                children = [
                    #symlink_spec{symlink_value = {custom_label, <<"target">>}}
                ]
            }]
        }
    ]).

recall_containing_nested_external_symlink_archive_base(#{layout := Layout, include_dip := IncludeDip}) ->
    recall_test_base([
        #file_spec{custom_label = <<"target">>},
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip, create_nested_archives = true}}]},
            metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
            children = [#dir_spec{
                dataset = #dataset_spec{},
                metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
                children = [
                    #symlink_spec{symlink_value = {custom_label, <<"target">>}}
                ]
            }]
        }
    ]).

recall_containing_nested_parent_symlink_archive_base(#{layout := Layout, include_dip := IncludeDip}) ->
    recall_test_base([
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip, create_nested_archives = true}}]},
            metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
            children = [
                #file_spec{custom_label = <<"target">>},
                #dir_spec{
                    dataset = #dataset_spec{},
                    metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
                    children = [
                        #symlink_spec{symlink_value = {custom_label, <<"target">>}}
                    ]
                }
            ]
        }
    ], nested_archive_only).

recall_containing_nested_child_symlink_archive_base(#{layout := Layout, include_dip := IncludeDip}) ->
    recall_test_base([
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = Layout, include_dip = IncludeDip, create_nested_archives = true}}]},
            metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
            children = [
                #dir_spec{
                    dataset = #dataset_spec{},
                    metadata = #metadata_spec{json = ?RAND_JSON_METADATA()},
                    children = [
                        #file_spec{custom_label = <<"target">>}
                    ]
                },
                #symlink_spec{symlink_value = {custom_label, <<"target">>}}
            ]
        }
    ]).


recall_test_base(StructureSpec) ->
    recall_test_base(StructureSpec, true).

recall_test_base(StructureSpec, SymlinkResolutionMode) ->
    SessId = oct_background:get_user_session_id(?USER1, krakow),
    {ArchiveId, TargetParentGuid, _RootFileGuid} = recall_test_setup(StructureSpec),
    {ok, ArchiveDataDirGuid} = opw_test_rpc:call(krakow, archive, get_data_dir_guid, [ArchiveId]),
    archive_tests_utils:assert_copied(oct_background:get_random_provider_node(krakow), SessId, 
        get_direct_child(ArchiveDataDirGuid), get_direct_child(TargetParentGuid), SymlinkResolutionMode, ?ATTEMPTS),
    ?assertEqual(?ERROR_ALREADY_EXISTS, opt_archives:recall(krakow, SessId, ArchiveId, TargetParentGuid, default)).

recall_test_setup(StructureSpec) ->
    recall_test_setup(StructureSpec, default).

recall_test_setup(StructureSpec, RootFileName) ->
    SessId = oct_background:get_user_session_id(?USER1, krakow),
    CreatedTreeObject = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, StructureSpec, krakow),
    ArchiveId = lists:foldl(fun
        (#object{dataset = #dataset_object{archives = [#archive_object{id = Id}]}}, undefined) ->
            Id;
        (_, Acc) -> 
            Acc
    end, undefined, utils:ensure_list(CreatedTreeObject)),
    #object{guid = TargetParentGuid} = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #dir_spec{}),
    ?assertMatch({ok, #archive_info{state = ?ARCHIVE_PRESERVED}}, opt_archives:get_info(krakow, SessId, ArchiveId), ?ATTEMPTS),
    {ok, RecallRootFileGuid} = opt_archives:recall(krakow, SessId, ArchiveId, TargetParentGuid, RootFileName),
    FinalArchiveId = case opw_test_rpc:call(krakow, archive, get_related_dip_id, [ArchiveId]) of
        {ok, undefined} -> ArchiveId;
        {ok, DipArchiveId} -> DipArchiveId
    end,
    {FinalArchiveId, TargetParentGuid, RecallRootFileGuid}.


recall_details_test_base(Spec, TotalFileCount, TotalByteSize) ->
    {ArchiveId, _TargetParentGuid, RecallRootFileGuid} = recall_test_setup(Spec),
    
    SessId = fun(P) -> oct_background:get_user_session_id(?USER1, P) end,
    % wait for archive recall traverse to finish (mocked in init_per_testcase)
    Pid = wait_for_recall_traverse_finish(),
    
    % check archive_recall document
    Providers = oct_background:get_space_supporting_providers(?SPACE),
    Timestamp = time_test_utils:get_frozen_time_millis(),
    lists:foreach(fun(Provider) ->
        ?assertMatch({ok, #archive_recall_details{
            archive_id = ArchiveId,
            start_timestamp = Timestamp,
            finish_timestamp = undefined,
            total_file_count = TotalFileCount,
            total_byte_size = TotalByteSize
        }}, opt_archives:get_recall_details(Provider, SessId(Provider), RecallRootFileGuid), ?ATTEMPTS)
    end, Providers),
    
    % check recall stats (stats are only stored on provider performing recall)
    Layout = #{
        <<"bytesCopied">> => [<<"hour">>, <<"minute">>, <<"day">>, <<"total">>],
        <<"filesCopied">> => [<<"hour">>, <<"minute">>, <<"day">>, <<"total">>],
        <<"filesFailed">> => [ <<"total">>]
    },
    ?assertMatch({ok, #{
        <<"bytesCopied">> := #{
            <<"hour">> := [#window_info{value = TotalByteSize}],
            <<"minute">> := [#window_info{value = TotalByteSize}],
            <<"day">> := [#window_info{value = TotalByteSize}],
            <<"total">> := [#window_info{value = TotalByteSize}]
        },
        <<"filesCopied">> := #{
            <<"hour">> := [#window_info{value = TotalFileCount}],
            <<"minute">> := [#window_info{value = TotalFileCount}],
            <<"day">> := [#window_info{value = TotalFileCount}],
            <<"total">> := [#window_info{value = TotalFileCount}]
        },
        <<"filesFailed">> := #{
            <<"total">> := []
        }
        %% @TODO VFS-9307 - use op_archives after implementing histogram API
    }}, opw_test_rpc:call(krakow, archive_recall, get_stats, [
        file_id:guid_to_uuid(RecallRootFileGuid), Layout, #{}
    ]), ?ATTEMPTS),
    
    ?assertMatch({ok, #{
        <<"bytesCopied">>  := TotalByteSize,
        <<"filesCopied">> := TotalFileCount,
        <<"filesFailed">> := 0
    }}, opt_archives:get_recall_progress(krakow, SessId(krakow), RecallRootFileGuid), ?ATTEMPTS),
    ?assertEqual(?ERROR_NOT_FOUND, opt_archives:get_recall_progress(paris, SessId(paris), RecallRootFileGuid)),
    
    time_test_utils:simulate_millis_passing(8),
    FinishTimestamp = Timestamp + 8,
    
    finish_recall(Pid),
    
    lists:foreach(fun(Provider) ->
        ?assertMatch({ok, #archive_recall_details{
            finish_timestamp = FinishTimestamp
        }},
            opt_archives:get_recall_details(Provider, SessId(Provider), RecallRootFileGuid), ?ATTEMPTS)
    end, Providers),
    ok.


recall_to_recalling_dir_test_base(Method) ->
    {ArchiveId, TargetParentGuid, RecallRootFileGuid} = recall_test_setup(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = ?ARCHIVE_PLAIN_LAYOUT}}]},
        children = [
            #file_spec{metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}}
        ]
    }),
    
    Pid = wait_for_recall_traverse_finish(),
    SessId = oct_background:get_user_session_id(?USER1, krakow),
    Node = oct_background:get_random_provider_node(krakow),
    
    NewTargetParentGuid = case Method of
        standard ->
            RecallRootFileGuid;
        symlink ->
            SymlinkValue = onenv_file_test_utils:prepare_symlink_value(Node, SessId, RecallRootFileGuid),
            {ok, #file_attr{guid = G}} = lfm_proxy:make_symlink(Node, SessId, #file_ref{guid = TargetParentGuid}, ?RAND_NAME(), SymlinkValue),
            G
    end,
    
    ?assertEqual(?ERROR_RECALL_TARGET_CONFLICT, opt_archives:recall(krakow, SessId, ArchiveId, NewTargetParentGuid, default)),
    
    finish_recall(Pid),
    
    ?assertMatch({ok, #archive_recall_details{finish_timestamp = T}} when is_integer(T),
        opt_archives:get_recall_details(krakow, SessId, RecallRootFileGuid), ?ATTEMPTS),
    
    {ok, NestedRecallRootFileGuid} = ?assertMatch({ok, _}, opt_archives:recall(krakow, SessId, ArchiveId, NewTargetParentGuid, default)),
    ?assertEqual({ok, RecallRootFileGuid}, lfm_proxy:get_parent(Node, SessId, #file_ref{guid = NestedRecallRootFileGuid})).


recall_error_test_base(Spec, FunName) ->
    SessId = fun(P) -> oct_background:get_user_session_id(?USER1, P) end,
    Errors = [
        {?ERROR_NOT_FOUND, errors:to_json(?ERROR_NOT_FOUND)},
        {{badmatch, {error, ?EPERM}}, errors:to_json(?ERROR_POSIX(?EPERM))}
    ],
    lists:foreach(fun({Error, ExpectedReason}) ->
        mock_traverse_error(FunName, Error),
        {_ArchiveId, _TargetParentGuid, RecallRootFileGuid} = recall_test_setup(Spec),
        ?assertMatch({ok, #{
            <<"bytesCopied">>  := 0,
            <<"filesCopied">> := 0,
            <<"filesFailed">> := 1
        }}, opt_archives:get_recall_progress(krakow, SessId(krakow), RecallRootFileGuid), ?ATTEMPTS),
        ?assertMatch({ok, #{
            <<"logEntries">> := [
                #{<<"content">> := #{<<"reason">> := ExpectedReason}}
            ]
        }}, opt_archives:browse_recall_log(krakow, SessId(krakow), RecallRootFileGuid, #{}), ?ATTEMPTS),
        % simulate expiration of the audit log
        opw_test_rpc:call(krakow, audit_log, delete, [<<(file_id:guid_to_uuid(RecallRootFileGuid))/binary, "el">>]),
        % browsing should return a proper error
        ?assertEqual(?ERROR_NOT_FOUND, opt_archives:browse_recall_log(krakow, SessId(krakow), RecallRootFileGuid, #{}))
    end, Errors).


cancel_recall_test_base(CancellingProvider) ->
    {ArchiveId, _TargetParentGuid, RecallRootFileGuid} = recall_test_setup(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = ?ARCHIVE_PLAIN_LAYOUT}}]},
        children = [#file_spec{}, #file_spec{}, #file_spec{}]
    }),
    SessId = fun(P) -> oct_background:get_user_session_id(?USER1, P) end,
    
    StartTimestamp = time_test_utils:get_frozen_time_millis(),
    time_test_utils:simulate_millis_passing(1),
    CancelTimestamp = time_test_utils:get_frozen_time_millis(),
    Providers = oct_background:get_space_supporting_providers(?SPACE),
    
    lists:foreach(fun(Provider) ->
        ?assertMatch({ok, #archive_recall_details{
            archive_id = ArchiveId,
            start_timestamp = StartTimestamp
        }}, opt_archives:get_recall_details(Provider, SessId(Provider), RecallRootFileGuid), ?ATTEMPTS)
    end, Providers),
    
    ?assertEqual(ok, opt_archives:cancel_recall(CancellingProvider, SessId(CancellingProvider), RecallRootFileGuid)),
    
    lists:foreach(fun(Provider) ->
        ?assertMatch({ok, #archive_recall_details{
            cancel_timestamp = CancelTimestamp
        }}, opt_archives:get_recall_details(Provider, SessId(Provider), RecallRootFileGuid), ?ATTEMPTS)
    end, Providers),
    
    time_test_utils:simulate_millis_passing(1),
    FinishTimestamp = time_test_utils:get_frozen_time_millis(),
    
    check_mocked_slave_jobs_cancelled(3, ?ATTEMPTS),
    
    lists:foreach(fun(Provider) ->
        ?assertMatch({ok, #archive_recall_details{
            finish_timestamp = FinishTimestamp
        }}, opt_archives:get_recall_details(Provider, SessId(Provider), RecallRootFileGuid), ?ATTEMPTS)
    end, Providers),
    ok.


cancel_race_test_setup() ->
    {_ArchiveId, _TargetParentGuid, RecallRootFileGuid} = recall_test_setup(#dir_spec{
        dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = ?ARCHIVE_PLAIN_LAYOUT}}]},
        children = [
            #file_spec{metadata = #metadata_spec{json = ?RAND_JSON_METADATA()}}
        ]
    }),
    SessId = fun(P) -> oct_background:get_user_session_id(?USER1, P) end,
    Providers = oct_background:get_space_supporting_providers(?SPACE),
    
    Pid = wait_for_recall_traverse_finish(),
    
    lists:foreach(fun(Provider) ->
        ?assertMatch({ok, #archive_recall_details{}},
            opt_archives:get_recall_details(Provider, SessId(Provider), RecallRootFileGuid), ?ATTEMPTS)
    end, Providers),
    {Pid, RecallRootFileGuid}.
    

%===================================================================
% Helper functions
%===================================================================

get_direct_child(Guid) ->
    SessionId = oct_background:get_user_session_id(?USER1, krakow),
    {ok, [{ChildGuid, _}], ListingToken} = ?assertMatch({ok, [_], _}, lfm_proxy:get_children(
        oct_background:get_random_provider_node(krakow), SessionId, #file_ref{guid = Guid}, #{tune_for_large_continuous_listing => false}), ?ATTEMPTS),
    ?assert(file_listing:is_finished(ListingToken)),
    ChildGuid.


mock_traverse_error(FunName, Error) ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_new(Nodes, archive_recall_traverse),
    test_utils:mock_expect(Nodes, archive_recall_traverse, FunName, fun(_, _) ->
        error(Error)
    end).


check_effective_cache_values(RecallRootFileGuid) ->
    SessId = oct_background:get_user_session_id(?USER1, krakow),
    Node = oct_background:get_random_provider_node(krakow),
    Pid = wait_for_recall_traverse_finish(),
    {ok, [{ChildGuid, _}], _} = ?assertMatch({ok, [_], _}, lfm_proxy:get_children(Node, SessId, #file_ref{guid = RecallRootFileGuid}, #{tune_for_large_continuous_listing => false})),
    SpaceId = file_id:guid_to_space_id(RecallRootFileGuid),
    Uuid = file_id:guid_to_uuid(RecallRootFileGuid),
    lists:foreach(fun(N) ->
        ?assertEqual({ok, {ongoing, Uuid}}, opw_test_rpc:call(N, archive_recall_cache, get, [SpaceId, Uuid]), ?ATTEMPTS),
        ?assertEqual({ok, {ongoing, Uuid}}, opw_test_rpc:call(N, archive_recall_cache, get, [SpaceId, file_id:guid_to_uuid(ChildGuid)]), ?ATTEMPTS)
    end, oct_background:get_all_providers_nodes()),
    
    finish_recall(Pid),
    
    lists:foreach(fun(N) ->
        ?assertEqual({ok, {finished, Uuid}}, opw_test_rpc:call(N, archive_recall_cache, get, [SpaceId, Uuid]), ?ATTEMPTS),
        ?assertEqual({ok, {finished, Uuid}}, opw_test_rpc:call(N, archive_recall_cache, get, [SpaceId, file_id:guid_to_uuid(ChildGuid)]), ?ATTEMPTS)
    end, oct_background:get_all_providers_nodes()).


mock_recall_traverse_finished() ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_new(Nodes, archive_recall_traverse),
    TestProcess = self(),
    test_utils:mock_expect(Nodes, archive_recall_traverse, task_finished, fun(TaskId, Pool) ->
        TestProcess ! {recall_traverse_finished, self()},
        receive continue ->
            meck:passthrough([TaskId, Pool])
        end
    end).


%% below functions require calling mock_recall_traverse_finished/0 function beforehand.
wait_for_recall_traverse_finish() ->
    receive
        {recall_traverse_finished, P} -> P
    after timer:seconds(?ATTEMPTS) ->
        throw({error, recall_traverse_did_not_finish})
    end.


finish_recall(Pid) ->
    Pid ! continue.


mock_cancel_test_slave_job(TraverseModule) ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_new(Nodes, TraverseModule),
    TestProcess = self(),
    test_utils:mock_expect(Nodes, TraverseModule, do_slave_job_unsafe, fun(_, TaskId) ->
        TestProcess ! {slave_job, self()},
        receive check_cancel ->
            TestProcess ! {result, traverse:is_job_cancelled(TaskId)}
        end,
        ok
    end).


% this function require calling mock_cancel_test_slave_job/1 beforehand
check_mocked_slave_jobs_cancelled(0 = _JobsToCheck, _TimeoutSeconds) ->
    ok;
check_mocked_slave_jobs_cancelled(SlaveJobsLeft, TimeoutSeconds) ->
    receive {slave_job, Pid} ->
        Pid ! check_cancel,
        receive {result, Res} ->
            ?assertEqual(true, Res)
        after 1000 ->
            throw(cancel_result_not_received)
        end
    after timer:seconds(TimeoutSeconds) ->
        throw(slave_job_not_started)
    end,
    check_mocked_slave_jobs_cancelled(SlaveJobsLeft - 1, TimeoutSeconds).


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [?MODULE, archive_tests_utils, dir_stats_test_utils]} | Config],
        #onenv_test_config{
            onenv_scenario = "2op",
            envs = [{op_worker, op_worker, [
                {fuse_session_grace_period_seconds, 24 * 60 * 60},
                {provider_token_ttl_sec, 24 * 60 * 60}
            ]}],
            posthook = fun dir_stats_test_utils:disable_stats_counting_ct_posthook/1
        }).

end_per_suite(Config) ->
    oct_background:end_per_suite(),
    dir_stats_test_utils:enable_stats_counting(Config).

init_per_group(_Group, Config) ->
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2, false).

end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).

init_per_testcase(Case, Config) when 
    Case =:= recall_details_test;
    Case =:= recall_details_nested_test;
    Case =:= recall_to_recalling_dir_test;
    Case =:= recall_to_recalling_dir_by_symlink_test;
    Case =:= recall_effective_cache_test;
    Case =:= cancel_finish_race_test;
    Case =:= cancel_cancel_race_test
->
    mock_recall_traverse_finished(),
    time_test_utils:freeze_time(Config),
    Config;
init_per_testcase(Case, Config) when
    Case =:= cancel_local_recall_test;
    Case =:= cancel_remote_recall_test
    ->
    mock_cancel_test_slave_job(archive_recall_traverse),
    time_test_utils:freeze_time(Config),
    Config;
init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Case, Config) when
    Case =:= recall_details_test;
    Case =:= recall_details_nested_test;
    Case =:= recall_to_recalling_dir_test;
    Case =:= recall_to_recalling_dir_by_symlink_test;
    Case =:= recall_effective_cache_test;
    Case =:= cancel_local_recall_test;
    Case =:= cancel_remote_recall_test;
    Case =:= cancel_finish_race_test;
    Case =:= cancel_cancel_race_test
->
    time_test_utils:unfreeze_time(Config),
    test_utils:mock_unload(oct_background:get_all_providers_nodes(), archive_recall_traverse),
    ok;
end_per_testcase(_Case, _Config) ->
    ok.
