%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Sequential tests of archives mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_sequential_test_SUITE).
-author("Jakub Kudzia").


-include("onenv_test_utils.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/test/assertions.hrl").
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
    archive_dataset_attached_to_space_dir/1,
    archive_big_tree_plain_layout/1,
    archive_directory_with_number_of_files_exceeding_batch_size_plain_layout/1,
    
    verification_plain_modify_file/1,
    verification_plain_modify_file_metadata/1,
    verification_plain_modify_dir_metadata/1,
    verification_plain_create_file/1,
    verification_plain_remove_file/1,
    verification_plain_recreate_file/1,
    dip_verification_plain/1,
    nested_verification_plain/1,
    
    error_during_archivisation_slave_job/1,
    error_during_archivisation_master_job/1,
    error_during_verification_slave_job/1,
    error_during_verification_master_job/1,
    delete_nested_archive_error/1,
    delete_not_finished_archive_error/1,
    
    audit_log_test/1,
    audit_log_failed_file_test/1,
    audit_log_failed_dir_test/1,
    audit_log_failed_file_verification_test/1,
    audit_log_failed_dir_verification_test/1
]).


groups() -> [
    {archivisation_tests, [
        archive_dataset_attached_to_space_dir,
        archive_big_tree_plain_layout,
        archive_directory_with_number_of_files_exceeding_batch_size_plain_layout
    ]},
    {verification_tests, [
        verification_plain_modify_file,
        verification_plain_modify_file_metadata,
        verification_plain_modify_dir_metadata,
        verification_plain_create_file,
        verification_plain_remove_file,
        verification_plain_recreate_file,
        dip_verification_plain,
        nested_verification_plain
    ]},
    {errors_tests, [
        error_during_archivisation_slave_job,
        error_during_archivisation_master_job,
        error_during_verification_slave_job,
        error_during_verification_master_job,
        delete_nested_archive_error,
        delete_not_finished_archive_error
    ]},
    {audit_log_tests, [
        audit_log_test,
        audit_log_failed_file_test,
        audit_log_failed_dir_test,
        audit_log_failed_file_verification_test,
        audit_log_failed_dir_verification_test
    ]}
].


all() -> [
    {group, archivisation_tests},
    {group, verification_tests},
    {group, errors_tests},
    {group, audit_log_tests}
].

-define(SPACE, space_krk_par_p).
-define(USER1, user1).

-define(ATTEMPTS, 60).

%===================================================================
% Sequential tests - tests which must be performed one after another
% to ensure that they do not interfere with each other
%===================================================================

archive_dataset_attached_to_space_dir(_Config) ->
    SpaceId = oct_background:get_space_id(?SPACE),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    #dataset_object{
        id = DatasetId,
        archives = [#archive_object{id = ArchiveId}]
    } = onenv_dataset_test_utils:set_up_and_sync_dataset(?USER1, SpaceGuid, #dataset_spec{archives = 1}),
    archive_sequential_test_base:archive_simple_dataset_test(SpaceGuid, DatasetId, ArchiveId).

archive_big_tree_plain_layout(_Config) ->
    archive_sequential_test_base:archive_big_tree_test(?ARCHIVE_PLAIN_LAYOUT).

archive_directory_with_number_of_files_exceeding_batch_size_plain_layout(_Config) ->
    archive_sequential_test_base:archive_directory_with_number_of_files_exceeding_batch_size_test(?ARCHIVE_PLAIN_LAYOUT).


%===================================================================
% Verification tests - can not be run in parallel as they use mocks.
%===================================================================

verification_plain_modify_file(_Config) ->
    archive_sequential_test_base:verification_modify_file_base(?ARCHIVE_PLAIN_LAYOUT).

verification_plain_modify_file_metadata(_Config) ->
    archive_sequential_test_base:verification_modify_file_metadata_base(?ARCHIVE_PLAIN_LAYOUT).

verification_plain_modify_dir_metadata(_Config) ->
    archive_sequential_test_base:verification_modify_dir_metadata_base(?ARCHIVE_PLAIN_LAYOUT).

verification_plain_create_file(_Config) ->
    archive_sequential_test_base:verification_create_file_base(?ARCHIVE_PLAIN_LAYOUT).

verification_plain_remove_file(_Config) ->
    archive_sequential_test_base:verification_remove_file_base(?ARCHIVE_PLAIN_LAYOUT).

verification_plain_recreate_file(_Config) ->
    archive_sequential_test_base:verification_recreate_file_base(?ARCHIVE_PLAIN_LAYOUT).

dip_verification_plain(_Config) ->
    archive_sequential_test_base:dip_verification_test_base(?ARCHIVE_PLAIN_LAYOUT).

nested_verification_plain(_Config) ->
    archive_sequential_test_base:nested_verification_test_base(?ARCHIVE_PLAIN_LAYOUT).


%===================================================================
% Errors tests - can not be run in parallel as they use mocks.
%===================================================================

error_during_archivisation_slave_job(_Config) ->
    errors_test_base(archivisation, slave_job).

error_during_archivisation_master_job(_Config) ->
    errors_test_base(archivisation, master_job).

error_during_verification_slave_job(_Config) ->
    errors_test_base(verification, slave_job).

error_during_verification_master_job(_Config) ->
    errors_test_base(verification, master_job).


delete_nested_archive_error(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessionId = oct_background:get_user_session_id(?USER1, krakow),
    #object{
        dataset = #dataset_object{archives = [#archive_object{id = ParentArchiveId}]},
        children = [
            #object{
                guid = FileGuid,
                dataset = #dataset_object{id = DatasetId}
            }
        ]
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE,
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{
                config = #archive_config{create_nested_archives = true, layout = ?ARCHIVE_PLAIN_LAYOUT}
            }]},
            children = [
                #file_spec{dataset = #dataset_spec{}}
            ]
        }
    ),
    {ok, {[{_, ArchiveId}], _}} = ?assertMatch({ok, {[_], true}},
        opt_archives:list(Node, SessionId, DatasetId, #{offset => 0, limit => 10}), ?ATTEMPTS),
    
    archive_tests_utils:assert_archive_is_preserved(Node, SessionId, ArchiveId, DatasetId, FileGuid, 1, 0, ?ATTEMPTS),
    ?assertEqual(?ERROR_NESTED_ARCHIVE_DELETION_FORBIDDEN(ParentArchiveId), opt_archives:delete(Node, SessionId, ArchiveId)),
    ?assertEqual(ok, opt_archives:delete(Node, SessionId, ParentArchiveId)).


delete_not_finished_archive_error(_Config) ->
    Node = oct_background:get_random_provider_node(krakow),
    SessionId = oct_background:get_user_session_id(?USER1, krakow),
    #object{
        guid = Guid,
        dataset = #dataset_object{id = DatasetId, archives = [#archive_object{id = ArchiveId}]}
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE,
        #dir_spec{
            dataset = #dataset_spec{archives = [#archive_spec{config = #archive_config{layout = ?ARCHIVE_PLAIN_LAYOUT}}]}
        }
    ),
    
    % archive staying in pending state is mocked in init_per_test
    ?assertEqual(?ERROR_FORBIDDEN_FOR_CURRENT_ARCHIVE_STATE(?ARCHIVE_PENDING, [?ARCHIVE_PRESERVED, ?ARCHIVE_FAILED, ?ARCHIVE_DELETING,
        ?ARCHIVE_VERIFICATION_FAILED, ?ARCHIVE_CANCELLED]), opt_archives:delete(Node, SessionId, ArchiveId)),
    finalize_archive_creation(?FUNCTION_NAME),
    archive_tests_utils:assert_archive_is_preserved(Node, SessionId, ArchiveId, DatasetId, Guid, 0, 0, ?ATTEMPTS),
    ?assertEqual(ok, opt_archives:delete(Node, SessionId, ArchiveId)).


%===================================================================
% Audit log tests - can not be run in parallel as they use mocks.
%===================================================================

audit_log_test(_Config) ->
    audit_log_test_base(?ARCHIVE_PRESERVED, undefined).

audit_log_failed_file_test(_Config) ->
    mock_job_function_error(archivisation_traverse, do_slave_job_unsafe),
    audit_log_test_base(?ARCHIVE_FAILED, reg_file).

audit_log_failed_dir_test(_Config) ->
    mock_job_function_error(archivisation_traverse, do_dir_master_job_unsafe),
    audit_log_test_base(?ARCHIVE_FAILED, dir).

audit_log_failed_file_verification_test(_Config) ->
    mock_job_function_error(archive_verification_traverse, do_slave_job_unsafe),
    audit_log_test_base(?ARCHIVE_VERIFICATION_FAILED, reg_file).

audit_log_failed_dir_verification_test(_Config) ->
    mock_job_function_error(archive_verification_traverse, do_dir_master_job_unsafe),
    audit_log_test_base(?ARCHIVE_VERIFICATION_FAILED, dir).


%===================================================================
% Test bases
%===================================================================

errors_test_base(TraverseType, JobType) ->
    mock_error_requested_job(TraverseType, JobType),
    #object{dataset = #dataset_object{archives = [#archive_object{id = ArchiveId}]}} =
        onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #dir_spec{
            dataset = #dataset_spec{archives = 1},
            children = [#file_spec{}, #dir_spec{}]
        }),
    ExpectedState = case TraverseType of
        archivisation -> ?ARCHIVE_FAILED;
        verification -> ?ARCHIVE_VERIFICATION_FAILED
    end,
    archive_tests_utils:assert_archive_state(ArchiveId, ExpectedState, ?ATTEMPTS).


audit_log_test_base(ExpectedState, FailedFileType) ->
    Timestamp = opw_test_rpc:call(oct_background:get_random_provider_node(krakow), global_clock, timestamp_millis, []),
    #object{
        name = DirName,
        dataset = #dataset_object{archives = [#archive_object{id = ArchiveId}]},
        children = [
            #object{name = ChildFileName}
        ]
    } = onenv_file_test_utils:create_and_sync_file_tree(?USER1, ?SPACE, #dir_spec{
        dataset = #dataset_spec{archives = [
            #archive_spec{config = #archive_config{layout = ?ARCHIVE_PLAIN_LAYOUT}}
        ]},
        children = [#file_spec{}]
    }, krakow),
    archive_tests_utils:assert_archive_state(ArchiveId, ExpectedState, ?ATTEMPTS),
    PathFun = fun
        ([]) -> <<>>;
        (Tokens) -> filename:join(Tokens)
    end,
    
    % error mocked in mock_job_function_error/2
    ErrorReasonJson = #{
        <<"description">> => <<"Operation failed with POSIX error: enoent.">>,
        <<"details">> => #{<<"errno">> => <<"enoent">>},
        <<"id">> => <<"posix">>
    },
    
    ExpectedLogsTemplates = case {ExpectedState, FailedFileType} of
        {?ARCHIVE_PRESERVED, _} -> [
            {ok, PathFun([DirName, ChildFileName]), ?REGULAR_FILE_TYPE},
            {ok, PathFun([DirName]), ?DIRECTORY_TYPE}
        ];
        {?ARCHIVE_FAILED, dir} -> [
            {archivisation_failed, PathFun([DirName]), ?DIRECTORY_TYPE, ErrorReasonJson}
        ];
        {?ARCHIVE_FAILED, reg_file} -> [
            {archivisation_failed, PathFun([DirName, ChildFileName]), ?REGULAR_FILE_TYPE, ErrorReasonJson},
            {ok, PathFun([DirName]), ?DIRECTORY_TYPE}
        ];
        {?ARCHIVE_VERIFICATION_FAILED, dir} ->
            [
                {ok, PathFun([DirName, ChildFileName]), ?REGULAR_FILE_TYPE},
                {ok, PathFun([DirName]), ?DIRECTORY_TYPE},
                {verification_failed, PathFun([]), ?DIRECTORY_TYPE}
            ];
        {?ARCHIVE_VERIFICATION_FAILED, reg_file} ->
            [
                {ok, PathFun([DirName, ChildFileName]), ?REGULAR_FILE_TYPE},
                {ok, PathFun([DirName]), ?DIRECTORY_TYPE},
                {verification_failed, PathFun([DirName, ChildFileName]), ?REGULAR_FILE_TYPE}
            ]
    end,
    GetAuditLogFun = fun() ->
        case opw_test_rpc:call(krakow, archivisation_audit_log, browse, [ArchiveId, #{}]) of
            {ok, #{<<"isLast">> := IsLast, <<"logEntries">> := LogEntries}} ->
                LogEntriesWithoutIndices = lists:map(fun(Entry) ->
                    maps:remove(<<"index">>, Entry)
                end, LogEntries),
                {ok, {IsLast, LogEntriesWithoutIndices}};
            {error, _} = Error ->
                Error
        end
    end,
    ExpectedLogs = lists:map(fun(Template) ->
        log_template_to_expected_entry(Timestamp, Template)
    end, ExpectedLogsTemplates),
    ?assertMatch({ok, {true, ExpectedLogs}}, GetAuditLogFun()).


log_template_to_expected_entry(Timestamp, {ok, Path, Type}) ->
    #{
        <<"content">> => #{
            <<"description">> =>
            string:titlecase(<<(type_to_description(Type))/binary, " archivisation finished.">>),
            <<"path">> => Path,
            <<"fileType">> => type_to_binary(Type),
            <<"startTimestamp">> => Timestamp},
        <<"severity">> => <<"info">>,
        <<"timestamp">> => Timestamp
    };
log_template_to_expected_entry(Timestamp, {archivisation_failed, Path, Type, ErrorReasonJson}) ->
    #{
        <<"content">> => #{
            <<"description">> =>
            string:titlecase(<<(type_to_description(Type))/binary, " archivisation failed.">>),
            <<"path">> => Path,
            <<"fileType">> => type_to_binary(Type),
            <<"startTimestamp">> => Timestamp,
            <<"reason">> => ErrorReasonJson},
        <<"severity">> => <<"error">>,
        <<"timestamp">> => Timestamp
    };
log_template_to_expected_entry(Timestamp, {verification_failed, Path, Type}) ->
    #{
        <<"content">> => #{
            <<"description">> =>
            <<"Verification of the archived ", (type_to_description(Type))/binary, " failed.">>,
            <<"fileType">> => type_to_binary(Type),
            <<"path">> => Path
        },
        <<"severity">> => <<"error">>,
        <<"timestamp">> => Timestamp
    }.


type_to_description(?REGULAR_FILE_TYPE) -> <<"regular file">>;
type_to_description(?DIRECTORY_TYPE) -> <<"directory">>;
type_to_description(?SYMLINK_TYPE) -> <<"symbolic link">>.


type_to_binary(?REGULAR_FILE_TYPE) -> <<"REG">>;
type_to_binary(?DIRECTORY_TYPE) -> <<"DIR">>;
type_to_binary(?SYMLINK_TYPE) -> <<"SYMLNK">>.


mock_error_requested_job(archivisation, slave_job) ->
    mock_job_function_error(archivisation_traverse, do_slave_job_unsafe);
mock_error_requested_job(archivisation, master_job) ->
    mock_job_function_error(archivisation_traverse, do_dir_master_job_unsafe);
mock_error_requested_job(verification, slave_job) ->
    mock_job_function_error(archive_verification_traverse, do_slave_job_unsafe);
mock_error_requested_job(verification, master_job) ->
    mock_job_function_error(archive_verification_traverse, do_dir_master_job_unsafe).

mock_job_function_error(Module, FunctionName) ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_new(Nodes, Module),
    {FunctionName, Arity} = lists:keyfind(FunctionName, 1, Module:module_info(exports)),
    MockFun = case Arity of
        2 -> fun(_, _) -> error(?ERROR_POSIX(?ENOENT)) end;
        3 -> fun(_, _, _) -> error(?ERROR_POSIX(?ENOENT)) end
    end,
    test_utils:mock_expect(Nodes, Module, FunctionName, MockFun).


mock_archive_creation_pending(FunctionName) ->
    Nodes = oct_background:get_all_providers_nodes(),
    test_utils:mock_new(Nodes, archivisation_traverse),
    Self = self(),
    test_utils:mock_expect(Nodes, archivisation_traverse, do_master_job, fun(Job, Args) ->
        Self ! {FunctionName, self()},
        receive continue ->
            meck:passthrough([Job, Args])
        end
    end).


% this function requires earlier call to mock_archive_creation_pending/1
finalize_archive_creation(FunctionName) ->
    receive {FunctionName, Pid} ->
        Pid ! continue
    after timer:seconds(?ATTEMPTS) ->
        throw({error, archivisation_not_started})
    end.


%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op-archive",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {provider_token_ttl_sec, 24 * 60 * 60}
        ]}]
    }).

end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(audit_log_tests, Config) ->
    ok = clock_freezer_mock:setup_for_ct(oct_background:get_all_providers_nodes(), [global_clock]),
    init_per_group(default, Config);
init_per_group(_Group, Config) ->
    Config2 = oct_background:update_background_config(Config),
    lfm_proxy:init(Config2, false).

end_per_group(_Group, Config) ->
    clock_freezer_mock:teardown_for_ct(oct_background:get_all_providers_nodes()),
    lfm_proxy:teardown(Config).

init_per_testcase(delete_not_finished_archive_error, Config) ->
    mock_archive_creation_pending(delete_not_finished_archive_error),
    init_per_testcase(default, Config);
init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    test_utils:mock_unload(oct_background:get_all_providers_nodes(), archivisation_traverse),
    test_utils:mock_unload(oct_background:get_all_providers_nodes(), archive_verification_traverse),
    ok.
