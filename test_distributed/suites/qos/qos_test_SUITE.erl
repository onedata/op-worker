%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module contains tests for QoS management on single provider.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_test_SUITE).
-author("Michal Cwiertnia").

-include("modules/logical_file_manager/lfm.hrl").
-include("qos_tests_utils.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, init_per_testcase/2,
    end_per_suite/1, end_per_testcase/2
]).

%% test functions
-export([
    % QoS bounded cache tests
    qos_bounded_cache_should_be_periodically_cleaned_if_overfilled/1,
    qos_bounded_cache_should_not_be_cleaned_if_not_overfilled/1,

    simple_key_val_qos/1,
    effective_qos_for_file_in_directory/1,

    % QoS clean up tests
    qos_cleanup_test/1,

    % QoS entry audit log
    qos_audit_log_successful_synchronization/1,
    qos_audit_log_transfer_error/1,
    qos_audit_log_failure/1,
    effective_qos_audit_log_successful_synchronization/1,
    effective_qos_audit_log_transfer_error/1,
    effective_qos_audit_log_failure/1
]).

all() -> [
    % QoS bounded cache tests
    qos_bounded_cache_should_be_periodically_cleaned_if_overfilled,
    qos_bounded_cache_should_not_be_cleaned_if_not_overfilled,

    simple_key_val_qos,
    effective_qos_for_file_in_directory,

    % QoS clean up tests
    qos_cleanup_test,

    % QoS entry audit log
    qos_audit_log_successful_synchronization,
    qos_audit_log_transfer_error,
    qos_audit_log_failure,
    effective_qos_audit_log_successful_synchronization,
    effective_qos_audit_log_transfer_error,
    effective_qos_audit_log_failure
].


-define(SPACE_PLACEHOLDER, space1).
-define(SPACE_PATH1, <<"/space1">>).
-define(PATH(Name), filename:join(?SPACE_PATH1, Name)).
-define(USER_PLACEHOLDER, user2).
-define(SESS_ID(ProviderPlaceholder), oct_background:get_user_session_id(?USER_PLACEHOLDER, ProviderPlaceholder)).


-define(GET_CACHE_TABLE_SIZE(NODE, SPACE_ID),
    element(2, lists:keyfind(size, 1, opw_test_rpc:call(Node, ets, info, [?CACHE_TABLE_NAME(SPACE_ID)])))
).

-define(QOS_CACHE_TEST_OPTIONS(Size),
    #{
        size => Size,
        group => true,
        name => ?QOS_BOUNDED_CACHE_GROUP,
        check_frequency => timer:seconds(300)
    }
).

-define(NESTED_DIR_STRUCTURE(ParentDirname), {?SPACE_PATH1, [
    {ParentDirname, [
        {<<"dir2">>, [
            {<<"dir3">>, [
                {<<"file31">>, <<"data">>},
                {<<"dir4">>, [
                    {<<"file41">>, <<"data">>}
                ]}
            ]}
        ]}
    ]}
]}).

-define(ATTEMPTS, 60).

%%%===================================================================
%%% Tests functions.
%%%===================================================================

qos_bounded_cache_should_be_periodically_cleaned_if_overfilled(_Config) ->
    bounded_cache_cleanup_test_base(overfilled).


qos_bounded_cache_should_not_be_cleaned_if_not_overfilled(_Config) ->
    bounded_cache_cleanup_test_base(unfilled).


bounded_cache_cleanup_test_base(Type) ->
    [ProviderId] = oct_background:get_provider_ids(),
    Node = oct_background:get_random_provider_node(ProviderId),
    Dirname = generator:gen_name(),
    Dir1Path = filename:join([?SPACE_PATH1, Dirname]),
    FilePath = filename:join([?SPACE_PATH1, Dirname, <<"dir2">>, <<"dir3">>, <<"dir4">>, <<"file41">>]),
    SpaceId = oct_background:get_space_id(?SPACE_PLACEHOLDER),

    EffQosTestSpec = #effective_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = ?NESTED_DIR_STRUCTURE(Dirname)
        },
        qos_to_add = [
            #qos_to_add{
                path = Dir1Path,
                qos_name = ?QOS1,
                expression = <<"providerId=", ProviderId/binary>>
            }
        ],
        expected_effective_qos = [
            #expected_file_qos{
                path = FilePath,
                qos_entries = [?QOS1],
                assigned_entries = #{opt_spaces:get_storage_id(ProviderId, SpaceId) => [?QOS1]}
            }
        ]
    },

    % add QoS and calculate effective QoS to fill in cache
    add_qos_for_dir_and_check_effective_qos(EffQosTestSpec),

    % check that QoS cache is overfilled
    SizeBeforeCleaning = ?GET_CACHE_TABLE_SIZE(Node, SpaceId),
    ?assertEqual(6, SizeBeforeCleaning),

    {CleanThreshold, ExpectedSizeAfter} = case Type of
        overfilled -> {1, 0};
        unfilled -> {SizeBeforeCleaning, SizeBeforeCleaning}
    end,
    % send message that checks cache size and cleans it if necessary
    ?assertMatch(ok, opw_test_rpc:call(Node, bounded_cache, check_cache_size, [?QOS_CACHE_TEST_OPTIONS(CleanThreshold)])),

    % check that cache has been cleaned
    SizeAfterCleaning = ?GET_CACHE_TABLE_SIZE(Node, SpaceId),
    ?assertEqual(ExpectedSizeAfter, SizeAfterCleaning).


simple_key_val_qos(_Config) ->
    [ProviderId] = oct_background:get_provider_ids(),
    run_tests([file, dir], fun(Path) ->
        qos_test_base:simple_key_val_qos_spec(Path, ProviderId, [ProviderId])
    end).


effective_qos_for_file_in_directory(_Config) ->
    Dirname = generator:gen_name(),
    DirPath = filename:join(?SPACE_PATH1, Dirname),
    FilePath = filename:join(DirPath, <<"file1">>),
    [ProviderId] = oct_background:get_provider_ids(),

    QosSpec = qos_test_base:effective_qos_for_file_in_directory_spec(DirPath, FilePath, ProviderId, [ProviderId]),
    TestSpec = #effective_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = {?SPACE_PATH1, [
                {Dirname, [
                    {<<"file1">>, ?TEST_DATA}
                ]}
            ]}
        },
        qos_to_add = QosSpec#qos_spec.qos_to_add,
        expected_qos_entries = QosSpec#qos_spec.expected_qos_entries,
        expected_effective_qos = QosSpec#qos_spec.expected_effective_qos
    },

    add_qos_for_dir_and_check_effective_qos(TestSpec).


qos_cleanup_test(_Config) ->
    [ProviderId] = oct_background:get_provider_ids(),
    Node = oct_background:get_random_provider_node(ProviderId),
    Name = generator:gen_name(),
    QosSpec = #fulfill_qos_test_spec{
        initial_dir_structure = #test_dir_structure{
            dir_structure = {?SPACE_PATH1, [
                {Name, ?TEST_DATA, [ProviderId]}
            ]}

        },
        qos_to_add = [
            #qos_to_add{
                provider_selector = ProviderId,
                qos_name = ?QOS1,
                path = filename:join([?SPACE_PATH1, Name]),
                expression = <<"providerId=", ProviderId/binary>>
            }
        ]
    },

    {GuidsAndPaths, QosNameIdMapping} = qos_tests_utils:fulfill_qos_test_base(QosSpec),

    #{files := [{FileGuid, _FilePath} | _]} = GuidsAndPaths,

    ok = lfm_proxy:unlink(Node, ?SESS_ID(ProviderId), ?FILE_REF(FileGuid)),
    FileUuid = file_id:guid_to_uuid(FileGuid),
    QosEntryId = maps:get(?QOS1, QosNameIdMapping),

    ?assertEqual({error, not_found}, opw_test_rpc:call(Node, datastore_model, get, [file_qos:get_ctx(), FileUuid])),
    ?assertEqual({error, {file_meta_missing, FileUuid}}, opw_test_rpc:call(Node, file_qos, get_effective, [FileUuid])),
    ?assertEqual({error, not_found}, opw_test_rpc:call(Node, qos_entry, get, [QosEntryId])).


%%%===================================================================
%%% QoS audit log tests
%%%===================================================================

qos_audit_log_successful_synchronization(_Config) ->
    qos_audit_log_base_test(<<"completed">>, single_file).


qos_audit_log_transfer_error(_Config) ->
    % error mocked in init_per_testcase
    qos_audit_log_base_test(<<"failed">>, single_file).


qos_audit_log_failure(_Config) ->
    % error mocked in init_per_testcase
    qos_audit_log_base_test(<<"failed">>, single_file).


effective_qos_audit_log_successful_synchronization(_Config) ->
    qos_audit_log_base_test(<<"completed">>, effective).


effective_qos_audit_log_transfer_error(_Config) ->
    % error mocked in init_per_testcase
    qos_audit_log_base_test(<<"failed">>, effective).


effective_qos_audit_log_failure(_Config) ->
    % error mocked in init_per_testcase
    qos_audit_log_base_test(<<"failed">>, effective).


qos_audit_log_base_test(ExpectedStatus, Type) ->
    ok = clock_freezer_mock:set_current_time_millis(123),
    [ProviderId] = oct_background:get_provider_ids(),
    Node = oct_background:get_random_provider_node(ProviderId),
    Timestamp = opw_test_rpc:call(Node, global_clock, timestamp_millis, []),
    FilePath = filename:join([?SPACE_PATH1, generator:gen_name()]),
    {RootGuid, FileIds} = prepare_audit_log_test_env(Type, Node, ?SESS_ID(ProviderId), FilePath),
    {ok, QosEntryId} = opt_qos:add_qos_entry(Node, ?SESS_ID(ProviderId), ?FILE_REF(RootGuid), <<"providerId=", ProviderId/binary>>, 1),
    {BaseExpectedContent, ExpectedSeverity} = case ExpectedStatus of
        <<"completed">> ->
            {#{<<"description">> => <<"Local replica reconciled.">>}, <<"info">>};
        <<"failed">> ->
            {
                #{
                    % error mocked in init_per_testcase
                    <<"reason">> => #{
                        <<"description">> => <<"Operation failed with POSIX error: enoent.">>,
                        <<"details">> => #{<<"errno">> => <<"enoent">>},
                        <<"id">> => <<"posix">>
                    },
                    <<"description">> => <<"Failed to reconcile local replica: Operation failed with POSIX error: enoent.">>
                },
                <<"error">>
            }
    end,
    SortFun = fun(#{<<"content">> := #{<<"fileId">> := FileIdA}}, #{<<"content">> := #{<<"fileId">> := FileIdB}}) ->
        FileIdA =< FileIdB
    end,
    Expected = lists:sort(SortFun, lists:flatmap(fun(ObjectId) ->
        [
            #{
                <<"timestamp">> => Timestamp,
                <<"severity">> => <<"info">>,
                <<"content">> => #{
                    <<"status">> => <<"scheduled">>,
                    <<"fileId">> => ObjectId,
                    <<"description">> => <<"Remote replica differs, reconciliation started.">>
                }
            },
            #{
                <<"timestamp">> => Timestamp,
                <<"severity">> => ExpectedSeverity,
                <<"content">> => BaseExpectedContent#{
                    <<"fileId">> => ObjectId,
                    <<"status">> => ExpectedStatus
                }
            }
        ]
    end, FileIds)),
    GetAuditLogFun = fun() ->
        case opw_test_rpc:call(Node, qos_entry_audit_log, browse_content, [QosEntryId, #{}]) of
            {ok, #{<<"isLast">> := IsLast, <<"logEntries">> := LogEntries}} ->
                LogEntriesWithoutIndices = lists:map(fun(Entry) ->
                    maps:remove(<<"index">>, Entry)
                end, LogEntries),
                {ok, {IsLast, lists:sort(SortFun, LogEntriesWithoutIndices)}};
            {error, _} = Error ->
                Error
        end
    end,
    ?assertMatch({ok, {true, Expected}}, GetAuditLogFun(), 10).


prepare_audit_log_test_env(single_file, Node, SessId, RootFilePath) ->
    {ok, Guid} = lfm_test_utils:create_file(<<"file">>, Node, SessId, RootFilePath),
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
    {Guid, [ObjectId]};
prepare_audit_log_test_env(effective, Node, SessId, RootFilePath) ->
    {ok, DirGuid} = lfm_test_utils:create_file(<<"dir">>, Node, SessId, RootFilePath),
    ChildrenNum = rand:uniform(50),
    ChildrenIds = lists_utils:pmap(fun(Num) ->
        FilePath = filename:join(RootFilePath, integer_to_binary(Num)),
        {ok, Guid} = lfm_test_utils:create_file(<<"file">>, Node, SessId, FilePath),
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        ObjectId
    end, lists:seq(1, ChildrenNum)),
    {DirGuid, ChildrenIds}.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite([{?LOAD_MODULES, [?MODULE, qos_tests_utils]} | Config], #onenv_test_config{
        onenv_scenario = "1op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60},
            {provider_token_ttl_sec, 24 * 60 * 60},
            {qos_retry_failed_files_interval_seconds, 5}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(Case, Config) when
    Case =:= qos_audit_log_transfer_error;
    Case =:= effective_qos_audit_log_transfer_error ->
    audit_log_tests_init_per_testcase(Config, ?ERROR_POSIX(?ENOENT)),
    init_per_testcase(default, Config);
init_per_testcase(Case, Config) when
    Case =:= qos_audit_log_failure;
    Case =:= effective_qos_audit_log_failure ->
    audit_log_tests_init_per_testcase(Config, {throw, ?ERROR_POSIX(?ENOENT)}),
    init_per_testcase(default, Config);
init_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    ok = clock_freezer_mock:setup_for_ct(Nodes, [global_clock, ?MODULE]),
    lfm_proxy:init(Config).

audit_log_tests_init_per_testcase(Config, ExpectedSynchronizer) ->
    Nodes = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Nodes, replica_synchronizer, [passthrough]),
    qos_tests_utils:mock_replica_synchronizer(Nodes, ExpectedSynchronizer),
    % mock retry failed files, so there is only one failed entry in audit log
    qos_tests_utils:mock_replica_synchronizer(Nodes, ?ERROR_POSIX(?ENOENT)),
    test_utils:mock_expect(Nodes, qos_hooks, retry_failed_files, fun(_SpaceId) -> ok end).


end_per_testcase(_, Config) ->
    Nodes = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    test_utils:mock_unload(Nodes).


%%%===================================================================
%%% Internal functions
%%%===================================================================

run_tests(FileTypes, TestSpecFun) ->
    lists:foreach(fun
        (file) ->
            ct:pal("Starting for file"),
            Path = create_test_file(),
            add_qos_and_check_qos_docs(TestSpecFun(Path));
        (dir) ->
            ct:pal("Starting for dir"),
            Path = create_test_dir_with_file(),
            add_qos_and_check_qos_docs(TestSpecFun(Path))
    end, FileTypes).


add_qos_and_check_qos_docs(#qos_spec{
    qos_to_add = QosToAddList,
    expected_qos_entries = ExpectedQosEntries,
    expected_file_qos = ExpectedFileQos
}) ->
    % add QoS for file and wait for appropriate QoS status
    QosNameIdMapping = qos_tests_utils:add_multiple_qos(QosToAddList),
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(QosNameIdMapping, ExpectedQosEntries),

    % check qos documents
    qos_tests_utils:assert_qos_entry_documents(ExpectedQosEntries, QosNameIdMapping),
    qos_tests_utils:assert_file_qos_documents(ExpectedFileQos, QosNameIdMapping, false).


add_qos_for_dir_and_check_effective_qos(#effective_qos_test_spec{
    initial_dir_structure = InitialDirStructure,
    qos_to_add = QosToAddList,
    expected_qos_entries = ExpectedQosEntries,
    expected_effective_qos = ExpectedEffectiveQos
}) ->
    % create initial dir structure
    qos_tests_utils:create_dir_structure(InitialDirStructure),

    % add QoS and wait for appropriate QoS status
    QosNameIdMapping = qos_tests_utils:add_multiple_qos(QosToAddList),
    qos_tests_utils:wait_for_qos_fulfillment_in_parallel(QosNameIdMapping, ExpectedQosEntries),

    % check qos_entry documents and effective QoS
    qos_tests_utils:assert_qos_entry_documents(ExpectedQosEntries, QosNameIdMapping),
    qos_tests_utils:assert_effective_qos(ExpectedEffectiveQos, QosNameIdMapping, false).


create_test_file() ->
    [ProviderId] = oct_background:get_provider_ids(),
    Name = generator:gen_name(),
    Path = ?PATH(Name),
    _Guid = qos_tests_utils:create_file(ProviderId, ?SESS_ID(ProviderId), Path, ?TEST_DATA),
    Path.


create_test_dir_with_file() ->
    [ProviderId] = oct_background:get_provider_ids(),
    Name = generator:gen_name(),
    DirPath = ?PATH(Name),
    _DirGuid = qos_tests_utils:create_directory(ProviderId, ?SESS_ID(ProviderId), DirPath),
    FilePath = filename:join(DirPath, <<"file1">>),
    _FileGuid = qos_tests_utils:create_file(ProviderId, ?SESS_ID(ProviderId), FilePath, ?TEST_DATA),
    DirPath.
