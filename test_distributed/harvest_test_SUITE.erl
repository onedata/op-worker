%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module tests mechanism for harvesting metadata
%%% @end
%%%-------------------------------------------------------------------
-module(harvest_test_SUITE).
-author("Jakub Kudzia").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    set_json_metadata_test/1,
    modify_json_metadata_test/1,
    delete_json_metadata_test/1,
    delete_file_with_json_metadata_test/1,
    modify_json_many_times/1,
    changes_should_be_submitted_to_all_harvesters_subscribed_for_the_space/1,
    changes_should_be_submitted_to_all_indices_subscribed_for_the_space/1,
    changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester/1,
    each_provider_should_submit_only_local_changes_to_the_harvester/1,
    each_provider_should_submit_only_local_changes_to_the_harvester_deletion_test/1,
    submit_entry_failure_test/1,
    delete_entry_failure_test/1
]).

all() ->
    ?ALL([
        set_json_metadata_test,
        modify_json_metadata_test,
        delete_json_metadata_test,
        delete_file_with_json_metadata_test,
        modify_json_many_times,
        changes_should_be_submitted_to_all_harvesters_subscribed_for_the_space,
        changes_should_be_submitted_to_all_indices_subscribed_for_the_space,
        changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester,
        each_provider_should_submit_only_local_changes_to_the_harvester,
        each_provider_should_submit_only_local_changes_to_the_harvester_deletion_test,
        submit_entry_failure_test,
        delete_entry_failure_test
    ]).

-define(SPACE_ID1, <<"space_id1">>).
-define(SPACE_ID2, <<"space_id2">>).
-define(SPACE_ID3, <<"space_id3">>).
-define(SPACE_ID4, <<"space_id4">>).
-define(SPACE_ID5, <<"space_id5">>).

-define(SPACE_NAME1, <<"space1">>).
-define(SPACE_NAME2, <<"space2">>).
-define(SPACE_NAME3, <<"space3">>).
-define(SPACE_NAME4, <<"space4">>).
-define(SPACE_NAME5, <<"space5">>).

-define(SPACE_NAMES, #{
    ?SPACE_ID1 => ?SPACE_NAME1,
    ?SPACE_ID2 => ?SPACE_NAME2,
    ?SPACE_ID3 => ?SPACE_NAME3,
    ?SPACE_ID4 => ?SPACE_NAME4,
    ?SPACE_ID5 => ?SPACE_NAME5
}).

-define(SPACE_NAME(__SpaceId), maps:get(__SpaceId, ?SPACE_NAMES)).

-define(PATH(FileName, SpaceId), filename:join(["/", ?SPACE_NAME(SpaceId), FileName])).

-define(HARVESTER1, <<"harvester1">>).
-define(HARVESTER2, <<"harvester2">>).
-define(HARVESTER3, <<"harvester3">>).

-define(USER_ID, <<"user1">>).
-define(SESS_ID(Worker),
    ?config({session_id, {?USER_ID, ?GET_DOMAIN(Worker)}}, Config)).

-define(XATTR_NAME, <<"xattr_name_", (?RAND_NAME)/binary>>).
-define(XATTR_VAL, <<"xattr_val_", (?RAND_NAME)/binary>>).

-define(FILE_NAME, <<"file_", (?RAND_NAME)/binary>>).

-define(RAND_NAME,
    <<(str_utils:to_binary(?FUNCTION))/binary, "_", (integer_to_binary(rand:uniform(?RAND_RANGE)))/binary>>).

-define(RAND_RANGE, 1000000000).
-define(ATTEMPTS, 20).
-define(TIMEOUT, timer:seconds(?ATTEMPTS)).

-define(PROVIDER_ID(Node), rpc:call(Node, oneprovider, get_id, [])).

%% Test config:
%% space_id1:
%%  * supported by: p1
%%  * harvesters: harvester1
%%  * indices: index1
%% space_id2:
%%  * supported by: p1
%%  * harvesters: harvester1, harvester2
%% space_id3:
%%  * supported by: p1
%%  * harvesters: harvester1
%% space_id4:
%%  * supported by: p1
%%  * harvesters: harvester1
%% space_id5:
%%  * supported by: p1, p2
%%  * harvesters: harvester3

-define(SUBMIT_ENTRY, submit_entry).
-define(SUBMIT_ENTRY(FileId, Harvester, JSON, Indices, ExpProviderId),
    {?SUBMIT_ENTRY, FileId, Harvester, JSON, Indices, ExpProviderId}).

-define(assertReceivedSubmitEntry(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId),
    ?assertReceivedSubmitEntry(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId, ?TIMEOUT)).
-define(assertReceivedSubmitEntry(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId, Timeout),
    ?assertReceivedEqual(?SUBMIT_ENTRY(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId), Timeout)).

-define(assertNotReceivedSubmitEntry(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId),
    ?assertNotReceivedSubmitEntry(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId, ?TIMEOUT)).
-define(assertNotReceivedSubmitEntry(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId, Timeout),
    ?assertNotReceivedMatch(?SUBMIT_ENTRY(ExpFileId, ExpHarvester, ExpJSON, ExpIndices, ExpProviderId), Timeout)).

-define(DELETE_ENTRY, delete_entry).
-define(DELETE_ENTRY(FileId, Harvester, ExpIndices, ExpProviderId),
    {?DELETE_ENTRY, FileId, Harvester, ExpIndices, ExpProviderId}).

-define(assertReceivedDeleteEntry(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId),
    ?assertReceivedDeleteEntry(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId, ?TIMEOUT)).
-define(assertReceivedDeleteEntry(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId, Timeout),
    ?assertReceivedEqual(?DELETE_ENTRY(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId), Timeout)).

-define(assertNotReceivedDeleteEntry(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId),
    ?assertNotReceivedDeleteEntry(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId, ?TIMEOUT)).
-define(assertNotReceivedDeleteEntry(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId, Timeout),
    ?assertNotReceivedEqual(?DELETE_ENTRY(ExpFileId, ExpHarvester, ExpIndices, ExpProviderId), Timeout)).


-define(INDEX(N), <<"index", (integer_to_binary(N))/binary>>).
-define(INDEX11, ?INDEX(11)).
-define(INDEX21, ?INDEX(21)).
-define(INDEX22, ?INDEX(22)).
-define(INDEX23, ?INDEX(23)).
-define(INDEX31, ?INDEX(31)).

-define(MOCK_HARVESTER_LOGIC_FAILURE, mock_harvester_logic_failure).

%%%====================================================================
%%% Test function
%%%====================================================================

set_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, JSON, [?INDEX11], ?PROVIDER_ID(Worker)).

modify_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, JSON, [?INDEX11], ?PROVIDER_ID(Worker)),

    JSON2 = #{<<"color">> => <<"blue">>, <<"size">> => <<"big">>},
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON2, []),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, JSON2, [?INDEX11], ?PROVIDER_ID(Worker)).

delete_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, JSON, [?INDEX11], ?PROVIDER_ID(Worker)),

    ok = lfm_proxy:remove_metadata(Worker, SessId, {guid, Guid}, json),
    ?assertReceivedDeleteEntry(FileId, ?HARVESTER1, [?INDEX11], ?PROVIDER_ID(Worker)).


delete_file_with_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, JSON, [?INDEX11], ?PROVIDER_ID(Worker)),

    ok = lfm_proxy:unlink(Worker, SessId, {guid, Guid}),
    ?assertReceivedDeleteEntry(FileId, ?HARVESTER1, [?INDEX11], ?PROVIDER_ID(Worker)).

modify_json_many_times(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    Modifications = 10000,
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ExpectedFinalJSON = lists:foldl(fun(I, _) ->
        Key = <<"key_", (integer_to_binary(I))/binary>>,
        Value = <<"value_", (integer_to_binary(I))/binary>>,
        JSON = #{Key => Value},
        ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
        JSON
    end, undefined, lists:seq(1, Modifications)),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, ExpectedFinalJSON, [?INDEX11], ?PROVIDER_ID(Worker)).

changes_should_be_submitted_to_all_harvesters_subscribed_for_the_space(Config) ->
    % ?HARVESTER1 and ?HARVESTER2 are subscribed for ?SPACE_ID2
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,

    JSON1 = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID2), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, JSON1, [?INDEX11], ?PROVIDER_ID(Worker)),
    ?assertReceivedSubmitEntry(FileId, ?HARVESTER2, JSON1, [?INDEX21], ?PROVIDER_ID(Worker)).

changes_should_be_submitted_to_all_indices_subscribed_for_the_space(Config) ->
    % ?HARVESTER1 and ?HARVESTER2 are subscribed for ?SPACE_ID2
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,

    JSON1 = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID2), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER2, JSON1, [?INDEX21], ?PROVIDER_ID(Worker)),
    ?assertReceivedSubmitEntry(FileId, ?HARVESTER2, JSON1, [?INDEX22], ?PROVIDER_ID(Worker)),
    ?assertReceivedSubmitEntry(FileId, ?HARVESTER2, JSON1, [?INDEX23], ?PROVIDER_ID(Worker)).

changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester(Config) ->
    % ?HARVESTER1 is subscribed for ?SPACE_ID3 and ?SPACE_ID4
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    FileName2 = ?FILE_NAME,
    JSON2 = #{<<"color">> => <<"red">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID3), 8#600),
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),

    {ok, Guid2} = lfm_proxy:create(Worker, SessId, ?PATH(FileName2, ?SPACE_ID4), 8#600),
    {ok, FileId2} = file_id:guid_to_objectid(Guid2),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid2}, json, JSON2, []),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, JSON1, [?INDEX11], ?PROVIDER_ID(Worker)),
    ?assertReceivedSubmitEntry(FileId2, ?HARVESTER1, JSON2, [?INDEX11], ?PROVIDER_ID(Worker)).

each_provider_should_submit_only_local_changes_to_the_harvester(Config) ->
    % ?HARVESTER3 is subscribed for ?SPACE_ID5 which is supported by both providers
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(WorkerP1),
    SessId2 = ?SESS_ID(WorkerP2),
    ProviderId1 = ?PROVIDER_ID(WorkerP1),
    ProviderId2 = ?PROVIDER_ID(WorkerP2),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    FileName2 = ?FILE_NAME,
    JSON2 = #{<<"color">> => <<"red">>},

    {ok, Guid} = lfm_proxy:create(WorkerP1, SessId, ?PATH(FileName, ?SPACE_ID5), 8#600),
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(WorkerP1, SessId, {guid, Guid}, json, JSON1, []),

    {ok, Guid2} = lfm_proxy:create(WorkerP2, SessId2, ?PATH(FileName2, ?SPACE_ID5), 8#600),
    {ok, FileId2} = file_id:guid_to_objectid(Guid2),
    ok = lfm_proxy:set_metadata(WorkerP2, SessId2, {guid, Guid2}, json, JSON2, []),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER3, JSON1, [?INDEX31], ProviderId1),
    ?assertReceivedSubmitEntry(FileId2, ?HARVESTER3, JSON2, [?INDEX31], ProviderId2),

    % calls to harvester_logic:create entry should not be duplicated
    ?assertNotReceivedSubmitEntry(FileId, ?HARVESTER3, _, _, ProviderId1),
    ?assertNotReceivedSubmitEntry(FileId2, ?HARVESTER3, _, _, ProviderId2).

each_provider_should_submit_only_local_changes_to_the_harvester_deletion_test(Config) ->
    % ?HARVESTER3 is subscribed for ?SPACE_ID5 which is supported by both providers
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(WorkerP1),
    SessId2 = ?SESS_ID(WorkerP2),
    ProviderId1 = ?PROVIDER_ID(WorkerP1),
    ProviderId2 = ?PROVIDER_ID(WorkerP2),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    FileName2 = ?FILE_NAME,
    JSON2 = #{<<"color">> => <<"red">>},

    {ok, Guid} = lfm_proxy:create(WorkerP1, SessId, ?PATH(FileName, ?SPACE_ID5), 8#600),
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(WorkerP1, SessId, {guid, Guid}, json, JSON1, []),

    {ok, Guid2} = lfm_proxy:create(WorkerP2, SessId2, ?PATH(FileName2, ?SPACE_ID5), 8#600),
    {ok, FileId2} = file_id:guid_to_objectid(Guid2),
    ok = lfm_proxy:set_metadata(WorkerP2, SessId2, {guid, Guid2}, json, JSON2, []),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER3, JSON1, [?INDEX31], ProviderId1),
    ?assertReceivedSubmitEntry(FileId2, ?HARVESTER3, JSON2, [?INDEX31], ProviderId2),

    % calls to harvester_logic:create entry should not be duplicated
    ?assertNotReceivedSubmitEntry(FileId, ?HARVESTER3, _, _, ProviderId1),
    ?assertNotReceivedSubmitEntry(FileId2, ?HARVESTER3, _, _, ProviderId2),

    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP1, SessId, {guid, Guid2}), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:stat(WorkerP2, SessId2, {guid, Guid}), ?ATTEMPTS),

    ok = lfm_proxy:unlink(WorkerP1, SessId, {guid, Guid2}),
    ok = lfm_proxy:unlink(WorkerP2, SessId2, {guid, Guid}),

    ?assertReceivedDeleteEntry(FileId2, ?HARVESTER3, [?INDEX31], ProviderId1),
    ?assertReceivedDeleteEntry(FileId, ?HARVESTER3, [?INDEX31], ProviderId2),

    % calls to harvester_logic:delete entry should not be duplicated
    ?assertNotReceivedDeleteEntry(FileId, ?HARVESTER3, [?INDEX31], ProviderId1),
    ?assertNotReceivedDeleteEntry(FileId2, ?HARVESTER3, [?INDEX31], ProviderId2).

submit_entry_failure_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    FileName2 = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},
    ProviderId1 = ?PROVIDER_ID(Worker),

    HSPid1 = get_harvest_stream_pid(Worker, ?HARVESTER1, ?SPACE_ID1, ?INDEX11),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, JSON1, [?INDEX11], ProviderId1),

    set_mock_harvester_logic_failure(Worker, true),

    JSON2 = #{<<"color">> => <<"red">>},
    JSON3 = #{<<"color">> => <<"green">>},

    {ok, Guid2} = lfm_proxy:create(Worker, SessId, ?PATH(FileName2, ?SPACE_ID1), 8#600),
    {ok, FileId2} = file_id:guid_to_objectid(Guid2),

    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON2, []),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid2}, json, JSON3, []),

    % changes should not be submitted as connection to onezone failed
    ?assertNotReceivedSubmitEntry(FileId, ?HARVESTER1, JSON2, _, ProviderId1),
    ?assertNotReceivedSubmitEntry(FileId2, ?HARVESTER1, JSON3, _, ProviderId1),

    set_mock_harvester_logic_failure(Worker, false),

    % harvest_stream should not have been restarted
    ?assertEqual(HSPid1,
        get_harvest_stream_pid(Worker, ?HARVESTER1, ?SPACE_ID1, ?INDEX11)),

    % previously sent change should not be submitted
    ?assertNotReceivedSubmitEntry(FileId, ?HARVESTER1, JSON1, _, ProviderId1),

    % missing changes should be submitted
    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, JSON2, [?INDEX11], ProviderId1),
    ?assertReceivedSubmitEntry(FileId2, ?HARVESTER1, JSON3, [?INDEX11], ProviderId1).

delete_entry_failure_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},
    ProviderId1 = ?PROVIDER_ID(Worker),

    HSPid1 = get_harvest_stream_pid(Worker, ?HARVESTER1, ?SPACE_ID1, ?INDEX11),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    {ok, FileId} = file_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, JSON1, [?INDEX11], ProviderId1),

    set_mock_harvester_logic_failure(Worker, true),

    ok = lfm_proxy:unlink(Worker, SessId, {guid, Guid}),

    % change should not be submitted as connection to onezone failed
    ?assertNotReceivedDeleteEntry(FileId, ?HARVESTER1, [?INDEX11], ProviderId1),

    set_mock_harvester_logic_failure(Worker, false),

    % harvest_stream should not have been restarted
    ?assertEqual(HSPid1,
        get_harvest_stream_pid(Worker, ?HARVESTER1, ?SPACE_ID1, ?INDEX11)),

    % previously sent change should not be submitted
    ?assertNotReceivedSubmitEntry(FileId, ?HARVESTER1, JSON1, _, ProviderId1),

    % missing changes should be submitted
    ?assertReceivedDeleteEntry(FileId, ?HARVESTER1, [?INDEX11], ProviderId1).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(_Case, Config) ->
    Config2 = sort_workers(Config),
    Workers = ?config(op_worker_nodes, Config2),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config2, "env_desc.json"), Config2),
    lists:foreach(fun(W) ->
        {ok, D} = rpc:call(W, provider_logic, get, [?PROVIDER_ID(W)]),
        % trigger od_provider posthooks
        rpc:call(W, od_provider, save_to_cache, [D])
    end, Workers),
    set_mock_harvester_logic_failure(Workers, false),
    mock_harvester_logic_submit_entry(Workers),
    mock_harvester_logic_delete_entry(Workers),
    mock_space_quota_checks(Workers),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, harvester_logic),
    lists:foreach(fun(W) ->
        SupervisorPid = whereis(W, harvest_stream_sup),
        exit(SupervisorPid, kill)
    end, Workers),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

whereis(Node, Name) ->
    rpc:call(Node, erlang, whereis, [Name]).

mock_harvester_logic_submit_entry(Node) ->
    Self = self(),
    ok = test_utils:mock_expect(Node, harvester_logic, submit_entry,
        fun(HarvesterId, FileId, JSON, Indices, _Seq, _MaxSeq) ->
            case application:get_env(op_worker, ?MOCK_HARVESTER_LOGIC_FAILURE, false) of
                true ->
                    {error, test_error};
                false ->
                    Self ! ?SUBMIT_ENTRY(FileId, HarvesterId, JSON, Indices, oneprovider:get_id()),
                    {ok, []}
            end
        end
    ).

mock_harvester_logic_delete_entry(Node) ->
    Self = self(),
    ok = test_utils:mock_expect(Node, harvester_logic, delete_entry,
        fun(HarvesterId, FileId, Indices, _Seq, _MaxSeq) ->
            case application:get_env(op_worker, ?MOCK_HARVESTER_LOGIC_FAILURE, false) of
                true ->
                    {error, test_error};
                false ->
                    Self ! ?DELETE_ENTRY(FileId, HarvesterId, Indices, oneprovider:get_id()),
                    {ok, []}
            end
        end
    ).

set_mock_harvester_logic_failure(Nodes, Boolean) ->
    test_utils:set_env(Nodes, op_worker, ?MOCK_HARVESTER_LOGIC_FAILURE, Boolean).

mock_space_quota_checks(Node) ->
    % mock space_quota to mock error logs due to some test environment issues
    ok = test_utils:mock_new(Node, space_quota),
    ok = test_utils:mock_expect(Node, space_quota, get_disabled_spaces, fun() -> {ok, []} end).

sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).

which_children(Node, SupRef) ->
    rpc:call(Node, supervisor, which_children, [SupRef]).

get_harvest_stream_pid(Node, HarvesterId, SpaceId, IndexId) ->
    {_, HSPid, _, _} = ?assertMatch({_, _, _, _},
        lists:keyfind({HarvesterId, SpaceId, IndexId}, 1, which_children(Node, harvest_stream_sup)),
        ?ATTEMPTS),
    HSPid.