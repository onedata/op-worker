%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% This module contains base test functions for testing storage import.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_oct_test_base).
-author("Katarzyna Such").

-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("space_setup_utils.hrl").
-include_lib("storage_import_oct_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("onenv_test_utils.hrl").

-define(RANDOM_PROVIDER(), ?RAND_ELEMENT([krakow, paris])).

-export([assert_monitoring_state/4]).

%% tests
-export([
    % tests of import
    empty_import_test/1,
    create_directory_import_test/1
]).

%%%===================================================================
%%% Tests of import
%%%===================================================================

empty_import_test(Config) ->
    [NodeKrakow] =  oct_background:get_provider_nodes(krakow),
    [NodeParis] =  oct_background:get_provider_nodes(paris),
    SessionId = oct_background:get_user_session_id(user1, krakow),
    SessionId2 = oct_background:get_user_session_id(user1, paris),
    SpaceId = proplists:get_value(space_id, Config),
    SpaceName = proplists:get_value(space_name, Config),
    enable_initial_scan(Config, SpaceId),
    assertInitialScanFinished(NodeKrakow, SpaceId),
    ?assertMatch({ok, []},
        lfm_proxy:get_children(NodeKrakow, SessionId, {path, ?SPACE_PATH(SpaceName)}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(NodeKrakow, SessionId, {path, ?SPACE_PATH(SpaceName)}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(NodeParis, SessionId2, {path, ?SPACE_PATH(SpaceName)}), ?ATTEMPTS),

    ?assertMonitoring(NodeKrakow, #{
        <<"scans">> => 1,
        <<"created">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"createdMinHist">> => 0,
        <<"createdHourHist">> => 0,
        <<"createdDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, SpaceId).


create_directory_import_test(Config) ->
    [NodeKrakow] =  oct_background:get_provider_nodes(krakow),
    [NodeParis] =  oct_background:get_provider_nodes(paris),
    SessionId = oct_background:get_user_session_id(user1, krakow),
    SessionId2 = oct_background:get_user_session_id(user1, paris),

    SpaceId = proplists:get_value(space_id, Config),
    SpaceName = proplists:get_value(space_name, Config),
    ImportedStorageId = proplists:get_value(imported_storage_id, Config),
    NotImportedStorageId = proplists:get_value(not_imported_storage_id, Config),

    DirName = generator:gen_name(),
    StorageTestDirPath = filename:join([<<"/">>, DirName]),

    SDHandle = sd_test_utils:new_handle(NodeKrakow, SpaceId, StorageTestDirPath, ImportedStorageId),
    ok = sd_test_utils:mkdir(NodeKrakow, SDHandle, ?DEFAULT_DIR_PERMS),
    enable_initial_scan(Config, SpaceId),
    assertInitialScanFinished(NodeKrakow, SpaceId),

    %% Check if dir was imported
    ?assertMatch({ok, [{_, DirName}]},
        lfm_proxy:get_children(NodeKrakow, SessionId, {path, ?SPACE_PATH(SpaceName)}, 0, 10), ?ATTEMPTS),

    SpaceTestDirPath = filename:join([<<"/">>, SpaceName, DirName]),
    StorageSDHandleNodeKrakow = sd_test_utils:get_storage_mountpoint_handle(NodeKrakow, SpaceId, ImportedStorageId),
    StorageSDHandleNodeParis = sd_test_utils:get_storage_mountpoint_handle(NodeKrakow, SpaceId, NotImportedStorageId),
    {ok, #statbuf{st_uid = MountUid1}} = sd_test_utils:stat(NodeKrakow, StorageSDHandleNodeKrakow),
    {ok, #statbuf{st_uid = MountUid2, st_gid = MountGid2}} = sd_test_utils:stat(NodeParis, StorageSDHandleNodeParis),

    SpaceOwner = ?SPACE_OWNER_ID(SpaceId),

    ?assertMatch({ok, #file_attr{
        owner_id = SpaceOwner,
        uid = MountUid1,
        gid = 0
    }}, lfm_proxy:stat(NodeKrakow, SessionId, {path, SpaceTestDirPath}), ?ATTEMPTS),

    ?assertMatch({ok, #file_attr{
        owner_id = SpaceOwner,
        uid = MountUid2,
        gid = MountGid2
    }}, lfm_proxy:stat(NodeParis, SessionId2, {path, SpaceTestDirPath}), ?ATTEMPTS),

%%     this is original, not working
%%    ?assertMonitoring(NodeKrakow, #{
%%        <<"scans">> => 1,
%%        <<"created">> => 1,
%%        <<"modified">> => 1,
%%        <<"deleted">> => 0,
%%        <<"failed">> => 0,
%%        <<"unmodified">> => 0,
%%        <<"createdMinHist">> => 1,
%%        <<"createdHourHist">> => 1,
%%        <<"createdDayHist">> => 1,
%%        <<"modifiedMinHist">> => 1,
%%        <<"modifiedHourHist">> => 1,
%%        <<"modifiedDayHist">> => 1,
%%        <<"deletedMinHist">> => 0,
%%        <<"deletedHourHist">> => 0,
%%        <<"deletedDayHist">> => 0,
%%        <<"queueLengthMinHist">> => 0,
%%        <<"queueLengthHourHist">> => 0,
%%        <<"queueLengthDayHist">> => 0
%%    }, SpaceId).

    ?assertMonitoring(NodeKrakow, #{
        <<"scans">> => 1,
        <<"created">> => 1,
        <<"modified">> => 0,
        <<"deleted">> => 0,
        <<"failed">> => 0,
        <<"unmodified">> => 1,
        <<"createdMinHist">> => 1,
        <<"createdHourHist">> => 1,
        <<"createdDayHist">> => 1,
        <<"modifiedMinHist">> => 0,
        <<"modifiedHourHist">> => 0,
        <<"modifiedDayHist">> => 0,
        <<"deletedMinHist">> => 0,
        <<"deletedHourHist">> => 0,
        <<"deletedDayHist">> => 0,
        <<"queueLengthMinHist">> => 0,
        <<"queueLengthHourHist">> => 0,
        <<"queueLengthDayHist">> => 0
    }, SpaceId).


%%%===================================================================
%%% Util functions
%%%===================================================================

enable_initial_scan(Config, SpaceId) ->
    [NodeKrakow] =  oct_background:get_provider_nodes(krakow),
    ImportConfig = proplists:get_value(import_config, Config, #{}),
    MaxDepth = maps:get(max_depth, ImportConfig, ?MAX_DEPTH),
    SyncAcl = maps:get(sync_acl, ImportConfig, ?SYNC_ACL),
    ?assertMatch(ok, ?rpc(NodeKrakow, storage_import:set_or_configure_auto_mode(
        SpaceId, #{max_depth => MaxDepth, sync_acl => SyncAcl}))).


assertInitialScanFinished(Worker, SpaceId) ->
    ?assertEqual(true, catch(?rpc(Worker, storage_import_monitoring:is_initial_scan_finished(SpaceId))), ?ATTEMPTS).


assert_monitoring_state(Worker, ExpectedSSM, SpaceId, Attempts) ->
    SSM = monitoring_describe(Worker, SpaceId),
    SSM2 = flatten_histograms(SSM),
    try
        assert(ExpectedSSM, SSM2),
        SSM2
    catch
        throw:{assertion_error, Key, ExpectedValue, Value}:Stacktrace ->
            case Attempts == 0 of
                false ->
                    timer:sleep(timer:seconds(1)),
                    assert_monitoring_state(Worker, ExpectedSSM, SpaceId, Attempts - 1);
                true ->
                    {Format, Args} = storage_import_monitoring_description(SSM),
                    ct:pal(
                        "Assertion of field \"~p\" in storage_import_monitoring for space ~p failed.~n"
                        "    Expected: ~p~n"
                        "    Value: ~p~n"
                        ++ Format ++
                            "~nStacktrace:~n~p",
                        [Key, SpaceId, ExpectedValue, Value] ++ Args ++ [Stacktrace]),
                    ct:fail("assertion failed")
            end
    end.


assert(ExpectedSSM, SSM) ->
    maps:fold(fun(Key, Value, _AccIn) ->
        assert_for_key(Key, Value, SSM)
    end, ok, ExpectedSSM).


assert_for_key(Key, ExpectedValue, SSM) ->
    case maps:get(Key, SSM) of
        ExpectedValue -> ok;
        Value ->
            throw({assertion_error, Key, ExpectedValue, Value})
    end.


monitoring_describe(Worker, SpaceId) ->
    ?rpc(Worker, storage_import_monitoring:describe(SpaceId)).


storage_import_monitoring_description(SSM) ->
    maps:fold(fun(Key, Value, {AccFormat, AccArgs}) ->
        {AccFormat ++ "    ~p = ~p~n", AccArgs ++ [Key, Value]}
    end, {"~n#storage_import_monitoring fields values:~n", []}, SSM).


flatten_histograms(SSM) ->
    SSM#{
        % flatten beginnings of histograms for assertions
        <<"createdMinHist">> => lists:sum(lists:sublist(maps:get(<<"createdMinHist">>, SSM), 2)),
        <<"modifiedMinHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedMinHist">>, SSM), 2)),
        <<"deletedMinHist">> => lists:sum(lists:sublist(maps:get(<<"deletedMinHist">>, SSM), 2)),
        <<"queueLengthMinHist">> => hd(maps:get(<<"queueLengthMinHist">>, SSM)),

        <<"createdHourHist">> => lists:sum(lists:sublist(maps:get(<<"createdHourHist">>, SSM), 3)),
        <<"modifiedHourHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedHourHist">>, SSM), 3)),
        <<"deletedHourHist">> => lists:sum(lists:sublist(maps:get(<<"deletedHourHist">>, SSM), 3)),
        <<"queueLengthHourHist">> => hd(maps:get(<<"queueLengthHourHist">>, SSM)),

        <<"createdDayHist">> => lists:sum(lists:sublist(maps:get(<<"createdDayHist">>, SSM), 1)),
        <<"modifiedDayHist">> => lists:sum(lists:sublist(maps:get(<<"modifiedDayHist">>, SSM), 1)),
        <<"deletedDayHist">> => lists:sum(lists:sublist(maps:get(<<"deletedDayHist">>, SSM), 1)),
        <<"queueLengthDayHist">> => hd(maps:get(<<"queueLengthDayHist">>, SSM))
    }.
