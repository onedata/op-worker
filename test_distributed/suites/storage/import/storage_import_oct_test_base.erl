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
-include("modules/fslogic/file_attr.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("onenv_test_utils.hrl").

-type import_config() :: #import_config{}.

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
    Krakow =  oct_background:get_random_provider_node(krakow),
    Paris =  oct_background:get_random_provider_node(paris),
    SessionIdKrakow = oct_background:get_user_session_id(user1, krakow),
    SessionIdParis = oct_background:get_user_session_id(user1, paris),

    #storage_import_test_config{space_id = SpaceId} = Config,

    enable_initial_scan(Config, Krakow, SpaceId),
    assert_initial_scan_finished(Krakow, SpaceId),

    ?assertMatch({ok, []},
        lfm_proxy:get_children(Krakow, SessionIdKrakow, {path, ?SPACE_PATH(?FUNCTION_NAME)}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(Krakow, SessionIdKrakow, {path, ?SPACE_PATH(?FUNCTION_NAME)}), ?ATTEMPTS),
    ?assertMatch({ok, #file_attr{}},
        lfm_proxy:stat(Paris, SessionIdParis, {path, ?SPACE_PATH(?FUNCTION_NAME)}), ?ATTEMPTS),

    ?assertMonitoring(Krakow, #{
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
    Krakow =  oct_background:get_random_provider_node(krakow),
    Paris =  oct_background:get_random_provider_node(paris),
    SessionIdKrakow = oct_background:get_user_session_id(user1, krakow),
    SessionIdParis = oct_background:get_user_session_id(user1, paris),

    #storage_import_test_config{
        space_id = SpaceId,
        imported_storage_id = ImportedStorageId,
        not_imported_storage_id = NotImportedStorageId
    } = Config,

    DirName = generator:gen_name(),
    StorageTestDirPath = filename:join([<<"/">>, DirName]),

    SDHandle = sd_test_utils:new_handle(Krakow, SpaceId, StorageTestDirPath, ImportedStorageId),
    ok = sd_test_utils:mkdir(Krakow, SDHandle, ?DEFAULT_DIR_PERMS),
    enable_initial_scan(Config, Krakow, SpaceId),
    assert_initial_scan_finished(Krakow, SpaceId),

    %% Check if dir was imported
    ?assertMatch({ok, [{_, DirName}]},
        lfm_proxy:get_children(Krakow, SessionIdKrakow, {path, ?SPACE_PATH(?FUNCTION_NAME)}, 0, 10), ?ATTEMPTS),

    SpaceTestDirPath = filename:join([<<"/">>, ?FUNCTION_NAME, DirName]),
    StorageSDHandleKrakow = sd_test_utils:get_storage_mountpoint_handle(Krakow, SpaceId, ImportedStorageId),
    StorageSDHandleParis = sd_test_utils:get_storage_mountpoint_handle(Krakow, SpaceId, NotImportedStorageId),
    {ok, #statbuf{st_uid = MountUid1}} = sd_test_utils:stat(Krakow, StorageSDHandleKrakow),
    {ok, #statbuf{st_uid = MountUid2, st_gid = MountGid2}} = sd_test_utils:stat(Paris, StorageSDHandleParis),

    SpaceOwner = ?SPACE_OWNER_ID(SpaceId),

    ?assertMatch({ok, #file_attr{
        owner_id = SpaceOwner,
        uid = MountUid1,
        gid = 0
    }}, lfm_proxy:stat(Krakow, SessionIdKrakow, {path, SpaceTestDirPath}), ?ATTEMPTS),

    ?assertMatch({ok, #file_attr{
        owner_id = SpaceOwner,
        uid = MountUid2,
        gid = MountGid2
    }}, lfm_proxy:stat(Paris, SessionIdParis, {path, SpaceTestDirPath}), ?ATTEMPTS),

%%    TODO VFS-11780 adjust when it will be researched how statistics work
%%     this is original, not working
%%    ?assertMonitoring(Krakow, #{
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

    ?assertMonitoring(Krakow, #{
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

%% @private
enable_initial_scan(Config, Worker, SpaceId) ->
    ImportConfig = Config#storage_import_test_config.import_config,
    MaxDepth = maps:get(max_depth, ImportConfig, ?MAX_DEPTH),
    SyncAcl = maps:get(sync_acl, ImportConfig, ?SYNC_ACL),
    ?assertMatch(ok, ?rpc(Worker, storage_import:set_or_configure_auto_mode(
        SpaceId, #{max_depth => MaxDepth, sync_acl => SyncAcl}))).


%% @private
assert_initial_scan_finished(Worker, SpaceId) ->
    ?assertEqual(true, catch(?rpc(Worker, storage_import_monitoring:is_initial_scan_finished(SpaceId))), ?ATTEMPTS).


%% @private
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


%% @private
assert(ExpectedSSM, SSM) ->
    maps:fold(fun(Key, Value, _AccIn) ->
        assert_for_key(Key, Value, SSM)
    end, ok, ExpectedSSM).


%% @private
assert_for_key(Key, ExpectedValue, SSM) ->
    case maps:get(Key, SSM) of
        ExpectedValue -> ok;
        Value -> throw({assertion_error, Key, ExpectedValue, Value})
    end.


%% @private
monitoring_describe(Worker, SpaceId) ->
    ?rpc(Worker, storage_import_monitoring:describe(SpaceId)).


%% @private
storage_import_monitoring_description(SSM) ->
    maps:fold(fun(Key, Value, {AccFormat, AccArgs}) ->
        {AccFormat ++ "    ~p = ~p~n", AccArgs ++ [Key, Value]}
    end, {"~n#storage_import_monitoring fields values:~n", []}, SSM).


%% @private
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
