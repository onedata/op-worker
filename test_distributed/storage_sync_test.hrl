%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Macros used in tests of storage_sync.
%%% @end
%%%-------------------------------------------------------------------
-author("Jakub Kudzia").

-include_lib("ctool/include/test/test_utils.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").

-define(ATTEMPTS, 30).

-define(SYNCED_STORAGE, todo).
-define(RDWR_STORAGE, todo).

%% defaults
-define(SCAN_INTERVAL, 10).
-define(WRITE_ONCE, false).
-define(DELETE_ENABLE, false).
-define(SYNC_ACL, true).
-define(MAX_DEPTH, 9999999999999999999999).

%% test data
-define(USER, <<"user1">>).
-define(USER2, <<"user2">>).
-define(GROUP, <<"group1">>).
-define(GROUP2, <<"group2">>).
-define(SPACE_ID, <<"space1">>).
-define(SPACE_NAME, <<"space_name1">>).
-define(SPACE_PATH, <<"/", (?SPACE_NAME)/binary>>).
-define(SPACE_CANONICAL_PATH, <<"/", (?SPACE_ID)/binary>>).
-define(TEST_DIR, <<"test_dir">>).
-define(TEST_DIR2, <<"test_dir2">>).
-define(TEST_FILE1, <<"test_file">>).
-define(TEST_FILE2, <<"test_file2">>).
-define(TEST_FILE3, <<"test_file3">>).
-define(TEST_FILE4, <<"test_file4">>).
-define(INIT_FILE, <<".__onedata__init_file">>).
-define(SPACE_TEST_DIR_PATH(DirName), filename:join(["/", ?SPACE_NAME, DirName])).
-define(SPACE_TEST_FILE_IN_DIR_PATH(DirName, FileName), filename:join(["/", ?SPACE_NAME, DirName, FileName])).
-define(SPACE_TEST_DIR_PATH, filename:join(["/", ?SPACE_NAME, ?TEST_DIR])).
-define(SPACE_TEST_DIR_PATH2, filename:join(["/", ?SPACE_NAME, ?TEST_DIR2])).
-define(SPACE_TEST_FILE_PATH, filename:join(["/", ?SPACE_NAME, ?TEST_FILE1])).
-define(SPACE_TEST_FILE_PATH2, filename:join(["/", ?SPACE_NAME, ?TEST_FILE2])).
-define(SPACE_TEST_FILE_PATH3, filename:join(["/", ?SPACE_NAME, ?TEST_FILE3])).
-define(SPACE_TEST_FILE_PATH4, filename:join(["/", ?SPACE_NAME, ?TEST_FILE4])).
-define(SPACE_TEST_FILE_IN_DIR_PATH, filename:join([?SPACE_TEST_DIR_PATH, ?TEST_FILE1])).
-define(SPACE_TEST_FILE_IN_DIR_PATH2, filename:join([?SPACE_TEST_DIR_PATH, ?TEST_FILE2])).
-define(SPACE_TEST_FILE_IN_DIR_PATH3, filename:join([?SPACE_TEST_DIR_PATH, ?TEST_FILE3])).
-define(SPACE_INIT_FILE_PATH, filename:join(["/", ?SPACE_NAME, ?INIT_FILE])).
-define(TEST_DATA, <<"test_data">>).
-define(TEST_DATA_SIZE, byte_size(?TEST_DATA)).
-define(CHANGED_BYTE, <<"-">>).
-define(CHANGED_BYTE_OFFSET, 4).
-define(TEST_DATA_ONE_BYTE_CHANGED, <<"test-data">>).
-define(TEST_DATA_CHANGED, <<"test_modified_file">>).
-define(TEST_DATA2, <<"test_data2">>).
-define(TEST_DATA_SIZE2, byte_size(?TEST_DATA2)).

-define(TEST_UID, 1000).
-define(TEST_GID, 1000).

-define(TEST_URL, <<"http://127.0.0.1:5000">>).

-define(VERIFY_POOL, verify_pool).

-define(DEFAULT_TIMEOUT, timer:minutes(5)).

-define(LUMA_CONFIG, ?LUMA_CONFIG(?DEFAULT_TIMEOUT)).
-define(LUMA_CONFIG(CacheTimeout), #luma_config{
    url = ?TEST_URL,
    api_key = <<"test_api_key">>
}).

-define(assertMonitoring(Worker, ExpectedSSM, SpaceId, Attempts),
    storage_sync_test_base:assert_monitoring_state(Worker, ExpectedSSM, SpaceId, Attempts)).

-define(assertMonitoring(Worker, ExpectedSSM, SpaceId),
    ?assertMonitoring(Worker, ExpectedSSM, SpaceId, 1)).

-define(assertHashChangedFun(StorageFileId, SpaceId, ExpectedResult0),
    fun
        ({_, {storage_sync_hash, children_attrs_hash_has_changed, Args}, ExpectedResult0}) ->
            Id = storage_sync_info:id(StorageFileId, SpaceId),
            case hd(Args) of
                #document{key = Id} -> 1;
                _ -> 0
            end;
        (_) ->
            0
    end
).

-define(assertMtimeChangedFun(StorageFileId, SpaceId, ExpectedResult0),
    fun
        ({_, {storage_sync_traverse, mtime_has_changed, Args}, ExpectedResult0}) ->
            Id = storage_sync_info:id(StorageFileId, SpaceId),
            case hd(Args) of
                #document{key = Id} -> 1;
                _ -> 0
            end;
        (_) ->
            0
    end
).

-define(ACL, [
    #access_control_entity{
        acetype = ?allow_mask,
        aceflags = ?no_flags_mask,
        identifier = <<"OWNER@">>,
        acemask = ?read_acl_mask
    },
    #access_control_entity{
        acetype = ?deny_mask,
        aceflags = ?no_flags_mask,
        identifier = <<"GROUP@">>,
        acemask = ?write_acl_mask
    },
    #access_control_entity{
        acetype = ?allow_mask,
        aceflags = ?no_flags_mask,
        identifier = <<"EVERYONE@">>,
        acemask = ?read_acl_mask
    },
    #access_control_entity{
        acetype = ?deny_mask,
        aceflags = ?no_flags_mask,
        identifier = <<"ala@nfsdomain.org">>,
        acemask = ?write_attributes_mask
    }
]).

-define(BITMASK_TO_BINARY(Mask), <<"0x", (integer_to_binary(Mask, 16))/binary>>).

-define(ACL_JSON, [
    #{
        <<"acetype">> => ?BITMASK_TO_BINARY(?allow_mask),
        <<"aceflags">> => ?BITMASK_TO_BINARY(?no_flags_mask),
        <<"identifier">> => <<"OWNER@">>,
        <<"acemask">> => ?BITMASK_TO_BINARY(?read_acl_mask)
    },
    #{
        <<"acetype">> => ?BITMASK_TO_BINARY(?deny_mask),
        <<"aceflags">> => ?BITMASK_TO_BINARY(?no_flags_mask),
        <<"identifier">> => <<"GROUP@">>,
        <<"acemask">> => ?BITMASK_TO_BINARY(?write_acl_mask)
    },
    #{
        <<"acetype">> => ?BITMASK_TO_BINARY(?allow_mask),
        <<"aceflags">> => ?BITMASK_TO_BINARY(?no_flags_mask),
        <<"identifier">> => <<"EVERYONE@">>,
        <<"acemask">> => ?BITMASK_TO_BINARY(?read_acl_mask)
    },
    #{
        <<"acetype">> => ?BITMASK_TO_BINARY(?deny_mask),
        <<"aceflags">> => ?BITMASK_TO_BINARY(?no_flags_mask),
        <<"identifier">> => <<"name_user1#user1">>,
        <<"acemask">> => ?BITMASK_TO_BINARY(?write_attributes_mask)
    }
]).


-define(ACL2, [
    #access_control_entity{
        acetype = ?allow_mask,
        aceflags = ?identifier_group_mask,
        identifier = <<"group2@nfsdomain.org">>,
        acemask = ?read_acl_mask
    },
    #access_control_entity{
        acetype = ?deny_mask,
        aceflags = ?no_flags_mask,
        identifier = <<"EVERYONE@">>,
        acemask = ?read_acl_mask
    }
]).

-define(ACL2_JSON, [
    #{
        <<"acetype">> => ?BITMASK_TO_BINARY(?allow_mask),
        <<"aceflags">> => ?BITMASK_TO_BINARY(?identifier_group_mask),
        <<"identifier">> => <<"group2#group2">>,
        <<"acemask">> => ?BITMASK_TO_BINARY(?read_acl_mask)
    },
    #{
        <<"acetype">> => ?BITMASK_TO_BINARY(?deny_mask),
        <<"aceflags">> => ?BITMASK_TO_BINARY(?no_flags_mask),
        <<"identifier">> => <<"EVERYONE@">>,
        <<"acemask">> => ?BITMASK_TO_BINARY(?read_acl_mask)
    }
]).