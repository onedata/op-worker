%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of trash.
%%% @end
%%%-------------------------------------------------------------------
-module(trash_test_SUITE).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("distribution_assert.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").



%% exported for CT
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
    trash_dir_should_exist/1,
    create_dir_with_trash_dir_name_is_forbidden/1,
    create_file_with_trash_dir_name_is_forbidden/1,
    remove_trash_dir_is_forbidden/1,
    rename_trash_dir_is_forbidden/1,
    rename_other_dir_to_trash_dir_is_forbidden/1,
    chmod_on_trash_dir_is_forbidden/1,
    set_xattr_on_trash_dir_is_forbidden/1,
    remove_xattr_on_trash_dir_is_forbidden/1,
    set_acl_on_trash_dir_is_forbidden/1,
    remove_acl_on_trash_dir_is_forbidden/1,
    set_metadata_on_trash_dir_is_forbidden/1,
    set_cdmi_metadata_on_trash_dir_is_forbidden/1,
    create_share_from_trash_dir_is_forbidden/1,
    add_qos_entry_for_trash_dir_is_forbidden/1,
    remove_metadata_on_trash_dir_is_forbidden/1,
    schedule_replication_transfer_on_trash_dir_is_forbidden/1,
    schedule_eviction_transfer_on_trash_dir_is_allowed/1,
    schedule_migration_transfer_on_trash_dir_is_forbidden/1,
    schedule_replication_transfer_on_space_does_not_replicate_trash/1,
    schedule_eviction_transfer_on_space_evicts_trash/1,
    schedule_migration_transfer_on_space_does_not_replicate_trash/1,
    move_to_trash_test/1,
    move_to_trash_and_delete_test/1,
    files_from_trash_are_not_reimported/1,
    qos_set_on_file_does_not_affect_file_in_trash/1,
    qos_set_on_parent_directory_does_not_affect_files_in_trash/1,
    qos_set_on_space_directory_does_not_affect_files_in_trash/1
]).


all() -> ?ALL([
    trash_dir_should_exist,
    create_dir_with_trash_dir_name_is_forbidden,
    create_file_with_trash_dir_name_is_forbidden,
    remove_trash_dir_is_forbidden,
    rename_trash_dir_is_forbidden,
    rename_other_dir_to_trash_dir_is_forbidden,
    chmod_on_trash_dir_is_forbidden,
    set_xattr_on_trash_dir_is_forbidden,
    remove_xattr_on_trash_dir_is_forbidden,
    set_acl_on_trash_dir_is_forbidden,
    remove_acl_on_trash_dir_is_forbidden,
    set_metadata_on_trash_dir_is_forbidden,
    set_cdmi_metadata_on_trash_dir_is_forbidden,
    create_share_from_trash_dir_is_forbidden,
    add_qos_entry_for_trash_dir_is_forbidden,
    remove_metadata_on_trash_dir_is_forbidden,
    schedule_replication_transfer_on_trash_dir_is_forbidden,
    schedule_eviction_transfer_on_trash_dir_is_allowed,
    schedule_migration_transfer_on_trash_dir_is_forbidden,
    schedule_replication_transfer_on_space_does_not_replicate_trash,
    schedule_eviction_transfer_on_space_evicts_trash,
    schedule_migration_transfer_on_space_does_not_replicate_trash,
    move_to_trash_test,
    move_to_trash_and_delete_test,
    files_from_trash_are_not_reimported,
    qos_set_on_file_does_not_affect_file_in_trash,
    qos_set_on_parent_directory_does_not_affect_files_in_trash,
    qos_set_on_space_directory_does_not_affect_files_in_trash
]).

-define(SPACE1_PLACEHOLDER, space1).
-define(SPACE_ID, oct_background:get_space_id(?SPACE1_PLACEHOLDER)).
-define(SPACE_NAME, oct_background:get_space_name(?SPACE1_PLACEHOLDER)).
-define(SPACE2_PLACEHOLDER, space2).
-define(SPACE_ID2, oct_background:get_space_id(?SPACE2_PLACEHOLDER)).
-define(SPACE_NAME2, oct_background:get_space_name(?SPACE2_PLACEHOLDER)).

-define(SPACE_UUID, ?SPACE_UUID(?SPACE_ID)).
-define(SPACE_UUID(SpaceId), fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)).
-define(SPACE_GUID, ?SPACE_GUID(?SPACE_ID)).
-define(SPACE_GUID(SpaceId), fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)).
-define(TRASH_DIR_GUID(SpaceId), fslogic_uuid:spaceid_to_trash_dir_guid(SpaceId)).

-define(ATTEMPTS, 30).
-define(RAND_NAME(Prefix), <<Prefix/binary, (integer_to_binary(rand:uniform(1000)))/binary>>).
-define(RAND_DIR_NAME, ?RAND_NAME(<<"dir_">>)).
-define(RAND_FILE_NAME, ?RAND_NAME(<<"file_">>)).

%%%===================================================================
%%% Test functions
%%%===================================================================

trash_dir_should_exist(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user1, paris),
    % TODO VFS-7064 uncomment after introducing links to trash directory
%%    % trash dir should be visible in the space on both providers
%%    ?assertMatch({ok, [{_, ?TRASH_DIR_NAME}]},
%%        lfm_proxy:get_children(P1Node, UserSessIdP1, {guid, ?SPACE_GUID}, 0, 10)),
%%    ?assertMatch({ok, [{_, ?TRASH_DIR_NAME}]},
%%        lfm_proxy:get_children(P2Node, UserSessIdP2, {guid, ?SPACE_GUID}, 0, 10)),

    % trash dir should be empty
    ?assertMatch({ok, #file_attr{name = ?TRASH_DIR_NAME}},
        lfm_proxy:stat(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)})),
    ?assertMatch({ok, #file_attr{name = ?TRASH_DIR_NAME}},
        lfm_proxy:stat(P2Node, UserSessIdP2, {guid, ?TRASH_DIR_GUID(?SPACE_ID)})),
    ?assertMatch({ok, []}, lfm_proxy:get_children(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10)),
    ?assertMatch({ok, []}, lfm_proxy:get_children(P2Node, UserSessIdP2, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10)).

create_dir_with_trash_dir_name_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    % TODO VFS-7064 change this error to EEXIST after adding link from space to trash directory
    ?assertMatch({error, ?EPERM},
        lfm_proxy:mkdir(P1Node, UserSessIdP1, ?SPACE_GUID, ?TRASH_DIR_NAME, ?DEFAULT_DIR_PERMS)).

create_file_with_trash_dir_name_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    % TODO VFS-7064 change this error to EEXIST after adding link from space to trash directory
    ?assertMatch({error, ?EPERM},
        lfm_proxy:create(P1Node, UserSessIdP1, ?SPACE_GUID, ?TRASH_DIR_NAME, ?DEFAULT_FILE_PERMS)).


remove_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:rm_recursive(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)})),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:unlink(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)})),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:unlink(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)})).

rename_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    TargetPath = filename:join([?DIRECTORY_SEPARATOR, ?SPACE_NAME, <<"other_trash_name">>]),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:mv(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, TargetPath)).

rename_other_dir_to_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    DirName = ?RAND_DIR_NAME,
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ?SPACE_GUID, DirName, ?DEFAULT_DIR_PERMS),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:mv(P1Node, UserSessIdP1, {guid, DirGuid}, filename:join([?SPACE_NAME, ?TRASH_DIR_NAME]))).

chmod_on_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:set_perms(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 8#777)).

set_xattr_on_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:set_xattr(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, #{<<"key">> => <<"value">>})).

remove_xattr_on_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:remove_xattr(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, <<"key">>)).

set_acl_on_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:set_acl(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, [])).

remove_acl_on_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:remove_acl(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)})).

set_metadata_on_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    JSON = #{<<"key">> => <<"value">>},
    ?assertMatch({error, ?EPERM},
        lfm_proxy:set_metadata(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, json, JSON, [])).

set_cdmi_metadata_on_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:set_mimetype(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, <<"mimetype">>)),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:set_cdmi_completion_status(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, <<"COMPLETED">>)),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:set_transfer_encoding(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, <<"base64">>)).

create_share_from_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:create_share(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, <<"MY SHARE">>)).

add_qos_entry_for_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:add_qos_entry(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, <<"key=value">>, 1)).

remove_metadata_on_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:remove_metadata(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, json)).

schedule_replication_transfer_on_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    P2Id = oct_background:get_provider_id(paris),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:schedule_file_replication(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, P2Id)).

schedule_eviction_transfer_on_trash_dir_is_allowed(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    P1Id = oct_background:get_provider_id(krakow),
    {ok, TransferId} = ?assertMatch({ok, _},
        lfm_proxy:schedule_file_replica_eviction(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, P1Id, undefined)),
    ?assertMatch({ok, #document{value = #transfer{eviction_status = completed}}},
        rpc:call(P1Node, transfer, get, [TransferId]), ?ATTEMPTS).

schedule_migration_transfer_on_trash_dir_is_forbidden(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    P1Id = oct_background:get_provider_id(krakow),
    P2Id = oct_background:get_provider_id(paris),
    ?assertMatch({error, ?EPERM},
        lfm_proxy:schedule_file_replica_eviction(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, P1Id, P2Id)).

schedule_replication_transfer_on_space_does_not_replicate_trash(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    DirName = ?RAND_DIR_NAME,
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),

    % create file and directory
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ?SPACE_GUID, DirName, ?DEFAULT_DIR_PERMS),
    lfm_test_utils:create_files_tree(P1Node, UserSessIdP1, [{10, 10}], DirGuid),

    % move subtree to trash
    ok = lfm_proxy:rm_recursive(P1Node, UserSessIdP1, {guid, DirGuid}),

    % wait till moving directory to trash is synchronized
    ?assertMatch({ok, [{DirGuid, _}]},
        lfm_proxy:get_children(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10), ?ATTEMPTS),

    P2Id = oct_background:get_provider_id(paris),
    {ok, TransferId} = ?assertMatch({ok, _},
        lfm_proxy:schedule_file_replication(P1Node, UserSessIdP1, {guid, ?SPACE_GUID}, P2Id)),

    ?assertMatch({ok, #document{value = #transfer{
        replication_status = completed,
        files_replicated = 0
    }}}, rpc:call(P1Node, transfer, get, [TransferId]), ?ATTEMPTS).


schedule_eviction_transfer_on_space_evicts_trash(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    DirName = ?RAND_DIR_NAME,
    FileName = ?RAND_FILE_NAME,
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user1, paris),

    % create file and directory
    TestData = <<"test data">>,
    Size = byte_size(TestData),
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ?SPACE_GUID, DirName, ?DEFAULT_DIR_PERMS),
    {ok, {FileGuid, H}} =
        ?assertMatch({ok, _}, lfm_proxy:create_and_open(P1Node, UserSessIdP1, DirGuid, FileName, ?DEFAULT_FILE_PERMS), ?ATTEMPTS),
    ?assertMatch({ok, _}, lfm_proxy:write(P1Node, H,  0, TestData), ?ATTEMPTS),
    lfm_proxy:close(P1Node, H),

    % read file on P2 to replicate it
    {ok, H2} =
        ?assertMatch({ok, _}, lfm_proxy:open(P2Node, UserSessIdP2, {guid, FileGuid}, read), ?ATTEMPTS),
    ?assertEqual(Size, try
        {ok, Bytes} = lfm_proxy:read(P2Node, H2,  0, Size),
        byte_size(Bytes)
    catch
        _:_ ->
            error
    end, ?ATTEMPTS),
    lfm_proxy:close(P2Node, H2),

    P1Id = oct_background:get_provider_id(krakow),
    P2Id = oct_background:get_provider_id(paris),

    ?assertDistribution(P1Node, UserSessIdP1, ?DISTS([P1Id, P2Id], [Size, Size]), FileGuid, ?ATTEMPTS),

    % evict whole space
    {ok, TransferId} = ?assertMatch({ok, _},
        lfm_proxy:schedule_file_replica_eviction(P1Node, UserSessIdP1, {guid, ?SPACE_GUID}, P1Id, undefined)),

    ?assertMatch({ok, #document{value = #transfer{
        eviction_status = completed,
        files_evicted = 1
    }}}, rpc:call(P1Node, transfer, get, [TransferId]), ?ATTEMPTS),

    ?assertDistribution(P1Node, UserSessIdP1, ?DISTS([P1Id, P2Id], [0, Size]), FileGuid, ?ATTEMPTS).


schedule_migration_transfer_on_space_does_not_replicate_trash(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    DirName = ?RAND_DIR_NAME,
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user1, paris),

    % create file and directory
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ?SPACE_GUID, DirName, ?DEFAULT_DIR_PERMS),
    lfm_test_utils:create_files_tree(P1Node, UserSessIdP1, [{0, 10}], DirGuid),

    % move subtree to trash
    DirCtx = file_ctx:new_by_guid(DirGuid),
    move_to_trash(P1Node, DirCtx, UserSessIdP1),

    % wait till moving directory to trash is synchronized
    ?assertMatch({ok, [{DirGuid, _}]},
        lfm_proxy:get_children(P2Node, UserSessIdP2, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10), ?ATTEMPTS),

    P1Id = oct_background:get_provider_id(krakow),
    P2Id = oct_background:get_provider_id(paris),
    {ok, TransferId} = ?assertMatch({ok, _},
        lfm_proxy:schedule_file_replica_eviction(P1Node, UserSessIdP1, {guid, ?SPACE_GUID}, P1Id, P2Id)),

    ?assertMatch({ok, #document{value = #transfer{
        replication_status = completed,
        eviction_status = completed,
        files_replicated = 0,
        files_evicted = 0
    }}}, rpc:call(P1Node, transfer, get, [TransferId]), ?ATTEMPTS).

move_to_trash_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user1, paris),
    DirName = ?RAND_DIR_NAME,
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ?SPACE_GUID, DirName, ?DEFAULT_DIR_PERMS),
    DirCtx = file_ctx:new_by_guid(DirGuid),
    lfm_test_utils:create_files_tree(P1Node, UserSessIdP1, [{10, 10}, {10, 10}, {10, 10}], DirGuid),

    move_to_trash(P1Node, DirCtx, UserSessIdP1),

    lfm_test_utils:assert_space_dir_empty(P1Node, ?SPACE_ID, ?ATTEMPTS),
    lfm_test_utils:assert_space_dir_empty(P2Node, ?SPACE_ID, ?ATTEMPTS),
    ?assertMatch({ok, [{DirGuid, _}]}, lfm_proxy:get_children(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10)),
    ?assertMatch({ok, [{DirGuid, _}]}, lfm_proxy:get_children(P2Node, UserSessIdP2, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10), ?ATTEMPTS),


    StorageFileId = filename:join(["/", DirName]),
    StorageId = op_test_rpc:get_supporting_storage_id(P1Node, ?SPACE_ID),

    % file registration should fail because there is a deletion marker added for the file
    % which prevents file to be imported
    ?assertMatch({ok, ?HTTP_400_BAD_REQUEST, _, _}, register_file(P1Node, user1, #{
        <<"spaceId">> => ?SPACE_ID,
        <<"destinationPath">> => DirName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => global_clock:timestamp_seconds(),
        <<"size">> => 10,
        <<"mode">> => <<"664">>,
        <<"autoDetectAttributes">> => false
    })).

move_to_trash_and_delete_test(_Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    DirName = ?RAND_DIR_NAME,
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user1, paris),
    {ok, DirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, ?SPACE_GUID, DirName, ?DEFAULT_DIR_PERMS),
    {DirGuids, FileGuids} = lfm_test_utils:create_files_tree(P1Node, UserSessIdP1, [{10, 10}, {10, 10}, {10, 10}], DirGuid),
    DirCtx = file_ctx:new_by_guid(DirGuid),

    move_to_trash(P1Node, DirCtx, UserSessIdP1),
    delete_from_trash(P1Node, DirCtx, UserSessIdP1, ?SPACE_UUID),

    lfm_test_utils:assert_space_and_trash_are_empty(P1Node, ?SPACE_ID, ?ATTEMPTS),
    lfm_test_utils:assert_space_and_trash_are_empty(P2Node, ?SPACE_ID, ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(P1Node, UserSessIdP1, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10), ?ATTEMPTS),
    ?assertMatch({ok, []}, lfm_proxy:get_children(P2Node, UserSessIdP2, {guid, ?TRASH_DIR_GUID(?SPACE_ID)}, 0, 10), ?ATTEMPTS),

    lists:foreach(fun(G) ->
        ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(P1Node, UserSessIdP1, {guid, G}), ?ATTEMPTS),
        ?assertMatch({error, ?ENOENT}, lfm_proxy:stat(P2Node, UserSessIdP2, {guid, G}), ?ATTEMPTS)
    end, DirGuids ++ FileGuids ++ [DirGuid]),

    StorageFileId = filename:join([?DIRECTORY_SEPARATOR, DirName]),
    StorageId = op_test_rpc:get_supporting_storage_id(P1Node, ?SPACE_ID),

    Size = 10,
    % file registration should succeed because the file has already been deleted

    ?assertMatch({ok, ?HTTP_201_CREATED, _, _}, register_file(P1Node, user1, #{
        <<"spaceId">> => ?SPACE_ID,
        <<"destinationPath">> => DirName,
        <<"storageFileId">> => StorageFileId,
        <<"storageId">> => StorageId,
        <<"mtime">> => global_clock:timestamp_seconds(),
        <<"size">> => Size,
        <<"mode">> => <<"664">>,
        <<"autoDetectAttributes">> => false
    })).


files_from_trash_are_not_reimported(_Config) ->
    % this test is performed in ?SPACE2 which is supported by ImportedNullStorage2
    % on which legacy dataset is simulated with
    % structure 1-0:10-10 (1 root directory with 10 subdirectories and 10 files)
    [P1Node] = oct_background:get_provider_nodes(krakow),
    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),

    % ensure that 1st scan has been finished
    ?assertEqual(true, rpc:call(P1Node, storage_import_monitoring, is_initial_scan_finished, [?SPACE_ID2]), ?ATTEMPTS),

    {ok, [{DirGuid, _}]} = lfm_proxy:get_children(P1Node, UserSessIdP1, {guid, ?SPACE_GUID(?SPACE_ID2)}, 0, 1000),
    DirCtx = file_ctx:new_by_guid(DirGuid),

    % move imported directory to trash
    move_to_trash(P1Node, DirCtx, UserSessIdP1),

    % start scan and wait till it's finished
    ok = rpc:call(P1Node, storage_import, start_auto_scan, [?SPACE_ID2]),
    ?assertEqual(true, rpc:call(P1Node, storage_import_monitoring, is_scan_finished, [?SPACE_ID2, 2]), ?ATTEMPTS),

    % files which are currently in trash shouldn't have been reimported
    ?assertMatch({ok, []}, lfm_proxy:get_children(P1Node, UserSessIdP1, {guid, ?SPACE_GUID(?SPACE_ID2)}, 0, 1000)).

qos_set_on_file_does_not_affect_file_in_trash(Config) ->
    qos_does_not_affect_files_in_trash_test_base(Config, file).

qos_set_on_parent_directory_does_not_affect_files_in_trash(Config) ->
    qos_does_not_affect_files_in_trash_test_base(Config, parent_dir).

qos_set_on_space_directory_does_not_affect_files_in_trash(Config) ->
    qos_does_not_affect_files_in_trash_test_base(Config, space_dir).

%===================================================================
% Test base functions
%===================================================================

qos_does_not_affect_files_in_trash_test_base(_Config, SetQosOn) ->
    % this test creates the following structure in the space directory:
    % /space_dir/parent_dir/file
    % It adds QoS entry for file determined by SetQosOn parameter
    % and checks whether file which is in trash is not replicated by QoS.
    % Parameter SetQosOn can have the following values:
    %  - space_dir
    %  - parent_dir
    %  - file
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    StorageId = op_test_rpc:get_supporting_storage_id(P1Node, ?SPACE_ID),
    ok = rpc:call(P1Node, storage_logic, set_qos_parameters, [StorageId, #{<<"key">> => <<"value">>}]),

    UserSessIdP1 = oct_background:get_user_session_id(user1, krakow),
    UserSessIdP2 = oct_background:get_user_session_id(user1, paris),

    P1Id = oct_background:get_provider_id(krakow),
    P2Id = oct_background:get_provider_id(paris),

    DirName = ?RAND_DIR_NAME,
    FileName = ?RAND_FILE_NAME,
    {ok, DirGuid} = lfm_proxy:mkdir(P2Node, UserSessIdP2, ?SPACE_GUID, DirName, ?DEFAULT_DIR_PERMS),
    DirCtx = file_ctx:new_by_guid(DirGuid),
    {ok, {FileGuid, H1}} = lfm_proxy:create_and_open(P2Node, UserSessIdP2, DirGuid, FileName, ?DEFAULT_FILE_PERMS),
    TestData1 = <<"first part ">>,
    TestData2 = <<"seconds part">>,
    Size1 = byte_size(TestData1),
    Size2 = Size1 + byte_size(TestData2),
    {ok, _} = lfm_proxy:write(P2Node, H1, 0, TestData1),
    lfm_proxy:fsync(P2Node, H1),

    GuidWithQos = case SetQosOn of
        space_dir -> ?SPACE_GUID;
        parent_dir -> DirGuid;
        file -> FileGuid
    end,

    {ok, QosEntryId} = lfm_proxy:add_qos_entry(P1Node, UserSessIdP1, {guid, GuidWithQos}, <<"key=value">>, 1),

    % check whether QoS synchronized the file
    ?assertMatch({ok, {#{QosEntryId := fulfilled}, _}},
        lfm_proxy:get_effective_file_qos(P1Node, UserSessIdP1, {guid, GuidWithQos}), ?ATTEMPTS),

    ?assertDistribution(P1Node, UserSessIdP1, ?DISTS([P1Id, P2Id], [Size1, Size1]), FileGuid, ?ATTEMPTS),

    % move the file to trash
    move_to_trash(P1Node, DirCtx, UserSessIdP1),

    % write new blocks to file which is in trash
    {ok, _} = lfm_proxy:write(P2Node, H1, Size1, TestData2),

    % file shouldn't have been synchronized because it's in trash
    ?assertDistribution(P1Node, UserSessIdP1, ?DISTS([P1Id, P2Id], [Size1, Size2]), FileGuid, ?ATTEMPTS).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    ssl:start(),
    hackney:start(),
    oct_background:init_per_suite(Config, #onenv_test_config{onenv_scenario = "trash_tests"}).

end_per_suite(_Config) ->
    hackney:stop(),
    ssl:stop().

init_per_testcase(_Case, Config) ->
    lfm_proxy:init(Config).

end_per_testcase(_Case, Config) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    AllNodes = [P1Node, P2Node],
    lfm_test_utils:clean_space(P1Node, AllNodes, ?SPACE_ID, ?ATTEMPTS),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

move_to_trash(Worker, FileCtx, SessId) ->
    UserCtx = rpc:call(Worker, user_ctx, new, [SessId]),
    rpc:call(Worker, trash, move_to_trash, [FileCtx, UserCtx]).

delete_from_trash(Worker, FileCtx, SessId, RootOriginalParentUuid) ->
    UserCtx = rpc:call(Worker, user_ctx, new, [SessId]),
    rpc:call(Worker, trash, delete_from_trash, [FileCtx, UserCtx, false, RootOriginalParentUuid]).

register_file(Worker, User, Body) ->
    Headers = #{
        ?HDR_X_AUTH_TOKEN => oct_background:get_user_access_token(User),
        ?HDR_CONTENT_TYPE => <<"application/json">>
    },
    rest_test_utils:request(Worker, <<"data/register">>, post, Headers, json_utils:encode(Body)).