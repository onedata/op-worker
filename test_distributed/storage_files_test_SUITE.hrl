%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros used in space_storage_test_SUITE
%%% @end
%%%-------------------------------------------------------------------

-ifndef(STORAGE_FILES_TEST_SUITE_HRL).
-define(STORAGE_FILES_TEST_SUITE_HRL, 1).

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("kernel/include/file.hrl").

% Utility macros
-define(SPACE_ID1, <<"space1">>).
-define(SPACE_ID2, <<"space2">>).
-define(SPACE_ID3, <<"space3">>).
-define(SPACE_ID4, <<"space4">>).
-define(SPACE_ID5, <<"space5">>).
-define(SPACE_ID6, <<"space6">>).
-define(SPACE_ID7, <<"space7">>).
-define(SPACE_ID8, <<"space8">>).
-define(SPACE_ID9, <<"space9">>).
-define(SPACE_ID10, <<"space10">>).

-define(STORAGE_ID1, <<"/mnt/st1">>). % posix storage with auto feed LUMA
-define(STORAGE_ID2, <<"/mnt/st2">>). % posix storage with external feed LUMA
-define(STORAGE_ID3, <<"/mnt/st3">>). % posix storage with local feed LUMA

-define(STORAGE_ID4, <<"/mnt/st4">>). % synced (mount_in_root) posix storage with auto feed LUMA
-define(STORAGE_ID5, <<"/mnt/st5">>). % synced (mount_in_root) posix storage with external feed LUMA
-define(STORAGE_ID6, <<"/mnt/st6">>). % synced (mount_in_root) posix storage with local feed LUMA

-define(STORAGE_ID7, <<"s3_auto_luma_7">>). % s3 storage with auto feed LUMA
-define(STORAGE_ID8, <<"s3_external_feed_luma_8">>). % s3 storage with external feed LUMA
-define(STORAGE_ID9, <<"s3_local_feed_luma_9">>). % s3 storage with local feed LUMA

-define(AUTO_FEED_LUMA_STORAGES, [?STORAGE_ID1, ?STORAGE_ID4, ?STORAGE_ID7]).
-define(EXTERNAL_FEED_LUMA_STORAGES, [?STORAGE_ID2, ?STORAGE_ID5, ?STORAGE_ID8]).

-define(SPACE_NAME(SpaceId, Config), ?config(SpaceId, ?config(spaces, Config))).
-define(SPACE_GUID(SpaceId), fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)).

-define(USER1, <<"user1">>).
-define(USER2, <<"user2">>).
-define(USER3, <<"user3">>).
-define(USER4, <<"user4">>).
-define(UNKNOWN_USER, <<"UNKNOWN OWNER ID">>).

% Macros for test file names
-define(DIR_NAME(Suffix), <<"dir_", (atom_to_binary(Suffix, latin1))/binary>>).
-define(FILE_NAME(Suffix), <<"file_", (atom_to_binary(Suffix, latin1))/binary>>).

-define(SESS_ID(Worker, Config, User),
    case User =:= ?ROOT_USER_ID of
        true -> ?ROOT_SESS_ID;
        false -> ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config)
    end
).

-define(FILE_MODE(Perms), Perms bor 8#100000).
-define(DIR_MODE(Perms), Perms bor 8#40000).

% Macros used to define posix compatible ownerships (UID, GID)
-define(UID(UserId), luma_auto_feed:generate_posix_identifier(UserId, ?UID_RANGE)).
-define(GID(SpaceId), luma_auto_feed:generate_posix_identifier(SpaceId, ?GID_RANGE)).

-define(UID_RANGE, {100000, 2000000}).
-define(GID_RANGE, {100000, 2000000}).


-define(ROOT_OWNER, ?OWNER(?ROOT_UID, ?ROOT_GID)).

% Generated UID and GID, where UID=hash(UserId) and GID is GID of storage mountpoint
-define(GEN_OWNER(Worker, StorageId, UserId), ?MOUNT_DIR_OWNER(Worker, StorageId, ?UID(UserId))).
% Generated UID and GID, where UID=hash(?SPACE_OWNER_ID(SpaceId)) and GID=hash(SpaceId)
-define(GEN_SPACE_OWNER(SpaceId), ?OWNER(?UID(?SPACE_OWNER_ID(SpaceId)), ?GID(SpaceId))).
% Generated UID and GID, where UID=hash(UserId) and GID=hash(SpaceId)
-define(GEN_OWNER(UserId, SpaceId), ?OWNER(?UID(UserId), ?GID(SpaceId))).

-define(MOUNT_DIR_OWNER(Worker, StorageId), storage_files_test_SUITE:mount_dir_owner(Worker, StorageId)).
-define(MOUNT_DIR_OWNER(Worker, StorageId, Uid), storage_files_test_SUITE:mount_dir_owner(Worker, StorageId, Uid)).

-define(OWNER(Uid, Gid), #{uid => Uid, gid => Gid}).

-endif.