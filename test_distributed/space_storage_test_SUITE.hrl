%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------

-ifndef(SPACE_STORAGE_TEST_SUITE_HRL).
-define(SPACE_STORAGE_TEST_SUITE_HRL, 1).

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

% Utility macros
-define(SPACE_ID1, <<"space1">>).
-define(SPACE_ID2, <<"space2">>).
-define(SPACE_ID3, <<"space3">>).
-define(SPACE_ID4, <<"space4">>).
-define(SPACE_ID5, <<"space5">>).
-define(SPACE_ID6, <<"space6">>).
-define(SPACE_ID7, <<"space7">>).

-define(STORAGE_ID1, <<"/mnt/st1">>).
-define(STORAGE_ID2, <<"/mnt/st2">>).
-define(STORAGE_ID3, <<"/mnt/st3">>).
-define(STORAGE_ID4, <<"/mnt/st4">>).
-define(STORAGE_ID5, <<"s3_no_luma">>).
-define(STORAGE_ID6, <<"s3_luma">>).

-define(SPACE_NAME(SpaceId, Config), ?config(SpaceId, ?config(spaces, Config))).
-define(SPACE_GUID(SpaceId), fslogic_uuid:spaceid_to_space_dir_guid(SpaceId)).

-define(USER1, <<"user1">>).
-define(USER2, <<"user2">>).
-define(USER3, <<"user3">>).
-define(USER4, <<"user4">>).

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
-define(DEFAULT_FILE_MODE, ?FILE_MODE(?DEFAULT_FILE_PERMS)).

-define(DIR_MODE(Perms), Perms bor 8#40000).
-define(DEFAULT_DIR_MODE, ?DIR_MODE(?DEFAULT_DIR_PERMS)).

% Macros used to define posix compatible ownerships (UID, GID)
-define(UID(UserId), luma_utils:generate_posix_identifier(UserId, ?UID_RANGE)).
-define(GID(SpaceId), luma_utils:generate_posix_identifier(SpaceId, ?GID_RANGE)).

-define(UID_RANGE, {100000, 2000000}).
-define(GID_RANGE, {100000, 2000000}).


-define(ROOT_OWNER, ?OWNER(?ROOT_UID, ?ROOT_GID)).

% Generated UID and GID, where UID=hash(UserId) and GID is GID of storage mountpoint
-define(GEN_OWNER(UserId), ?OWNER(?UID(UserId), ?MOUNT_DIR_GID)).
% Generated UID and GID, where UID=hash(?SPACE_OWNER_ID(SpaceId)) and GID=hash(SpaceId)
-define(GEN_SPACE_OWNER(SpaceId), ?OWNER(?UID(?SPACE_OWNER_ID(SpaceId)), ?GID(SpaceId))).
% Generated UID and GID, where UID=hash(UserId) and GID=hash(SpaceId)
-define(GEN_OWNER(UserId, SpaceId), ?OWNER(?UID(UserId), ?GID(SpaceId))).


% UID and GID of storages' mountpoints in docker container
-define(MOUNT_DIR_UID, 1000).
-define(MOUNT_DIR_GID, 1000).
-define(MOUNT_DIR_OWNER(Uid), ?OWNER(Uid, ?MOUNT_DIR_GID)).
-define(MOUNT_DIR_OWNER, ?OWNER(?MOUNT_DIR_UID, ?MOUNT_DIR_GID)).

-define(OWNER(Uid, Gid), #{uid => Uid, gid => Gid}).

% Mocked LUMA mappings
-define(LUMA_MAPPING_MOCK, #{
        {?USER1, ?STORAGE_ID2} => #{
            <<"storageCredentials">> => #{<<"uid">> => 2001},
            <<"displayUid">> => <<"2221">>
        },
        {?USER2, ?STORAGE_ID2} => #{
            <<"storageCredentials">> => #{<<"uid">> => <<"2002">>}
        },
        {?USER4, ?STORAGE_ID2} => #{
            <<"storageCredentials">> => #{<<"uid">> => <<"forbidden type">>}
        },
        {?USER1, ?STORAGE_ID4} => #{
            <<"storageCredentials">> => #{<<"uid">> => <<"4001">>},
            <<"displayUid">> => 4441
        },
        {?USER2, ?STORAGE_ID4} => #{
            <<"storageCredentials">> => #{<<"uid">> => 4002}
        },
        {?USER4, ?STORAGE_ID4} => #{
            <<"storageCredentials">> => #{<<"uid">> => 4004},
            <<"displayUid">> => <<"forbidden type">>
        },
        {?USER1, ?STORAGE_ID6} => #{
            <<"storageCredentials">> => #{
                <<"accessKey">> => <<"AccessKey">>,
                <<"secretKey">> => <<"SecretKey">>
            },
            <<"displayUid">> => 6661
        },
        {?USER2, ?STORAGE_ID6} => #{
            <<"storageCredentials">> => #{
                <<"accessKey">> => <<"AccessKey">>,
                <<"secretKey">> => <<"SecretKey">>
            }
        },
        {?USER4, ?STORAGE_ID6} => #{
            <<"storageCredentials">> => #{
                <<"accessKey">> => <<"AccessKey">>
                % secretKey is intentionally missing
            }
        }
    }
).

-define(LUMA_DEFAULT_OWNER_MOCK, #{
    {?SPACE_ID2, ?STORAGE_ID2} => #{<<"uid">> => 2000, <<"gid">> => 2000},
    {?SPACE_ID4, ?STORAGE_ID4} => #{<<"uid">> => 4000, <<"gid">> => 4000}
}).

-define(LUMA_DEFAULT_DISPLAY_OWNER_MOCK, #{
    {?SPACE_ID2, ?STORAGE_ID2} => #{<<"uid">> => 2222, <<"gid">> => 2222},
    {?SPACE_ID4, ?STORAGE_ID4} => #{<<"uid">> => 4444, <<"gid">> => 4444},
    {?SPACE_ID6, ?STORAGE_ID6} => #{<<"uid">> => 6666, <<"gid">> => 6666}
}).

-endif.