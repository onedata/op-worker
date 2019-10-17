%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Common macros used across logic tests and gs_channel tests.
%%% Two instances of each record synchronized via graph sync channel are mocked.
%%% For simplicity, all entities are related to each other.
%%% @end
%%%-------------------------------------------------------------------
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/handshake_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

-define(DUMMY_PROVIDER_ID, <<"dummyProviderId">>).

-define(MOCK_PROVIDER_IDENTITY_TOKEN(__ProviderId), <<"DUMMY-PROVIDER-IDENTITY-TOKEN-", __ProviderId/binary>>).
-define(MOCK_PROVIDER_ACCESS_TOKEN(__ProviderId), <<"DUMMY-PROVIDER-ACCESS-TOKEN-", __ProviderId/binary>>).

% WebSocket path is used to control gs_client:start_link mock behaviour
-define(PATH_CAUSING_CONN_ERROR, "/conn_err").
-define(PATH_CAUSING_NOBODY_IDENTITY, "/nobody_iden").
-define(PATH_CAUSING_CORRECT_CONNECTION, "/graph_sync").

% Mocked records
-define(USER_1, <<"user1Id">>).
-define(USER_2, <<"user2Id">>).
-define(USER_3, <<"user3Id">>). % This user is not related to any entity
-define(USER_INCREASING_REV, <<"userIncRev">>). % This record has a higher rev every time it is fetched
-define(GROUP_1, <<"group1Id">>).
-define(GROUP_2, <<"group2Id">>).
-define(SPACE_1, <<"space1Id">>).
-define(SPACE_2, <<"space2Id">>).
-define(SHARE_1, <<"share1Id">>).
-define(SHARE_2, <<"share2Id">>).
-define(PROVIDER_1, <<"provider1Id">>).
-define(PROVIDER_2, <<"provider2Id">>).
-define(HANDLE_SERVICE_1, <<"hservice1Id">>).
-define(HANDLE_SERVICE_2, <<"hservice2Id">>).
-define(HANDLE_1, <<"handle1Id">>).
-define(HANDLE_2, <<"handle2Id">>).
-define(HARVESTER_1, <<"harvester1Id">>).
-define(HARVESTER_2, <<"harvester2Id">>).
-define(STORAGE_1, <<"storage1Id">>).
-define(STORAGE_2, <<"storage2Id">>).

% User authorizations
% Token auth is translated to {token, Token} before graph sync request.
-define(USER_INTERNAL_TOKEN_AUTH(__User), #token_auth{token = __User}).
-define(USER_GS_TOKEN_AUTH(__User), {token, __User}).


-define(USER_PERMS_IN_GROUP_VALUE_BINARIES, #{?USER_1 => [atom_to_binary(?GROUP_VIEW, utf8)], ?USER_2 => [atom_to_binary(?GROUP_VIEW, utf8)]}).
-define(USER_PERMS_IN_GROUP_MATCHER_ATOMS, #{?USER_1 := [?GROUP_VIEW], ?USER_2 := [?GROUP_VIEW]}).
-define(GROUP_PERMS_IN_GROUP_VALUE_BINARIES, #{?GROUP_1 => [atom_to_binary(?GROUP_VIEW, utf8)], ?GROUP_2 => [atom_to_binary(?GROUP_VIEW, utf8)]}).
-define(GROUP_PERMS_IN_GROUP_MATCHER_ATOMS, #{?GROUP_1 := [?GROUP_VIEW], ?GROUP_2 := [?GROUP_VIEW]}).

-define(USER_PERMS_IN_SPACE_VALUE_BINARIES, #{?USER_1 => [atom_to_binary(?SPACE_VIEW, utf8)], ?USER_2 => [atom_to_binary(?SPACE_VIEW, utf8)]}).
-define(USER_PERMS_IN_SPACE_MATCHER_ATOMS, #{?USER_1 := [?SPACE_VIEW], ?USER_2 := [?SPACE_VIEW]}).
-define(GROUP_PERMS_IN_SPACE_VALUE_BINARIES, #{?GROUP_1 => [atom_to_binary(?SPACE_VIEW, utf8)], ?GROUP_2 => [atom_to_binary(?SPACE_VIEW, utf8)]}).
-define(GROUP_PERMS_IN_SPACE_MATCHER_ATOMS, #{?GROUP_1 := [?SPACE_VIEW], ?GROUP_2 := [?SPACE_VIEW]}).

-define(USER_PERMS_IN_HSERVICE_VALUE_BINARIES, #{?USER_1 => [atom_to_binary(?HANDLE_SERVICE_VIEW, utf8)], ?USER_2 => [atom_to_binary(?HANDLE_SERVICE_VIEW, utf8)]}).
-define(USER_PERMS_IN_HSERVICE_MATCHER_ATOMS, #{?USER_1 := [?HANDLE_SERVICE_VIEW], ?USER_2 := [?HANDLE_SERVICE_VIEW]}).
-define(GROUP_PERMS_IN_HSERVICE_VALUE_BINARIES, #{?GROUP_1 => [atom_to_binary(?HANDLE_SERVICE_VIEW, utf8)], ?GROUP_2 => [atom_to_binary(?HANDLE_SERVICE_VIEW, utf8)]}).
-define(GROUP_PERMS_IN_HSERVICE_MATCHER_ATOMS, #{?GROUP_1 := [?HANDLE_SERVICE_VIEW], ?GROUP_2 := [?HANDLE_SERVICE_VIEW]}).

-define(USER_PERMS_IN_HANDLE_VALUE_BINARIES, #{?USER_1 => [atom_to_binary(?HANDLE_VIEW, utf8)], ?USER_2 => [atom_to_binary(?HANDLE_VIEW, utf8)]}).
-define(USER_PERMS_IN_HANDLE_MATCHER_ATOMS, #{?USER_1 := [?HANDLE_VIEW], ?USER_2 := [?HANDLE_VIEW]}).
-define(GROUP_PERMS_IN_HANDLE_VALUE_BINARIES, #{?GROUP_1 => [atom_to_binary(?HANDLE_VIEW, utf8)], ?GROUP_2 => [atom_to_binary(?HANDLE_VIEW, utf8)]}).
-define(GROUP_PERMS_IN_HANDLE_MATCHER_ATOMS, #{?GROUP_1 := [?HANDLE_VIEW], ?GROUP_2 := [?HANDLE_VIEW]}).

% Mocked user data
-define(USER_FULL_NAME(__User), __User).
-define(USER_USERNAME(__User), __User).
-define(USER_EMAIL_LIST(__User), [__User]).
-define(USER_LINKED_ACCOUNTS_VALUE(__User), [#{<<"userId">> => __User}]).
-define(USER_LINKED_ACCOUNTS_MATCHER(__User), [#{<<"userId">> := __User}]).
-define(USER_DEFAULT_SPACE(__User), __User).
-define(USER_SPACE_ALIASES(__User), #{}).
-define(USER_EFF_GROUPS(__User), [?GROUP_1, ?GROUP_2]).
-define(USER_EFF_SPACES(__User), [?SPACE_1, ?SPACE_2]).
-define(USER_EFF_HANDLE_SERVICES(__User), [?HANDLE_SERVICE_1, ?HANDLE_SERVICE_2]).
-define(USER_EFF_HANDLES(__User), [?HANDLE_1, ?HANDLE_2]).

% Mocked group data
-define(GROUP_NAME(__Group), __Group).
-define(GROUP_TYPE_JSON(__Group), <<"role">>).
-define(GROUP_TYPE_ATOM(__Group), role).

% Mocked space data
-define(SPACE_NAME(__Space), __Space).
-define(SPACE_DIRECT_USERS_VALUE(__Space), ?USER_PERMS_IN_SPACE_VALUE_BINARIES).
-define(SPACE_DIRECT_USERS_MATCHER(__Space), ?USER_PERMS_IN_SPACE_MATCHER_ATOMS).
-define(SPACE_EFF_USERS_VALUE(__Space), ?USER_PERMS_IN_SPACE_VALUE_BINARIES).
-define(SPACE_EFF_USERS_MATCHER(__Space), ?USER_PERMS_IN_SPACE_MATCHER_ATOMS).
-define(SPACE_DIRECT_GROUPS_VALUE(__Space), ?GROUP_PERMS_IN_SPACE_VALUE_BINARIES).
-define(SPACE_DIRECT_GROUPS_MATCHER(__Space), ?GROUP_PERMS_IN_SPACE_MATCHER_ATOMS).
-define(SPACE_EFF_GROUPS_VALUE(__Space), ?GROUP_PERMS_IN_SPACE_VALUE_BINARIES).
-define(SPACE_EFF_GROUPS_MATCHER(__Space), ?GROUP_PERMS_IN_SPACE_MATCHER_ATOMS).
-define(SPACE_PROVIDERS_VALUE(__Space), #{?PROVIDER_1 => 1000000000, ?PROVIDER_2 => 1000000000}).
-define(SPACE_PROVIDERS_MATCHER(__Space), #{?PROVIDER_1 := 1000000000, ?PROVIDER_2 := 1000000000}).
-define(SPACE_SHARES(__Space), [?SHARE_1, ?SHARE_2]).
-define(SPACE_HARVESTERS(__Space), [?HARVESTER_1, ?HARVESTER_2]).
-define(SPACE_STORAGES_VALUE(__Space), #{?STORAGE_1 => 1000000000, ?STORAGE_2 => 1000000000}).
-define(SPACE_STORAGES_MATCHER(__Space), #{?STORAGE_1 := 1000000000, ?STORAGE_2 := 1000000000}).

% Mocked share data
-define(SHARE_NAME(__Share), __Share).
-define(SHARE_PUBLIC_URL(__Share), __Share).
-define(SHARE_SPACE(__Share), ?SPACE_1).
-define(SHARE_HANDLE(__Share), ?HANDLE_1).
-define(SHARE_ROOT_FILE(__Share), __Share).

% Mocked provider data
-define(PROVIDER_NAME(__Provider), __Provider).
-define(PROVIDER_ADMIN_EMAIL(__Provider), __Provider).
-define(PROVIDER_DOMAIN(__Provider), __Provider).
-define(PROVIDER_ONLINE(__Provider), true).
-define(PROVIDER_SUBDOMAIN_DELEGATION(__Provider), false).
-define(PROVIDER_SUBDOMAIN(__Provider), undefined).
-define(PROVIDER_SPACES_VALUE(__Provider), #{?SPACE_1 => 1000000000, ?SPACE_2 => 1000000000}).
-define(PROVIDER_SPACES_MATCHER(__Provider), #{?SPACE_1 := 1000000000, ?SPACE_2 := 1000000000}).
-define(PROVIDER_STORAGES(__Provider), [?STORAGE_1, ?STORAGE_2]).
-define(PROVIDER_EFF_USERS(__Provider), [?USER_1, ?USER_2]).
-define(PROVIDER_EFF_GROUPS(__Provider), [?GROUP_1, ?GROUP_2]).
-define(PROVIDER_LATITUDE(__Provider), 0.0).
-define(PROVIDER_LONGITUDE(__Provider), 0.0).

% Mocked handle service data
-define(HANDLE_SERVICE_NAME(__HService), __HService).
-define(HANDLE_SERVICE_EFF_USERS_VALUE(__HService), ?USER_PERMS_IN_HSERVICE_VALUE_BINARIES).
-define(HANDLE_SERVICE_EFF_USERS_MATCHER(__HService), ?USER_PERMS_IN_HSERVICE_MATCHER_ATOMS).
-define(HANDLE_SERVICE_EFF_GROUPS_VALUE(__HService), ?GROUP_PERMS_IN_HSERVICE_VALUE_BINARIES).
-define(HANDLE_SERVICE_EFF_GROUPS_MATCHER(__HService), ?GROUP_PERMS_IN_HSERVICE_MATCHER_ATOMS).

% Mocked handle data
-define(HANDLE_PUBLIC_HANDLE(__Handle), __Handle).
-define(HANDLE_RESOURCE_TYPE(__Handle), <<"Share">>).
-define(HANDLE_RESOURCE_ID(__Handle), ?SHARE_1).
-define(HANDLE_METADATA(__Handle), __Handle).
-define(HANDLE_TIMESTAMP_VALUE(__Handle), <<"2017-08-30T10:00:00Z">>).
-define(HANDLE_TIMESTAMP_MATCHER(__Handle), {{2017, 08, 30}, {10, 00, 00}}).
-define(HANDLE_H_SERVICE(__Handle), ?HANDLE_SERVICE_1).
-define(HANDLE_EFF_USERS_VALUE(__Handle), ?USER_PERMS_IN_HANDLE_VALUE_BINARIES).
-define(HANDLE_EFF_USERS_MATCHER(__Handle), ?USER_PERMS_IN_HANDLE_MATCHER_ATOMS).
-define(HANDLE_EFF_GROUPS_VALUE(__Handle), ?GROUP_PERMS_IN_HANDLE_VALUE_BINARIES).
-define(HANDLE_EFF_GROUPS_MATCHER(__Handle), ?GROUP_PERMS_IN_HANDLE_MATCHER_ATOMS).

% Mocked harvester data
-define(HARVESTER_SPACE1(__Harvester), <<"harvesterSpace1">>).
-define(HARVESTER_SPACE2(__Harvester), <<"harvesterSpace2">>).
-define(HARVESTER_SPACE3(__Harvester), <<"harvesterSpace3">>).

-define(HARVESTER_INDEX1(__Harvester), <<"harvesterIndex1">>).
-define(HARVESTER_INDEX2(__Harvester), <<"harvesterIndex2">>).
-define(HARVESTER_INDEX3(__Harvester), <<"harvesterIndex3">>).

-define(HARVESTER_SPACES(__Harvester), [
    ?HARVESTER_SPACE1(__Harvester)
]).
-define(HARVESTER_SPACES2(__Harvester), [
    ?HARVESTER_SPACE2(__Harvester),
    ?HARVESTER_SPACE3(__Harvester)
]).

-define(HARVESTER_INDICES(__Harvester), [
    ?HARVESTER_INDEX1(__Harvester)
]).
-define(HARVESTER_INDICES2(__Harvester), [
    ?HARVESTER_INDEX2(__Harvester),
    ?HARVESTER_INDEX3(__Harvester)
]).


-define(MOCK_JOIN_GROUP_TOKEN, <<"mockJoinGroupToken">>).
-define(MOCK_JOINED_GROUP_ID, <<"mockJoinedGroupId">>).
-define(MOCK_CREATED_GROUP_ID, <<"mockCreatedGroupId">>).
-define(MOCK_JOIN_SPACE_TOKEN, <<"mockJoinSpaceToken">>).
-define(MOCK_JOINED_SPACE_ID, <<"mockJoinedSpaceId">>).
-define(MOCK_CREATED_SPACE_ID, <<"mockCreatedSpaceId">>).
-define(MOCK_CREATED_SHARE_ID, <<"mockCreatedHandleId">>).
-define(MOCK_CREATED_HANDLE_ID, <<"mockCreatedHandleId">>).

-define(MOCK_INVITE_USER_TOKEN, <<"mockInviteUserToken">>).
-define(MOCK_INVITE_GROUP_TOKEN, <<"mockInviteGroupToken">>).
-define(MOCK_INVITE_PROVIDER_TOKEN, <<"mockInviteProviderToken">>).
-define(MOCK_IDP_ACCESS_TOKEN, <<"mockIdPAccessToken">>).
-define(MOCK_IDP, <<"mockIdP">>).

-define(USER_PRIVATE_DATA_MATCHER(__User), #document{key = __User, value = #od_user{
    full_name = ?USER_FULL_NAME(__User),
    username = ?USER_USERNAME(__User),
    emails = ?USER_EMAIL_LIST(__User),
    linked_accounts = ?USER_LINKED_ACCOUNTS_MATCHER(__User),
    default_space = ?USER_DEFAULT_SPACE(__User),
    space_aliases = ?USER_SPACE_ALIASES(__User),
    eff_groups = ?USER_EFF_GROUPS(__User),
    eff_spaces = ?USER_EFF_SPACES(__User),
    eff_handle_services = ?USER_EFF_HANDLE_SERVICES(__User),
    eff_handles = ?USER_EFF_HANDLES(__User)
}}).
-define(USER_PROTECTED_DATA_MATCHER(__User), #document{key = __User, value = #od_user{
    full_name = ?USER_FULL_NAME(__User),
    username = ?USER_USERNAME(__User),
    emails = ?USER_EMAIL_LIST(__User),
    linked_accounts = ?USER_LINKED_ACCOUNTS_MATCHER(__User),
    default_space = undefined,
    space_aliases = #{},
    eff_groups = [],
    eff_spaces = [],
    eff_handle_services = [],
    eff_handles = []
}}).
-define(USER_SHARED_DATA_MATCHER(__User), #document{key = __User, value = #od_user{
    full_name = ?USER_FULL_NAME(__User),
    username = ?USER_USERNAME(__User),
    emails = [],
    linked_accounts = [],
    default_space = undefined,
    space_aliases = #{},
    eff_groups = [],
    eff_spaces = [],
    eff_handle_services = [],
    eff_handles = []
}}).


-define(GROUP_SHARED_DATA_MATCHER(__Group), #document{key = __Group, value = #od_group{
    name = ?GROUP_NAME(__Group),
    type = ?GROUP_TYPE_ATOM(__Group)
}}).


-define(SPACE_PRIVATE_DATA_MATCHER(__Space), #document{key = __Space, value = #od_space{
    name = ?SPACE_NAME(__Space),
    direct_users = ?SPACE_DIRECT_USERS_MATCHER(__Space),
    eff_users = ?SPACE_EFF_USERS_MATCHER(__Space),
    direct_groups = ?SPACE_DIRECT_GROUPS_MATCHER(__Space),
    eff_groups = ?SPACE_EFF_GROUPS_MATCHER(__Space),
    providers = ?SPACE_PROVIDERS_MATCHER(__Space),
    shares = ?SPACE_SHARES(__Space),
    harvesters = ?SPACE_HARVESTERS(__Space),
    storages = ?SPACE_STORAGES_MATCHER(__Space)
}}).
-define(SPACE_PROTECTED_DATA_MATCHER(__Space), #document{key = __Space, value = #od_space{
    name = ?SPACE_NAME(__Space),
    direct_users = #{},
    eff_users = #{},
    direct_groups = #{},
    eff_groups = #{},
    providers = #{},
    shares = [],
    harvesters = []
}}).


-define(SHARE_PRIVATE_DATA_MATCHER(__Share), #document{key = __Share, value = #od_share{
    name = ?SHARE_NAME(__Share),
    public_url = ?SHARE_PUBLIC_URL(__Share),
    space = ?SHARE_SPACE(__Share),
    handle = ?SHARE_HANDLE(__Share),
    root_file = ?SHARE_ROOT_FILE(__Share)
}}).
-define(SHARE_PUBLIC_DATA_MATCHER(__Share), #document{key = __Share, value = #od_share{
    name = ?SHARE_NAME(__Share),
    public_url = ?SHARE_PUBLIC_URL(__Share),
    space = undefined,
    handle = ?SHARE_HANDLE(__Share),
    root_file = ?SHARE_ROOT_FILE(__Share)
}}).


-define(PROVIDER_PRIVATE_DATA_MATCHER(__Provider), #document{key = __Provider, value = #od_provider{
    name = ?PROVIDER_NAME(__Provider),
    admin_email = ?PROVIDER_ADMIN_EMAIL(__Provider),
    subdomain_delegation = ?PROVIDER_SUBDOMAIN_DELEGATION(__Provider),
    domain = ?PROVIDER_DOMAIN(__Provider),
    online = ?PROVIDER_ONLINE(__Provider),
    storages = ?PROVIDER_STORAGES(__Provider),
    spaces = ?PROVIDER_SPACES_MATCHER(__Provider),
    eff_users = ?PROVIDER_EFF_USERS(__Provider),
    eff_groups = ?PROVIDER_EFF_GROUPS(__Provider)
}}).
-define(PROVIDER_PROTECTED_DATA_MATCHER(__Provider), #document{key = __Provider, value = #od_provider{
    name = ?PROVIDER_NAME(__Provider),
    domain = ?PROVIDER_DOMAIN(__Provider),
    online = ?PROVIDER_ONLINE(__Provider),
    spaces = #{},
    eff_users = [],
    eff_groups = []
}}).


-define(HANDLE_SERVICE_PRIVATE_DATA_MATCHER(__HService), #document{key = __HService, value = #od_handle_service{
    name = ?HANDLE_SERVICE_NAME(__HService),
    eff_users = ?HANDLE_SERVICE_EFF_USERS_MATCHER(__HService),
    eff_groups = ?HANDLE_SERVICE_EFF_GROUPS_MATCHER(__HService)
}}).


-define(HANDLE_PRIVATE_DATA_MATCHER(__Handle), #document{key = __Handle, value = #od_handle{
    public_handle = ?HANDLE_PUBLIC_HANDLE(__Handle),
    resource_type = ?HANDLE_RESOURCE_TYPE(__Handle),
    resource_id = ?HANDLE_RESOURCE_ID(__Handle),
    metadata = ?HANDLE_METADATA(__Handle),
    timestamp = ?HANDLE_TIMESTAMP_MATCHER(__Handle),
    handle_service = ?HANDLE_H_SERVICE(__Handle),
    eff_users = ?HANDLE_EFF_USERS_MATCHER(__HService),
    eff_groups = ?HANDLE_EFF_GROUPS_MATCHER(__HService)
}}).
-define(HANDLE_PUBLIC_DATA_MATCHER(__Handle), #document{key = __Handle, value = #od_handle{
    public_handle = ?HANDLE_PUBLIC_HANDLE(__Handle),
    resource_type = undefined,
    resource_id = undefined,
    metadata = ?HANDLE_METADATA(__Handle),
    timestamp = ?HANDLE_TIMESTAMP_MATCHER(__Handle),
    handle_service = undefined,
    eff_users = #{},
    eff_groups = #{}

}}).

-define(HARVESTER_PRIVATE_DATA_MATCHER(__Harvester), #document{key = __Harvester, value = #od_harvester{
    indices = ?HARVESTER_INDICES(__Harvester),
    spaces = ?HARVESTER_SPACES(__Harvester)
}}).

-define(STORAGE_PRIVATE_DATA_MATCHER(__Storage), #document{key = __Storage, value = #od_storage{
    provider = ?PROVIDER_1,
    qos_parameters = #{}
}}).


-define(USER_SHARED_DATA_VALUE(__UserId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_user, id = __UserId, aspect = instance, scope = shared}),
    <<"fullName">> => ?USER_FULL_NAME(__UserId),
    <<"username">> => ?USER_USERNAME(__UserId),
    % @TODO deprecated, included for backward compatibility
    <<"name">> => ?USER_FULL_NAME(__UserId),
    <<"login">> => ?USER_USERNAME(__UserId),
    <<"alias">> => ?USER_USERNAME(__UserId)
}).
-define(USER_PROTECTED_DATA_VALUE(__UserId), begin
    __SharedData = ?USER_SHARED_DATA_VALUE(__UserId),
    __SharedData#{
        <<"gri">> => gri:serialize(#gri{type = od_user, id = __UserId, aspect = instance, scope = protected}),
        <<"emailList">> => ?USER_EMAIL_LIST(__UserId),
        <<"linkedAccounts">> => ?USER_LINKED_ACCOUNTS_VALUE(__UserId)
    }
end).
-define(USER_PRIVATE_DATA_VALUE(__UserId), begin
    __ProtectedData = ?USER_PROTECTED_DATA_VALUE(__UserId),
    __ProtectedData#{
        <<"gri">> => gri:serialize(#gri{type = od_user, id = __UserId, aspect = instance, scope = private}),
        <<"defaultSpaceId">> => ?USER_DEFAULT_SPACE(__UserId),
        <<"spaceAliases">> => ?USER_SPACE_ALIASES(__UserId),

        <<"effectiveGroups">> => ?USER_EFF_GROUPS(__UserId),
        <<"effectiveSpaces">> => ?USER_EFF_SPACES(__UserId),
        <<"effectiveHandleServices">> => ?USER_EFF_HANDLE_SERVICES(__UserId),
        <<"effectiveHandles">> => ?USER_EFF_HANDLES(__UserId)
    }
end).


-define(GROUP_SHARED_DATA_VALUE(__GroupId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_group, id = __GroupId, aspect = instance, scope = shared}),
    <<"name">> => ?GROUP_NAME(__GroupId),
    <<"type">> => ?GROUP_TYPE_JSON(__GroupId)
}).


-define(SPACE_PROTECTED_DATA_VALUE(__SpaceId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_space, id = __SpaceId, aspect = instance, scope = protected}),
    <<"name">> => ?SPACE_NAME(__SpaceId),
    <<"providers">> => ?SPACE_PROVIDERS_VALUE(__SpaceId)
}).
-define(SPACE_PRIVATE_DATA_VALUE(__SpaceId), begin
    (?SPACE_PROTECTED_DATA_VALUE(__SpaceId))#{
        <<"gri">> => gri:serialize(#gri{type = od_space, id = __SpaceId, aspect = instance, scope = private}),
        <<"users">> => ?SPACE_DIRECT_USERS_VALUE(__SpaceId),
        <<"effectiveUsers">> => ?SPACE_EFF_USERS_VALUE(__SpaceId),

        <<"groups">> => ?SPACE_DIRECT_GROUPS_VALUE(__SpaceId),
        <<"effectiveGroups">> => ?SPACE_EFF_GROUPS_VALUE(__SpaceId),

        <<"storages">> => ?SPACE_STORAGES_VALUE(__SpaceId),
        <<"providers">> => ?SPACE_PROVIDERS_VALUE(__SpaceId),
        <<"shares">> => ?SPACE_SHARES(__SpaceId),
        <<"harvesters">> => ?SPACE_HARVESTERS(__SpaceId)
    }
end).


-define(SHARE_PUBLIC_DATA_VALUE(__ShareId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_share, id = __ShareId, aspect = instance, scope = public}),
    <<"name">> => ?SHARE_NAME(__ShareId),
    <<"publicUrl">> => ?SHARE_PUBLIC_URL(__ShareId),
    <<"handleId">> => ?SHARE_HANDLE(__ShareId),
    <<"rootFileId">> => ?SHARE_ROOT_FILE(__ShareId)
}).
-define(SHARE_PRIVATE_DATA_VALUE(__ShareId), begin
    __PublicData = ?SHARE_PUBLIC_DATA_VALUE(__ShareId),
    __PublicData#{
        <<"gri">> => gri:serialize(#gri{type = od_share, id = __ShareId, aspect = instance, scope = private}),
        <<"spaceId">> => ?SHARE_SPACE(__ShareId)
    }
end).


-define(PROVIDER_PROTECTED_DATA_VALUE(__ProviderId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_provider, id = __ProviderId, aspect = instance, scope = protected}),
    <<"name">> => ?PROVIDER_NAME(__ProviderId),
    <<"domain">> => ?PROVIDER_DOMAIN(__ProviderId),
    <<"online">> => ?PROVIDER_ONLINE(__ProviderId),
    <<"latitude">> => ?PROVIDER_LATITUDE(__ProviderId),
    <<"longitude">> => ?PROVIDER_LONGITUDE(__ProviderId)
}).
-define(PROVIDER_PRIVATE_DATA_VALUE(__ProviderId), begin
    __ProtectedData = ?PROVIDER_PROTECTED_DATA_VALUE(__ProviderId),
    __ProtectedData#{
        <<"adminEmail">> => ?PROVIDER_ADMIN_EMAIL(__ProviderId),
        <<"subdomainDelegation">> => ?PROVIDER_SUBDOMAIN_DELEGATION(__ProviderId),
        <<"subdomain">> => ?PROVIDER_SUBDOMAIN(__ProviderId),
        <<"gri">> => gri:serialize(#gri{type = od_provider, id = __ProviderId, aspect = instance, scope = private}),
        <<"spaces">> => case __ProviderId of
            ?DUMMY_PROVIDER_ID -> #{};
            _ -> ?PROVIDER_SPACES_VALUE(__ProviderId)
        end,
        <<"storages">> => ?PROVIDER_STORAGES(__ProviderId),
        <<"effectiveUsers">> => case __ProviderId of
            ?DUMMY_PROVIDER_ID -> [];
            _ -> ?PROVIDER_EFF_USERS(__ProviderId)
        end,
        <<"effectiveGroups">> => case __ProviderId of
            ?DUMMY_PROVIDER_ID -> [];
            _ -> ?PROVIDER_EFF_GROUPS(__ProviderId)
        end
    }
end).


-define(HANDLE_SERVICE_PRIVATE_DATA_VALUE(__HServiceId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_handle_service, id = __HServiceId, aspect = instance, scope = private}),
    <<"name">> => ?HANDLE_SERVICE_NAME(__HServiceId),
    <<"effectiveUsers">> => ?HANDLE_SERVICE_EFF_USERS_VALUE(__HServiceId),
    <<"effectiveGroups">> => ?HANDLE_SERVICE_EFF_GROUPS_VALUE(__HServiceId)
}).


-define(HANDLE_PUBLIC_DATA_VALUE(__HandleId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_handle, id = __HandleId, aspect = instance, scope = public}),
    <<"publicHandle">> => ?HANDLE_PUBLIC_HANDLE(__HandleId),
    <<"metadata">> => ?HANDLE_METADATA(__HandleId),
    <<"timestamp">> => ?HANDLE_TIMESTAMP_VALUE(__HandleId)
}).
-define(HANDLE_PRIVATE_DATA_VALUE(__HandleId), begin
    __PublicData = ?HANDLE_PUBLIC_DATA_VALUE(__HandleId),
    __PublicData#{
        <<"gri">> => gri:serialize(#gri{type = od_handle, id = __HandleId, aspect = instance, scope = private}),
        <<"resourceType">> => ?HANDLE_RESOURCE_TYPE(__HandleId),
        <<"resourceId">> => ?HANDLE_RESOURCE_ID(__HandleId),
        <<"handleServiceId">> => ?HANDLE_H_SERVICE(__HandleId),
        <<"effectiveUsers">> => ?HANDLE_EFF_USERS_VALUE(__HandleId),
        <<"effectiveGroups">> => ?HANDLE_EFF_GROUPS_VALUE(__HandleId)
    }
end).

-define(HARVESTER_PRIVATE_DATA_VALUE(__HarvesterId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_harvester, id = __HarvesterId, aspect = instance, scope = private}),
    <<"indices">> => ?HARVESTER_INDICES(__HarvesterId),
    <<"spaces">> => ?HARVESTER_SPACES(__HarvesterId)
}).

-define(STORAGE_PRIVATE_DATA_VALUE(__StorageId), #{
    <<"revision">> => 1,
    <<"gri">> => gri:serialize(#gri{type = od_storage, id = __StorageId, aspect = instance, scope = private}),
    <<"provider">> => ?PROVIDER_1,
    <<"qos_parameters">> => #{}
}).
