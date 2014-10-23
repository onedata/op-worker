%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc dao_users header
%% @end
%% ===================================================================

-include_lib("public_key/include/public_key.hrl").
-include_lib("ctool/include/global_registry/gr_groups.hrl").

-ifndef(DAO_USERS).
-define(DAO_USERS, 1).

%% This record defines a user and is handled as a database document
-record(user, {
    global_id = "",
    logins = [],
    name = "",
    teams = [],
    spaces = [] :: [binary()],
    groups = [] :: [binary()],
    email_list = [],
    dn_list = [],
    unverified_dn_list = [],
    quota_doc,
    access_token = <<>> :: binary(),
    refresh_token = <<>> :: binary(),
    access_expiration_time = 0 :: non_neg_integer(),
    role = user}).

%% This is the special value that denote default quota in DB (default quota is defined as default_quota in default.yml)
-define(DEFAULT_QUOTA_DB_TAG, -1).

%% This record defines a users' quota and is handled as a database document
-record(quota, {size = ?DEFAULT_QUOTA_DB_TAG, exceeded = false}).

%% Declarations of lowest and highest user IDs. Those UIDs are used as #user record UUID. 
-define(LOWEST_USER_ID, 20000).
-define(HIGHEST_USER_ID, 65000).


%% Mapping of erlang macros to DN string attributes
-define(oid_code_to_shortname_mapping,
    [
        {?'id-at-name', "name"},
        {?'id-at-surname', "SN"},
        {?'id-at-givenName', "GN"},
        {?'id-at-initials', "initials"},
        {?'id-at-generationQualifier', "generationQualifier"},
        {?'id-at-commonName', "CN"},
        {?'id-at-localityName', "L"},
        {?'id-at-stateOrProvinceName', "ST"},
        {?'id-at-organizationName', "O"},
        {?'id-at-organizationalUnitName', "OU"},
        {?'id-at-title', "title"},
        {?'id-at-dnQualifier', "dnQualifier"},
        {?'id-at-countryName', "C"},
        {?'id-at-serialNumber', "serialNumber"},
        {?'id-at-pseudonym', "pseudonym"}
    ]).

-endif.