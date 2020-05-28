%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header file contains definitions of macros used in
%%% luma_external_feed.erl module
%%% @end
%%%-------------------------------------------------------------------

-ifndef(EXTERNAL_LUMA_HRL).
-define(EXTERNAL_LUMA_HRL, 1).

-define(TO_PATH(Tokens), str_utils:join_binary(Tokens, <<"/">>)).

%% Macros used in definitions of LUMA endpoints
-define(STORAGE_ACCESS_PATH, <<"storage_access">>).
-define(STORAGE_ACCESS_ALL_PATH, ?TO_PATH([?STORAGE_ACCESS_PATH, <<"all">>])).
-define(STORAGE_ACCESS_POSIX_PATH, ?TO_PATH([?STORAGE_ACCESS_PATH, <<"posix_compatible">>])).
-define(STORAGE_SYNC_POSIX_PATH, ?TO_PATH([<<"storage_sync">>, <<"posix_compatible">>])).
-define(ONECLIENT_DISPLAY_CREDENTIALS, <<"oneclient_display_credentials">>).

%% LUMA endpoints
-define(ONEDATA_USER_TO_CREDENTIALS_PATH, ?TO_PATH([?STORAGE_ACCESS_ALL_PATH, <<"onedata_user_to_credentials">>])).
-define(DEFAULT_POSIX_CREDENTIALS_PATH, ?TO_PATH([?STORAGE_ACCESS_POSIX_PATH, <<"default_credentials">>])).
-define(ONECLIENT_DISPLAY_CREDENTIALS_PATH, ?TO_PATH([?ONECLIENT_DISPLAY_CREDENTIALS, <<"all">>, <<"default">>])).

%% Reverse LUMA endpoints
-define(UID_TO_ONEDATA_USER_PATH, ?TO_PATH([?STORAGE_SYNC_POSIX_PATH, <<"uid_to_onedata_user">>])).
-define(ACL_USER_TO_ONEDATA_USER_PATH, ?TO_PATH([?STORAGE_SYNC_POSIX_PATH, <<"acl_user_to_onedata_user">>])).
-define(ACL_GROUP_TO_ONEDATA_GROUP_PATH, ?TO_PATH([?STORAGE_SYNC_POSIX_PATH, <<"acl_group_to_onedata_group">>])).

-define(LUMA_URL(LumaConfig, Path),
    str_utils:format_bin("~s/~s", [luma_config:get_url(LumaConfig), Path])
).

-endif.