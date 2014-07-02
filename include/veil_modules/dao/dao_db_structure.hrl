%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: View declarations for DAO.
%% @end
%% ===================================================================
-author("Rafal Slota").

-ifndef(DAO_DB_STRUCTURE_HRL).
-define(DAO_DB_STRUCTURE_HRL, 1).

-include("veil_modules/dao/common.hrl").

%% ====================================================================
%% DB definitions
%% ====================================================================
%% DB Names
-ifdef(TEST).
-define(SYSTEM_DB_NAME, "system_data_test").
-define(FILES_DB_NAME, "files_test").
-define(DESCRIPTORS_DB_NAME, "file_descriptors_test").
-define(USERS_DB_NAME, "users_test").
-else.
-define(SYSTEM_DB_NAME, "system_data").
-define(FILES_DB_NAME, "files").
-define(DESCRIPTORS_DB_NAME, "file_descriptors").
-define(USERS_DB_NAME, "users").
-endif.

%% Views
-define(FILE_TREE_VIEW, #view_info{name = "file_tree", db_name = ?FILES_DB_NAME, version = 1}).
-define(WAITING_FILES_TREE_VIEW, #view_info{name = "waiting_files_tree", db_name = ?FILES_DB_NAME, version = 1}).
-define(FILE_SUBDIRS_VIEW, #view_info{name = "file_subdirs", db_name = ?FILES_DB_NAME, version = 5}).
-define(FD_BY_FILE_VIEW, #view_info{name = "fd_by_name", db_name = ?DESCRIPTORS_DB_NAME, version = 1}).
-define(FD_BY_EXPIRED_BEFORE_VIEW, #view_info{name = "fd_by_expired_before", db_name = ?DESCRIPTORS_DB_NAME, version = 1}).
-define(ALL_STORAGE_VIEW, #view_info{name = "all_storage", db_name = ?SYSTEM_DB_NAME, version = 1}).
-define(STORAGE_BY_ID_VIEW, #view_info{name = "storage_by_id", db_name = ?SYSTEM_DB_NAME, version = 1}).
-define(FILES_BY_UID_AND_FILENAME, #view_info{name = "files_by_uid_and_filename", db_name = ?FILES_DB_NAME, version = 1}).
-define(FILE_META_BY_TIMES, #view_info{name = "file_meta_by_times", db_name = ?FILES_DB_NAME, version = 1}).
-define(FILES_BY_META_DOC, #view_info{name = "files_by_meta_doc", db_name = ?FILES_DB_NAME, version = 1}).

-define(USER_BY_EMAIL_VIEW, #view_info{name = "user_by_email", db_name = ?USERS_DB_NAME, version = 1}).
-define(USER_BY_LOGIN_VIEW, #view_info{name = "user_by_login", db_name = ?USERS_DB_NAME, version = 1}).
-define(USER_BY_DN_VIEW, #view_info{name = "user_by_dn", db_name = ?USERS_DB_NAME, version = 1}).
-define(USER_BY_UID_VIEW, #view_info{name = "user_by_uid", db_name = ?USERS_DB_NAME, version = 1}).

-define(SHARE_BY_FILE_VIEW, #view_info{name = "share_by_file", db_name = ?FILES_DB_NAME, version = 1}).
-define(SHARE_BY_USER_VIEW, #view_info{name = "share_by_user", db_name = ?FILES_DB_NAME, version = 1}).

-define(USER_FILES_NUMBER_VIEW, #view_info{name = "user_files_number", db_name = ?FILES_DB_NAME, version = 1}).
-define(USER_FILES_SIZE_VIEW, #view_info{name = "user_files_size", db_name = ?FILES_DB_NAME, version = 1}).
-define(GROUP_FILES_NUMBER_VIEW, #view_info{name = "group_files_number", db_name = ?FILES_DB_NAME, version = 1}).

%% FUSE Sessions
-define(FUSE_CONNECTIONS_VIEW, #view_info{name = "fuse_connections", db_name = ?SYSTEM_DB_NAME, version = 1}).
-define(EXPIRED_FUSE_SESSIONS_VIEW, #view_info{name = "expired_fuse_sessions", db_name = ?SYSTEM_DB_NAME, version = 1}).
-define(FUSE_SESSIONS_BY_USER_ID_VIEW, #view_info{name = "fuse_sessions_by_user_id", db_name = ?SYSTEM_DB_NAME, version = 1}).


%% List of all used databases :: [string()]
-define(DB_LIST, [?SYSTEM_DB_NAME, ?FILES_DB_NAME, ?DESCRIPTORS_DB_NAME, ?USERS_DB_NAME]).
%% List of all used views :: [#view_info]
-define(VIEW_LIST, [?FILE_TREE_VIEW, ?WAITING_FILES_TREE_VIEW, ?FILE_SUBDIRS_VIEW, ?FD_BY_FILE_VIEW, ?FD_BY_EXPIRED_BEFORE_VIEW, ?ALL_STORAGE_VIEW,
    ?FILES_BY_UID_AND_FILENAME, ?FILE_META_BY_TIMES, ?FILES_BY_META_DOC,
    ?USER_BY_EMAIL_VIEW, ?USER_BY_LOGIN_VIEW, ?USER_BY_DN_VIEW, ?USER_BY_UID_VIEW, ?STORAGE_BY_ID_VIEW,
    ?SHARE_BY_FILE_VIEW, ?SHARE_BY_USER_VIEW, ?USER_FILES_NUMBER_VIEW, ?USER_FILES_SIZE_VIEW, ?GROUP_FILES_NUMBER_VIEW,
    ?FUSE_CONNECTIONS_VIEW, ?EXPIRED_FUSE_SESSIONS_VIEW, ?FUSE_SESSIONS_BY_USER_ID_VIEW]).


%% Default database name
-define(DEFAULT_DB, lists:nth(1, ?DB_LIST)).


-endif.
