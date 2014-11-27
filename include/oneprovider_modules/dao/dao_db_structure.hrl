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

-include_lib("dao/include/common.hrl").

%% ====================================================================
%% DB definitions
%% ====================================================================
%% DB Names
-ifdef(TEST).
-define(SYSTEM_DB_NAME, "system_data_test").
-define(FILES_DB_NAME, "files_test").
-define(DESCRIPTORS_DB_NAME, "file_descriptors_test").
-define(AVAILABLE_BLOCKS_DB_NAME, "available_blocks_test").
-define(USERS_DB_NAME, "users_test").
-define(COOKIES_DB_NAME, "cookies_test").
-define(GROUPS_DB_NAME, "groups_test").
-else.
-define(SYSTEM_DB_NAME, "system_data").
-define(FILES_DB_NAME, "files").
-define(DESCRIPTORS_DB_NAME, "file_descriptors").
-define(AVAILABLE_BLOCKS_DB_NAME, "available_blocks").
-define(USERS_DB_NAME, "users").
-define(COOKIES_DB_NAME, "cookies").
-define(GROUPS_DB_NAME, "groups").
-endif.

%% Views
-define(FILE_TREE_VIEW, #view_info{name = "file_tree", db_name = ?FILES_DB_NAME, version = 1}).
-define(WAITING_FILES_TREE_VIEW, #view_info{name = "waiting_files_tree", db_name = ?FILES_DB_NAME, version = 1}).
-define(FILE_CHILDS_VIEW, #view_info{name = "file_childs", db_name = ?FILES_DB_NAME, version = 1}).
-define(FD_BY_FILE_VIEW, #view_info{name = "fd_by_name", db_name = ?DESCRIPTORS_DB_NAME, version = 1}).
-define(FD_BY_EXPIRED_BEFORE_VIEW, #view_info{name = "fd_by_expired_before", db_name = ?DESCRIPTORS_DB_NAME, version = 1}).
-define(ATTR_WATCHERS_BY_EXPIRED_BEFORE_VIEW, #view_info{name = "attr_watchers_by_expired_before", db_name = ?DESCRIPTORS_DB_NAME, version = 1}).
-define(ATTR_WATCHERS_BY_NAME_VIEW, #view_info{name = "attr_watchers_by_name", db_name = ?DESCRIPTORS_DB_NAME, version = 1}).
-define(ALL_STORAGE_VIEW, #view_info{name = "all_storage", db_name = ?SYSTEM_DB_NAME, version = 1}).
-define(STORAGE_BY_ID_VIEW, #view_info{name = "storage_by_id", db_name = ?SYSTEM_DB_NAME, version = 1}).
-define(FILES_BY_UID_AND_FILENAME, #view_info{name = "files_by_uid_and_filename", db_name = ?FILES_DB_NAME, version = 1}).
-define(FILE_META_BY_TIMES, #view_info{name = "file_meta_by_times", db_name = ?FILES_DB_NAME, version = 1}).
-define(FILES_BY_META_DOC, #view_info{name = "files_by_meta_doc", db_name = ?FILES_DB_NAME, version = 1}).
-define(FILE_LOCATIONS_BY_FILE, #view_info{name = "file_locations_by_file", db_name = ?DESCRIPTORS_DB_NAME, version = 1}).
-define(FILE_BLOCKS_BY_FILE_LOCATION, #view_info{name = "file_blocks_by_file_location", db_name = ?DESCRIPTORS_DB_NAME, version = 1}).
-define(AVAILABLE_BLOCKS_BY_FILE_ID, #view_info{name = "available_blocks_by_file_id", db_name = ?FILES_DB_NAME, version = 1}). %todo change db to AVAILABLE_BLOCKS_DB_NAME

-define(USER_BY_GLOBAL_ID_VIEW, #view_info{name = "user_by_global_id", db_name = ?USERS_DB_NAME, version = 1}).
-define(USER_BY_NAME_VIEW, #view_info{name = "user_by_name", db_name = ?USERS_DB_NAME, version = 1}).
-define(USER_BY_EMAIL_VIEW, #view_info{name = "user_by_email", db_name = ?USERS_DB_NAME, version = 1}).
-define(USER_BY_LOGIN_VIEW, #view_info{name = "user_by_login", db_name = ?USERS_DB_NAME, version = 2}).
-define(USER_BY_DN_VIEW, #view_info{name = "user_by_dn", db_name = ?USERS_DB_NAME, version = 1}).
-define(USER_BY_UNVERIFIED_DN_VIEW, #view_info{name = "user_by_unverified_dn", db_name = ?USERS_DB_NAME, version = 1}).
-define(USER_BY_UID_VIEW, #view_info{name = "user_by_uid", db_name = ?USERS_DB_NAME, version = 1}).

-define(SHARE_BY_FILE_VIEW, #view_info{name = "share_by_file", db_name = ?FILES_DB_NAME, version = 1}).
-define(SHARE_BY_USER_VIEW, #view_info{name = "share_by_user", db_name = ?FILES_DB_NAME, version = 1}).

-define(USER_FILES_NUMBER_VIEW, #view_info{name = "user_files_number", db_name = ?FILES_DB_NAME, version = 1}).
-define(USER_FILES_SIZE_VIEW, #view_info{name = "user_files_size", db_name = ?FILES_DB_NAME, version = 1}).
-define(GROUP_FILES_NUMBER_VIEW, #view_info{name = "group_files_number", db_name = ?FILES_DB_NAME, version = 1}).

-define(COOKIES_BY_EXPIRED_BEFORE_VIEW, #view_info{name = "cookies_by_expired_before", db_name = ?COOKIES_DB_NAME, version = 1}).

%% FUSE Sessions
-define(FUSE_CONNECTIONS_VIEW, #view_info{name = "fuse_connections", db_name = ?SYSTEM_DB_NAME, version = 1}).
-define(EXPIRED_FUSE_SESSIONS_VIEW, #view_info{name = "expired_fuse_sessions", db_name = ?SYSTEM_DB_NAME, version = 1}).
-define(FUSE_SESSIONS_BY_USER_ID_VIEW, #view_info{name = "fuse_sessions_by_user_id", db_name = ?SYSTEM_DB_NAME, version = 1}).

%% Spaces
-define(SPACES_BY_GRUID_VIEW, #view_info{name = "spaces_by_gruid", db_name = ?FILES_DB_NAME, version = 1}).

%% Groups
-define(GROUP_BY_NAME_VIEW, #view_info{name = "group_by_name", db_name = ?GROUPS_DB_NAME, version = 1}).

%% List of all used databases :: [string()]
-define(DB_LIST, [?SYSTEM_DB_NAME, ?FILES_DB_NAME, ?DESCRIPTORS_DB_NAME, ?USERS_DB_NAME, ?COOKIES_DB_NAME, ?GROUPS_DB_NAME, ?AVAILABLE_BLOCKS_DB_NAME]).

%% List of all used views :: [#view_info]
-define(VIEW_LIST, [?FILE_TREE_VIEW, ?WAITING_FILES_TREE_VIEW, ?FILE_CHILDS_VIEW, ?FD_BY_FILE_VIEW, ?FD_BY_EXPIRED_BEFORE_VIEW, ?ALL_STORAGE_VIEW,
    ?FILES_BY_UID_AND_FILENAME, ?FILE_META_BY_TIMES, ?FILES_BY_META_DOC, ?FILE_LOCATIONS_BY_FILE, ?FILE_BLOCKS_BY_FILE_LOCATION, ?GROUP_BY_NAME_VIEW,
    ?USER_BY_GLOBAL_ID_VIEW, ?USER_BY_EMAIL_VIEW, ?USER_BY_LOGIN_VIEW, ?USER_BY_NAME_VIEW, ?USER_BY_DN_VIEW, ?USER_BY_UNVERIFIED_DN_VIEW, ?USER_BY_UID_VIEW,
    ?STORAGE_BY_ID_VIEW, ?SHARE_BY_FILE_VIEW, ?SHARE_BY_USER_VIEW, ?USER_FILES_NUMBER_VIEW, ?USER_FILES_SIZE_VIEW, ?GROUP_FILES_NUMBER_VIEW,
    ?FUSE_CONNECTIONS_VIEW, ?EXPIRED_FUSE_SESSIONS_VIEW, ?FUSE_SESSIONS_BY_USER_ID_VIEW, ?SPACES_BY_GRUID_VIEW, ?COOKIES_BY_EXPIRED_BEFORE_VIEW,
    ?AVAILABLE_BLOCKS_BY_FILE_ID, ?ATTR_WATCHERS_BY_EXPIRED_BEFORE_VIEW, ?ATTR_WATCHERS_BY_NAME_VIEW]).

%% Default database name
-define(DEFAULT_DB, lists:nth(1, ?DB_LIST)).


%% Do not try to read this macro (3 nested list comprehensions). All it does is:
%% Create an list containing #db_info structures base on ?DB_LIST
%% Inside every #db_info, list of #design_info is created based on views list (?VIEW_LIST)
%% Inside every #design_info, list of #view_info is created based on views list (?VIEW_LIST)
%% Such structural representation of views, makes it easier to initialize views in DBMS
%% WARNING: Do not evaluate this macro anywhere but dao:init/cleanup because it's
%% potentially slow - O(db_count * view_count^2)
-define(DATABASE_DESIGN_STRUCTURE, [#db_info{name = DbName,
                                        designs = [#design_info{name = dao_utils:get_versioned_view_name(ViewName, Vsn),
                                                views = [ViewInfo || #view_info{name = ViewName1, db_name = DbName2} = ViewInfo <- ?VIEW_LIST,
                                                    DbName2 == DbName, ViewName1 == ViewName]
                                            } || #view_info{db_name = DbName1, name = ViewName, version = Vsn} <- ?VIEW_LIST, DbName1 == DbName]
                                        } || DbName <- ?DB_LIST]).

-endif.
