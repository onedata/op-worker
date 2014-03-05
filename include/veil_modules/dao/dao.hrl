%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: dao header
%% @end
%% ===================================================================

-ifndef(DAO_HRL).
-define(DAO_HRL, 1).

-include_lib("records.hrl").
-include_lib("veil_modules/dao/dao_vfs.hrl").
-include_lib("veil_modules/dao/dao_users.hrl").
-include_lib("veil_modules/dao/dao_share.hrl").
-include_lib("veil_modules/dao/dao_cluster.hrl").
-include_lib("veil_modules/dao/common.hrl").

%% record definition used in record registration example
-record(some_record, {field1 = "", field2 = "", field3 = ""}).

%% Helper macro. See macro ?dao_record_info/1 for more details.
-define(record_info_gen(X), {record_info(size, X), record_info(fields, X), #X{}}).

%% Every record that will be saved to DB have to be "registered" with this define.
%% Each registered record should be listed in defined below 'case' block as fallow:
%% record_name -> ?record_info_gen(record_name);
%% where 'record_name' is the name of the record. 'some_record' is an example.
-define(dao_record_info(R),
    case R of
        some_record         -> ?record_info_gen(some_record);
        cm_state            -> ?record_info_gen(cm_state);
        host_state          -> ?record_info_gen(host_state);
        node_state          -> ?record_info_gen(node_state);
        file                -> ?record_info_gen(file);
        file_location       -> ?record_info_gen(file_location);
        file_descriptor     -> ?record_info_gen(file_descriptor);
        file_meta           -> ?record_info_gen(file_meta);
        file_lock           -> ?record_info_gen(file_lock);
        storage_info        -> ?record_info_gen(storage_info);
        user                -> ?record_info_gen(user);
        share_desc          -> ?record_info_gen(share_desc);
        fuse_session        -> ?record_info_gen(fuse_session);
        connection_info     -> ?record_info_gen(connection_info);
        quota               -> ?record_info_gen(quota);
        %next_record        -> ?record_info_gen(next_record);
        _ -> {error, unsupported_record}
    end).


%% Record-wrapper for regular records that needs to be saved in DB. Adds UUID and Revision info to each record.
%% `uuid` is document UUID, `rev_info` is documents' current revision number
%% `record` is an record representing this document (its data), `force_update` is a flag
%% that forces dao:save_record/1 to update this document even if rev_info isn't valid or up to date.
-record(veil_document, {uuid = "", rev_info = 0, record = none, force_update = false}).

%% Those records represent view result Each #view_resault contains list of #view_row.
%% If the view has been queried with `include_docs` option, #view_row.doc will contain #veil_document, therefore
%% #view_row.id == #view_row.doc#veil_document.uuid. Unfortunately wrapping demanded record in #veil_document is
%% necessary because we may need revision info for that document.
-record(view_row, {id = "", key = "", value = 0, doc = none}).
-record(view_result, {total = 0, offset = 0, rows = []}).

%% These records allows representing databases, design documents and their views.
%% Used in DAO initial configuration in order to easily setup/update views in database.
-record(db_info, {name = "", designs = []}).
-record(design_info, {name = "", version = 0, views = []}).
-record(view_info, {name = "", design = "", db_name = ""}).

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

%% Design Names
-define(VFS_BASE_DESIGN_NAME, "vfs_base").
-define(WAITING_FILES_DESIGN_NAME, "waiting_files_tree").
-define(USER_BY_LOGIN_DESIGN_NAME, "user_by_login").
-define(USER_BY_EMAIL_DESIGN_NAME, "user_by_email").
-define(USER_BY_DN_DESIGN_NAME, "user_by_dn").
-define(USER_BY_UID_DESIGN_NAME, "user_by_uid").
-define(SHARE_BY_FILE_DESIGN_NAME, "share_by_file").
-define(SHARE_BY_USER_DESIGN_NAME, "share_by_user").
-define(FILES_NUMBER_DESIGN_NAME, "files_number").
-define(FILES_SIZE_DESIGN_NAME, "files_size").
-define(FUSE_SESSIONS_DESIGN_NAME, "fuse_sessions").

%% Views
-define(FILE_TREE_VIEW, #view_info{name = "file_tree", design = ?VFS_BASE_DESIGN_NAME, db_name = ?FILES_DB_NAME}).
-define(WAITING_FILES_TREE_VIEW, #view_info{name = "waiting_files_tree", design = ?WAITING_FILES_DESIGN_NAME, db_name = ?FILES_DB_NAME}).
-define(FILE_SUBDIRS_VIEW, #view_info{name = "file_subdirs", design = ?VFS_BASE_DESIGN_NAME, db_name = ?FILES_DB_NAME}).
-define(FD_BY_FILE_VIEW, #view_info{name = "fd_by_name", design = ?VFS_BASE_DESIGN_NAME, db_name = ?DESCRIPTORS_DB_NAME}).
-define(FD_BY_EXPIRED_BEFORE_VIEW, #view_info{name = "fd_by_expired_before", design = ?VFS_BASE_DESIGN_NAME, db_name = ?DESCRIPTORS_DB_NAME}).
-define(ALL_STORAGE_VIEW, #view_info{name = "all_storage", design = ?VFS_BASE_DESIGN_NAME, db_name = ?SYSTEM_DB_NAME}).
-define(STORAGE_BY_ID_VIEW, #view_info{name = "storage_by_id", design = ?VFS_BASE_DESIGN_NAME, db_name = ?SYSTEM_DB_NAME}).
-define(FILES_BY_UID_AND_FILENAME, #view_info{name = "files_by_uid_and_filename", design = ?VFS_BASE_DESIGN_NAME, db_name = ?FILES_DB_NAME}).
-define(FILE_META_BY_TIMES, #view_info{name = "file_meta_by_times", design = ?VFS_BASE_DESIGN_NAME, db_name = ?FILES_DB_NAME}).
-define(FILES_BY_META_DOC, #view_info{name = "files_by_meta_doc", design = ?VFS_BASE_DESIGN_NAME, db_name = ?FILES_DB_NAME}).

-define(USER_BY_EMAIL_VIEW, #view_info{name = "user_by_email", design = ?USER_BY_EMAIL_DESIGN_NAME, db_name = ?USERS_DB_NAME}).
-define(USER_BY_LOGIN_VIEW, #view_info{name = "user_by_login", design = ?USER_BY_LOGIN_DESIGN_NAME, db_name = ?USERS_DB_NAME}).
-define(USER_BY_DN_VIEW, #view_info{name = "user_by_dn", design = ?USER_BY_DN_DESIGN_NAME, db_name = ?USERS_DB_NAME}).
-define(USER_BY_UID_VIEW, #view_info{name = "user_by_uid", design = ?USER_BY_UID_DESIGN_NAME, db_name = ?USERS_DB_NAME}).

-define(SHARE_BY_FILE_VIEW, #view_info{name = "share_by_file", design = ?SHARE_BY_FILE_DESIGN_NAME, db_name = ?FILES_DB_NAME}).
-define(SHARE_BY_USER_VIEW, #view_info{name = "share_by_user", design = ?SHARE_BY_USER_DESIGN_NAME, db_name = ?FILES_DB_NAME}).

-define(USER_FILES_NUMBER_VIEW, #view_info{name = "user_files_number", design = ?FILES_NUMBER_DESIGN_NAME, db_name = ?FILES_DB_NAME}).
-define(USER_FILES_SIZE_VIEW, #view_info{name = "user_files_size", design = ?FILES_SIZE_DESIGN_NAME, db_name = ?FILES_DB_NAME}).
-define(GROUP_FILES_NUMBER_VIEW, #view_info{name = "group_files_number", design = ?FILES_NUMBER_DESIGN_NAME, db_name = ?FILES_DB_NAME}).

%% FUSE Sessions
-define(FUSE_CONNECTIONS_VIEW, #view_info{name = "fuse_connections", design = ?FUSE_SESSIONS_DESIGN_NAME, db_name = ?SYSTEM_DB_NAME}).
-define(EXPIRED_FUSE_SESSIONS_VIEW, #view_info{name = "expired_fuse_sessions", design = ?FUSE_SESSIONS_DESIGN_NAME, db_name = ?SYSTEM_DB_NAME}).

%% Others
-define(RECORD_INSTANCES_DOC_PREFIX, "record_instances_").
-define(RECORD_FIELD_BINARY_PREFIX, "__bin__: ").
-define(RECORD_FIELD_ATOM_PREFIX, "__atom__: ").
-define(RECORD_FIELD_PID_PREFIX, "__pid__: ").
-define(RECORD_TUPLE_FIELD_NAME_PREFIX, "tuple_field_").
-define(RECORD_META_FIELD_NAME, "record__").

%% List of all used databases :: [string()]
-define(DB_LIST, [?SYSTEM_DB_NAME, ?FILES_DB_NAME, ?DESCRIPTORS_DB_NAME, ?USERS_DB_NAME]).
%% List of all used views :: [#view_info]
-define(VIEW_LIST, [?FILE_TREE_VIEW, ?WAITING_FILES_TREE_VIEW, ?FILE_SUBDIRS_VIEW, ?FD_BY_FILE_VIEW, ?FD_BY_EXPIRED_BEFORE_VIEW, ?ALL_STORAGE_VIEW,
                    ?FILES_BY_UID_AND_FILENAME, ?FILE_META_BY_TIMES, ?FILES_BY_META_DOC,
                    ?USER_BY_EMAIL_VIEW, ?USER_BY_LOGIN_VIEW, ?USER_BY_DN_VIEW, ?USER_BY_UID_VIEW, ?STORAGE_BY_ID_VIEW,
                    ?SHARE_BY_FILE_VIEW, ?SHARE_BY_USER_VIEW, ?USER_FILES_NUMBER_VIEW, ?USER_FILES_SIZE_VIEW, ?GROUP_FILES_NUMBER_VIEW,
                    ?FUSE_CONNECTIONS_VIEW, ?EXPIRED_FUSE_SESSIONS_VIEW]).
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
                                        designs = [#design_info{name = DesignName,
                                                views = [ViewInfo || #view_info{design = Design, db_name = DbName2} = ViewInfo <- ?VIEW_LIST,
                                                    Design == DesignName, DbName2 == DbName]
                                            } || #view_info{db_name = DbName1, design = DesignName} <- ?VIEW_LIST, DbName1 == DbName]
                                        } || DbName <- ?DB_LIST]).
-endif.