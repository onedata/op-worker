%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: dao_vfs header
%% @end
%% ===================================================================

-ifndef(DAO_VFS_HRL).
-define(DAO_VFS_HRL, 1).

-include_lib("files_common.hrl").

%% Files' location (storage helper id and its relative file ID). Designed for use within #file record (`location` filed).
-record(file_location, {storage_id = "", file_id = ""}).
%% Files' locks. Designed for use within #file record (`locks` field).
-record(file_lock, {type = ?REG_TYPE, uid = "", sid = "", pid = 0, offset = 0, size = 0}).
%% Veil File
-record(file, {type = 1, name = "", size = 0, uid = "", gids = [], perms = 0, parent = "", ref_file = "", location = #file_location{}, locks = []}).

%% Those record contains meta data for file which UUID match #file_meta.file field
-record(file_tag, {key = "", value = []}).
-record(file_meta, {file = "", tags = [], mime_type = "", last_modified = 0, description = ""}).

%% This record describes state of file for each user. #file_descriptor.file is an UUID of #file.
%% For regular files can be used to mark file as used in order to block e.g. physical file transfer
%% For dirs it should be used as FUSE's readdir state. Cursor says how many dir has been read by now
-record(file_descriptor, {file = "", mode = 0, fuse_id = "", create_time = "", validity_time = 60*15, cursor = 0}).

%% This record containg info about storage helper instance. i.e. its name and initialization arguments
-record(storage_helper_info, {name = "", init_args = []}).

%% FUSE specific storage_helper config
-record(fuse_group_info, {name = "", storage_helper = #storage_helper_info{}}).

%% This record contains information about storage (ways to access it)
-record(storage_info, {id = 0, name = "", last_update = 0, default_storage_helper = #storage_helper_info{}, fuse_groups = []}).

-endif.