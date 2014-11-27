%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc dao_vfs header
%% @end
%% ===================================================================

-ifndef(DAO_VFS_HRL).
-define(DAO_VFS_HRL, 1).

-include_lib("files_common.hrl").
-include("oneprovider_modules/dao/dao_spaces.hrl").

%% The 'infinity' block size is actually an INT64_MAX
-define(FILE_BLOCK_SIZE_INF, 16#FFFFFFFFFFFFFFFF).

%% Available blocks of the file per file location
-record(file_block, {file_location_id = "", offset = 0, size = 0}).
%% Files' location (storage helper id and its relative file ID). Designed for use within #file record (`location` filed).
-record(file_location, {file_id = "", storage_uuid = "", storage_file_id = ""}).
% File remote location informs about global location of file (what parts do each provider have), it is defined as a list of remote_file_part
-record(available_blocks, {file_id = "", provider_id = "", file_size = {0, 0}, file_parts = []}).
%% Files' locks. Designed for use within #file record (`locks` field).
-record(file_lock, {type = ?REG_TYPE, uid = "", sid = "", pid = 0, offset = 0, size = 0}).
%% onedata file
-record(file, {
    type = 1, name = "", uid = "", perms = 0, parent = "", ref_file = "",
    locks = [], meta_doc, created = true,
    extensions = [] %% General use field for extending #file{} capabilities. Shall have fallowing format: [{ExtName :: atom(), ExtValue :: term()}]
}).

%% Those record contains meta data for file which UUID match #file_meta.file field
-record(file_tag, {key = "", value = []}).
-record(file_meta, {uid = "", tags = [], size = 0, ctime = 0, atime = 0, mtime = 0, description = "", xattrs = [], acl = []}).

%% This type represents time relation, useful in time_criteria record
-type time_relation() :: older_than | newer_than | the_same.

%% This record desribes single time criteria, it consists of time_relation and time to compare to.
-record(time_criteria, {time_relation :: time_relation(), time = 0}).

%% This record describes criteria for searching files.
%% It should be considered as a subject to change, in future more options will be added.
-record(file_criteria,
  {file_pattern = "",
   uid = null,
   ctime = #time_criteria{},
   mtime = #time_criteria{},
   include_files = true :: boolean(),
   include_dirs = false :: boolean(),
   include_links = false :: boolean()}).

%% This record describes state of file for each user. #file_descriptor.file is an UUID of #file.
%% For regular files can be used to mark file as used in order to block e.g. physical file transfer
%% For dirs it should be used as FUSE's readdir state. Cursor says how many dir has been read by now
-record(file_descriptor, {file = "", mode = 0, fuse_id = "", create_time = "", validity_time = 60*15, cursor = 0}).

-record(file_attr_watcher, {file, fuse_id, create_time, validity_time = timer:minutes(5)}).

%% This record containg info about storage helper instance. i.e. its name and initialization arguments
-record(storage_helper_info, {name = "", init_args = []}).

%% FUSE specific storage_helper config
-record(fuse_group_info, {name = "", storage_helper = #storage_helper_info{}}).

%% This record contains information about storage (ways to access it)
-record(storage_info, {id = 0, name = "", last_update = 0, default_storage_helper = #storage_helper_info{}, fuse_groups = []}).

-endif.
