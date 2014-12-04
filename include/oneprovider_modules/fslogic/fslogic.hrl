%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: fslogic main header
%% @end
%% ===================================================================

-ifndef(FSLOGIC_HRL).
-define(FSLOGIC_HRL, 1).

-include("oneprovider_modules/fslogic/fslogic_types.hrl").

%% POSIX error names
-define(VOK,        "ok").       %% Everything is just great
-define(VENOENT,    "enoent").   %% File not found
-define(VEACCES,    "eacces").   %% User doesn't have access to requested resource (e.g. file)
-define(VEEXIST,    "eexist").   %% Given file already exist
%% Cluster should not use EIO error. It's resaved for client side errors. Use VEREMOTEIO instead.
%-define(VEIO,       "eio").      %% Input/output error - default error code for unknown errors
-define(VENOTSUP, 	"enotsup").  %% Operation not supported
-define(VENOTEMPTY, "enotempty").%% Directory is not empty
-define(VEREMOTEIO, "eremoteio").%% Remote I/O error
-define(VEPERM,     "eperm").    %% Operation not permitted
-define(VEINVAL,    "einval").   %% Invalid argument
-define(VEDQUOT,    "edquot").   %% Quota exceeded
-define(VENOATTR,   "enoattr").  %% The named attribute does not exist, or the process has no access to this attribute.
-define(VECOMM,     "ecomm").    %% Communication error (unknown user's token, unable to communicate on his behalf)


%% @todo: add test that verifies if the macro contains all available error code
-define(ALL_ERROR_CODES, [?VOK, ?VENOENT, ?VEACCES, ?VEEXIST, ?VENOTSUP, ?VENOTEMPTY, ?VEREMOTEIO,
                          ?VEPERM, ?VEINVAL, ?VEDQUOT, ?VECOMM, ?VENOATTR]).



%% POSIX & FUSE C structures definitions ported to erlang. For documentation please refer linux & fuse man pages.
%% Names of these records are the same as correspondent C struct, except "st_" prefix.
-record(st_statvfs, {f_bsize = 0, f_frsize = 0, f_blocks = 0, f_bfree = 0, f_bavail = 0, f_files = 0, f_ffree = 0, f_favail = 0, f_fsid = 0, f_flag = 0, f_namemax = 0}).
-record(st_stat, {st_dev = 0, st_ino = 0, st_mode = 0, st_nlink = 0, st_uid = 0, st_gid = 0, st_rdev = 0, st_size = 0, st_blksize = 0, st_blocks = 0, st_atime = 0, st_mtime = 0, st_ctime = 0}).
-record(st_fuse_file_info, {flags = 0, fh_old = 0, writepage = 0, direct_io = 0, keep_cache = 0, flush = 0, nonseekable = 0, padding = 0, fh = 0, lock_owner = 0}).

% File attributes
-record(fileattributes, {
  mode = 0,
  uid = 0,
  gid = 0,
  atime = 0,
  mtime = 0,
  ctime = 0,
  type = 0,
  size = 0,
  uname = [],
  gname = [],
  links = 0,
  has_acl = false
}).

%% Directory entry: name :: string(), type :: file_type_protocol().
%% This is an logical_files_manager's abstraction for similar ClientProtocol's message.
-record(dir_entry, {name = "", type = ""}).

% Callbacks management
-record(callback, {fuse = 0, pid = 0, node = non, action = non}).

-define(CLUSTER_USER_ID, "cluster_uid").
-define(CLUSTER_FUSE_ID, "cluster_fid").

%% Name of direcotry that contains all group dirs
-define(SPACES_BASE_DIR_NAME, "spaces").

%% burst size for listing
-define(DAO_LIST_BURST_SIZE,100).


-define(LOCATION_VALIDITY, 60*15).

-define(FILE_COUNTING_BASE, 256).

%% Which fuse operations (messages) are allowed to operate on base group directory ("/groups")
-define(GROUPS_BASE_ALLOWED_ACTIONS,    [getfileattr, getfileuuid, updatetimes, getfilechildren, getfilechildrencount, getacl, getxattr, listxattr, checkfileperms, attrunsubscribe]).

%% Which fuse operations (messages) are allowed to operate on second level group directory (e.g. "/groups/grpName")
-define(GROUPS_ALLOWED_ACTIONS,         [getfileattr, getfileuuid, getnewfilelocation, createdir, updatetimes, createlink, getfilechildren, getfilechildrencount, getacl, getxattr, listxattr, checkfileperms, attrunsubscribe]).

% File open modes (open flags)
-define(UNSPECIFIED_MODE,"").
-define(READ_MODE,"read").
-define(WRITE_MODE,"write").
-define(RDWR_MODE,"rdwr").

%% Storage test file prefix for testing client;s storage availability
-define(STORAGE_TEST_FILE_PREFIX, "storage_test_").

%% Maximum time (in ms) after which document conflict resolution shall occur
-define(MAX_SLEEP_TIME_CONFLICT_RESOLUTION, 100).

%% Default permissions for space directory (i.e. /spaces/SpaceName)
-define(SpaceDirPerm, 8#1770).

%% Persmission cache definitions
-define(CACHE_TREE_MAX_DEPTH, 6).
-define(CACHE_TREE_MAX_WIDTH, 10).
-define(CACHE_REQUEST_TIMEOUT, 60000).

%% default buffer size for copy operation
-define(default_copy_buffer_size, 1048576).

%% Name of fslogic's state for file attributes events
-define(fslogic_attr_events_state, fslogic_attr_events_state).

-endif.