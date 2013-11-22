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
  gname = []
}).

% Callbacks management
-record(callback, {fuse = 0, pid = 0, node = non, action = non}).

-define(REMOTE_HELPER_SEPARATOR, "///").
-define(CLUSTER_USER_ID, cluster_uid).
-define(CLUSTER_FUSE_ID, "cluster_fid").