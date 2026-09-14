%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Definitions of records and macros used by helpers modules.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(HELPERS_HRL).
-define(HELPERS_HRL, 1).

%% File attributes returned by storage helpers - equivalent of stat structure
-record(statbuf, {
    st_dev, st_ino, st_mode, st_nlink, st_uid,
    st_gid, st_rdev, st_size, st_atime, st_mtime,
    st_ctime, st_blksize, st_blocks
}).

%% Helper types
-define(CEPH_HELPER_NAME, <<"ceph">>).
-define(CEPHRADOS_HELPER_NAME, <<"cephrados">>).
-define(POSIX_HELPER_NAME, <<"posix">>).
-define(PROXY_HELPER_NAME, <<"proxy">>).
-define(S3_HELPER_NAME, <<"s3">>).
-define(SWIFT_HELPER_NAME, <<"swift">>).
-define(GLUSTERFS_HELPER_NAME, <<"glusterfs">>).
-define(WEBDAV_HELPER_NAME, <<"webdav">>).
-define(XROOTD_HELPER_NAME, <<"xrootd">>).
-define(NFS_HELPER_NAME, <<"nfs">>).
-define(HTTP_HELPER_NAME, <<"http">>).
-define(NULL_DEVICE_HELPER_NAME, <<"nulldevice">>).

%% NOTE: which storage types are posix compatible, are object storages or
%% support importing data is defined in the storage_type module.

%% Storage path types
-define(CANONICAL_STORAGE_PATH, <<"canonical">>).
-define(FLAT_STORAGE_PATH, <<"flat">>).

% storage access types
-define(READWRITE, readwrite).
-define(READONLY, readonly).

%% This type determines the filename and path generation
%% on the storage. Currently 2 modes are supported:
%% - 'canonical' - posix-style
%% - 'flat' - based on UUID
-type storage_path_type() :: binary().
-export_type([storage_path_type/0]).

%% NOTE: #helper_spec{} is defined in datastore_models.hrl - it is persisted
%% as part of the storage_config model, so changing it requires bumping that
%% model's record struct.

-define(CONFIDENTIAL_MASK, <<"*****">>).


-endif.
