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
-define(HTTP_HELPER_NAME, <<"http">>).
-define(NULL_DEVICE_HELPER_NAME, <<"nulldevice">>).

-define(POSIX_COMPATIBLE_HELPERS, [?POSIX_HELPER_NAME, ?GLUSTERFS_HELPER_NAME, ?NULL_DEVICE_HELPER_NAME]).

-define(OBJECT_HELPERS, [?SWIFT_HELPER_NAME, ?S3_HELPER_NAME, ?CEPHRADOS_HELPER_NAME, ?CEPH_HELPER_NAME]).
-define(AUTO_IMPORT_HELPERS, [
    ?POSIX_HELPER_NAME,
    ?GLUSTERFS_HELPER_NAME,
    ?NULL_DEVICE_HELPER_NAME,
    ?WEBDAV_HELPER_NAME,
    ?XROOTD_HELPER_NAME
] ++ ?AUTO_IMPORT_OBJECT_HELPERS).

-define(AUTO_IMPORT_OBJECT_HELPERS, [
    ?S3_HELPER_NAME
]).

%% Storage path types
-define(CANONICAL_STORAGE_PATH, <<"canonical">>).
-define(FLAT_STORAGE_PATH, <<"flat">>).

-define(DEFAULT_HELPER_TIMEOUT, 120000).

%% This type determines the filename and path generation
%% on the storage. Currently 2 modes are supported:
%% - 'canonical' - posix-style
%% - 'flat' - based on UUID
-type storage_path_type() :: binary().
-export_type([storage_path_type/0]).

-record(helper, {
    name :: helper:name(),
    args = #{} :: helper:args(),
    admin_ctx = #{} :: helper:user_ctx()
}).

-endif.
