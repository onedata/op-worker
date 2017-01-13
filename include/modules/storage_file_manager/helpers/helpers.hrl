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

%% Helpers types
-define(CEPH_HELPER_NAME, <<"ceph">>).
-define(POSIX_HELPER_NAME, <<"posix">>).
-define(PROXY_HELPER_NAME, <<"proxy">>).
-define(S3_HELPER_NAME, <<"s3">>).
-define(SWIFT_HELPER_NAME, <<"swift">>).

-record(helper, {
    name :: helper:name(),
    args = #{} :: helper:args(),
    admin_ctx = #{} :: helper:user_ctx(),
    insecure = false :: boolean()
}).

-endif.