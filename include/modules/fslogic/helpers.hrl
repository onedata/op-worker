%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Public definitions of records / settings used by helpers and helpers_nif modules.
%%% @end
%%%-------------------------------------------------------------------
-author("Rafal Slota").

-ifndef(HELPERS_HRL).
-define(HELPERS_HRL, 1).

%% File attributes returned by storage helpers
%% Eqiv of standard POSIX 'struct stat'
-record(statbuf, {
    st_dev, st_ino, st_mode, st_nlink, st_uid,
    st_gid, st_rdev, st_size, st_atime, st_mtime,
    st_ctime, st_blksize, st_blocks
}).


%% Names of helpers
-define(CEPH_HELPER_NAME, <<"Ceph">>).
-define(DIRECTIO_HELPER_NAME, <<"DirectIO">>).
-define(S3_HELPER_NAME, <<"AmazonS3">>).
-define(SWIFT_HELPER_NAME, <<"Swift">>).

%% Record holding user's identity that may be used on Ceph storage system
-record(ceph_user_ctx, {
    user_name :: binary(),
    user_key :: binary()
}).

%% Record holding user's identity that may be used on POSIX compliant systems
-record(posix_user_ctx, {
    uid :: non_neg_integer(),
    gid :: non_neg_integer()
}).

%% Record holding user's identity that may be used on Amazon S3 storage system
-record(s3_user_ctx, {
    access_key :: binary(),
    secret_key :: binary()
}).

%% Record holding user's identity that may be used on Openstack Swift
%% storage system
-record(swift_user_ctx, {
    user_name :: binary(),
    password :: binary()
}).

-endif.