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

-endif.