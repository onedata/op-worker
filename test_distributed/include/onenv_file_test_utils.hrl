%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions of macros and records used in file tests.
%%% @end
%%%-------------------------------------------------------------------
-author("Bartosz Walkowicz").

-ifndef(FILE_TEST_UTILS_HRL).
-define(FILE_TEST_UTILS_HRL, 1).


-include("modules/fslogic/fslogic_common.hrl").


-record(share_spec, {
    name = <<"share">> :: binary(),
    description = <<>> :: binary()
}).

-record(file_spec, {
    name = undefined :: undefined | binary(),
    mode = ?DEFAULT_FILE_MODE :: file_meta:mode(),
    shares = [] :: [onenv_file_test_utils:share_spec()],
    content = <<"">> :: binary()
}).

-record(dir_spec, {
    name = undefined :: undefined | binary(),
    mode = ?DEFAULT_DIR_MODE :: file_meta:mode(),
    shares = [] :: [onenv_file_test_utils:share_spec()],
    children = [] :: [#dir_spec{} | #file_spec{}]
}).

-record(symlink_spec, {
    name = undefined :: undefined | binary(),
    shares = [] :: [onenv_file_test_utils:share_spec()],
    symlink_value :: binary()
}).

-record(object, {
    guid :: file_id:file_guid(),
    name :: binary(),
    type :: file_meta:type(),
    mode :: file_meta:mode(),
    shares :: [od_share:id()],
    content = undefined :: undefined | binary(),  % set only for files
    children = undefined :: undefined | [onenv_file_test_utils:object()],  % set only for dirs
    symlink_value = undefined :: undefined | file_meta_symlinks:symlink()  % set only for symlinks
}).


-endif.
