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


-record(file, {
    name = undefined :: undefined | binary(),
    mode = ?DEFAULT_FILE_MODE :: file_meta:mode(),
    shares = 0 :: onenv_file_test_utils:shares_desc()
}).

-record(dir, {
    name = undefined :: undefined | binary(),
    mode = ?DEFAULT_DIR_MODE :: file_meta:mode(),
    shares = 0 :: onenv_file_test_utils:shares_desc(),
    children = [] :: [#dir{} | #file{}]
}).


-endif.
