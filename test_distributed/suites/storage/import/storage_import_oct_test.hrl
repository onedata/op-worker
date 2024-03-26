%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros used in tests of storage import.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(STORAGE_IMPORT_OCT_TEST_HRL).
-define(STORAGE_IMPORT_OCT_TEST_HRL, 1).


-include("onenv_test_utils.hrl").
-include("space_setup_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-record(storage_import_test_suite_ctx, {
    storage_type :: posix,
    importing_provider_selector :: oct_background:entity_selector(),
    other_provider_selector :: oct_background:entity_selector(),
    space_owner_selector :: oct_background:entity_selector(),
    other_space_member_selector :: oct_background:entity_selector()
}).


-endif.