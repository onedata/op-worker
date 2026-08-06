%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions shared by the lfm test suites (file_lfm_posix_test_SUITE and
%%% file_lfm_s3_test_SUITE), which hold only the wiring - the test bodies live
%%% in the per-topic file_lfm_*_tests modules and are dispatched to by the
%%% ?RUN_*_TEST macros below.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FILE_LFM_TEST_HRL).
-define(FILE_LFM_TEST_HRL, 1).

-include_lib("ctool/include/test/test_utils.hrl").

% Both scenarios the family runs on (1op, 1op_s3) deploy a single provider
% supporting a single space under these names.
-define(PROVIDER_SELECTOR, krakow).
-define(SPACE_SELECTOR, space_krk).

% Only the 1op scenario deploys a second space, so this may be used solely by
% tests wired into file_lfm_posix_test_SUITE.
-define(OTHER_SPACE_SELECTOR, space1).

% NOTE: neither acting user may be the space owner. The owner omits ALL file
% permission checks (data_access_control:assert_access_granted_for_space_owner/3),
% which would not fail a test outright - it would silently turn every EACCES
% assertion into a no-op. In 1op.yaml the owner is user1, so the family acts as
% plain space members instead.
-define(USER_SELECTOR, user2).
-define(OTHER_USER_SELECTOR, user3).

-define(RUN_TEST(__TESTS_MODULE), ?RUN_TEST(__TESTS_MODULE, [])).

% __ARGS are passed on to the test body, for the few tests whose expectations
% differ between the storage types the suites run on.
-define(RUN_TEST(__TESTS_MODULE, __ARGS),
    try
        erlang:apply(__TESTS_MODULE, ?FUNCTION_NAME, __ARGS)
    catch __TYPE:__REASON:__STACKTRACE ->
        ?ct_pal_exception("Test failed due to", __TYPE, __REASON, __STACKTRACE),
        error(test_failed)
    end
).

-define(RUN_LISTING_TEST(), ?RUN_TEST(file_lfm_listing_tests)).
-define(RUN_CRUD_TEST(), ?RUN_TEST(file_lfm_crud_tests)).
-define(RUN_COPY_TEST(), ?RUN_TEST(file_lfm_copy_tests)).
-define(RUN_STORAGE_TEST(), ?RUN_TEST(file_lfm_storage_tests)).
-define(RUN_STORAGE_TEST(__ARGS), ?RUN_TEST(file_lfm_storage_tests, __ARGS)).
-define(RUN_HANDLES_TEST(), ?RUN_TEST(file_lfm_handles_tests)).
-define(RUN_SHARES_TEST(), ?RUN_TEST(file_lfm_shares_tests)).

-endif.
