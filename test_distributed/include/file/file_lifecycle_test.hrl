%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions shared by file_lifecycle_races_test_SUITE, which holds only the wiring,
%%% and the per-aspect file_*_tests modules holding the test bodies, which the
%%% ?RUN_*_TEST macros below dispatch to.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FILE_LIFECYCLE_TEST_HRL).
-define(FILE_LIFECYCLE_TEST_HRL, 1).

-include_lib("ctool/include/test/test_utils.hrl").

-define(PROVIDER_SELECTOR, krakow).

% NOTE: unlike in most suites, the acting user is deliberately the owner of the
% space - every case creates a space of its own with this user as its owner. The
% owner omits all file permission checks, which is harmless here, as no test in
% this family asserts anything about permissions.
-define(USER_SELECTOR, user1).

% Id of the space set up for the currently running test case.
-define(SPACE_ID(__CONFIG), ?config(space_id, __CONFIG)).

-define(TIMEOUT, timer:seconds(30)).

% Used inside the mocks to suspend the operation being tested until the test
% process lets it through. Every mock must first make sure the call concerns the
% file under test - the deployment is shared, so suspending calls indiscriminately
% would stall whatever else runs on the provider at that moment.
-define(SUSPEND_UNTIL_RESUMED(__MASTER), begin
    __MASTER ! {suspended, self()},
    ok = receive
        resume -> ok
    after ?TIMEOUT ->
        timeout
    end
end).

-define(RUN_TEST(__TESTS_MODULE, __CONFIG),
    try
        erlang:apply(__TESTS_MODULE, ?FUNCTION_NAME, [__CONFIG])
    catch __TYPE:__REASON:__STACKTRACE ->
        ?ct_pal_exception("Test failed due to", __TYPE, __REASON, __STACKTRACE),
        error(test_failed)
    end
).

-define(RUN_CREATION_TEST(__CONFIG), ?RUN_TEST(file_creation_tests, __CONFIG)).
-define(RUN_HANDLES_TEST(__CONFIG), ?RUN_TEST(file_handles_tests, __CONFIG)).
-define(RUN_DELETION_TEST(__CONFIG), ?RUN_TEST(file_deletion_tests, __CONFIG)).

-endif.
