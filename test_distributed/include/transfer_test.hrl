%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions of records used in transfer (replication, replica eviction
%%% and replica migration) tests.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(TRANSFER_TEST_HRL).
-define(TRANSFER_TEST_HRL, 1).


-record(transfer_test_suite_ctx, {
    transfer_type :: transfer_test_utils:transfer_type(),
    space_selector :: oct_background:entity_selector(),
    user_selector :: oct_background:entity_selector(),
    % provider on which file trees are created (holds the initial replica)
    creation_provider_selector :: oct_background:entity_selector(),
    % the second provider supporting the space - depending on the transfer
    % type it is the replication target or the eviction counterpart
    other_provider_selector :: oct_background:entity_selector()
}).


-define(ATTEMPTS, 60).
% for awaiting the dbsync of view definitions and the emissions they produce - the
% view doc rides the space changes stream behind whatever the preceding test cases
% wrote, so the lag is not proportional to the awaiting case's own file count
-define(VIEW_SYNC_ATTEMPTS, 180).
% for transfers that take long to process (file trees with many nodes or a lot of data)
-define(LARGE_TRANSFER_ATTEMPTS, 120).
% for tests processing many files across one or many transfers
-define(SCALE_TRANSFER_ATTEMPTS, 600).

-define(RAND_CONTENT_MAX_SIZE, 1000).
-define(RAND_CONTENT(), ?RAND_CONTENT(rand:uniform(?RAND_CONTENT_MAX_SIZE))).
-define(RAND_CONTENT(__SIZE), crypto:strong_rand_bytes(__SIZE)).


-endif.
