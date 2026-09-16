%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Vocabulary for declaring the expected distribution of a file's blocks among
%%% providers. The assertion itself is file_test_utils:await_distribution/5 -
%%% these macros only build its expectation and name the arguments in the order
%%% the suites using them read best.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DISTRIBUTION_ASSERT_HRL).
-define(DISTRIBUTION_ASSERT_HRL, 1).

%% Expected distribution of a single provider: the size it holds, or its exact
%% blocks (as [[Offset, Size]], the shape the product reports them in).
-define(DIST(__ProviderId, __SizeOrBlocks), [{__ProviderId, __SizeOrBlocks}]).

-define(DISTS(ProviderIds, Sizes), lists:zip(ProviderIds, Sizes)).

-define(assertDistribution(Worker, SessionId, ExpectedDistribution, FileGuid, Attempts),
    file_test_utils:await_distribution(
        Worker, SessionId, FileGuid, ExpectedDistribution, Attempts
    )).


-endif.