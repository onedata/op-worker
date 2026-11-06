%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions for view (db index) tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(VIEW_TEST_HRL).
-define(VIEW_TEST_HRL, 1).

% for awaiting the dbsync of view definitions and the emissions they produce - the
% view doc rides the space changes stream behind whatever the preceding test cases
% wrote, so the lag is not proportional to the awaiting case's own file count; on
% top of that, a freshly defined view has its index built lazily, on first query
-define(VIEW_SYNC_ATTEMPTS, 180).

-endif.
