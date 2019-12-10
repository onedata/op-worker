%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header defines common macros used in fuse client tests.
%%% @end
%%%-------------------------------------------------------------------
-author("Michal Stanisz").

-define(TIMEOUT, timer:minutes(1)).
-define(MACAROON, <<"DUMMY-MACAROON">>).
-define(DISCH_MACAROONS, [<<"DM1">>, <<"DM2">>]).
-define(MACAROON2, <<"DUMMY-MACAROON2">>).
-define(DISCH_MACAROONS2, [<<"DM3">>, <<"DM4">>]).
-define(TEST_MACAROONS, [{?MACAROON, ?DISCH_MACAROONS}, {?MACAROON2, ?DISCH_MACAROONS2}]).
