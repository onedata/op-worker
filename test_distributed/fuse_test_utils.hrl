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
