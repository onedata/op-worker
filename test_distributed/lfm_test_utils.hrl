%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------

-ifndef(LFM_TEST_UTILS_HRL).
-define(LFM_TEST_UTILS_HRL, 1).

-include_lib("ctool/include/test/test_utils.hrl").

-define(SESS_ID(User, Worker, Config),
    ?config({session_id, {User, ?GET_DOMAIN(Worker)}}, Config)).

-endif.