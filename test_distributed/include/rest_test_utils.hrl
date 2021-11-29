%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used in tests using
%%% op_worker REST API.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(REST_TEST_UTILS_HRL).
-define(REST_TEST_UTILS_HRL, 1).

-include_lib("ctool/include/test/test_utils.hrl").

-define(USER_TOKEN_HEADER(Config, User),
    rest_test_utils:user_token_header(?config({access_token, <<"user1">>}, Config))
).

-define(USER_AUTH_HEADERS(Config, User), ?USER_AUTH_HEADERS(Config, User, [])).
-define(USER_AUTH_HEADERS(Config, User, OtherHeaders),
    [?USER_TOKEN_HEADER(Config, User) | OtherHeaders]
).

-endif.
