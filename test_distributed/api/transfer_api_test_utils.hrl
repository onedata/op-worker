%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions of macros and records used in transfer API tests.
%%% @end
%%%-------------------------------------------------------------------
-author("Bartosz Walkowicz").

-ifndef(TRANSFER_API_TEST_UTILS_HRL).
-define(TRANSFER_API_TEST_UTILS_HRL, 1).

-include("api_test_runner.hrl").


-define(CLIENT_SPEC_FOR_TRANSFER_SCENARIOS(__CONFIG), #client_spec{
    correct = [?USER_IN_BOTH_SPACES_AUTH],
    unauthorized = [?NOBODY],
    forbidden_not_in_space = [?USER_IN_SPACE_KRK_AUTH],
    forbidden_in_space = [
        % forbidden by lack of privileges (even though being owner of files)
        ?USER_IN_SPACE_2_AUTH
    ],
    supported_clients_per_node = ?SUPPORTED_CLIENTS_PER_NODE(__CONFIG)
}).


-endif.
