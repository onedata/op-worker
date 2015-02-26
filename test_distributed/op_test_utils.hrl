%% ===================================================================
%% @author Michał Wrzeszcz
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc Common definitions for ct tests.
%% @end
%% ===================================================================

-include_lib("ctool/include/test/test_utils.hrl").

-ifndef(OP_TEST_UTILS_HRL).
-define(OP_TEST_UTILS_HRL, 1).

%% Initializes test environment
-define(TEST_INIT(Config, EnvDescription),
    ?TEST_INIT(Config, EnvDescription, "provider_up.py")
).

-endif.
