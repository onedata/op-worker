%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Shorthands for executing an expression on an op-worker node selected
%%% by provider selector.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(TEST_RPC_HRL).
-define(TEST_RPC_HRL, 1).


-define(rpc(__PROVIDER_SELECTOR, __EXPRESSION), opw_test_rpc:call(__PROVIDER_SELECTOR, fun() ->
    __EXPRESSION
end)).

-define(erpc(__PROVIDER_SELECTOR, __EXPRESSION), opw_test_rpc:insecure_call(__PROVIDER_SELECTOR, fun() ->
    __EXPRESSION
end)).


-endif.
