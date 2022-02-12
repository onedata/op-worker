%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Definitions of macros used for op rpc.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(TEST_RPC_HRL).
-define(TEST_RPC_HRL, 1).


-define(rpc(__PROVIDER_SELECTOR, __EXPRESSION),
    (fun() ->
        try
            erpc:call(
                lists_utils:random_element(oct_background:get_provider_nodes(__PROVIDER_SELECTOR)),
                fun() -> __EXPRESSION end
            )
        catch __TYPE:__REASON:__STACKTRACE ->
            ct:pal(
                "Test RPC on node ~p failed!~n"
                "Stacktrace: ~s~n",
                [__PROVIDER_SELECTOR, iolist_to_binary(lager:pr_stacktrace(__STACKTRACE, {__TYPE, __REASON}))]
            ),
            error({test_rpc_failed, __PROVIDER_SELECTOR, __REASON})
        end
    end)()
).


-endif.
