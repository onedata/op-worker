%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%-------------------------------------------------------------------

-ifndef(TEST_RPC_HRL).
-define(TEST_RPC_HRL, 1).


-define(erpc(__NODE_SELECTOR, __EXPRESSION),
    erpc:call(
        case lists:member(__NODE_SELECTOR, nodes(known)) of
            true -> __NODE_SELECTOR;
            false -> lists_utils:random_element(oct_background:get_provider_nodes(__NODE_SELECTOR))
        end,
        fun() -> __EXPRESSION end
    )
).

-define(rpc(__NODE_SELECTOR, __EXPRESSION),
    (fun() ->
        try
            ?erpc(__NODE_SELECTOR, __EXPRESSION)
        catch __TYPE:__REASON:__STACKTRACE ->
            ct:pal(
                "Test RPC on node ~p failed!~n"
                "Stacktrace: ~s~n",
                [__NODE_SELECTOR, iolist_to_binary(lager:pr_stacktrace(__STACKTRACE, {__TYPE, __REASON}))]
            ),
            error({test_rpc_failed, __NODE_SELECTOR, __REASON})
        end
    end)()
).


-endif.
