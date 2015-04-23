%%%--------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for event_utils module.
%%% @end
%%%--------------------------------------------------------------------
-module(event_utils_test).
-author("Krzysztof Trzepla").

-ifdef(TEST).

-include("proto/oneclient/common_messages.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BLOCK(Offset, Size), #file_block{offset = Offset, size = Size}).

%%%===================================================================
%%% Test functions
%%%===================================================================

aggregate_blocks_test() ->
    ?assertEqual([], event_utils:aggregate_blocks([], [])),
    ?assertEqual([?BLOCK(1, 1)], event_utils:aggregate_blocks([?BLOCK(1, 1)], [])),
    ?assertEqual([?BLOCK(1, 1)], event_utils:aggregate_blocks([], [?BLOCK(1, 1)])),
    ?assertEqual([?BLOCK(1, 4)], event_utils:aggregate_blocks(
        [?BLOCK(1, 1)],
        [?BLOCK(2, 3)]
    )),
    ?assertEqual([?BLOCK(1, 4)], event_utils:aggregate_blocks(
        [?BLOCK(3, 2)],
        [?BLOCK(1, 2)]
    )),
    ?assertEqual([?BLOCK(1, 1), ?BLOCK(3, 1), ?BLOCK(5, 1), ?BLOCK(7, 1)],
        event_utils:aggregate_blocks(
            [?BLOCK(3, 1), ?BLOCK(7, 1)],
            [?BLOCK(1, 1), ?BLOCK(5, 1)]
        )
    ),
    ?assertEqual([?BLOCK(1, 1), ?BLOCK(3, 1), ?BLOCK(5, 1), ?BLOCK(7, 1)],
        event_utils:aggregate_blocks(
            [?BLOCK(1, 1), ?BLOCK(3, 1)],
            [?BLOCK(5, 1), ?BLOCK(7, 1)]
        )
    ),
    ?assertEqual([?BLOCK(0, 100)], event_utils:aggregate_blocks(
        [?BLOCK(Offset, 1) || Offset <- lists:seq(0, 98, 2)],
        [?BLOCK(0, 100)]
    )),
    ?assertEqual([?BLOCK(0, 100)], event_utils:aggregate_blocks(
        [?BLOCK(Offset, 1) || Offset <- lists:seq(0, 98, 2)],
        [?BLOCK(Offset, 1) || Offset <- lists:seq(1, 99, 2)]
    )).

-endif.