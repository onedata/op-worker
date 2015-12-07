%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for fslogic_blocks module.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_blocks_test).
-author("Rafal Slota").

-ifdef(TEST).

-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BLOCK(Offset, Size), #file_block{offset = Offset, size = Size}).

%%%===================================================================
%%% Test functions
%%%===================================================================

%% tests function aggregate/2
aggregate_test() ->
    ?assertEqual([], fslogic_blocks:aggregate([], [])),
    ?assertEqual([?BLOCK(1, 1)], fslogic_blocks:aggregate([?BLOCK(1, 1)], [])),
    ?assertEqual([?BLOCK(1, 1)], fslogic_blocks:aggregate([], [?BLOCK(1, 1)])),
    ?assertEqual([?BLOCK(1, 4)], fslogic_blocks:aggregate(
        [?BLOCK(1, 1)],
        [?BLOCK(2, 3)]
    )),
    ?assertEqual([?BLOCK(1, 4)], fslogic_blocks:aggregate(
        [?BLOCK(3, 2)],
        [?BLOCK(1, 2)]
    )),
    ?assertEqual([?BLOCK(1, 1), ?BLOCK(3, 1), ?BLOCK(5, 1), ?BLOCK(7, 1)],
        fslogic_blocks:aggregate(
            [?BLOCK(3, 1), ?BLOCK(7, 1)],
            [?BLOCK(1, 1), ?BLOCK(5, 1)]
        )
    ),
    ?assertEqual([?BLOCK(1, 1), ?BLOCK(3, 1), ?BLOCK(5, 1), ?BLOCK(7, 1)],
        fslogic_blocks:aggregate(
            [?BLOCK(1, 1), ?BLOCK(3, 1)],
            [?BLOCK(5, 1), ?BLOCK(7, 1)]
        )
    ),
    ?assertEqual([?BLOCK(0, 100)], fslogic_blocks:aggregate(
        [?BLOCK(Offset, 1) || Offset <- lists:seq(0, 98, 2)],
        [?BLOCK(0, 100)]
    )),
    ?assertEqual([?BLOCK(0, 100)], fslogic_blocks:aggregate(
        [?BLOCK(Offset, 1) || Offset <- lists:seq(0, 98, 2)],
        [?BLOCK(Offset, 1) || Offset <- lists:seq(1, 99, 2)]
    )).

%% tests function upper/1
upper_test() ->
    ?assertEqual(6, fslogic_blocks:upper(
        #file_block{offset = 5, size = 1})),

    ?assertEqual(7, fslogic_blocks:upper(
        #file_block{offset = 0, size = 7})),

    ?assertEqual(6, fslogic_blocks:upper([
        #file_block{offset = 5, size = 1}
    ])),

    ?assertEqual(6, fslogic_blocks:upper([
        #file_block{offset = 2, size = 3},
        #file_block{offset = 5, size = 1}
    ])),

    ?assertEqual(6, fslogic_blocks:upper([
        #file_block{offset = 1, size = 3},
        #file_block{offset = 5, size = 1}
    ])),

    ?assertEqual(6, fslogic_blocks:upper([
        #file_block{offset = 1, size = 3},
        #file_block{offset = 0, size = 5},
        #file_block{offset = 6, size = 0},
        #file_block{offset = 5, size = 1}
    ])),

    ?assertEqual(10, fslogic_blocks:upper([
        #file_block{offset = 1, size = 9},
        #file_block{offset = 0, size = 5},
        #file_block{offset = 6, size = 0},
        #file_block{offset = 5, size = 1}
    ])),

    ok.

%% tests function lower/1
lower_test() ->
    ?assertEqual(5, fslogic_blocks:lower(
        #file_block{offset = 5, size = 1})),

    ?assertEqual(0, fslogic_blocks:lower(
        #file_block{offset = 0, size = 7})),

    ?assertEqual(5, fslogic_blocks:lower([
        #file_block{offset = 5, size = 1}
    ])),

    ?assertEqual(2, fslogic_blocks:lower([
        #file_block{offset = 2, size = 3},
        #file_block{offset = 5, size = 1}
    ])),

    ?assertEqual(1, fslogic_blocks:lower([
        #file_block{offset = 1, size = 3},
        #file_block{offset = 5, size = 1}
    ])),

    ?assertEqual(0, fslogic_blocks:lower([
        #file_block{offset = 1, size = 3},
        #file_block{offset = 0, size = 5},
        #file_block{offset = 6, size = 0},
        #file_block{offset = 5, size = 1}
    ])),

    ?assertEqual(1, fslogic_blocks:lower([
        #file_block{offset = 1, size = 9},
        #file_block{offset = 4, size = 5},
        #file_block{offset = 6, size = 0},
        #file_block{offset = 5, size = 1}
    ])),

    ok.

-endif.