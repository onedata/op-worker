%%%--------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for replica_synchronizer module.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_synchronizer_test).
-author("Bartosz Walkowicz").

-ifdef(TEST).

-include("proto/oneclient/common_messages.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(BLOCK(Offset, Size), #file_block{offset = Offset, size = Size}).

-define(IN_PROGRESS(Offset, Size, Priority),
    {?BLOCK(Offset, Size), undefined, Priority}).

%%%===================================================================
%%% Test functions
%%%===================================================================

%% tests function aggregate/2
find_overlapping_test_() ->
    BlocksInProgress = ordsets:from_list([
        ?IN_PROGRESS(0, 10, 0),
        ?IN_PROGRESS(6, 4, 100),
        ?IN_PROGRESS(15, 5, 96),
        ?IN_PROGRESS(19, 6, 122),
        ?IN_PROGRESS(19, 6, 244),
        ?IN_PROGRESS(40, 20, 96),
        ?IN_PROGRESS(65, 5, 97),
        ?IN_PROGRESS(75, 5, 98),
        ?IN_PROGRESS(85, 5, 99),
        ?IN_PROGRESS(120, 10, 32),
        ?IN_PROGRESS(125, 10, 16),
        ?IN_PROGRESS(150, 50, 0),
        ?IN_PROGRESS(190, 40, 120),
        ?IN_PROGRESS(250, 50, 156)
    ]),

    [
        ?_assertEqual(
            [
                ?IN_PROGRESS(0, 10, 0)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(5, 10), 98, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(0, 10, 0),
                ?IN_PROGRESS(15, 5, 96)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(7, 9), 98, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(0, 10, 0),
                ?IN_PROGRESS(6, 4, 100),
                ?IN_PROGRESS(15, 5, 96)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(7, 9), 102, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [],
            replica_synchronizer:find_overlapping(
                ?BLOCK(18, 4), 95, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(15, 5, 96)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(18, 4), 96, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(15, 5, 96),
                ?IN_PROGRESS(19, 6, 122)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(19, 6), 122, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(19, 6, 122),
                ?IN_PROGRESS(19, 6, 244),
                ?IN_PROGRESS(40, 20, 96)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(20, 25), 250, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [],
            replica_synchronizer:find_overlapping(
                ?BLOCK(25, 15), 255, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(65, 5, 97)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(64, 2), 97, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(65, 5, 97)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(65, 2), 97, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(65, 5, 97)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(66, 2), 97, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(65, 5, 97)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(68, 2), 97, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(65, 5, 97)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(69, 2), 97, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(65, 5, 97),
                ?IN_PROGRESS(75, 5, 98),
                ?IN_PROGRESS(85, 5, 99)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(60, 35), 100, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(65, 5, 97),
                ?IN_PROGRESS(75, 5, 98)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(60, 35), 98, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [],
            replica_synchronizer:find_overlapping(
                ?BLOCK(60, 35), 0, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [
                ?IN_PROGRESS(125, 10, 16),
                ?IN_PROGRESS(150, 50, 0)
            ],
            replica_synchronizer:find_overlapping(
                ?BLOCK(100, 100), 20, BlocksInProgress
            )
        ),

        ?_assertEqual(
            [],
            replica_synchronizer:find_overlapping(
                ?BLOCK(280, 10), 155, BlocksInProgress
            ))
    ].

-endif.
