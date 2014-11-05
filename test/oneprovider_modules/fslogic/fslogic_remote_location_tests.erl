%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module tests functionality of remote location module, which helps
%% providers to check if their files are in sync
%% @end
%% ===================================================================
-module(fslogic_remote_location_tests).

-include_lib("eunit/include/eunit.hrl").
-include("oneprovider_modules/fslogic/fslogic_remote_location.hrl").

byte_to_block_range_test() ->
    ?assertEqual(#block_range{from = 0, to = 1}, fslogic_remote_location:byte_to_block_range(#byte_range{from = 123, to = ?remote_block_size + 1})),
    ?assertEqual(#block_range{from = 1, to = 1}, fslogic_remote_location:byte_to_block_range(#byte_range{from = ?remote_block_size, to = ?remote_block_size})),
    ?assertEqual(#block_range{from = 0, to = 2}, fslogic_remote_location:byte_to_block_range(#byte_range{from = 0, to = 2*?remote_block_size})).

mark_as_modified_test() ->
    Pr1 = "uuid1",
    Pr2 = "uuid2",
    Pr3 = "uuid3",
    List = [#remote_file_part{range = #block_range{from = 0, to = 100}, providers = [Pr1]}],
    List2 = fslogic_remote_location:mark_as_modified(#block_range{from = 0, to = 1}, List, Pr2),
    ?assertEqual([
        #remote_file_part{range = #block_range{from = 0, to = 1}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 2, to = 100}, providers = [Pr1]}
    ], List2),

    List3 = fslogic_remote_location:mark_as_modified(#block_range{from = 5, to = 10}, List2, Pr2),
    ?assertEqual([
        #remote_file_part{range = #block_range{from = 0, to = 1}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 2, to = 4}, providers = [Pr1]},
        #remote_file_part{range = #block_range{from = 5, to = 10}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 11, to = 100}, providers = [Pr1]}
    ], List3),

    List4 = fslogic_remote_location:mark_as_modified(#block_range{from = 10, to = 10}, List3, Pr3),
    ?assertEqual([
        #remote_file_part{range = #block_range{from = 0, to = 1}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 2, to = 4}, providers = [Pr1]},
        #remote_file_part{range = #block_range{from = 5, to = 9}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 10, to = 10}, providers = [Pr3]},
        #remote_file_part{range = #block_range{from = 11, to = 100}, providers = [Pr1]}
    ], List4),

    List5 = fslogic_remote_location:mark_as_modified(#block_range{from = 8, to = 10}, List4, Pr1),
    ?assertEqual([
        #remote_file_part{range = #block_range{from = 0, to = 1}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 2, to = 4}, providers = [Pr1]},
        #remote_file_part{range = #block_range{from = 5, to = 7}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 8, to = 100}, providers = [Pr1]}
    ], List5),

    List6 = fslogic_remote_location:mark_as_modified(#block_range{from = 1, to = 11}, List5, Pr2),
    ?assertEqual([
        #remote_file_part{range = #block_range{from = 0, to = 11}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 12, to = 100}, providers = [Pr1]}
    ], List6),

    List7 = fslogic_remote_location:mark_as_modified(#block_range{from = 95, to = 105}, List6, Pr3),
    ?assertEqual([
        #remote_file_part{range = #block_range{from = 0, to = 11}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 12, to = 94}, providers = [Pr1]},
        #remote_file_part{range = #block_range{from = 95, to = 105}, providers = [Pr3]}
    ], List7),

    List8 = fslogic_remote_location:mark_as_modified(#block_range{from = 110, to = 120}, List7, Pr3),
    ?assertEqual([
        #remote_file_part{range = #block_range{from = 0, to = 11}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 12, to = 94}, providers = [Pr1]},
        #remote_file_part{range = #block_range{from = 95, to = 120}, providers = [Pr3]}
    ], List8).

mark_as_available_test() ->
    Pr1 = "uuid1",
    Pr2 = "uuid2",
    Pr3 = "uuid3",

    List1 = [
        #remote_file_part{range = #block_range{from = 0, to = 1}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 2, to = 4}, providers = [Pr1]},
        #remote_file_part{range = #block_range{from = 5, to = 9}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 10, to = 10}, providers = [Pr3]},
        #remote_file_part{range = #block_range{from = 11, to = 100}, providers = [Pr1]}
    ],
    Result1 = fslogic_remote_location:mark_as_available(
        [#block_range{from = 1, to = 3}, #block_range{from = 4, to = 4}, #block_range{from = 10, to = 12}, #block_range{from = 22, to = 30}],
        List1,
        Pr3
    ),
    ?assertEqual(
        [
            #remote_file_part{range = #block_range{from = 0, to = 0}, providers = [Pr2]},
            #remote_file_part{range = #block_range{from = 1, to = 1}, providers = [Pr3, Pr2]},
            #remote_file_part{range = #block_range{from = 2, to = 4}, providers = [Pr3, Pr1]},
            #remote_file_part{range = #block_range{from = 5, to = 9}, providers = [Pr2]},
            #remote_file_part{range = #block_range{from = 10, to = 10}, providers = [Pr3]},
            #remote_file_part{range = #block_range{from = 11, to = 12}, providers = [Pr3, Pr1]},
            #remote_file_part{range = #block_range{from = 13, to = 21}, providers = [Pr1]},
            #remote_file_part{range = #block_range{from = 22, to = 30}, providers = [Pr3,Pr1]},
            #remote_file_part{range = #block_range{from = 31, to = 100}, providers = [Pr1]}
        ], Result1).


check_if_synchronized_test() ->
    Pr1 = "uuid1",
    Pr2 = "uuid2",
    Pr3 = "uuid3",

    List = [
        #remote_file_part{range = #block_range{from = 0, to = 1}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 2, to = 4}, providers = [Pr1, Pr3]},
        #remote_file_part{range = #block_range{from = 5, to = 9}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 10, to = 10}, providers = [Pr3]},
        #remote_file_part{range = #block_range{from = 11, to = 100}, providers = [Pr1, Pr2, Pr3]}
    ],

    ?assertEqual([
        #remote_file_part{range = #block_range{from = 0, to = 0}, providers = [Pr2]}
    ], fslogic_remote_location:check_if_synchronized(#block_range{from = 0, to = 0}, List, Pr1)),

    ?assertEqual([], fslogic_remote_location:check_if_synchronized(#block_range{from = 0, to = 0}, List, Pr2)),

    ?assertEqual([
        #remote_file_part{range = #block_range{from = 1, to = 1}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 5, to = 9}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 10, to = 10}, providers = [Pr3]}
    ], fslogic_remote_location:check_if_synchronized(#block_range{from = 1, to = 11}, List, Pr1)),

    ?assertEqual([
        #remote_file_part{range = #block_range{from = 2, to = 4}, providers = [Pr1, Pr3]}
    ], fslogic_remote_location:check_if_synchronized(#block_range{from = 2, to = 9}, List, Pr2)).

minimize_remote_parts_list_test() ->
    Pr1 = "uuid1",
    Pr2 = "uuid2",
    Pr3 = "uuid3",
    List = [
        #remote_file_part{range = #block_range{from = 0, to = 5}, providers = [Pr1]},
        #remote_file_part{range = #block_range{from = 6, to = 6}, providers = [Pr1]},
        #remote_file_part{range = #block_range{from = 7, to = 14}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 15, to = 17}, providers = [Pr3]},
        #remote_file_part{range = #block_range{from = 18, to = 22}, providers = [Pr1, Pr3]},
        #remote_file_part{range = #block_range{from = 23, to = 25}, providers = [Pr1, Pr3]},
        #remote_file_part{range = #block_range{from = 26, to = 29}, providers = [Pr1, Pr3]}
    ],
    MinimizedList = [
        #remote_file_part{range = #block_range{from = 0, to = 6}, providers = [Pr1]},
        #remote_file_part{range = #block_range{from = 7, to = 14}, providers = [Pr2]},
        #remote_file_part{range = #block_range{from = 15, to = 17}, providers = [Pr3]},
        #remote_file_part{range = #block_range{from = 18, to = 29}, providers = [Pr1, Pr3]}
    ],
    ?assertEqual(MinimizedList, fslogic_remote_location:minimize_remote_parts_list(List)).
