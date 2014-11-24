%% ===================================================================
%% @author Tomasz Lichon
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This module tests data strunture that stores ranges with timestamps
%% @end
%% ===================================================================
-module(ranges_struct_tests).
-include("oneprovider_modules/fslogic/ranges_struct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([merge_test/0, truncate_test/0, subtract_newer_test/0, intersection_test/0]).

merge_test() ->
    R1 = #range{from=5, to=7, timestamp = 1},
    R2 = #range{from=10, to=12, timestamp = 2},
    ?assertEqual([R1], ranges_struct:merge([R1], [])),
    ?assertEqual([R2], ranges_struct:merge([], [R2])),
    ?assertEqual([R1, R2], ranges_struct:merge([R1], [R2])),

    R3 = #range{from = 0, to = 100, timestamp = 1},
    R4 = #range{from = 3, to = 5, timestamp = 2},
    ?assertEqual([#range{from = 0, to = 2, timestamp = 1}, R4, #range{from = 6, to = 100, timestamp = 1}], ranges_struct:merge([R3], [R4])),

    R5 = #range{from = 0, to = 15, timestamp = 1},
    R6 = #range{from = 8, to = 20, timestamp = 2},
    ?assertEqual([#range{from = 0, to = 7, timestamp = 1}, R6], ranges_struct:minimize(ranges_struct:merge([R6], [R5]))),

    ?assertEqual([#range{from = 1, to = 1, timestamp = 2}], ranges_struct:minimize(ranges_struct:merge([#range{from = 1, to = 1, timestamp = 1}], [#range{from = 1, to = 1, timestamp = 2}]))).


truncate_test() ->
    List = [
        #range{from = 0, to = 5, timestamp = 1},
        #range{from = 6, to = 6, timestamp = 2},
        #range{from = 7, to = 14, timestamp = 3},
        #range{from = 15, to = 17, timestamp = 4},
        #range{from = 18, to = 22, timestamp = 5},
        #range{from = 23, to = 25, timestamp = 6},
        #range{from = 26, to = 29, timestamp = 7}
    ],
    TruncatedList1 = [
        #range{from = 0, to = 5, timestamp = 1},
        #range{from = 6, to = 6, timestamp = 2},
        #range{from = 7, to = 13, timestamp = 3}
    ],
    ?assertEqual(TruncatedList1, ranges_struct:truncate(List, #range{to=13})),
    TruncatedList2 = [
        #range{from = 0, to = 5, timestamp = 1},
        #range{from = 6, to = 6, timestamp = 2},
        #range{from = 7, to = 10, timestamp = 3}
    ],
    ?assertEqual(TruncatedList2, ranges_struct:truncate(List, #range{to=10})),
    TruncatedList3 = [
        #range{from = 0, to = 5, timestamp = 1},
        #range{from = 6, to = 6, timestamp = 2},
        #range{from = 7, to = 14, timestamp = 3},
        #range{from = 15, to = 17, timestamp = 4},
        #range{from = 18, to = 22, timestamp = 5},
        #range{from = 23, to = 25, timestamp = 6},
        #range{from = 26, to = 29, timestamp = 7},
        #range{from = 30, to = 34, timestamp = 8}
    ],
    ?assertEqual(TruncatedList3, ranges_struct:truncate(List, #range{to=34, timestamp = 8})),
    ?assertEqual([#range{from = 0, to = 4, timestamp = 1}], ranges_struct:truncate([], #range{to=4, timestamp = 1})).

subtract_newer_test() ->
    List1 = [
        #range{from = 0, to = 5, timestamp = 1},
        #range{from = 6, to = 6, timestamp = 2},
        #range{from = 7, to = 14, timestamp = 3}
    ],
    NewList1 = [
        #range{from = 0, to = 5, timestamp = 6}
    ],
    SubtractResult1 = [
        #range{from = 6, to = 6, timestamp = 2},
        #range{from = 7, to = 14, timestamp = 3}
    ],
    ?assertEqual(SubtractResult1, ranges_struct:minimize(ranges_struct:subtract_newer(List1, NewList1))),

    NewList2 = [
        #range{from = 0, to = 6, timestamp = 6}
    ],
    SubtractResult2 = [
        #range{from = 7, to = 14, timestamp = 3}
    ],
    ?assertEqual(SubtractResult2, ranges_struct:minimize(ranges_struct:subtract_newer(List1, NewList2))),

    NewList3 = [
        #range{from = 0, to = 1, timestamp = 1},
        #range{from = 3, to = 8, timestamp = 6}
    ],
    SubtractResult3 = [
        #range{from = 0, to = 2, timestamp = 1},
        #range{from = 9, to = 14, timestamp = 3}
    ],
    ?assertEqual(SubtractResult3, ranges_struct:minimize(ranges_struct:subtract_newer(List1, NewList3))),

    ?assertEqual([], ranges_struct:minimize(ranges_struct:subtract_newer([#range{from = 1, to = 1, timestamp = 1}], [#range{from = 1, to = 1, timestamp = 2}]))),

    List4 = [#range{from = 5, to = 30}],
    NewList4 = [
        #range{from = 11, to = 14, timestamp = 1},
        #range{from = 21, to = 24, timestamp = 1}
    ],
    SubtractResult4 = [
        #range{from = 5, to = 10},
        #range{from = 15, to = 20},
        #range{from = 25, to = 30}
    ],
    ?assertEqual(SubtractResult4, ranges_struct:minimize(ranges_struct:subtract_newer(List4, NewList4))).

intersection_test() ->
    List11 = [
        #range{from = 0, to = 5, timestamp = 1},
        #range{from = 6, to = 6, timestamp = 2},
        #range{from = 7, to = 14, timestamp = 3}
    ],
    List12 = [
        #range{from = 3, to = 7, timestamp = 2}
    ],
    Result1 = ranges_struct:minimize(ranges_struct:intersection(List11, List12)),
    ?assertEqual([
        #range{from = 3, to = 6, timestamp = 2},
        #range{from = 7, to = 7, timestamp = 3}
    ], Result1).
