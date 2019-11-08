%%%--------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for file_distribution_data_backend module.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_gui_gs_test).
-author("Lukasz Opiola").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(CB_WIDTH, 320).
-define(seq(From, To), lists:seq(From, To)).
-define(seq(From, To, Skip), lists:seq(From, To, Skip)).


interpolate_chunks_test() ->
    {timeout, 15, ?_test(run_test_cases(1))}.


run_test_cases(CaseNum) ->
    case test_case(CaseNum, f_size) of
        finish ->
            ok;
        FileSize ->
            Blocks = test_case(CaseNum, blocks),
            Expected = test_case(CaseNum, expect),
            ?assertEqual(Expected, replica_gui_gs_translator:interpolate_chunks(Blocks, FileSize)),
            run_test_cases(CaseNum + 1)
    end.


test_case(1, f_size) -> 0;
test_case(1, blocks) -> [];
test_case(1, expect) -> [{0, 0}];

test_case(2, f_size) -> 10;
test_case(2, blocks) -> [[0, 10]];
test_case(2, expect) -> [{0, 100}];

test_case(3, f_size) -> 10;
test_case(3, blocks) -> [[0, 5]];
test_case(3, expect) -> [{0, 100}, {160, 0}];

test_case(4, f_size) -> ?CB_WIDTH * 1573;
test_case(4, blocks) -> [[0, 637]];
test_case(4, expect) -> [{0, round(637 * 100 / 1573)}, {1, 0}];

test_case(5, f_size) -> ?CB_WIDTH;
test_case(5, blocks) -> [[S, 1] || S <- ?seq(0, (?CB_WIDTH - 1))];
%                       [0,1, 1,1, 2,1, 3,1, ..., 319,1]
test_case(5, expect) -> [{0, 100}];

test_case(6, f_size) -> 746372;
test_case(6, blocks) -> [[S, 1] || S <- ?seq(0, 746371)];
test_case(6, expect) -> [{0, 100}];

test_case(7, f_size) -> ?CB_WIDTH;
test_case(7, blocks) -> [[S, 1] || S <- ?seq(0, (?CB_WIDTH - 1), 2)];
%                       [0,1, 2,1, 4,1, 6,1, ..., 318,1]
test_case(7, expect) -> [{S, 100 * (1 - (S rem 2))} || S <- ?seq(0, (?CB_WIDTH - 1), 1)];
%                       [{0,100}, {1, 0}, {2,100}, {3, 0}, ..., {319, 0}]

test_case(8, f_size) -> ?CB_WIDTH * 2;
test_case(8, blocks) -> [[S, 1] || S <- ?seq(0, (?CB_WIDTH * 2 - 1), 2)];
%                       [0,1, 2,1, 4,1, 6,1, ..., 638,1]
test_case(8, expect) -> [{0, 50}];

test_case(9, f_size) -> 592671;
test_case(9, blocks) -> [[S, 1] || S <- ?seq(0, 592670, 3)];
test_case(9, expect) -> [{0, 33}];

test_case(10, f_size) -> 1000000;
test_case(10, blocks) -> [[S, 3] || S <- ?seq(100, 12000, 3) ++ ?seq(16000, 999999, 5)];
test_case(10, expect) -> [{0, 97}, {1, 100}, {3, 84}, {4, 0}, {5, 53}, {6, 60}];

test_case(11, f_size) -> 9600;
test_case(11, blocks) -> [[3000, 2760]];
test_case(11, expect) -> [{0, 0}, {100, 100}, {192, 0}];

test_case(12, f_size) -> ?CB_WIDTH * 5;
test_case(12, blocks) -> [[S * 5, S rem 6] || S <- ?seq(0, (?CB_WIDTH - 1))];
%                        [0,0, 5,1, 10,2, 15,3, 20,4, 25,5, 30,0, ...]
test_case(12, expect) -> [{S, round((S rem 6) * 100 / 5)} || S <- ?seq(0, (?CB_WIDTH - 1))];
%                        [{0,0}, {1, 20}, {2,40}, {3, 60}, {4, 80}, {5, 100}, {6, 0}, ...]

test_case(_, _) -> finish.



-endif.