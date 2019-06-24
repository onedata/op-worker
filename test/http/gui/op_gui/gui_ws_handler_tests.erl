%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This suite contains unit tests for functions in gui_ws_handler module.
%%% @end
%%%-------------------------------------------------------------------
-module(gui_ws_handler_tests).
-author("Lukasz Opiola").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


%%%===================================================================
%%% Tests functions
%%%===================================================================

split_into_sublists_test() ->
    Test1 = [1,2,3,4,5,6],
    Result1 = op_gui_ws_handler:split_into_sublists(Test1, 10),
    % The list should be split in 6 parts
    ?assertEqual(6, length(Result1)),
    % One element each
    lists:foreach(
        fun(Sublist) ->
            ?assertEqual(1, length(Sublist))
        end, Result1),
    % And it should contain all the elements from the original list
    ?assertEqual(Test1, lists:sort(lists:flatten(Result1))),
    Test2 = lists:seq(1, 1500),
    Result2 = op_gui_ws_handler:split_into_sublists(Test2, 13),
    % 1500/13 ~= 115.38, so we expect that result contains 13 lists, every one
    % with 115 or 116 elements.
    ?assertEqual(13, length(Result2)),
    lists:foreach(
        fun(Sublist) ->
            ?assert(length(Sublist) =:= 115 orelse length(Sublist) =:= 116)
        end, Result2),
    ?assertEqual(Test2, lists:sort(lists:flatten(Result2))).

-endif.