%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module tests the functionality of rest_utils, using eunit tests.
%% @end
%% ===================================================================
-module(rest_utils_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-record(test_record_a, {id, message, text}).
-record(test_record_b, {key, value, opts}).
-record(test_record_c, {field1, field2, field3, field4}).

% These tests check mapping and unmapping functions from rest_utils module, on arbitrary records. 
map_and_unmap_test_() ->
        [
            {"map test",
                fun() ->               
                    TestRecord1 = #test_record_a { id=1, message=2, text=3 }, 
                    TestRecord2 = #test_record_b { key=4, value=5, opts=6 }, 
                    TestRecord3 = #test_record_c { field1=7, field2=8, field3=9, field4=10 }, 

                    Ans1 = rest_utils:map(TestRecord1, record_info(fields, test_record_a)),
                    Ans2 = rest_utils:map(TestRecord2, record_info(fields, test_record_b)),
                    Ans3 = rest_utils:map(TestRecord3, record_info(fields, test_record_c)),

                    ?assertEqual(Ans1, [{id, 1}, {message, 2}, {text, 3}]),
                    ?assertEqual(Ans2, [{key, 4}, {value, 5}, {opts, 6}]),
                    ?assertEqual(Ans3, [{field1, 7}, {field2, 8}, {field3, 9}, {field4, 10}])
                end},

            {"unmap test",
                fun() ->
                    TestProplist1 = [{<<"id">>, 1}, {<<"message">>, 2}, {<<"text">>, 3}], 
                    TestProplist2 = [{<<"key">>, 4}, {<<"value">>, 5}, {<<"opts">>, 6}], 
                    TestProplist3 = [{<<"field1">>, 7}, {<<"field2">>, 8}, {<<"field3">>, 9}, {<<"field4">>, 10}], 

                    Ans1 = rest_utils:unmap(TestProplist1, #test_record_a{}, record_info(fields, test_record_a)),
                    Ans2 = rest_utils:unmap(TestProplist2, #test_record_b{}, record_info(fields, test_record_b)),
                    Ans3 = rest_utils:unmap(TestProplist3, #test_record_c{}, record_info(fields, test_record_c)),

                    ?assertEqual(Ans1, #test_record_a { id=1, message=2, text=3 }),
                    ?assertEqual(Ans2, #test_record_b { key=4, value=5, opts=6 }),
                    ?assertEqual(Ans3, #test_record_c { field1=7, field2=8, field3=9, field4=10 })
                end}      
        ].

-endif.