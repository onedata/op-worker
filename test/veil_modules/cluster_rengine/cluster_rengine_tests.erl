%% ==================================================================
%% @author Michal Sitko
%% @copyright (C) 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ==================================================================
%% @doc:
%%
%% @end
%% ==================================================================
-module(cluster_rengine_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("veil_modules/cluster_rengine/cluster_rengine.hrl").
-endif.

-ifdef(TEST).

setup() ->
  %% function returned by fun_from_config exptects that ets with name passed as third argument exists
  ets:new(fun_from_config_test, [set, named_table, public]).

teardown(_) ->
  ets:delete(fun_from_config_test).

cluster_rengine_test_() ->
  {foreach, fun setup/0, fun teardown/1,
    [fun fun_from_config/0, fun fun_from_config_with_wrapped/0
    ]}.

%% Checks if handler functions are created correctly from stream configuration.
%% Verifies function construction by invoking constructed functions and checking if returns expected results.
fun_from_config() ->
  AggregatorConfig = #aggregator_config{field_name = user_id, fun_field_name = bytes, threshold = 100},
  Config = #event_stream_config{config = AggregatorConfig},
  Fun = cluster_rengine:fun_from_config(Config),

  Event1 = [{user_id, "1"}, {bytes, 30}, {file, "file1"}],
  repeat(3, fun() ->
    Output1 = Fun(1, {final_stage_tree, "TreeId1", Event1}, fun_from_config_test),
    ?assertEqual(non, Output1)
  end),

  Event2 = [{user_id, "2"}, {bytes, 55}, {file, "file1"}],
  Output1 = Fun(1, {final_stage_tree, "TreeId1", Event2}, fun_from_config_test),
  ?assertEqual(non, Output1),

  Output2 = Fun(1, {final_stage_tree, "TreeId1", Event1}, fun_from_config_test),
  ?assertEqual([{user_id, "1"}, {bytes, 120}], Output2),

  Output3 = Fun(1, {final_stage_tree, "TreeId1", Event2}, fun_from_config_test),
  ?assertEqual([{user_id, "2"}, {bytes, 110}], Output3).

%% Checks if handler functions are created correctly from stream configuration with wrapped configuration.
%% Verifies function construction by invoking constructed functions and checking if returns expected results.
fun_from_config_with_wrapped() ->
  AggregatorConfig = #aggregator_config{field_name = user_id, fun_field_name = bytes, threshold = 100},
  FilterConfig = #filter_config{field_name = type, desired_value = write_event},
  Config = #event_stream_config{config = AggregatorConfig, wrapped_config = #event_stream_config{config = FilterConfig}},
  Fun = cluster_rengine:fun_from_config(Config),

  WriteEvent1 = [{type, write_event}, {user_id, "1"}, {bytes, 30}, {file, "file1"}],
  ReadEvent1 = [{type, read_event}, {user_id, "1"}, {bytes, 30}, {file, "file1"}],

  repeat(3, fun() ->
    Output1 = Fun(1, {final_stage_tree, "TreeId1", WriteEvent1}, fun_from_config_test),
    ?assertEqual(non, Output1)
  end),

  Output1 = Fun(1, {final_stage_tree, "TreeId1", ReadEvent1}, fun_from_config_test),
  ?assertEqual(non, Output1),

  WriteEvent2 = [{type, write_event}, {user_id, "2"}, {bytes, 30}, {file, "file1"}],
  Output2 = Fun(1, {final_stage_tree, "TreeId1", WriteEvent2}, fun_from_config_test),
  ?assertEqual(non, Output2),

  Output3 = Fun(1, {final_stage_tree, "TreeId1", WriteEvent1}, fun_from_config_test),
  ?assertEqual([{user_id, "1"}, {bytes, 120}], Output3),

  repeat(3, fun() ->
    Output1 = Fun(1, {final_stage_tree, "TreeId1", WriteEvent1}, fun_from_config_test),
    ?assertEqual(non, Output1)
  end),
  Output4 = Fun(1, {final_stage_tree, "TreeId1", WriteEvent1}, fun_from_config_test),
  ?assertEqual([{user_id, "1"}, {bytes, 120}], Output4).

repeat(N, F) -> for(1, N, F).
for(N, N, F) -> [F()];
for(I, N, F) -> [F() | for(I + 1, N, F)].

-endif.