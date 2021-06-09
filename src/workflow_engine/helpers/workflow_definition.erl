%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Just a mock module for test.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_definition).
-author("Michal Wrzeszcz").

%% API
-export([get/1, get_lane/2]).

%% @formatter:off
-type id() :: binary().
-type execution_spec() :: #{
    id := id(),
    lanes_count := non_neg_integer()
}.

-type box_index() :: non_neg_integer().
-type task_index() :: non_neg_integer().
-type lane_index() :: non_neg_integer().
-type box_spec() :: #{
    task_index() => task_executor:task_id()
}.
-type boxes_map() :: #{
    box_index() => box_spec()
}.

-type lane_spec() :: #{
    parallel_boxes := boxes_map(),
    iterator := workflow_store:iterator()
}.
%% @formatter:on

-export_type([execution_spec/0, lane_spec/0, boxes_map/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(id()) -> execution_spec().
get(Id) ->
    #{
        id => Id,
        lanes_count => 5
    }.

-spec get_lane(id(), lane_index()) -> lane_spec().
get_lane(WorkflowId, LaneIndex) ->
    Boxes = lists:foldl(fun(BoxIndex, BoxAcc) ->
        Tasks = lists:foldl(fun(TaskIndex, TaskAcc) ->
            TaskAcc#{TaskIndex => <<WorkflowId/binary, "_task", (integer_to_binary(LaneIndex))/binary, "_",
                (integer_to_binary(BoxIndex))/binary, "_", (integer_to_binary(TaskIndex))/binary>>}
        end, #{}, lists:seq(1, BoxIndex)),
        BoxAcc#{BoxIndex => Tasks}
    end, #{}, lists:seq(1, LaneIndex)),
    #{
        parallel_boxes => Boxes,
        iterator => workflow_store:get_iterator(WorkflowId)
    }.