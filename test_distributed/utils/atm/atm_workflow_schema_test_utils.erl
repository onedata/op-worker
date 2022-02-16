%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Automation workflow schema utility functions used in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_schema_test_utils).
-author("Bartosz Walkowicz").

-include("atm_test_schema.hrl").
-include("onenv_test_utils.hrl").

-export([query/2]).


%%%===================================================================
%%% API
%%%===================================================================


query(Target, []) ->
    Target;

query(Map, [Key | Rest]) when
    is_map(Map),
    is_map_key(Key, Map)
->
    query(maps:get(Key, Map), Rest);

query(List, [Index | Rest]) when
    is_list(List),
    is_integer(Index),
    Index >= 1,
    Index =< length(List)
->
    query(lists:nth(Index, List), Rest);

query(Record, [FieldName | Rest]) when
    is_tuple(Record),
    is_atom(FieldName)
->
    query(
        element(1 + lists_utils:index_of(FieldName, get_fields(Record)), Record),
        Rest
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
get_fields(#od_atm_workflow_schema{}) ->
    record_info(fields, od_atm_workflow_schema);
get_fields(#atm_workflow_schema_revision_registry{}) ->
    record_info(fields, atm_workflow_schema_revision_registry);
get_fields(#atm_workflow_schema_revision{}) ->
    record_info(fields, atm_workflow_schema_revision);
get_fields(#atm_lane_schema{}) ->
    record_info(fields, atm_lane_schema);
get_fields(#atm_parallel_box_schema{}) ->
    record_info(fields, atm_parallel_box_schema);
get_fields(#atm_task_schema{}) ->
    record_info(fields, atm_task_schema);
get_fields(#atm_task_schema_result_mapper{}) ->
    record_info(fields, atm_task_schema_result_mapper);
get_fields(#atm_task_schema_argument_mapper{}) ->
    record_info(fields, atm_task_schema_argument_mapper);
get_fields(#atm_task_argument_value_builder{}) ->
    record_info(fields, atm_task_argument_value_builder);
get_fields(#atm_store_iterator_spec{}) ->
    record_info(fields, atm_store_iterator_spec);
get_fields(#atm_store_schema{}) ->
    record_info(fields, atm_store_schema);
get_fields(#atm_data_spec{}) ->
    record_info(fields, atm_data_spec);
get_fields(#atm_resource_spec{}) ->
    record_info(fields, atm_resource_spec).
