%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_data_validator`, `atm_tree_forest_container_iterator` 
%%% and `atm_data_compressor` functionality for `atm_dataset_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_dataset_value).
-author("Michal Stanisz").

-behaviour(atm_data_validator).
-behaviour(atm_data_compressor).
-behaviour(atm_tree_forest_container_iterator).

-include("modules/automation/atm_tmp.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% atm_data_validator callbacks
-export([validate/3]).

%% atm_tree_forest_container_iterator callbacks
-export([
    list_children/4,
    initial_listing_options/0,
    encode_listing_options/1, decode_listing_options/1
]).

%% atm_data_compressor callbacks
-export([compress/1, expand/2]).


-type id() :: atm_value:compressed(). % More precisely dataset:id() but without undefined. Specified this way for dialyzer.
-type list_opts() :: dataset_api:listing_opts().

%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================

-spec validate(
    atm_workflow_execution_ctx:record(),
    atm_value:expanded(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().
validate(AtmWorkflowExecutionCtx, #{<<"datasetId">> := DatasetId} = Value, _ValueConstraints) ->
    try has_access(AtmWorkflowExecutionCtx, DatasetId) of
        true -> ok;
        false -> throw(?ERROR_NOT_FOUND)
    catch _:_ ->
        throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_dataset_type))
    end;
validate(_AtmWorkflowExecutionCtx, Value, _ValueConstraints) ->
    throw(?ERROR_ATM_DATA_TYPE_UNVERIFIED(Value, atm_dataset_type)).


%%%===================================================================
%%% atm_tree_forest_container_iterator callbacks
%%%===================================================================

-spec list_children(atm_workflow_execution_ctx:record(), id(), list_opts(), non_neg_integer()) ->
    {[{id(), dataset:name()}], [], list_opts(), IsLast :: boolean()} | no_return().
list_children(AtmWorkflowExecutionCtx, DatasetId, ListOpts, BatchSize) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:list_children_datasets(SessionId, DatasetId, ListOpts#{limit => BatchSize}) of
        {ok, Entries, IsLast} when length(Entries) > 0 ->
            {_LastId, _LastName, LastIndex} = lists:last(Entries),
            ResultEntries = lists:map(fun({Id, Name, _}) -> {Id, Name} end, Entries),
            % all datasets are traversable, so returned list of nontraversable items is always empty
            % set offset to 1 to ensure that listing is exclusive
            {ResultEntries, [], #{offset => 1, start_index => LastIndex}, IsLast};
        {ok, [], IsLast} ->
            {[], [], #{}, IsLast};
        {error, Type} = Error ->
            case atm_value:is_error_ignored(Type) of
                true ->
                    {[], [], #{}, true};
                false ->
                    throw(Error)
            end
    end.


-spec has_access(atm_workflow_execution_ctx:record(), id()) -> boolean().
has_access(AtmWorkflowExecutionCtx, DatasetId) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:get_dataset_info(SessionId, DatasetId) of
        {ok, _} -> true;
        {error, Type} = Error ->
            case atm_value:is_error_ignored(Type) of
                true -> false;
                false -> throw(Error)
            end
    end.


-spec initial_listing_options() -> list_opts().
initial_listing_options() ->
    #{
        start_id => <<>>,
        offset => 0
    }.


-spec encode_listing_options(list_opts()) -> json_utils:json_term().
encode_listing_options(#{offset := Offset, start_index := StartIndex}) ->
    #{<<"offset">> => Offset, <<"startIndex">> => StartIndex}.


-spec decode_listing_options(json_utils:json_term()) -> list_opts().
decode_listing_options(#{<<"offset">> := Offset, <<"startIndex">> := StartIndex}) ->
    #{offset => Offset, start_index => StartIndex}.


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================

-spec compress(atm_value:expanded()) -> id().
compress(#{<<"datasetId">> := DatasetId}) -> DatasetId.


-spec expand(atm_workflow_execution_ctx:record(), id()) -> 
    {ok, atm_value:expanded()} | {error, term()}.
expand(AtmWorkflowExecutionCtx, DatasetId) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:get_dataset_info(SessionId, DatasetId) of
        {ok, DatasetInfo} -> {ok, dataset_utils:dataset_info_to_json(DatasetInfo)};
        {error, _} = Error -> Error
    end.
