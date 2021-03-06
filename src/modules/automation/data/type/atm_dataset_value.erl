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
-behaviour(atm_tree_forest_store_container_iterator).

-include("modules/automation/atm_tmp.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% atm_data_validator callbacks
-export([assert_meets_constraints/3]).

%% atm_tree_forest_store_container_iterator callbacks
-export([
    list_children/4,
    initial_listing_options/0,
    encode_listing_options/1, decode_listing_options/1
]).

%% atm_data_compressor callbacks
-export([compress/1, expand/2]).


-type list_opts() :: #{
    start_index := dataset_api:index(),
    offset := non_neg_integer()
}.


%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================


-spec assert_meets_constraints(
    atm_workflow_execution_ctx:record(),
    atm_value:expanded(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().
assert_meets_constraints(AtmWorkflowExecutionCtx, #{<<"datasetId">> := DatasetId}, _ValueConstraints) ->
    check_implicit_constraints(AtmWorkflowExecutionCtx, DatasetId).


%%%===================================================================
%%% atm_tree_forest_store_container_iterator callbacks
%%%===================================================================


-spec list_children(atm_workflow_execution_ctx:record(), dataset:id(), list_opts(), non_neg_integer()) ->
    {[{dataset:id(), dataset:name()}], [], list_opts(), IsLast :: boolean()} | no_return().
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
            case fslogic_errors:is_access_error(Type) of
                true ->
                    {[], [], #{}, true};
                false ->
                    throw(Error)
            end
    end.


-spec initial_listing_options() -> list_opts().
initial_listing_options() ->
    #{
        start_index => <<>>,
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


-spec compress(atm_value:expanded()) -> dataset:id().
compress(#{<<"datasetId">> := DatasetId}) -> DatasetId.


-spec expand(atm_workflow_execution_ctx:record(), dataset:id()) ->
    {ok, atm_value:expanded()} | {error, term()}.
expand(AtmWorkflowExecutionCtx, DatasetId) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:get_dataset_info(SessionId, DatasetId) of
        {ok, DatasetInfo} -> {ok, dataset_utils:dataset_info_to_json(DatasetInfo)};
        {error, _} = Error -> Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_implicit_constraints(atm_workflow_execution_ctx:record(), dataset:id()) ->
    ok | no_return().
check_implicit_constraints(AtmWorkflowExecutionCtx, DatasetId) ->
    SpaceId = atm_workflow_execution_ctx:get_space_id(AtmWorkflowExecutionCtx),
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),

    case lfm:get_dataset_info(SessionId, DatasetId) of
        {ok, #dataset_info{root_file_guid = RootFileGuid}} ->
            case file_id:guid_to_space_id(RootFileGuid) of
                SpaceId ->
                    ok;
                _ ->
                    throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(#{<<"inSpace">> => SpaceId}))
            end;
        {error, Type} = Error ->
            case fslogic_errors:is_access_error(Type) of
                true ->
                    throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(#{<<"hasAccess">> => true}));
                false ->
                    throw(Error)
            end
    end.
