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
    list_children/4, exists/2,
    initial_listing_options/0,
    encode_listing_options/1, decode_listing_options/1
]).

%% atm_data_compressor callbacks
-export([compress/1, expand/2]).


-type list_opts() :: dataset_api:listing_opts().

%%%===================================================================
%%% atm_data_validator callbacks
%%%===================================================================

-spec validate(
    atm_workflow_execution_ctx:record(),
    atm_api:item(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().
validate(AtmWorkflowExecutionCtx, #{<<"datasetId">> := DatasetId} = Value, _ValueConstraints) ->
    try exists(AtmWorkflowExecutionCtx, DatasetId) of
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

-spec list_children(atm_workflow_execution_ctx:record(), dataset:id(), list_opts(), non_neg_integer()) ->
    {[{dataset:id(), binary()}], [dataset:id()], list_opts(), IsLast :: boolean()} | no_return().
list_children(AtmWorkflowExecutionCtx, DatasetId, ListOpts, BatchSize) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:list_children_datasets(SessionId, DatasetId, ListOpts#{limit => BatchSize}) of
        {ok, Entries, IsLast} when length(Entries) > 0 ->
            PrevOffset = maps:get(offset, ListOpts),
            ResultEntries = lists:map(fun({Id, Name, _}) -> {Id, Name} end, Entries),
            {ResultEntries, [], #{offset => PrevOffset + length(Entries)}, IsLast};
        {ok, [], _} ->
            {[], [], #{}, true};
        {error, Type} = Error ->
            case atm_data_utils:is_error_ignored(Type) of
                true ->
                    {[], [], #{}, true};
                false ->
                    throw(Error)
            end
    end.


-spec exists(atm_workflow_execution_ctx:record(), dataset:id()) -> boolean().
exists(AtmWorkflowExecutionCtx, DatasetId) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:get_dataset_info(SessionId, DatasetId) of
        {ok, _} -> true;
        {error, Type} = Error ->
            case atm_data_utils:is_error_ignored(Type) of
                true -> false;
                false -> throw(Error)
            end
    end.


-spec initial_listing_options() -> list_opts().
initial_listing_options() ->
    #{
        offset => 0
    }.

-spec encode_listing_options(list_opts()) -> json_utils:json_term().
encode_listing_options(#{offset := Offset}) ->
    #{<<"offset">> => Offset}.

-spec decode_listing_options(json_utils:json_term()) -> list_opts().
decode_listing_options(#{<<"offset">> := Offset}) ->
    #{offset => Offset}.


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================

-spec compress(atm_api:item()) -> dataset:id().
compress(#{<<"datasetId">> := DatasetId}) -> DatasetId.


-spec expand(atm_workflow_execution_ctx:record(), dataset:id()) -> 
    {ok, atm_api:item()} | {error, term()}.
expand(AtmWorkflowExecutionCtx, DatasetId) ->
    SessionId = atm_workflow_execution_ctx:get_session_id(AtmWorkflowExecutionCtx),
    case lfm:get_dataset_info(SessionId, DatasetId) of
        {ok, DatasetInfo} -> {ok, dataset_utils:translate_dataset_info(DatasetInfo)};
        {error, _} = Error -> Error
    end.
