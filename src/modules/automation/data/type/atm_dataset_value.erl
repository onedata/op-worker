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

-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/errors.hrl").

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
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_data_type:value_constraints()
) ->
    ok | no_return().
assert_meets_constraints(AtmWorkflowExecutionAuth, Value, _ValueConstraints) ->
    check_implicit_constraints(AtmWorkflowExecutionAuth, Value).


%%%===================================================================
%%% atm_tree_forest_store_container_iterator callbacks
%%%===================================================================


-spec list_children(atm_workflow_execution_auth:record(), dataset:id(), list_opts(), non_neg_integer()) ->
    {[{dataset:id(), dataset:name()}], [], list_opts(), IsLast :: boolean()} | no_return().
list_children(AtmWorkflowExecutionAuth, DatasetId, ListOpts, BatchSize) ->
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),
    case opl_datasets:list_children_datasets(SessionId, DatasetId, ListOpts#{limit => BatchSize}, undefined) of
        {ok, {Entries, IsLast}} when length(Entries) > 0 ->
            {_LastId, _LastName, LastIndex} = lists:last(Entries),
            ResultEntries = lists:map(fun({Id, Name, _}) -> {Id, Name} end, Entries),
            % all datasets are traversable, so returned list of nontraversable items is always empty
            % set offset to 1 to ensure that listing is exclusive
            {ResultEntries, [], #{offset => 1, start_index => LastIndex}, IsLast};
        {ok, {[], IsLast}} ->
            {[], [], #{}, IsLast};
        {error, Errno} ->
            case fslogic_errors:is_access_error(Errno) of
                true ->
                    {[], [], #{}, true};
                false ->
                    throw(?ERROR_POSIX(Errno))
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


-spec expand(atm_workflow_execution_auth:record(), dataset:id()) ->
    {ok, atm_value:expanded()} | {error, term()}.
expand(AtmWorkflowExecutionAuth, DatasetId) ->
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),
    case opl_datasets:get_info(SessionId, DatasetId) of
        {ok, DatasetInfo} -> {ok, dataset_utils:dataset_info_to_json(DatasetInfo)};
        {error, Errno} -> ?ERROR_POSIX(Errno)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_implicit_constraints(atm_workflow_execution_auth:record(), atm_value:expanded()) ->
    ok | no_return().
check_implicit_constraints(AtmWorkflowExecutionAuth, #{<<"datasetId">> := DatasetId} = Value) ->
    SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),

    case opl_datasets:get_info(SessionId, DatasetId) of
        {ok, #dataset_info{root_file_guid = RootFileGuid}} ->
            case file_id:guid_to_space_id(RootFileGuid) of
                SpaceId ->
                    ok;
                _ ->
                    throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                        Value, atm_dataset_type, #{<<"inSpace">> => SpaceId}
                    ))
            end;
        {error, Errno} ->
            case fslogic_errors:is_access_error(Errno) of
                true ->
                    throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(
                        Value, atm_dataset_type, #{<<"hasAccess">> => true}
                    ));
                false ->
                    throw(?ERROR_POSIX(Errno))
            end
    end.
