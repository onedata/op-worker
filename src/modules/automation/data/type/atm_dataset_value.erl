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

-include("modules/automation/atm_execution.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_data_validator callbacks
-export([assert_meets_constraints/3]).

%% atm_tree_forest_store_container_iterator callbacks
-export([
    list_tree/4
]).

%% atm_data_compressor callbacks
-export([compress/2, expand/3]).


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


-spec list_tree(
    atm_workflow_execution_auth:record(),
    recursive_listing:pagination_token() | undefined,
    atm_value:compressed(),
    atm_store_container_iterator:batch_size()
) ->
    {[atm_value:expanded()], recursive_listing:pagination_token() | undefined}.
list_tree(AtmWorkflowExecutionAuth, PrevToken, CompressedRoot, BatchSize) ->
    list_internal(AtmWorkflowExecutionAuth, CompressedRoot, 
        maps_utils:remove_undefined(#{limit => BatchSize, pagination_token => PrevToken})
    ).


%%%===================================================================
%%% atm_data_compressor callbacks
%%%===================================================================


-spec compress(atm_value:expanded(), atm_data_type:value_constraints()) ->
    dataset:id().
compress(#{<<"datasetId">> := DatasetId}, _ValueConstraints) -> DatasetId.


-spec expand(atm_workflow_execution_auth:record(), dataset:id(), atm_data_type:value_constraints()) ->
    {ok, atm_value:expanded()} | {error, term()}.
expand(AtmWorkflowExecutionAuth, DatasetId, _ValueConstraints) ->
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),
    try
        DatasetInfo = mi_datasets:get_info(SessionId, DatasetId),
        {ok, dataset_utils:dataset_info_to_json(DatasetInfo)}
    catch throw:Error ->
        Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec list_internal(atm_workflow_execution_auth:record(), atm_value:compressed(), dataset_req:recursive_listing_opts()) ->
    {[atm_value:expanded()], recursive_listing:pagination_token() | undefined}.
list_internal(AtmWorkflowExecutionAuth, CompressedRoot, Opts) ->
    UserCtx = user_ctx:new(atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth)),
    try
        SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
        {ok, #recursive_listing_result{entries = Entries, pagination_token = PaginationToken}} =
            dataset_req:list_recursively(SpaceId, CompressedRoot, Opts, UserCtx),
        MappedEntries = lists:map(fun({_Path, DatasetInfo}) -> 
            dataset_utils:dataset_info_to_json(DatasetInfo) 
        end, Entries),
        {MappedEntries, PaginationToken}
    catch _:Error ->
        case datastore_runner:normalize_error(Error) of
            not_found -> {[], undefined};
            ?EPERM -> {[], undefined};
            ?EACCES -> {[], undefined};
            _ -> error(Error)
        end
    end.


%% @private
-spec check_implicit_constraints(atm_workflow_execution_auth:record(), atm_value:expanded()) ->
    ok | no_return().
check_implicit_constraints(AtmWorkflowExecutionAuth, #{<<"datasetId">> := DatasetId} = Value) ->
    SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),

    try
        DatasetInfo = mi_datasets:get_info(SessionId, DatasetId),
        file_id:guid_to_space_id(DatasetInfo#dataset_info.root_file_guid)
    of
        SpaceId ->
            ok;
        _ ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(Value, atm_dataset_type, #{
                <<"inSpace">> => SpaceId
            }))
    catch throw:Error ->
        case middleware_utils:is_file_access_error(Error) of
            true ->
                throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(Value, atm_dataset_type, #{
                    <<"hasAccess">> => true
                }));
            false ->
                throw(Error)
        end
    end.
