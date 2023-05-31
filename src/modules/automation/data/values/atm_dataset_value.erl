%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_value` and `atm_tree_forest_container_iterator`
%%% functionality for `atm_dataset_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_dataset_value).
-author("Michal Stanisz").

-behaviour(atm_value).
-behaviour(atm_tree_forest_store_container_iterator).

-include("modules/automation/atm_execution.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/errors.hrl").

%% atm_value callbacks
-export([
    validate/3,
    to_store_item/2,
    from_store_item/3,
    describe/3,
    resolve_lambda_parameter/3
]).

%% atm_tree_forest_store_container_iterator callbacks
-export([list_tree/4]).


%%%===================================================================
%%% atm_value callbacks
%%%===================================================================


-spec validate(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_dataset_data_spec:record()
) ->
    ok | no_return().
validate(AtmWorkflowExecutionAuth, Value, _AtmDataSpec) ->
    resolve_internal(AtmWorkflowExecutionAuth, Value),
    ok.


-spec to_store_item(automation:item(), atm_dataset_data_spec:record()) ->
    atm_store:item().
to_store_item(#{<<"datasetId">> := DatasetId}, _AtmDataSpec) ->
    DatasetId.


-spec from_store_item(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_dataset_data_spec:record()
) ->
    {ok, automation:item()}.
from_store_item(_AtmWorkflowExecutionAuth, DatasetId, _AtmDataSpec) ->
    {ok, #{<<"datasetId">> => DatasetId}}.


-spec describe(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_dataset_data_spec:record()
) ->
    {ok, automation:item()} | errors:error().
describe(AtmWorkflowExecutionAuth, DatasetId, _AtmDataSpec) ->
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),

    ?catch_exceptions({ok, dataset_utils:dataset_info_to_json(mi_datasets:get_info(
        SessionId, DatasetId
    ))}).


-spec resolve_lambda_parameter(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_dataset_data_spec:record()
) ->
    automation:item().
resolve_lambda_parameter(AtmWorkflowExecutionAuth, Value, _AtmParameterDataSpec) ->
    DatasetInfo = resolve_internal(AtmWorkflowExecutionAuth, Value),
    dataset_utils:dataset_info_to_json(DatasetInfo).


%%%===================================================================
%%% atm_tree_forest_store_container_iterator callbacks
%%%===================================================================


-spec list_tree(
    atm_workflow_execution_auth:record(),
    recursive_listing:pagination_token() | undefined,
    atm_store:item(),
    atm_store_container_iterator:batch_size()
) ->
    {[automation:item()], recursive_listing:pagination_token() | undefined}.
list_tree(AtmWorkflowExecutionAuth, PrevToken, CompressedRoot, BatchSize) ->
    list_internal(AtmWorkflowExecutionAuth, CompressedRoot, 
        maps_utils:remove_undefined(#{limit => BatchSize, pagination_token => PrevToken})
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec list_internal(atm_workflow_execution_auth:record(), atm_store:item(), dataset_req:recursive_listing_opts()) ->
    {[automation:item()], recursive_listing:pagination_token() | undefined}.
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
-spec resolve_internal(atm_workflow_execution_auth:record(), automation:item()) ->
    dataset_api:info() | no_return().
resolve_internal(AtmWorkflowExecutionAuth, #{<<"datasetId">> := DatasetId} = Value) ->
    SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),

    try
        DatasetInfo = mi_datasets:get_info(SessionId, DatasetId),
        file_id:guid_to_space_id(DatasetInfo#dataset_info.root_file_guid)
    of
        SpaceId ->
            DatasetInfo;
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
