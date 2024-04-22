%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_value` functionality for `atm_group_type`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_group_value).
-author("Bartosz Walkowicz").

-behaviour(atm_value).

-include("graph_sync/provider_graph_sync.hrl").
-include("modules/automation/atm_execution.hrl").

%% atm_value callbacks
-export([
    validate_constraints/3,
    to_store_item/2,
    from_store_item/3,
    describe_store_item/3,
    transform_to_data_spec_conformant/3
]).


%%%===================================================================
%%% atm_value callbacks
%%%===================================================================


-spec validate_constraints(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_group_data_spec:record()
) ->
    ok | no_return().
validate_constraints(AtmWorkflowExecutionAuth, #{<<"groupId">> := GroupId} = Value, _AtmDataSpec) ->
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),
    SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
    UserId = atm_workflow_execution_auth:get_user_id(AtmWorkflowExecutionAuth),

    case space_logic:can_view_group_through_space(SessionId, SpaceId, UserId, GroupId) of
        true ->
            ok;
        false ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(Value, atm_group_type, ?ATM_ACCESS_CONSTRAINT))
    end.


-spec to_store_item(automation:item(), atm_group_data_spec:record()) ->
    atm_store:item().
to_store_item(#{<<"groupId">> := GroupId}, _AtmDataSpec) ->
    GroupId.


-spec from_store_item(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_group_data_spec:record()
) ->
    {ok, automation:item()}.
from_store_item(_AtmWorkflowExecutionAuth, GroupId, _AtmDataSpec) ->
    {ok, #{<<"groupId">> => GroupId}}.


-spec describe_store_item(
    atm_workflow_execution_auth:record(),
    atm_store:item(),
    atm_group_data_spec:record()
) ->
    {ok, automation:item()} | errors:error().
describe_store_item(AtmWorkflowExecutionAuth, GroupId, _AtmDataSpec) ->
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),
    SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),

    case group_logic:get_shared_data(SessionId, GroupId, ?THROUGH_SPACE(SpaceId)) of
        {ok, #document{value = #od_group{name = GroupName, type = GroupType}}} ->
            {ok, #{
                <<"groupId">> => GroupId,
                <<"name">> => GroupName,
                <<"type">> => GroupType
            }};
        {error, _} = Error ->
            Error
    end.


-spec transform_to_data_spec_conformant(
    atm_workflow_execution_auth:record(),
    automation:item(),
    atm_group_data_spec:record()
) ->
    automation:item().
transform_to_data_spec_conformant(
    AtmWorkflowExecutionAuth,
    #{<<"groupId">> := GroupId} = Value,
    #atm_group_data_spec{attributes = Attributes} = AtmDataSpec
) ->
    case describe_store_item(AtmWorkflowExecutionAuth, GroupId, AtmDataSpec) of
        {ok, FullItem} ->
            maps:with(lists:map(fun atm_group_data_spec:attribute_name_to_json/1, Attributes), FullItem);
        {error, _} ->
            throw(?ERROR_ATM_DATA_VALUE_CONSTRAINT_UNVERIFIED(Value, atm_group_type, ?ATM_ACCESS_CONSTRAINT))
    end.
