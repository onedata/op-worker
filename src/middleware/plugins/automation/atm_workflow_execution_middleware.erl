%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to automation workflow executions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_workflow_execution_middleware).
-author("Bartosz Walkowicz").

-behaviour(middleware_plugin).

-include("middleware/middleware.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(middleware:operation(), gri:aspect(),
    middleware:scope()) -> boolean().
operation_supported(create, instance, private) -> true;

operation_supported(get, instance, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = instance}}) ->
    #{
        required => #{
            <<"spaceId">> => {binary, non_empty},
            <<"atmWorkflowSchemaId">> => {binary, non_empty}
        },
        optional => #{
            <<"storeInitialValues">> => {json, non_empty}
        }
    };

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback fetch_entity/1.
%%
%% For now fetches only records for authorized users.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{gri = #gri{id = AtmWorkflowExecutionId, scope = private}}) ->
    case atm_workflow_execution:get(AtmWorkflowExecutionId) of
        {ok, #document{value = AtmWorkflowExecution}} ->
            {ok, {AtmWorkflowExecution, 1}};
        {error, _} = Error ->
            Error
    end;
fetch_entity(_) ->
    ?ERROR_FORBIDDEN.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{auth = ?GUEST}, _) ->
    false;

authorize(#op_req{operation = create, auth = ?USER(UserId), data = Data, gri = #gri{
    aspect = instance
}}, _) ->
    % Check only space privileges as access checks for atm_workflow_schema and atm_lambda
    % will be performed later by fslogic layer
    SpaceId = maps:get(<<"spaceId">>, Data),
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_SCHEDULE_ATM_WORKFLOW_EXECUTIONS);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    aspect = instance
}}, #atm_workflow_execution{space_id = SpaceId}) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_ATM_WORKFLOW_EXECUTIONS).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, data = Data, gri = #gri{aspect = instance}}, _) ->
    SpaceId = maps:get(<<"spaceId">>, Data),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{aspect = instance}}, _) ->
    % Doc was already fetched in 'fetch_entity' so space must be supported locally
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = ?USER(_UserId, SessionId), data = Data, gri = #gri{aspect = instance} = GRI}) ->
    SpaceId = maps:get(<<"spaceId">>, Data),
    AtmWorkflowSchemaId = maps:get(<<"atmWorkflowSchemaId">>, Data),
    AtmStoreInitialValues = maps:get(<<"storeInitialValues">>, Data, #{}),

    {ok, AtmWorkflowExecutionId, AtmWorkflowExecution} = ?check_atm(lfm:schedule_atm_workflow_execution(
        SessionId, SpaceId, AtmWorkflowSchemaId, AtmStoreInitialValues
    )),
    {ok, resource, {GRI#gri{id = AtmWorkflowExecutionId}, AtmWorkflowExecution}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{aspect = instance, scope = private}}, AtmWorkflowExecution) ->
    {ok, AtmWorkflowExecution}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.
