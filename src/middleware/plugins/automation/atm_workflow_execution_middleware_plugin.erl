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
-module(atm_workflow_execution_middleware_plugin).
-author("Bartosz Walkowicz").

-behaviour(middleware_router).
-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

%% middleware_router callbacks
-export([resolve_handler/3]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).

%% Utility functions
-export([has_access_to_workflow_execution_details/2]).


%%%===================================================================
%%% middleware_router callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_router} callback resolve_handler/3.
%% @end
%%--------------------------------------------------------------------
-spec resolve_handler(middleware:operation(), gri:aspect(), middleware:scope()) ->
    module() | no_return().
resolve_handler(create, instance, private) -> ?MODULE;
resolve_handler(create, cancel, private) -> ?MODULE;

resolve_handler(get, instance, private) -> ?MODULE;
resolve_handler(get, summary, private) -> ?MODULE;

resolve_handler(_, _, _) -> throw(?ERROR_NOT_SUPPORTED).


%%%===================================================================
%%% middleware_handler callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback data_spec/1.
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
            <<"storeInitialValues">> => {json, any},
            <<"callback">> => {binary, fun(Callback) -> url_utils:is_valid(Callback) end}
        }
    };
data_spec(#op_req{operation = create, gri = #gri{aspect = cancel}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = As}}) when
    As =:= instance;
    As =:= summary
->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback fetch_entity/1.
%%
%% For now fetches only records for authorized users.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{auth = ?NOBODY}) ->
    ?ERROR_UNAUTHORIZED;

fetch_entity(#op_req{gri = #gri{id = AtmWorkflowExecutionId, scope = private}}) ->
    case atm_workflow_execution_api:get(AtmWorkflowExecutionId) of
        {ok, AtmWorkflowExecution} ->
            {ok, {AtmWorkflowExecution, 1}};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback authorize/2.
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

authorize(#op_req{operation = create, auth = ?USER(UserId), gri = #gri{
    aspect = cancel
}}, #atm_workflow_execution{user_id = CreatorUserId, space_id = SpaceId}) ->
    UserId == CreatorUserId orelse space_logic:has_eff_privilege(
        SpaceId, UserId, ?SPACE_CANCEL_ATM_WORKFLOW_EXECUTIONS
    );

authorize(#op_req{operation = get, auth = Auth, gri = #gri{aspect = instance}}, AtmWorkflowExecution) ->
    has_access_to_workflow_execution_details(Auth, AtmWorkflowExecution);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    aspect = summary
}}, #atm_workflow_execution{space_id = SpaceId}) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_ATM_WORKFLOW_EXECUTIONS).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, data = Data, gri = #gri{aspect = instance}}, _) ->
    SpaceId = maps:get(<<"spaceId">>, Data),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = create, gri = #gri{aspect = cancel}}, _) ->
    % Doc was already fetched in 'fetch_entity' so space must be supported locally
    ok;

validate(#op_req{operation = get, gri = #gri{aspect = As}}, _) when
    As =:= instance;
    As =:= summary
->
    % Doc was already fetched in 'fetch_entity' so space must be supported locally
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = ?USER(_UserId, SessionId), data = Data, gri = #gri{aspect = instance} = GRI}) ->
    Result = lfm:schedule_atm_workflow_execution(
        SessionId,
        maps:get(<<"spaceId">>, Data),
        maps:get(<<"atmWorkflowSchemaId">>, Data),
        maps:get(<<"storeInitialValues">>, Data, #{}),
        maps:get(<<"callback">>, Data, undefined)
    ),
    case Result of
        {ok, AtmWorkflowExecutionId, AtmWorkflowExecution} ->
            {ok, resource, {GRI#gri{id = AtmWorkflowExecutionId}, AtmWorkflowExecution}};
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND;
        {error, ?ENOENT} ->
            ?ERROR_NOT_FOUND;
        {error, ?EACCES} ->
            ?ERROR_FORBIDDEN;
        {error, ?EPERM} ->
            ?ERROR_FORBIDDEN;
        {error, _} ->
            ?ERROR_MALFORMED_DATA
    end;

create(#op_req{auth = ?USER(_UserId, SessionId), gri = #gri{
    id = AtmWorkflowExecutionId,
    aspect = cancel
}}) ->
    case lfm:cancel_atm_workflow_execution(SessionId, AtmWorkflowExecutionId) of
        ok -> ok;
        {error, _} -> ?ERROR_MALFORMED_DATA
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{aspect = instance, scope = private}}, AtmWorkflowExecution) ->
    {ok, AtmWorkflowExecution};

get(#op_req{gri = #gri{
    id = AtmWorkflowExecutionId,
    aspect = summary,
    scope = private
}}, AtmWorkflowExecution) ->
    {ok, atm_workflow_execution_api:get_summary(AtmWorkflowExecutionId, AtmWorkflowExecution)}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.


%%%===================================================================
%%% Utility functions
%%%===================================================================


-spec has_access_to_workflow_execution_details(
    aai:auth(),
    atm_workflow_execution:record() | atm_workflow_execution:id()
) ->
    boolean().
has_access_to_workflow_execution_details(?GUEST, _) ->
    false;

has_access_to_workflow_execution_details(?USER(UserId, SessionId), #atm_workflow_execution{
    space_id = SpaceId,
    atm_inventory_id = AtmInventoryId
}) ->
    HasEffAtmInventory = user_logic:has_eff_atm_inventory(SessionId, UserId, AtmInventoryId),

    HasEffAtmInventory andalso space_logic:has_eff_privilege(
        SpaceId, UserId, ?SPACE_VIEW_ATM_WORKFLOW_EXECUTIONS
    );

has_access_to_workflow_execution_details(Auth, AtmWorkflowExecutionId) ->
    case atm_workflow_execution_api:get(AtmWorkflowExecutionId) of
        {ok, AtmWorkflowExecution} ->
            has_access_to_workflow_execution_details(Auth, AtmWorkflowExecution);
        {error, _} ->
            false
    end.
