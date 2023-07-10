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
resolve_handler(create, pause, private) -> ?MODULE;
resolve_handler(create, resume, private) -> ?MODULE;
resolve_handler(create, force_continue, private) -> ?MODULE;
resolve_handler(create, retry, private) -> ?MODULE;
resolve_handler(create, rerun, private) -> ?MODULE;

resolve_handler(get, instance, private) -> ?MODULE;
resolve_handler(get, summary, private) -> ?MODULE;

resolve_handler(delete, instance, private) -> ?MODULE;
resolve_handler(delete, batch, private) -> ?MODULE;

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
            <<"atmWorkflowSchemaId">> => {binary, non_empty},
            <<"atmWorkflowSchemaRevisionNumber">> => {integer, {not_lower_than, 1}}
        },
        optional => #{
            <<"storeInitialContentOverlay">> => {json, any},
            <<"logLevel">> => {binary, ?AUDIT_LOG_SEVERITY_LEVELS},
            <<"callback">> => {binary, fun(Callback) -> url_utils:is_valid(Callback) end}
        }
    };

data_spec(#op_req{operation = create, gri = #gri{aspect = Aspect}}) when
    Aspect =:= cancel;
    Aspect =:= pause;
    Aspect =:= resume;
    Aspect =:= force_continue
->
    undefined;

data_spec(#op_req{operation = create, gri = #gri{aspect = Aspect}}) when
    Aspect =:= retry;
    Aspect =:= rerun
->
    #{required => #{
        <<"laneSchemaId">> => {binary, non_empty},
        <<"laneRunNumber">> => {integer, {not_lower_than, 1}}
    }};

data_spec(#op_req{operation = get, gri = #gri{aspect = As}}) when
    As =:= instance;
    As =:= summary
->
    undefined;

data_spec(#op_req{operation = delete, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = delete, gri = #gri{aspect = batch}}) ->
    #{required => #{
        <<"ids">> => {list_of_binaries, non_empty}
    }}.


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

fetch_entity(#op_req{operation = delete, gri = #gri{scope = private, aspect = As}}) when
    As =:= instance;
    As =:= batch
->
    % Do not fetch entity - it will be done by 'delete' callback later
    {ok, {undefined, 1}};

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

authorize(
    #op_req{operation = create, auth = ?USER(UserId), gri = #gri{aspect = Aspect}},
    #atm_workflow_execution{user_id = CreatorUserId, space_id = SpaceId}
) when
    Aspect =:= cancel;
    Aspect =:= pause;
    Aspect =:= resume;
    Aspect =:= force_continue;
    Aspect =:= retry;
    Aspect =:= rerun
->
    RequiredPrivilege = case UserId of
        CreatorUserId -> ?SPACE_SCHEDULE_ATM_WORKFLOW_EXECUTIONS;
        _ -> ?SPACE_MANAGE_ATM_WORKFLOW_EXECUTIONS
    end,
    space_logic:has_eff_privilege(SpaceId, UserId, RequiredPrivilege);

authorize(#op_req{operation = get, auth = Auth, gri = #gri{aspect = instance}}, AtmWorkflowExecution) ->
    has_access_to_workflow_execution_details(Auth, AtmWorkflowExecution);

authorize(#op_req{operation = get, auth = ?USER(UserId), gri = #gri{
    aspect = summary
}}, #atm_workflow_execution{space_id = SpaceId}) ->
    space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW_ATM_WORKFLOW_EXECUTIONS);

authorize(#op_req{operation = delete, gri = #gri{aspect = As}}, _) when
    As =:= instance;
    As =:= batch
->
    % Do not check authorization - it will be done by 'delete' callback later
    % to correctly handle 'batch' aspect
    true.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = create, data = Data, gri = #gri{aspect = instance}}, _) ->
    SpaceId = maps:get(<<"spaceId">>, Data),
    middleware_utils:assert_space_supported_locally(SpaceId);

validate(#op_req{operation = create, gri = #gri{aspect = Aspect}}, _) when
    Aspect =:= cancel;
    Aspect =:= pause;
    Aspect =:= resume;
    Aspect =:= force_continue;
    Aspect =:= retry;
    Aspect =:= rerun
->
    % Doc was already fetched in 'fetch_entity' so space must be supported locally
    ok;

validate(#op_req{operation = get, gri = #gri{aspect = As}}, _) when
    As =:= instance;
    As =:= summary
->
    % Doc was already fetched in 'fetch_entity' so space must be supported locally
    ok;

validate(#op_req{operation = delete, gri = #gri{aspect = As}}, _) when
    As =:= instance;
    As =:= batch
->
    % Do not validate request - it will be done by 'delete' callback later
    % to correctly handle 'batch' aspect
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(#op_req{auth = ?USER(_UserId, SessionId), data = Data, gri = #gri{aspect = instance} = GRI}) ->
    {AtmWorkflowExecutionId, AtmWorkflowExecution} = mi_atm:schedule_workflow_execution(
        SessionId,
        maps:get(<<"spaceId">>, Data),
        maps:get(<<"atmWorkflowSchemaId">>, Data),
        maps:get(<<"atmWorkflowSchemaRevisionNumber">>, Data),
        maps:get(<<"storeInitialContentOverlay">>, Data, #{}),
        audit_log:severity_to_int(maps:get(<<"logLevel">>, Data, ?INFO_AUDIT_LOG_SEVERITY)),
        maps:get(<<"callback">>, Data, undefined)
    ),
    {ok, resource, {GRI#gri{id = AtmWorkflowExecutionId}, AtmWorkflowExecution}};

create(#op_req{auth = ?USER(_UserId, SessionId), gri = #gri{
    id = AtmWorkflowExecutionId,
    aspect = cancel
}}) ->
    mi_atm:init_cancel_workflow_execution(SessionId, AtmWorkflowExecutionId);

create(#op_req{auth = ?USER(_UserId, SessionId), gri = #gri{
    id = AtmWorkflowExecutionId,
    aspect = pause
}}) ->
    mi_atm:init_pause_workflow_execution(SessionId, AtmWorkflowExecutionId);

create(#op_req{auth = ?USER(_UserId, SessionId), gri = #gri{
    id = AtmWorkflowExecutionId,
    aspect = resume
}}) ->
    mi_atm:resume_workflow_execution(SessionId, AtmWorkflowExecutionId);

create(#op_req{auth = ?USER(_UserId, SessionId), gri = #gri{
    id = AtmWorkflowExecutionId,
    aspect = force_continue
}}) ->
    mi_atm:force_continue_workflow_execution(SessionId, AtmWorkflowExecutionId);

create(#op_req{auth = ?USER(_UserId, SessionId), data = Data, gri = #gri{
    id = AtmWorkflowExecutionId,
    aspect = Aspect
}}) when
    Aspect =:= retry;
    Aspect =:= rerun
->
    {ok, Record} = atm_workflow_execution_api:get(AtmWorkflowExecutionId),

    case infer_lane_selector(maps:get(<<"laneSchemaId">>, Data), Record) of
        {ok, AtmLaneSelector} ->
            AtmLaneRunSelector = {AtmLaneSelector, maps:get(<<"laneRunNumber">>, Data)},

            mi_atm:repeat_workflow_execution(
                SessionId, Aspect, AtmWorkflowExecutionId, AtmLaneRunSelector
            );
        {error, _} = Error ->
            Error
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
delete(#op_req{auth = ?USER(UserId, SessionId), gri = #gri{
    id = AtmWorkflowExecutionId,
    aspect = instance
}}) ->
    AtmWorkflowExecution = ?check(atm_workflow_execution_api:get(AtmWorkflowExecutionId)),
    #atm_workflow_execution{space_id = SpaceId, user_id = CreatorUserId} = AtmWorkflowExecution,

    IsAuthorized = space_logic:has_eff_privilege(SpaceId, UserId, case UserId of
        CreatorUserId -> ?SPACE_SCHEDULE_ATM_WORKFLOW_EXECUTIONS;
        _ -> ?SPACE_MANAGE_ATM_WORKFLOW_EXECUTIONS
    end),
    IsAuthorized orelse throw(?ERROR_FORBIDDEN),

    mi_atm:discard_workflow_execution(SessionId, SpaceId, AtmWorkflowExecutionId);

delete(OpReq = #op_req{data = Data, gri = GRI = #gri{aspect = batch}}) ->
    {ok, value, lists:foldl(fun(AtmWorkflowExecutionId, Acc) ->
        Acc#{AtmWorkflowExecutionId => try
            delete(OpReq#op_req{data = #{}, gri = GRI#gri{
                id = AtmWorkflowExecutionId,
                aspect = instance
            }})
        catch throw:Error ->
            Error
        end}
    end, #{}, maps:get(<<"ids">>, Data))}.


%%%===================================================================
%%% Utility functions
%%%===================================================================


%% @private
-spec infer_lane_selector(automation:id(), atm_workflow_execution:record()) ->
    {ok, atm_lane_execution:index()} | ?ERROR_NOT_FOUND.
infer_lane_selector(AtmLaneSchemaId, #atm_workflow_execution{lanes = AtmLaneExecutions}) ->
    Result = lists:search(
        fun({_, #atm_lane_execution{schema_id = SchemaId}}) -> SchemaId == AtmLaneSchemaId end,
        maps:to_list(AtmLaneExecutions)
    ),
    case Result of
        {value, {Index, _}} -> {ok, Index};
        false -> ?ERROR_NOT_FOUND
    end.


-spec has_access_to_workflow_execution_details(
    aai:auth(),
    atm_workflow_execution:record()
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
    ).
