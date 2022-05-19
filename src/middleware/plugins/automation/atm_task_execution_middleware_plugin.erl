%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to automation task executions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_task_execution_middleware_plugin).
-author("Bartosz Walkowicz").

-behaviour(middleware_router).
-behaviour(middleware_handler).

-include("middleware/middleware.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("cluster_worker/include/modules/datastore/infinite_log.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").

%% middleware_router callbacks
-export([resolve_handler/3]).

%% middleware_handler callbacks
-export([data_spec/1, fetch_entity/1, authorize/2, validate/2]).
-export([create/1, get/2, update/1, delete/1]).

-define(MAX_POD_EVENT_LOG_LIST_LIMIT, 1000).
-define(DEFAULT_POD_EVENT_LOG_LIST_LIMIT, 1000).


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
resolve_handler(get, instance, private) -> ?MODULE;
resolve_handler(get, openfaas_function_activity_registry, private) -> ?MODULE;
resolve_handler(get, {openfaas_function_pod_event_log, _}, private) -> ?MODULE;

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
data_spec(#op_req{operation = get, gri = #gri{aspect = Aspect}}) when
    Aspect =:= instance;
    Aspect =:= openfaas_function_activity_registry
->
    undefined;
data_spec(#op_req{operation = get, gri = #gri{aspect = {openfaas_function_pod_event_log, _}}}) -> #{
    optional => #{
        <<"index">> => {binary, any},
        <<"offset">> => {integer, any},
        <<"limit">> => {integer, {between, 1, ?MAX_POD_EVENT_LOG_LIST_LIMIT}}
    }
}.


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

fetch_entity(#op_req{gri = #gri{id = AtmTaskExecutionId, scope = private}}) ->
    case atm_task_execution:get(AtmTaskExecutionId) of
        {ok, #document{value = AtmTaskExecution}} ->
            {ok, {AtmTaskExecution, 1}};
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

authorize(#op_req{operation = get, auth = Auth, gri = #gri{aspect = Aspect}}, #atm_task_execution{
    workflow_execution_id = AtmWorkflowExecutionId
}) when
    Aspect =:= instance;
    Aspect =:= openfaas_function_activity_registry;
    element(1, Aspect) =:= openfaas_function_pod_event_log
->
    atm_workflow_execution_middleware_plugin:has_access_to_workflow_execution_details(
        Auth, AtmWorkflowExecutionId
    ).


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback validate/2.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{aspect = Aspect}}, _) when
    Aspect =:= instance;
    Aspect =:= openfaas_function_activity_registry;
    element(1, Aspect) =:= openfaas_function_pod_event_log
->
    % Doc was already fetched in 'fetch_entity' so space must be supported locally
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_handler} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{aspect = instance, scope = private}}, AtmTaskExecution) ->
    {ok, AtmTaskExecution};

get(#op_req{gri = #gri{aspect = openfaas_function_activity_registry, scope = private}}, AtmTaskExecution) ->
    {ok, get_openfaas_function_activity_registry(AtmTaskExecution)};

get(#op_req{data = Data, gri = #gri{aspect = {openfaas_function_pod_event_log, PodId}}}, AtmTaskExecution) ->
    #atm_openfaas_function_activity_registry{
        pod_status_registry = PodStatusRegistry
    } = get_openfaas_function_activity_registry(AtmTaskExecution),

    atm_openfaas_function_pod_status_registry:has_summary(PodId, PodStatusRegistry) orelse throw(?ERROR_NOT_FOUND),

    #atm_openfaas_function_pod_status_summary{
        event_log = EventLogId
    } = atm_openfaas_function_pod_status_registry:get_summary(PodId, PodStatusRegistry),

    BrowseOpts = json_infinite_log_model:build_browse_opts(Data),

    {ok, BrowseResult} = atm_openfaas_function_activity_registry:browse_pod_event_log(
        EventLogId, BrowseOpts#{direction => ?BACKWARD}
    ),
    {ok, value, BrowseResult}.


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
%%% Internal functions
%%%===================================================================


%% @private
-spec get_openfaas_function_activity_registry(atm_task_execution:record()) ->
    atm_openfaas_function_activity_registry:record() | no_return().
get_openfaas_function_activity_registry(#atm_task_execution{executor = Executor}) ->
    case atm_task_executor:get_type(Executor) of
        atm_openfaas_task_executor ->
            ActivityRegistryId = atm_openfaas_task_executor:get_activity_registry_id(Executor),
            {ok, ActivityRegistry} = atm_openfaas_function_activity_registry:get(ActivityRegistryId),
            ActivityRegistry
    end.