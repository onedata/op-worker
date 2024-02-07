%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO VFS-5621
%%% This module encapsulates all common logic available in provider.
%%% It is used to process requests in a standardized way, i.e.:
%%%     1) assert operation is supported.
%%%     2) sanitize request data.
%%%     3) fetch resource the request refers to.
%%%     4) check authorization.
%%%     5) check validity of request (e.g. whether space is locally supported).
%%%     6) process request.
%%% All this operations are carried out by middleware plugins (modules
%%% implementing `middleware_plugin` behaviour). Each such module is responsible
%%% for handling all request pointing to the same entity type (#gri.type field).
%%% @end
%%%-------------------------------------------------------------------
-module(middleware).
-author("Lukasz Opiola").
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

% Some of the types are just aliases for types from gs_protocol, this is
% for better readability of middleware modules.
% TODO VFS-5621
-type req() :: #op_req{}.
-type operation() :: gs_protocol:operation().
% The resource the request operates on (creates, gets, updates or deletes).
-type entity() :: undefined | tuple().
-type revision() :: gs_protocol:revision().
-type versioned_entity() :: gs_protocol:versioned_entity().
-type scope() :: gs_protocol:scope().
-type data_format() :: gs_protocol:data_format().
-type data() :: gs_protocol:data().
-type auth_hint() :: gs_protocol:auth_hint().

-type create_result() :: gs_protocol:graph_create_result().
-type get_result() :: gs_protocol:graph_get_result() | {ok, term()} | {ok, gri:gri(), term()}.
-type delete_result() :: gs_protocol:graph_delete_result().
-type update_result() :: gs_protocol:graph_update_result().
-type result() :: create_result() | get_result() | update_result() | delete_result().

-export_type([
    req/0,
    operation/0,
    entity/0,
    revision/0,
    versioned_entity/0,
    scope/0,
    data_format/0,
    data/0,
    auth_hint/0,
    create_result/0,
    get_result/0,
    update_result/0,
    delete_result/0,
    result/0
]).

% Internal record containing the request data and state.
-record(req_ctx, {
    req = #op_req{} :: req(),
    handler = undefined :: module(),
    versioned_entity = {undefined, 1} :: versioned_entity()
}).
-type req_ctx() :: #req_ctx{}.

%% API
-export([handle/1, handle/2]).
-export([is_authorized/2]).
-export([client_to_string/1]).
-export([load_known_atoms/0]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% @equiv handle(OpReq, {undefined, 1}).
%% @end
%%--------------------------------------------------------------------
-spec handle(req()) -> result().
handle(OpReq) ->
    handle(OpReq, {undefined, 1}).


%%--------------------------------------------------------------------
%% @doc
%% Handles an middleware request expressed by a #op_req{} record.
%% Entity can be provided if it was prefetched.
%% @end
%%--------------------------------------------------------------------
-spec handle(req(), versioned_entity()) -> result().
handle(#op_req{} = OpReq, VersionedEntity) ->
    try
        ReqCtx0 = #req_ctx{
            req = middleware_utils:switch_context_if_shared_file_request(OpReq),
            handler = get_handler(OpReq),
            versioned_entity = VersionedEntity
        },
        ReqCtx1 = sanitize_request(ReqCtx0),
        ReqCtx2 = maybe_fetch_entity(ReqCtx1),

        ensure_authorized(ReqCtx2),
        validate_request(ReqCtx2),
        process_request(ReqCtx2)
    catch
        % Intentional errors (throws) are be returned to client as is
        % (e.g. unauthorized, forbidden, space not supported, etc.)
        throw:Error ->
            Error;
        % Unexpected errors are logged and internal server error is returned
        % to client instead
        Type:Reason:Stacktrace ->
            ?error_stacktrace("Unexpected error in ~p - ~p:~p", [?MODULE, Type, Reason], Stacktrace),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


%%--------------------------------------------------------------------
%% @doc
%% Return if given client is authorized to perform given request, as specified
%% in the #op_req{} record. Entity can be provided if it was prefetched.
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(req(), versioned_entity()) ->
    {true, gri:gri()} | false.
is_authorized(#op_req{gri = GRI} = OpReq, VersionedEntity) ->
    try
        ensure_authorized(#req_ctx{
            req = OpReq,
            handler = get_handler(OpReq),
            versioned_entity = VersionedEntity
        }),
        {true, GRI}
    catch
        _:_ ->
            false
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns a readable string representing provided client.
%% @end
%%--------------------------------------------------------------------
-spec client_to_string(aai:auth()) -> string().
client_to_string(?GUEST) -> "nobody (unauthenticated client)";
client_to_string(?ROOT) -> "root";
client_to_string(?USER(UId)) -> str_utils:format("user:~s", [UId]).


%%--------------------------------------------------------------------
%% @doc
%% Loads atoms, that were found to be problematic (e.g. binary_to_existing_atom 
%% is called before loading a module with such an atom).
%% @end
%%--------------------------------------------------------------------
-spec load_known_atoms() -> [atom()].
load_known_atoms() ->
    [dir_stats_service_state].


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_handler(req()) -> module() | no_return().
get_handler(#op_req{operation = Operation, gri = #gri{
    type = EntityType,
    aspect = Aspect,
    scope = Scope
}}) ->
    Router = get_router(EntityType),

    try
        Router:resolve_handler(Operation, Aspect, Scope)
    catch _:_ ->
        % No need for log here, 'resolve_handler' may crash depending on
        % what the request contains and this is expected.
        throw(?ERROR_NOT_SUPPORTED)
    end.


%% @private
-spec get_router(gri:entity_type()) -> module() | no_return().
get_router(op_archive) -> archive_middleware_plugin;
get_router(op_atm_inventory) -> atm_inventory_middleware_plugin;
get_router(op_atm_lambda_snapshot) -> atm_lambda_snapshot_middleware_plugin;
get_router(op_atm_store) -> atm_store_middleware_plugin;
get_router(op_atm_task_execution) -> atm_task_execution_middleware_plugin;
get_router(op_atm_workflow_execution) -> atm_workflow_execution_middleware_plugin;
get_router(op_atm_workflow_schema) -> atm_workflow_schema_middleware_plugin;
get_router(op_atm_workflow_schema_snapshot) -> atm_workflow_schema_snapshot_middleware_plugin;
get_router(op_dataset) -> dataset_middleware_plugin;
get_router(op_file) -> file_middleware_router;
get_router(op_group) -> group_middleware_plugin;
get_router(op_handle) -> handle_middleware_plugin;
get_router(op_handle_service) -> handle_service_middleware_plugin;
get_router(op_metrics) -> metrics_middleware_plugin;
get_router(op_provider) -> provider_middleware_plugin;
get_router(op_qos) -> qos_middleware_plugin;
get_router(op_share) -> share_middleware_plugin;
get_router(op_space) -> space_middleware_router;
get_router(op_storage) -> storage_middleware_plugin;
get_router(op_transfer) -> transfer_middleware_plugin;
get_router(op_user) -> user_middleware_plugin;
get_router(_) -> throw(?ERROR_NOT_SUPPORTED).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sanitizes data specified in request, throws on errors.
%% @end
%%--------------------------------------------------------------------
-spec sanitize_request(req_ctx()) -> req_ctx().
sanitize_request(#req_ctx{handler = Handler, req = #op_req{
    gri = #gri{id = Id, aspect = Aspect},
    data = RawData
} = Req} = ReqCtx) ->
    case Handler:data_spec(Req) of
        undefined ->
            ReqCtx;
        DataSpec ->
            RawDataWithIdAndAspect = case RawData of
                undefined ->
                    #{id => Id, aspect => Aspect};
                _ when is_map(RawData) ->
                    RawData#{id => Id, aspect => Aspect};
                _ ->
                    throw(?ERROR_MALFORMED_DATA)
            end,
            SanitizedData = middleware_sanitizer:sanitize_data(
                RawDataWithIdAndAspect, DataSpec
            ),
            ReqCtx#req_ctx{req = Req#op_req{
                data = maps:without([id, aspect], SanitizedData)
            }}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves the entity specified in request by calling back proper
%% middleware handler. Does nothing if the entity is prefetched, GRI of the
%% request is not related to any entity or callback is not
%% implemented by handler.
%% @end
%%--------------------------------------------------------------------
-spec maybe_fetch_entity(req_ctx()) -> req_ctx().
maybe_fetch_entity(#req_ctx{versioned_entity = {Entity, _}} = ReqCtx) when Entity /= undefined ->
    ReqCtx;
maybe_fetch_entity(#req_ctx{req = #op_req{gri = #gri{id = undefined}}} = ReqCtx) ->
    % Skip when creating an instance with predefined Id, set revision to 1
    ReqCtx#req_ctx{versioned_entity = {undefined, 1}};
maybe_fetch_entity(#req_ctx{handler = Handler, req = Req} = ReqCtx) ->
    case Handler:fetch_entity(Req) of
        {ok, {_Entity, _Revision} = VersionedEntity} ->
            ReqCtx#req_ctx{versioned_entity = VersionedEntity};
        {error, _} = Error ->
            throw(Error)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Ensures client specified in request is authorized to perform the request,
%% throws on error.
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized(req_ctx()) -> ok | no_return().
ensure_authorized(#req_ctx{req = #op_req{auth = ?ROOT}}) ->
    % Root client is authorized to do everything (that client is only available
    % internally).
    ok;
ensure_authorized(#req_ctx{
    handler = Handler,
    versioned_entity = {Entity, _},
    req = #op_req{operation = Operation, auth = Auth, gri = GRI} = OpReq
}) ->
    case api_auth:check_authorization(Auth, ?OP_WORKER, Operation, GRI) of
        ok -> ok;
        {error, _} = Error -> throw(Error)
    end,

    Result = try
        Handler:authorize(OpReq, Entity)
    catch _:_ ->
        % No need for log here, 'authorize' may crash depending on what the
        % request contains and this is expected.
        false
    end,
    case Result of
        true ->
            ok;
        false ->
            case Auth of
                ?GUEST ->
                    % The client was not authenticated -> unauthorized
                    throw(?ERROR_UNAUTHORIZED);
                _ ->
                    % The client was authenticated but cannot access the
                    % aspect -> forbidden
                    throw(?ERROR_FORBIDDEN)
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Determines if given request can be further processed.
%% @end
%%--------------------------------------------------------------------
-spec validate_request(req_ctx()) -> ok | no_return().
validate_request(#req_ctx{handler = Handler, versioned_entity = {Entity, _}, req = Req}) ->
    ok = Handler:validate(Req, Entity).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles an middleware request based on operation,
%% should be wrapped in a try-catch.
%% @end
%%--------------------------------------------------------------------
-spec process_request(req_ctx()) -> result().
process_request(#req_ctx{
    handler = Handler,
    req = #op_req{operation = create, return_revision = RR} = Req
}) ->
    check_handler_function_exported(Handler, create, 1),
    Result = Handler:create(Req),
    case {Result, Req} of
        {{ok, resource, Resource}, #op_req{gri = #gri{aspect = instance}, auth = Cl}} ->
            % If an entity instance is created, log an information about it
            % (it's a significant operation and this information might be useful).
            {EntType, _EntId} = case Resource of
                {#gri{type = Type, id = Id}, _} -> {Type, Id};
                {#gri{type = Type, id = Id}, _, _} -> {Type, Id}
            end,
            ?debug("~s has been created by client: ~s", [
                EntType,
                client_to_string(Cl)
            ]),
            Result;
        _ ->
            Result
    end,

    case {Result, RR} of
        {{ok, resource, {ResultGri, ResultData}}, true} ->
            % TODO VFS-7344 use proper model/record revision number instead of 1 after implementing subscriptions
            {ok, resource, {ResultGri, {ResultData, 1}}};
        _ ->
            Result
    end;

process_request(#req_ctx{
    handler = Handler,
    req = #op_req{operation = get, return_revision = true} = Req,
    versioned_entity = {Entity, Rev}
}) ->
    check_handler_function_exported(Handler, get, 2),
    case Handler:get(Req, Entity) of
        {ok, value, _} = Res -> Res;
        {ok, ResultGri, Data} -> {ok, ResultGri, {Data, Rev}};
        {ok, Data} -> {ok, {Data, Rev}};
        {error, _} = Error -> Error
    end;

process_request(#req_ctx{
    handler = Handler,
    req = #op_req{operation = get} = Req,
    versioned_entity = {Entity, _}
}) ->
    check_handler_function_exported(Handler, get, 2),
    Handler:get(Req, Entity);

process_request(#req_ctx{
    handler = Handler,
    req = #op_req{operation = update} = Req
}) ->
    check_handler_function_exported(Handler, update, 1),
    Handler:update(Req);

process_request(#req_ctx{
    handler = Handler,
    req = #op_req{operation = delete, auth = Cl, gri = GRI} = Req
}) ->
    check_handler_function_exported(Handler, delete, 1),
    case {Handler:delete(Req), GRI} of
        {ok, #gri{type = Type, id = Id, aspect = instance}} ->
            % If an entity instance is deleted, log an information about it
            % (it's a significant operation and this information might be useful).
            ?debug("~s(~p) has been deleted by client: ~s", [
                Type, Id,
                client_to_string(Cl)
            ]),
            ok;
        {Result, _} ->
            Result
    end.


%% @private
-spec check_handler_function_exported(module(), atom(), non_neg_integer()) -> ok | no_return().
check_handler_function_exported(Handler, Function, Arity) ->
    case erlang:function_exported(Handler, Function, Arity) of
        true ->
            ok;
        false ->
            error({not_implemented, Handler, Function, Arity})
    end.
