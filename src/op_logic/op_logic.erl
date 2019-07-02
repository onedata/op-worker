%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module encapsulates all common logic available in provider.
%%% It is used to process requests in a standardized way, i.e.:
%%%     # checks existence of given entity
%%%     # checks authorization of client to perform certain action
%%%     # checks validity of data provided in the request
%%%     # handles all errors in a uniform way
%%% @end
%%%-------------------------------------------------------------------
-module(op_logic).
-author("Lukasz Opiola").
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

% Some of the types are just aliases for types from gs_protocol, this is
% for better readability of logic modules.
-type req() :: #op_req{}.
-type client() :: #client{}.
-type op_plugin() :: module().
-type operation() :: gs_protocol:operation().
-type entity_id() :: undefined | od_share:id() | transfer:id().
-type entity() :: undefined | #od_share{} | #transfer{}.
-type aspect() :: gs_protocol:aspect().
-type scope() :: gs_protocol:scope().
-type data_format() :: gs_protocol:data_format().
-type data() :: gs_protocol:data().
-type gri() :: gs_protocol:gri().
-type auth_hint() :: gs_protocol:auth_hint().

-type create_result() :: gs_protocol:graph_create_result().
-type get_result() :: gs_protocol:graph_get_result().
-type delete_result() :: gs_protocol:graph_delete_result().
-type update_result() :: gs_protocol:graph_update_result().
-type result() :: create_result() | get_result() | update_result() | delete_result().
-type error() :: gs_protocol:error().

-export_type([
    client/0,
    op_plugin/0,
    operation/0,
    entity_id/0,
    entity/0,
    aspect/0,
    scope/0,
    gri/0,
    data_format/0,
    data/0,
    auth_hint/0,
    create_result/0,
    get_result/0,
    update_result/0,
    delete_result/0,
    error/0,
    result/0
]).

% Internal record containing the request data and state.
-record(req_ctx, {
    req = #op_req{} :: req(),
    plugin = undefined :: op_plugin(),
    entity = undefined :: entity()
}).
-type req_ctx() :: #req_ctx{}.

%% API
-export([handle/1, handle/2]).
-export([client_to_string/1]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv handle(OpReq, undefined).
%% @end
%%--------------------------------------------------------------------
-spec handle(req()) -> result().
handle(OpReq) ->
    handle(OpReq, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Handles an entity logic request expressed by a #op_req{} record.
%% Entity can be provided if it was prefetched.
%% @end
%%--------------------------------------------------------------------
-spec handle(req(), entity()) -> result().
handle(#op_req{gri = #gri{type = EntityType}} = OpReq, Entity) ->
    try
        ReqCtx0 = #req_ctx{
            req = OpReq,
            plugin = EntityType:op_logic_plugin(),
            entity = Entity
        },
        ensure_operation_supported(ReqCtx0),
        ReqCtx1 = sanitize_request(ReqCtx0),
        ReqCtx2 = maybe_fetch_entity(ReqCtx1),

        ensure_exists(ReqCtx2),
        ensure_authorized(ReqCtx2),
        validate_request(ReqCtx2),

        process_request(ReqCtx2)
    catch
        throw:Error ->
            Error;
        Type:Reason ->
            ?error_stacktrace("Unexpected error in op_logic - ~p:~p", [
                Type, Reason
            ]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns a readable string representing provided client.
%% @end
%%--------------------------------------------------------------------
-spec client_to_string(client()) -> string().
client_to_string(?NOBODY) -> "nobody (unauthenticated client)";
client_to_string(?ROOT) -> "root";
client_to_string(?USER(UId)) -> str_utils:format("user:~s", [UId]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Ensures requested operation is supported by calling back
%% proper op logic plugin, throws a proper error if not.
%% @end
%%--------------------------------------------------------------------
-spec ensure_operation_supported(req_ctx()) -> ok | no_return().
ensure_operation_supported(#req_ctx{plugin = Plugin, req = #op_req{
    operation = Op,
    gri = #gri{aspect = Asp, scope = Scp}
}}) ->
    Result = try
        Plugin:operation_supported(Op, Asp, Scp)
    catch _:_ ->
        % No need for log here, 'operation_supported' may crash depending on
        % what the request contains and this is expected.
        false
    end,
    case Result of
        true -> ok;
        false -> throw(?ERROR_NOT_SUPPORTED)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sanitizes data specified in request, throws on errors.
%% @end
%%--------------------------------------------------------------------
-spec sanitize_request(req_ctx()) -> req_ctx().
sanitize_request(#req_ctx{plugin = Plugin, req = #op_req{
    gri = #gri{aspect = Aspect},
    data = RawData
} = Req} = ReqCtx) ->
    case Plugin:data_spec(Req) of
        undefined ->
            ReqCtx;
        DataSpec ->
            DataWithAspect = RawData#{aspect => Aspect},
            SanitizedData = op_sanitizer:sanitize_data(DataWithAspect, DataSpec),
            ReqCtx#req_ctx{req = Req#op_req{
                data = maps:remove(aspect, SanitizedData)
            }}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves the entity specified in request by calling back proper op
%% logic plugin. Does nothing if the entity is prefetched, GRI of the
%% request is not related to any entity or callback is not
%% implemented by plugin.
%% @end
%%--------------------------------------------------------------------
-spec maybe_fetch_entity(req_ctx()) -> req_ctx().
maybe_fetch_entity(#req_ctx{entity = Entity} = ReqCtx) when Entity /= undefined ->
    ReqCtx;
maybe_fetch_entity(#req_ctx{req = #op_req{gri = #gri{id = undefined}}} = ReqCtx) ->
    ReqCtx;
maybe_fetch_entity(#req_ctx{plugin = Plugin, req = Req} = ReqCtx) ->
    case Plugin:fetch_entity(Req) of
        {ok, Entity} ->
            ReqCtx#req_ctx{entity = Entity};
        ?ERROR_NOT_FOUND ->
            throw(?ERROR_NOT_FOUND)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Ensures aspect of entity specified in request exists, throws on error.
%% @end
%%--------------------------------------------------------------------
-spec ensure_exists(req_ctx()) -> ok | no_return().
ensure_exists(#req_ctx{req = #op_req{operation = create}}) ->
    % No need to check for create operations.
    ok;
ensure_exists(#req_ctx{req = #op_req{gri = #gri{id = undefined}}}) ->
    % Aspects where entity id is undefined always exist.
    ok;
ensure_exists(#req_ctx{plugin = Plugin, req = OpReq, entity = Entity}) ->
    Result = try
        Plugin:exists(OpReq, Entity)
    catch _:_ ->
        % No need for log here, 'exists' may crash depending on what the
        % request contains and this is expected.
        false
    end,
    case Result of
        true -> ok;
        false -> throw(?ERROR_NOT_FOUND)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Ensures client specified in request is authorized to perform the request,
%% throws on error.
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized(req_ctx()) -> ok | no_return().
ensure_authorized(#req_ctx{req = #op_req{client = ?ROOT}}) ->
    % Root client is authorized to do everything (that client is only available
    % internally).
    ok;
ensure_authorized(#req_ctx{plugin = Plugin, req = OpReq, entity = Entity}) ->
    Result = try
        Plugin:authorize(OpReq, Entity)
    catch _:_ ->
        % No need for log here, 'authorize' may crash depending on what the
        % request contains and this is expected.
        false
    end,
    case Result of
        true ->
            ok;
        false ->
            case OpReq#op_req.client of
                ?NOBODY ->
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
validate_request(#req_ctx{plugin = Plugin, entity = Entity, req = Req}) ->
    ok = Plugin:validate(Req, Entity).


%%--------------------------------------------------------------------
%% @doc
%% Handles an entity logic request based on operation,
%% should be wrapped in a try-catch.
%% @end
%%--------------------------------------------------------------------
-spec process_request(req_ctx()) -> result().
process_request(#req_ctx{
    plugin = Plugin,
    req = #op_req{operation = create} = Req,
    entity = Entity
}) ->
    Result = case Plugin:create(Req) of
        Fun when is_function(Fun, 1) ->
            Fun(Entity);
        Res ->
            Res
    end,
    case {Result, Req} of
        {{ok, resource, Resource}, #op_req{gri = #gri{aspect = instance}, client = Cl}} ->
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
    end;

process_request(#req_ctx{
    plugin = Plugin, 
    req = #op_req{operation = get} = Req, 
    entity = Entity
}) ->
    Plugin:get(Req, Entity);

process_request(#req_ctx{
    plugin = Plugin, 
    req = #op_req{operation = update} = Req
}) ->
    Plugin:update(Req);

process_request(#req_ctx{
    plugin = Plugin, 
    req = #op_req{operation = delete, client = Cl, gri = GRI} = Req
}) ->
    case {Plugin:delete(Req), GRI} of
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
