%%%-------------------------------------------------------------------
%%% @author Łukasz Opioła
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module encapsulates all common logic available in provider.
%%% It is used to process entity requests is a standardized way, i.e.:
%%%     # checks existence of given entity
%%%     # checks authorization of client to perform certain action
%%%     # checks validity of data provided in the request
%%%     # handles all errors in a uniform way
%%% @end
%%%-------------------------------------------------------------------
-module(op_logic).
-author("Łukasz Opioła").
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

% Some of the types are just aliases for types from gs_protocol, this is
% for better readability of logic modules.
-type req() :: #el_req{}.
-type client() :: #client{}.
-type el_plugin() :: module().
-type operation() :: gs_protocol:operation().

% TODO fix types
-type entity_id() ::
    undefined | od_user:id() | od_group:id() | od_space:id() |
    od_share:id() | od_provider:id() | od_handle_service:id() | od_handle:id().
-type entity_type() ::
    od_user | od_group | od_space | od_share | od_provider |
    od_handle_service | od_handle | oz_privileges.
-type entity() ::
    undefined | #od_user{} | #od_group{} | #od_space{} |
    #od_share{} | #od_provider{} | #od_handle_service{} | #od_handle{}.

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
    el_plugin/0,
    operation/0,
    entity_id/0,
    entity_type/0,
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
-record(state, {
    req = #el_req{} :: req(),
    plugin = undefined :: el_plugin(),
    entity = undefined :: entity()
}).
-type state() :: #state{}.

%% API
-export([handle/1, handle/2]).
-export([is_authorized/2]).
-export([client_to_string/1]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles an entity logic request expressed by a #el_req{} record.
%% @end
%%--------------------------------------------------------------------
-spec handle(req()) -> result().
handle(ElReq) ->
    handle(ElReq, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Handles an entity logic request expressed by a #el_req{} record. Entity can
%% be provided if it was prefetched.
%% @end
%%--------------------------------------------------------------------
-spec handle(req(), Entity :: entity()) -> result().
handle(#el_req{gri = #gri{type = EntityType}} = ElReq, Entity) ->
    try
        ElPlugin = EntityType:entity_logic_plugin(),
        handle_unsafe(#state{req = ElReq, plugin = ElPlugin, entity = Entity})
    catch
        throw:Error ->
            Error;
        Type:Message ->
            ?error_stacktrace("Unexpected error in entity_logic - ~p:~p", [
                Type, Message
            ]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


%%--------------------------------------------------------------------
%% @doc
%% Return if given client is authorized to perform given request, as specified
%% in the #el_req{} record. Entity can be provided if it was prefetched.
%% @end
%%--------------------------------------------------------------------
-spec is_authorized(req(), entity()) -> boolean().
is_authorized(#el_req{gri = #gri{type = EntityType}} = ElReq, Entity) ->
    try
        ElPlugin = EntityType:entity_logic_plugin(),
        % Existence must be checked too, as sometimes authorization depends
        % on that.
        ensure_authorized(
            ensure_exists(#state{
                req = ElReq, plugin = ElPlugin, entity = Entity
            })),
        true
    catch
        _:_ ->
            false
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns a readable string representing provided client.
%% @end
%%--------------------------------------------------------------------
-spec client_to_string(Client :: client()) -> string().
client_to_string(?NOBODY) -> "nobody (unauthenticated client)";
client_to_string(?ROOT) -> "root";
client_to_string(?USER(UId)) -> str_utils:format("user:~s", [UId]);
client_to_string(?PROVIDER(PId)) -> str_utils:format("provider:~s", [PId]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles an entity logic request based on operation,
%% should be wrapped in a try-catch.
%% @end
%%--------------------------------------------------------------------
-spec handle_unsafe(state()) -> result().
handle_unsafe(State = #state{req = Req = #el_req{operation = create}}) ->
    Result = call_create(
        ensure_authorized(
            fetch_entity(
                ensure_valid(
                    ensure_operation_supported(
                        State))))),
    case {Result, Req} of
        {{ok, resource, Resource}, #el_req{gri = #gri{aspect = instance}, client = Cl}} ->
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

handle_unsafe(State = #state{req = #el_req{operation = get}}) ->
    call_get(
        ensure_authorized(
            ensure_exists(
                fetch_entity(
                    ensure_valid(
                        ensure_operation_supported(
                            State))))));

handle_unsafe(State = #state{req = #el_req{operation = update}}) ->
    call_update(
        ensure_authorized(
            ensure_exists(
                fetch_entity(
                    ensure_valid(
                        ensure_operation_supported(
                            State))))));

handle_unsafe(State = #state{req = Req = #el_req{operation = delete}}) ->
    Result = call_delete(
        ensure_authorized(
            ensure_exists(
                fetch_entity(
                    ensure_valid(
                        ensure_operation_supported(
                            State)))))),
    case {Result, Req} of
        {ok, #el_req{gri = #gri{type = Type, id = _Id, aspect = instance}, client = Cl}} ->
            % If an entity instance is deleted, log an information about it
            % (it's a significant operation and this information might be useful).
            ?debug("~s has been deleted by client: ~s", [
                Type,
                client_to_string(Cl)
            ]),
            ok;
        _ ->
            Result
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves the entity specified in request by calling back proper entity
%% logic plugin. Does nothing if the entity is prefetched, or GRI of the
%% request is not related to any entity.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(state()) -> state().
fetch_entity(#state{entity = Entity} = State) when Entity /= undefined ->
    State;
fetch_entity(#state{req = #el_req{gri = #gri{id = undefined}}} = State) ->
    State;
fetch_entity(#state{plugin = Plugin, req = #el_req{gri = #gri{id = Id}}} = State) ->
    case Plugin:fetch_entity(Id) of
        {ok, Entity} ->
            State#state{entity = Entity};
        ?ERROR_NOT_FOUND ->
            throw(?ERROR_NOT_FOUND)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates an aspect of entity specified in request by calling back
%% proper entity logic plugin.
%% @end
%%--------------------------------------------------------------------
-spec call_create(state()) -> create_result().
call_create(#state{req = ElReq, plugin = Plugin, entity = Entity}) ->
    case Plugin:create(ElReq) of
        Fun when is_function(Fun, 1) ->
            Fun(Entity);
        Result ->
            Result
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Retrieves an aspect specified in request by calling back
%% proper entity logic plugin.
%% @end
%%--------------------------------------------------------------------
-spec call_get(state()) -> get_result().
call_get(#state{req = ElReq, plugin = Plugin, entity = Entity}) ->
    Plugin:get(ElReq, Entity).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates an aspect of entity specified in request by calling back
%% proper entity logic plugin.
%% @end
%%--------------------------------------------------------------------
-spec call_update(state()) -> update_result().
call_update(#state{req = ElReq, plugin = Plugin}) ->
    Plugin:update(ElReq).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes an aspect of entity specified in request by calling back
%% proper entity logic plugin.
%% @end
%%--------------------------------------------------------------------
-spec call_delete(state()) -> delete_result().
call_delete(#state{req = ElReq, plugin = Plugin}) ->
    Plugin:delete(ElReq).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Ensures requested operation is supported by calling back
%% proper entity logic plugin, throws a proper error of not.
%% @end
%%--------------------------------------------------------------------
-spec ensure_operation_supported(state()) -> state().
ensure_operation_supported(#state{req = Req, plugin = Plugin} = State) ->
    Result = try
        #el_req{operation = Op, gri = #gri{aspect = Asp, scope = Scp}} = Req,
        Plugin:operation_supported(Op, Asp, Scp)
    catch _:_ ->
        % No need for log here, 'operation_supported' may crash depending on
        % what the request contains and this is expected.
        false
    end,
    case Result of
        true -> State;
        false -> throw(?ERROR_NOT_SUPPORTED)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Ensures aspect of entity specified in request exists, throws on error.
%% @end
%%--------------------------------------------------------------------
-spec ensure_exists(state()) -> state().
ensure_exists(#state{req = #el_req{gri = #gri{id = undefined}}} = State) ->
    % Aspects where entity id is undefined always exist.
    State;
ensure_exists(#state{req = ElReq, plugin = Plugin, entity = Entity} = State) ->
    Result = try
        Plugin:exists(ElReq, Entity)
    catch _:_ ->
        % No need for log here, 'exists' may crash depending on what the
        % request contains and this is expected.
        false
    end,
    case Result of
        true -> State;
        false -> throw(?ERROR_NOT_FOUND)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Ensures client specified in request is authorized to perform the request,
%% throws on error.
%% @end
%%--------------------------------------------------------------------
-spec ensure_authorized(state()) -> state().
ensure_authorized(#state{req = #el_req{client = ?ROOT}} = State) ->
    % Root client is authorized to do everything (that client is only available
    % internally).
    State;
ensure_authorized(#state{req = ElReq, plugin = Plugin, entity = Entity} = State) ->
    Result = try
        Plugin:authorize(ElReq, Entity)
    catch _:_ ->
        % No need for log here, 'authorize' may crash depending on what the
        % request contains and this is expected.
        false
    end,
    case Result of
        true ->
            State;
        false ->
            case ElReq#el_req.client of
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
%% Ensures data specified in request is valid, throws on error.
%% @end
%%--------------------------------------------------------------------
-spec ensure_valid(state()) -> state().
ensure_valid(#state{
    req = #el_req{gri = #gri{aspect = Aspect}, data = Data} = Req,
    plugin = Plugin
} = State) ->
    ParamsSignature = Plugin:validate(Req),
    ParamsWithAspect = Data#{aspect => Aspect},
    SanitizedParams = op_validator:validate_params(ParamsWithAspect, ParamsSignature),
    State#state{req = Req#el_req{data = maps:remove(aspect, SanitizedParams)}}.
