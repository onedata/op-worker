%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements entity logic plugin behaviour and handles
%%% entity logic operations corresponding to od_share model.
%%% @end
%%%-------------------------------------------------------------------
-module(op_share).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-export([fetch_entity/1, operation_supported/3]).
-export([create/1, get/2, update/1, delete/1]).
-export([exists/2, authorize/2, validate/1]).
-export([entity_logic_plugin/0]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the entity logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
entity_logic_plugin() ->
    op_share.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves an entity from datastore based on its EntityId.
%% Should return ?ERROR_NOT_FOUND if the entity does not exist.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(entity_logic:entity_id()) ->
    {ok, entity_logic:entity()} | op_logic:error().
fetch_entity(_) ->
    {ok, none}.


%%--------------------------------------------------------------------
%% @doc
%% Determines if given operation is supported based on operation, aspect and
%% scope (entity type is known based on the plugin itself).
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(op_logic:operation(), op_logic:aspect(),
    op_logic:scope()) -> boolean().
operation_supported(create, shared_dir, private) -> true;

operation_supported(get, instance, private) -> true;
operation_supported(get, shared_dir, private) -> true;

operation_supported(update, instance, private) -> true;
operation_supported(update, shared_dir, private) -> true;

operation_supported(delete, instance, private) -> true;
operation_supported(delete, shared_dir, private) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% Creates a resource (aspect of entity) based on entity logic request.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#el_req{client = Cl, gri = #gri{id = DirGuid, aspect = shared_dir}} = Req) ->
    ensure_space_supported(Req),
    Name = maps:get(<<"name">>, Req#el_req.data),
    case logical_file_manager:create_share(Cl#client.id, {guid, DirGuid}, Name) of
        {ok, {ShareId, _ShareGuid}} ->
            {ok, value, ShareId};
        {error, ?EINVAL} ->
            ?ERROR_BAD_VALUE_DIRECTORY;
        {error, ?EEXIST} ->
            ?ERROR_ALREADY_EXISTS;
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on entity logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), entity_logic:entity()) ->
    op_logic:get_result().
get(#el_req{client = Cl, gri = #gri{id = DirGuid, aspect = shared_dir} = GRI} = Req, Entity) ->
    case get_share_id(Cl#client.id, DirGuid) of
        {ok, ShareId} ->
            get(Req#el_req{gri = GRI#gri{id = ShareId, aspect = instance}}, Entity);
        Error ->
            Error
    end;
get(#el_req{client = Cl, gri = #gri{id = ShareId, aspect = instance}}, _) ->
    case share_logic:get(Cl#client.id, ShareId) of
        {ok, #document{value = Share}} ->
            ensure_space_supported(Share#od_share.space),

            {ok, ObjectId} = file_id:guid_to_objectid(Share#od_share.root_file),
            HandleId = utils:ensure_defined(Share#od_share.handle, undefined, null),

            {ok, #{
                <<"shareId">> => ShareId,
                <<"name">> => Share#od_share.name,
                <<"publicUrl">> => Share#od_share.public_url,
                <<"rootFileId">> => ObjectId,
                <<"spaceId">> => Share#od_share.space,
                <<"handleId">> => HandleId
            }};
        Error->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Updates a resource (aspect of entity) based on entity logic request.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(#el_req{client = Cl, gri = #gri{id = DirGuid, aspect = shared_dir}} = Req) ->
    ensure_space_supported(Req),
    case get_share_id(Cl#client.id, DirGuid) of
        {ok, ShareId} ->
            NewName = maps:get(<<"name">>, Req#el_req.data),
            share_logic:update_name(Cl#client.id, ShareId, NewName);
        Error ->
            Error
    end;
update(#el_req{client = Cl, gri = #gri{id = ShareId, aspect = instance}, data = Data} = Req) ->
    ensure_space_supported(Req),
    NewName = maps:get(<<"name">>, Data),
    share_logic:update_name(Cl#client.id, ShareId, NewName).


%%--------------------------------------------------------------------
%% @doc
%% Deletes a resource (aspect of entity) based on entity logic request.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(#el_req{client = Cl, gri = #gri{id = DirGuid, aspect = shared_dir}} = Req) ->
    ensure_space_supported(Req),
    case get_share_id(Cl#client.id, DirGuid) of
        {ok, ShareId} ->
            logical_file_manager:remove_share(Cl#client.id, ShareId);
        Error ->
            Error
    end;
delete(#el_req{client = Cl, gri = #gri{id = ShareId, aspect = instance}} = Req) ->
    ensure_space_supported(Req),
    logical_file_manager:remove_share(Cl#client.id, ShareId).


%%--------------------------------------------------------------------
%% @doc
%% Determines if given resource (aspect of entity) exists, based on entity
%% logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec exists(op_logic:req(), entity_logic:entity()) -> boolean().
exists(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% Determines if requesting client is authorized to perform given operation,
%% based on entity logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), entity_logic:entity()) -> boolean().
authorize(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% Returns validity verificators for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_verificator, value_verificator}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req()) -> op_validator:op_logic_params_signature().
validate(#el_req{operation = create}) -> #{
    required => #{<<"name">> => {binary, name}}
};
validate(#el_req{operation = update}) -> #{
    required => #{<<"name">> => {binary, name}}
};
validate(_) -> #{}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec ensure_space_supported(od_space:id() | op_logic:req()) -> ok | no_return().
ensure_space_supported(#el_req{gri = #gri{id = DirGuid, aspect = shared_dir}}) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    ensure_space_supported(SpaceId);
ensure_space_supported(#el_req{client = Cl, gri = #gri{id = ShareId, aspect = instance}}) ->
    case share_logic:get(Cl#client.id, ShareId) of
        {ok, #document{value = #od_share{space = SpaceId}}} ->
            ensure_space_supported(SpaceId);
        _ ->
            throw(?ERROR_NOT_FOUND)
    end;
ensure_space_supported(SpaceId) when is_binary(SpaceId) ->
    case provider_logic:supports_space(SpaceId) of
        true -> ok;
        false -> throw(?ERROR_SPACE_NOT_SUPPORTED)
    end.


-spec get_share_id(session:id(), file_id:file_guid()) ->
    od_share:id() | ?ERROR_NOT_FOUND.
get_share_id(SessionId, DirGuid) ->
    case logical_file_manager:stat(SessionId, {guid, DirGuid}) of
        {ok, #file_attr{shares = [ShareId]}} ->
            {ok, ShareId};
        _ ->
            ?ERROR_NOT_FOUND
    end.
