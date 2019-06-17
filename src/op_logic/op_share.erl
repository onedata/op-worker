%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles op logic operations corresponding to op_share model.
%%% @end
%%%-------------------------------------------------------------------
-module(op_share).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-export([op_logic_plugin/0]).
-export([operation_supported/3]).
-export([create/1, get/2, update/1, delete/1]).
-export([authorize/2, data_signature/1]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns the op logic plugin module that handles model logic.
%% @end
%%--------------------------------------------------------------------
op_logic_plugin() ->
    op_share.


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
%% Creates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{client = Cl, gri = #gri{id = DirGuid, aspect = shared_dir}} = Req) ->
    ensure_space_supported(Req),
    Name = maps:get(<<"name">>, Req#op_req.data),
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
%% Retrieves a resource (aspect of entity) based on op logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{client = Cl, gri = #gri{id = DirGuid, aspect = shared_dir} = GRI} = Req, Entity) ->
    case get_share_id(Cl#client.id, DirGuid) of
        {ok, ShareId} ->
            get(Req#op_req{gri = GRI#gri{id = ShareId, aspect = instance}}, Entity);
        Error ->
            Error
    end;

get(#op_req{client = Cl, gri = #gri{id = ShareId, aspect = instance}}, _) ->
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
%% Updates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(#op_req{client = Cl, gri = #gri{id = DirGuid, aspect = shared_dir}} = Req) ->
    ensure_space_supported(Req),
    case get_share_id(Cl#client.id, DirGuid) of
        {ok, ShareId} ->
            NewName = maps:get(<<"name">>, Req#op_req.data),
            share_logic:update_name(Cl#client.id, ShareId, NewName);
        Error ->
            Error
    end;

update(#op_req{client = Cl, gri = #gri{id = ShareId, aspect = instance}, data = Data} = Req) ->
    ensure_space_supported(Req),
    NewName = maps:get(<<"name">>, Data),
    share_logic:update_name(Cl#client.id, ShareId, NewName).


%%--------------------------------------------------------------------
%% @doc
%% Deletes a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(#op_req{client = Cl, gri = #gri{id = DirGuid, aspect = shared_dir}} = Req) ->
    ensure_space_supported(Req),
    case get_share_id(Cl#client.id, DirGuid) of
        {ok, ShareId} ->
            logical_file_manager:remove_share(Cl#client.id, ShareId);
        Error ->
            Error
    end;

delete(#op_req{client = Cl, gri = #gri{id = ShareId, aspect = instance}} = Req) ->
    ensure_space_supported(Req),
    logical_file_manager:remove_share(Cl#client.id, ShareId).


%%--------------------------------------------------------------------
%% @doc
%% Returns true as authorization is checked later by oz.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), entity_logic:entity()) -> boolean().
authorize(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% Returns data signature for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_constraint, value_constraint}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-spec data_signature(op_logic:req()) -> op_validator:data_signature().
data_signature(#op_req{operation = create}) -> #{
    required => #{<<"name">> => {binary, name}}
};
data_signature(#op_req{operation = update}) -> #{
    required => #{<<"name">> => {binary, name}}
};
data_signature(_) -> #{}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec ensure_space_supported(od_space:id() | op_logic:req()) -> ok | no_return().
ensure_space_supported(#op_req{gri = #gri{id = DirGuid, aspect = shared_dir}}) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    ensure_space_supported(SpaceId);
ensure_space_supported(#op_req{client = Cl, gri = #gri{id = ShareId, aspect = instance}}) ->
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


%% @private
-spec get_share_id(session:id(), file_id:file_guid()) ->
    od_share:id() | ?ERROR_NOT_FOUND.
get_share_id(SessionId, DirGuid) ->
    case logical_file_manager:stat(SessionId, {guid, DirGuid}) of
        {ok, #file_attr{shares = [ShareId]}} ->
            {ok, ShareId};
        _ ->
            ?ERROR_NOT_FOUND
    end.
