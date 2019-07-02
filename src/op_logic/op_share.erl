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

-behaviour(op_logic_behaviour).

-include("op_logic.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-export([op_logic_plugin/0]).
-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    exists/2,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).

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
%% Returns data signature for given request.
%% Returns a map with 'required', 'optional' and 'at_least_one' keys.
%% Under each of them, there is a map:
%%      Key => {type_constraint, value_constraint}
%% Which means how value of given Key should be validated.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(op_logic:req()) -> undefined | op_sanitizer:data_spec().
data_spec(#op_req{operation = create, gri = #gri{aspect = shared_dir}}) -> #{
    required => #{<<"name">> => {binary, non_empty}}
};

data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = get, gri = #gri{aspect = shared_dir}}) ->
    undefined;

data_spec(#op_req{operation = update, gri = #gri{aspect = instance}}) ->
    #{required => #{<<"name">> => {binary, non_empty}}};

data_spec(#op_req{operation = update, gri = #gri{aspect = shared_dir}}) ->
    #{required => #{<<"name">> => {binary, non_empty}}};

data_spec(#op_req{operation = delete, gri = #gri{aspect = instance}}) ->
    undefined;

data_spec(#op_req{operation = delete, gri = #gri{aspect = shared_dir}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves an entity from datastore based on its EntityId.
%% Should return ?ERROR_NOT_FOUND if the entity does not exist.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(op_logic:req()) ->
    {ok, op_logic:entity()} | entity_logic:error().
fetch_entity(#op_req{operation = create, gri = #gri{aspect = shared_dir}}) ->
    {ok, undefined};

fetch_entity(#op_req{operation = get, client = Client, gri = #gri{
    id = ShareId,
    aspect = instance
}}) ->
    fetch_share(Client, ShareId);

fetch_entity(#op_req{operation = get, gri = #gri{aspect = shared_dir}}) ->
    {ok, undefined};

fetch_entity(#op_req{operation = update, client = Client, gri = #gri{
    id = ShareId,
    aspect = instance
}}) ->
    fetch_share(Client, ShareId);

fetch_entity(#op_req{operation = update, gri = #gri{aspect = shared_dir}}) ->
    {ok, undefined};

fetch_entity(#op_req{operation = delete, client = Client, gri = #gri{
    id = ShareId,
    aspect = instance
}}) ->
    fetch_share(Client, ShareId);

fetch_entity(#op_req{operation = delete, gri = #gri{aspect = shared_dir}}) ->
    {ok, undefined}.


%%--------------------------------------------------------------------
%% @doc
%% Determines if given resource (aspect of entity) exists, based on
%% op logic request and prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec exists(op_logic:req(), entity_logic:entity()) -> boolean().
exists(_, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% Returns true as authorization is checked later by oz.
%% @end
%%--------------------------------------------------------------------
-spec authorize(op_logic:req(), entity_logic:entity()) -> boolean().
authorize(#op_req{client = ?NOBODY}, _) ->
    false;

authorize(#op_req{operation = create, client = Client, gri = #gri{
    id = DirGuid,
    aspect = shared_dir
}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:is_eff_space_member(Client, SpaceId);

authorize(#op_req{operation = get, client = Client, gri = #gri{aspect = instance}},
    #od_share{space = SpaceId}
) ->
    op_logic_utils:is_eff_space_member(Client, SpaceId);

authorize(#op_req{operation = get, client = Client, gri = #gri{
    id = DirGuid,
    aspect = shared_dir
}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:is_eff_space_member(Client, SpaceId);

authorize(#op_req{operation = update, client = Client, gri = #gri{aspect = instance}},
    #od_share{space = SpaceId}
) ->
    op_logic_utils:is_eff_space_member(Client, SpaceId);

authorize(#op_req{operation = update, client = Client, gri = #gri{
    id = DirGuid,
    aspect = shared_dir
}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:is_eff_space_member(Client, SpaceId);

authorize(#op_req{operation = delete, client = Client, gri = #gri{aspect = instance}},
    #od_share{space = SpaceId}
) ->
    op_logic_utils:is_eff_space_member(Client, SpaceId);

authorize(#op_req{operation = delete, client = Client, gri = #gri{
    id = DirGuid,
    aspect = shared_dir
}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:is_eff_space_member(Client, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Determines if given request can be further processed
%% (e.g. checks whether space is supported locally).
%% Should throw custom error if not (e.g. ?ERROR_SPACE_NOT_SUPPORTED).
%% @end
%%--------------------------------------------------------------------
-spec validate(op_logic:req(), entity_logic:entity()) -> ok | no_return().
validate(#op_req{operation = create, gri = #gri{id = Guid, aspect = shared_dir}}, _) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    op_logic_utils:ensure_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{aspect = instance}}, #od_share{
    space = SpaceId
}) ->
    op_logic_utils:ensure_space_supported_locally(SpaceId);

validate(#op_req{operation = get, gri = #gri{id = DirGuid, aspect = shared_dir}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:ensure_space_supported_locally(SpaceId);

validate(#op_req{operation = update, gri = #gri{aspect = instance}}, #od_share{
    space = SpaceId
}) ->
    op_logic_utils:ensure_space_supported_locally(SpaceId);

validate(#op_req{operation = update, gri = #gri{id = DirGuid, aspect = shared_dir}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:ensure_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{aspect = instance}}, #od_share{
    space = SpaceId
}) ->
    op_logic_utils:ensure_space_supported_locally(SpaceId);

validate(#op_req{operation = delete, gri = #gri{id = DirGuid, aspect = shared_dir}}, _) ->
    SpaceId = file_id:guid_to_space_id(DirGuid),
    op_logic_utils:ensure_space_supported_locally(SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Creates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec create(op_logic:req()) -> op_logic:create_result().
create(#op_req{client = Cl, gri = #gri{id = DirGuid, aspect = shared_dir}} = Req) ->
    SessionId = Cl#client.session_id,
    Name = maps:get(<<"name">>, Req#op_req.data),
    case logical_file_manager:create_share(SessionId, {guid, DirGuid}, Name) of
        {ok, {ShareId, _ShareGuid}} ->
            {ok, value, ShareId};
        {error, Errno} ->
            ?ERROR_POSIX(Errno)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Retrieves a resource (aspect of entity) based on op logic request and
%% prefetched entity.
%% @end
%%--------------------------------------------------------------------
-spec get(op_logic:req(), op_logic:entity()) -> op_logic:get_result().
get(#op_req{client = Cl, gri = #gri{id = DirGuid, aspect = shared_dir} = GRI} = Req, _) ->
    ShareId = fetch_share_id(Cl, DirGuid),
    case fetch_share(Cl, ShareId) of
        {ok, Share} ->
            get(Req#op_req{gri = GRI#gri{id = ShareId, aspect = instance}}, Share);
        ?ERROR_NOT_FOUND ->
            ?ERROR_NOT_FOUND
    end;

get(#op_req{gri = #gri{id = ShareId, aspect = instance}}, #od_share{
    space = SpaceId,
    root_file = RootFile,
    name = ShareName,
    public_url = SharePublicUrl,
    handle = Handle
}) ->
    {ok, ObjectId} = file_id:guid_to_objectid(RootFile),

    {ok, #{
        <<"shareId">> => ShareId,
        <<"name">> => ShareName,
        <<"publicUrl">> => SharePublicUrl,
        <<"rootFileId">> => ObjectId,
        <<"spaceId">> => SpaceId,
        <<"handleId">> => utils:ensure_defined(Handle, undefined, null)
    }}.


%%--------------------------------------------------------------------
%% @doc
%% Updates a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec update(op_logic:req()) -> op_logic:update_result().
update(#op_req{client = Cl, gri = #gri{id = DirGuid, aspect = shared_dir}} = Req) ->
    ShareId = fetch_share_id(Cl, DirGuid),
    NewName = maps:get(<<"name">>, Req#op_req.data),
    share_logic:update_name(Cl#client.session_id, ShareId, NewName);

update(#op_req{client = Cl, gri = #gri{id = ShareId, aspect = instance}} = Req) ->
    NewName = maps:get(<<"name">>, Req#op_req.data),
    share_logic:update_name(Cl#client.session_id, ShareId, NewName).


%%--------------------------------------------------------------------
%% @doc
%% Deletes a resource (aspect of entity) based on op logic request.
%% @end
%%--------------------------------------------------------------------
-spec delete(op_logic:req()) -> op_logic:delete_result().
delete(#op_req{client = Cl, gri = #gri{id = DirGuid, aspect = shared_dir}}) ->
    SessionId = Cl#client.session_id,
    ShareId = fetch_share_id(Cl, DirGuid),
    case logical_file_manager:remove_share(SessionId, ShareId) of
        ok -> ok;
        {error, Errno} -> ?ERROR_POSIX(Errno)
    end;

delete(#op_req{client = Cl, gri = #gri{id = ShareId, aspect = instance}}) ->
    case logical_file_manager:remove_share(Cl#client.session_id, ShareId) of
        ok -> ok;
        {error, Errno} -> ?ERROR_POSIX(Errno)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec fetch_share(op_logic:client(), od_space:id()) ->
    {ok, #od_share{}} | ?ERROR_NOT_FOUND.
fetch_share(#client{session_id = SessionId}, ShareId) ->
    case share_logic:get(SessionId, ShareId) of
        {ok, #document{value = Share}} ->
            {ok, Share};
        _ ->
            ?ERROR_NOT_FOUND
    end.


%% @private
-spec fetch_share_id(op_logic:client(), file_id:file_guid()) ->
    od_share:id() | ?ERROR_NOT_FOUND.
fetch_share_id(#client{session_id = SessionId}, DirGuid) ->
    case logical_file_manager:stat(SessionId, {guid, DirGuid}) of
        {ok, #file_attr{shares = [ShareId]}} ->
            ShareId;
        _ ->
            throw(?ERROR_NOT_FOUND)
    end.
