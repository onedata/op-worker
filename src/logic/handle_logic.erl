%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_handle records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by Onezone.
%%% NOTE: This is the only valid way to interact with od_handle records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(handle_logic).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-export([get_public_data/2]).
-export([force_fetch/2]).
-export([create/6, update/3]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Retrieves handle doc restricted to public data by given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec get_public_data(gs_client_worker:client(), od_handle:id()) ->
    {ok, od_handle:doc()} | errors:error().
get_public_data(SessionId, HandleId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_handle, id = HandleId, aspect = instance, scope = public},
        subscribe = true
    }).


-spec force_fetch(gs_client_worker:client(), od_handle:id()) -> {ok, od_handle:doc()}.
force_fetch(SessionId, HandleId) ->
    gs_client_worker:force_fetch_entity(SessionId, #gri{
        type = od_handle, id = HandleId, aspect = instance, scope = public
    }).


-spec create(
    gs_client_worker:client(), 
    od_handle_service:id(),
    od_handle:resource_type(), 
    od_handle:resource_id(),
    od_handle:metadata_prefix(),
    od_handle:metadata()
) ->
    {ok, od_handle:id()} | errors:error().
create(SessionId, HandleServiceId, ResourceType, ResourceId, MetadataPrefix, Metadata) ->
    {ok, UserId} = session:get_user_id(SessionId),
    Res = ?CREATE_RETURN_ID(gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_handle, id = undefined, aspect = instance},
        auth_hint = ?AS_USER(UserId),
        data = #{
            <<"handleServiceId">> => HandleServiceId,
            <<"resourceType">> => ResourceType,
            <<"resourceId">> => ResourceId,
            <<"metadataPrefix">> => MetadataPrefix,
            <<"metadata">> => Metadata
        },
        subscribe = true
    })),
    ?ON_SUCCESS(Res, fun(_) ->
        case ResourceType of
            <<"Share">> ->
                share_logic:force_fetch(ResourceId);
            _ ->
                ok
        end
    end).


-spec update(gs_client_worker:client(), od_handle:id(), od_handle:metadata()) ->
    ok | errors:error().
update(SessionId, HandleId, NewMetadata) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_handle, id = HandleId, aspect = instance, scope = private},
        data = #{<<"metadata">> => NewMetadata}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        force_fetch(SessionId, HandleId)
    end).
