%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for reading and manipulating od_share records synchronized
%%% via Graph Sync. Requests are delegated to gs_client_worker, which decides
%%% if they should be served from cache or handled by OneZone.
%%% NOTE: This is the only valid way to interact with od_share records, to
%%% ensure consistency, no direct requests to datastore or OZ REST should
%%% be performed.
%%% @end
%%%-------------------------------------------------------------------
-module(share_logic).
-author("Lukasz Opiola").

-include("graph_sync/provider_graph_sync.hrl").
-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").

-export([get/2, get_public_data/2]).
-export([create/5, update_name/3, delete/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves share doc by given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec get(gs_client_worker:client(), od_share:id()) ->
    {ok, od_share:doc()} | gs_protocol:error().
get(SessionId, ShareId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_share, id = ShareId, aspect = instance},
        subscribe = true
    }).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves share doc restricted to public data by given SpaceId.
%% @end
%%--------------------------------------------------------------------
-spec get_public_data(gs_client_worker:client(), od_share:id()) ->
    {ok, od_share:doc()} | gs_protocol:error().
get_public_data(SessionId, ShareId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_share, id = ShareId, aspect = instance, scope = public},
        subscribe = true
    }).


-spec create(gs_client_worker:client(), od_share:id(), od_share:name(),
    od_space:id(), od_share:share_guid()) ->
    {ok, od_share:id()} | gs_protocol:error().
create(SessionId, ShareId, Name, SpaceId, ShareFileGuid) ->
    Res = ?CREATE_RETURN_ID(gs_client_worker:request(SessionId, #gs_req_graph{
        operation = create,
        gri = #gri{type = od_share, id = undefined, aspect = instance},
        data = #{
            <<"shareId">> => ShareId,
            <<"name">> => Name,
            <<"rootFileId">> => ShareFileGuid,
            <<"spaceId">> => SpaceId
        },
        subscribe = true
    })),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_space, SpaceId)
    end).


-spec update_name(gs_client_worker:client(), od_share:id(), od_share:name()) ->
    ok | gs_protocol:error().
update_name(SessionId, ShareId, NewName) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = update,
        gri = #gri{type = od_share, id = ShareId, aspect = instance},
        data = #{<<"name">> => NewName}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_share, ShareId)
    end).


-spec delete(gs_client_worker:client(), od_share:id()) -> ok | gs_protocol:error().
delete(SessionId, ShareId) ->
    Res = gs_client_worker:request(SessionId, #gs_req_graph{
        operation = delete,
        gri = #gri{type = od_share, id = ShareId, aspect = instance}
    }),
    ?ON_SUCCESS(Res, fun(_) ->
        gs_client_worker:invalidate_cache(od_share, ShareId)
    end).
