%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Interface to provider's handle cache.
%%% Operations may involve interactions with OZ api
%%% or cached records from the datastore.
%%% @end
%%%-------------------------------------------------------------------
-module(handle_logic).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").
-include_lib("ctool/include/logging.hrl").

-export([get/2, create/5]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves handle_info document.
%% Provided client should be authorised to access handle_info details.
%% @end
%%--------------------------------------------------------------------
-spec get(oz_endpoint:auth(), HandleId :: od_handle:id()) ->
    {ok, datastore:doc()} | {error, term()}.
get(Auth, HandleId) ->
  od_handle:get_or_fetch(Auth, HandleId).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new handle for given user.
%% @end
%%--------------------------------------------------------------------
-spec create(oz_endpoint:auth(), HandleServiceId :: od_handle_service:id(),
    ResourceType :: od_handle:resource_type(),
    ResourceId :: od_handle:resource_id(),
    Metadata :: od_handle:metadata()) ->
    {ok, od_share:id()} | {error, Reason :: term()}.
create(Auth, HandleServiceId, ResourceType, ResourceId, Metadata) ->
    oz_handles:create(Auth, [
        {<<"handleServiceId">>, HandleServiceId},
        {<<"resourceType">>, ResourceType},
        {<<"resourceId">>, ResourceId},
        {<<"metadata">>, Metadata}
    ]).
