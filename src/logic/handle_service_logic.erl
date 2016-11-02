%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Interface to provider's handle_service cache.
%%% Operations may involve interactions with OZ api
%%% or cached records from the datastore.
%%% @end
%%%-------------------------------------------------------------------
-module(handle_service_logic).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").

-export([get/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves od_handle_service document.
%% Provided client should be authorised to access od_handle_service details.
%% @end
%%--------------------------------------------------------------------
-spec get(oz_endpoint:auth(), HandleServiceId :: od_handle_service:id()) ->
    {ok, datastore:document()} | datastore:get_error().
get(Auth, HandleServiceId) ->
    od_handle_service:get_or_fetch(Auth, HandleServiceId).
