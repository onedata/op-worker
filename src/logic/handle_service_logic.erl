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
%% Retrieves handle_service_info document.
%% Provided client should be authorised to access handle_service_info details.
%% @end
%%--------------------------------------------------------------------
-spec get(oz_endpoint:auth(), HandleServiceId :: handle_service_info:id()) ->
    {ok, datastore:document()} | datastore:get_error().
get(Auth, HandleServiceId) ->
    handle_service_info:get_or_fetch(Auth, HandleServiceId).
