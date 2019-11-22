%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module handles translation of op logic results concerning
%%% QoS management into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_rest_translator).
-author("Michal Cwiertnia").

-include("middleware/middleware.hrl").
-include("http/rest.hrl").

-export([create_response/4, get_response/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback create_response/4.
%% @end
%%--------------------------------------------------------------------
-spec create_response(gri:gri(), middleware:auth_hint(),
    middleware:data_format(), Result :: term() | {gri:gri(), term()} |
    {gri:gri(), middleware:auth_hint(), term()}) -> #rest_resp{}.
create_response(#gri{aspect = instance}, _, value, QosEntryId) ->
    PathTokens = [<<"qos-id">>, QosEntryId],
    ?CREATED_REPLY(PathTokens, #{<<"qosEntryId">> => QosEntryId}).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(_, QosData) ->
    ?OK_REPLY(QosData).
