%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% QoS management into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_rest_translator).
-author("Michal Cwiertnia").

-include("http/rest.hrl").
-include("middleware/middleware.hrl").

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
create_response(#gri{aspect = instance}, _, resource, {#gri{id = QosEntryId}, _}) ->
    PathTokens = [<<"qos-requirement">>, QosEntryId],
    ?CREATED_REPLY(PathTokens, #{<<"qosRequirementId">> => QosEntryId}).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(#gri{id = QosEntryId, aspect = instance}, QosData) ->
    {Expression, QosData1} = maps:take(<<"expression">>, QosData),
    ?OK_REPLY(QosData1#{
        <<"expression">> => qos_expression:to_infix(Expression),
        <<"qosRequirementId">> => QosEntryId
    });
get_response(#gri{aspect = {available_parameters, _}}, QosParameters) ->
    ?OK_REPLY(QosParameters).
