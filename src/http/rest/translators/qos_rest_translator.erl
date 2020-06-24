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
get_response(#gri{id = QosEntryId}, QosData) ->
    {ExpressionRpn, QosData1} = maps:take(<<"expressionRpn">>, QosData),
    {ok, InfixExpression} = qos_expression:rpn_to_infix(ExpressionRpn),
    ?OK_REPLY(QosData1#{
        <<"expression">> => InfixExpression,
        <<"qosRequirementId">> => QosEntryId
    }).
