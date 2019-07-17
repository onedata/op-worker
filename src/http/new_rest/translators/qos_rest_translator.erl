%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
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

-include("op_logic.hrl").
-include("http/rest/rest.hrl").

-export([create_response/4, get_response/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback create_response/4.
%% @end
%%--------------------------------------------------------------------
-spec create_response(op_logic:gri(), op_logic:auth_hint(),
    op_logic:data_format(), Result :: term() | {op_logic:gri(), term()} |
    {op_logic:gri(), op_logic:auth_hint(), term()}) -> #rest_resp{}.
create_response(#gri{aspect = instance}, _, value, QosId) ->
    PathTokens = [<<"qos-id">>, QosId],
    ?CREATED_REPLY(PathTokens, #{<<"qosId">> => QosId}).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(op_logic:gri(), Resource :: term()) -> #rest_resp{}.
get_response(_, QosData) ->
    ?OK_REPLY(QosData).
