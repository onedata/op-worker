%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module handles translation of op logic results concerning
%%% replica entities into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(replica_rest_translator).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include("http/rest.hrl").

-export([create_response/4, get_response/2, delete_response/3]).


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
create_response(#gri{aspect = instance}, _, value, TransferId) ->
    PathTokens = [<<"transfers">>, TransferId],
    ?CREATED_REPLY(PathTokens, #{<<"transferId">> => TransferId});
create_response(#gri{aspect = replicate_by_index}, _, value, TransferId) ->
    PathTokens = [<<"transfers">>, TransferId],
    ?CREATED_REPLY(PathTokens, #{<<"transferId">> => TransferId}).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(op_logic:gri(), Resource :: term()) -> #rest_resp{}.
get_response(_, ReplicaData) ->
    ?OK_REPLY(ReplicaData).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback create_response/4.
%% @end
%%--------------------------------------------------------------------
-spec delete_response(op_logic:gri(), op_logic:data_format(),
    Result :: term() | {op_logic:gri(), term()} |
    {op_logic:gri(), op_logic:auth_hint(), term()}) -> #rest_resp{}.
delete_response(#gri{aspect = instance}, value, TransferId) ->
    PathTokens = [<<"transfers">>, TransferId],
    ?CREATED_REPLY(PathTokens, #{<<"transferId">> => TransferId});
delete_response(#gri{aspect = evict_by_index}, value, TransferId) ->
    PathTokens = [<<"transfers">>, TransferId],
    ?CREATED_REPLY(PathTokens, #{<<"transferId">> => TransferId}).
