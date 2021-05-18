%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for manipulating automation lambdas via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_lambda_logic).
-author("Michal Stanisz").

-include("middleware/middleware.hrl").
-include("graph_sync/provider_graph_sync.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").

-export([
    get/2, get_name/2, get_summary/2, get_description/2,
    get_operation_spec/2, get_argument_specs/2, get_result_specs/2
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec get(gs_client_worker:client(), od_atm_lambda:id()) ->
    {ok, od_atm_lambda:doc()} | errors:error().
get(SessionId, AtmLambdaId) ->
    gs_client_worker:request(SessionId, #gs_req_graph{
        operation = get,
        gri = #gri{type = od_atm_lambda, id = AtmLambdaId, aspect = instance, scope = private},
        subscribe = true
    }).


-spec get_name(gs_client_worker:client(), od_atm_lambda:id()) ->
    {ok, automation:name()} | errors:error().
get_name(SessionId, AtmLambdaId) ->
    case get(SessionId, AtmLambdaId) of
        {ok, #document{value = #od_atm_lambda{name = Name}}} ->
            {ok, Name};
        {error, _} = Error ->
            Error
    end.


-spec get_summary(gs_client_worker:client(), od_atm_lambda:id()) ->
    {ok, automation:summary()} | errors:error().
get_summary(SessionId, AtmLambdaId) ->
    case get(SessionId, AtmLambdaId) of
        {ok, #document{value = #od_atm_lambda{summary = Summary}}} ->
            {ok, Summary};
        {error, _} = Error ->
            Error
    end.


-spec get_description(gs_client_worker:client(), od_atm_lambda:id()) ->
    {ok, automation:description()} | errors:error().
get_description(SessionId, AtmLambdaId) ->
    case get(SessionId, AtmLambdaId) of
        {ok, #document{value = #od_atm_lambda{description = Description}}} ->
            {ok, Description};
        {error, _} = Error ->
            Error
    end.


-spec get_operation_spec(gs_client_worker:client(), od_atm_lambda:id()) ->
    {ok, atm_lambda_operation_spec:record()} | errors:error().
get_operation_spec(SessionId, AtmLambdaId) ->
    case get(SessionId, AtmLambdaId) of
        {ok, #document{value = #od_atm_lambda{operation_spec = OperationSpec}}} ->
            {ok, OperationSpec};
        {error, _} = Error ->
            Error
    end.


-spec get_argument_specs(gs_client_worker:client(), od_atm_lambda:id()) ->
    {ok, [atm_lambda_argument_spec:record()]} | errors:error().
get_argument_specs(SessionId, AtmLambdaId) ->
    case get(SessionId, AtmLambdaId) of
        {ok, #document{value = #od_atm_lambda{argument_specs = ArgumentSpecs}}} ->
            {ok, ArgumentSpecs};
        {error, _} = Error ->
            Error
    end.


-spec get_result_specs(gs_client_worker:client(), od_atm_lambda:id()) ->
    {ok, [atm_lambda_result_spec:record()]} | errors:error().
get_result_specs(SessionId, AtmLambdaId) ->
    case get(SessionId, AtmLambdaId) of
        {ok, #document{value = #od_atm_lambda{result_specs = ResultSpecs}}} ->
            {ok, ResultSpecs};
        {error, _} = Error ->
            Error
    end.
