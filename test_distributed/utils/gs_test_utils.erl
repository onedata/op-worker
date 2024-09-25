%%%-------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2015-2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------
%%% @doc
%%% Graph sync tests utils.
%%% @end
%%%-------------------------------------
-module(gs_test_utils).
-author("Katarzyna Such").

-include("api_test_runner.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").

-type gs_args() :: #gs_args{}.
-define(GS_RESP(Result), #gs_resp_graph{data = Result}).

%% API
-export([gs_request/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec gs_request(GsClient :: pid(), gs_args()) ->
    {ok, Result :: map()} | {error, term()}.
gs_request(GsClient, #gs_args{
    operation = Operation,
    gri = GRI,
    auth_hint = AuthHint,
    data = Data
}) ->
    case gs_client:graph_request(GsClient, GRI, Operation, Data, false, AuthHint) of
        {ok, ?GS_RESP(undefined)} ->
            ok;
        {ok, ?GS_RESP(Result)} ->
            {ok, Result};
        {error, _} = Error ->
            Error
    end.