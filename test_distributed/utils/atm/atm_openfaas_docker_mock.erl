%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains mock implementations of OpenFaaS lambda docker functions.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_docker_mock).
-author("Bartosz Walkowicz").

%% API
-export([exec/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec exec(DockerImage :: binary(), atm_task_executor:lambda_input()) ->
    atm_task_executor:lambda_output().
exec(<<"test/echo">>, #{<<"argsBatch">> := ArgsBatch}) ->
    #{<<"resultsBatch">> => ArgsBatch}.
