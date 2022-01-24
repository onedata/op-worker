%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Test implementation of docker images used in automation CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_test_docker_registry).
-author("Bartosz Walkowicz").

%% API
-export([exec/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec exec(DockerImage :: binary(), atm_task_executor:input()) ->
    atm_task_executor:outcome().
exec(<<"test/echo">>, #{<<"argsBatch">> := ArgsBatch}) ->
    #{<<"resultsBatch">> => ArgsBatch}.
