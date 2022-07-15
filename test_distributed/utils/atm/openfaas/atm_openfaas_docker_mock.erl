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

-include("atm/atm_test_schema_drafts.hrl").

%% API
-export([exec/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec exec(DockerImage :: binary(), atm_task_executor:lambda_input()) ->
    atm_task_executor:lambda_output().
exec(?ECHO_DOCKER_IMAGE_ID, #{<<"argsBatch">> := ArgsBatch}) ->
    #{<<"resultsBatch">> => ArgsBatch};

exec(?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_1, #{<<"argsBatch">> := ArgsBatch}) ->
    #{<<"resultsBatch">> => lists:map(fun
        (#{<<"value">> := #{<<"tsName">> := <<"size">>}}) -> #{};
        (Arg) -> Arg
    end, ArgsBatch)};

exec(?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_2, #{<<"argsBatch">> := ArgsBatch}) ->
    #{<<"resultsBatch">> => lists:map(fun
        (#{<<"value">> := #{<<"tsName">> := <<"size">>}}) ->
            #{<<"value">> => ?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_2_RET_VALUE};
        (Arg) ->
            Arg
    end, ArgsBatch)};

exec(?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_3, #{<<"argsBatch">> := ArgsBatch}) ->
    #{<<"resultsBatch">> => lists:map(fun
        (#{<<"value">> := #{<<"tsName">> := <<"size">>}}) ->
            #{<<"exception">> => ?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_3_EXCEPTION};
        (Arg) ->
            Arg
    end, ArgsBatch)};

exec(?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_4, _) ->
    <<"signal: illegal instruction (core dumped)\n">>.


%%%===================================================================
%%% Internal functions
%%%===================================================================
