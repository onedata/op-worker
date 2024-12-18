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

-include("modules/fslogic/fslogic_common.hrl").

-include("atm/atm_test_schema_drafts.hrl").

%% API
-export([exec/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec exec(DockerImage :: binary(), atm_task_executor:lambda_input()) ->
    json_utils:json_map().
exec(?ECHO_DOCKER_IMAGE_ID, #{<<"argsBatch">> := ArgsBatch}) ->
    #{<<"resultsBatch">> => ArgsBatch};

exec(?ECHO_WITH_SLEEP_DOCKER_IMAGE_ID, #{
    <<"ctx">> := #{<<"heartbeatUrl">> := HeartbeatUrl},
    <<"argsBatch">> := ArgsBatch
}) ->
    %% TODO VFS-10550 rm after fixing race when first heartbeat comes before job is registered
    timer:sleep(timer:seconds(1)),
    % Send heartbeat to inform op about job processing start

    Opts = [{ssl_options, [{cacerts, https_listener:get_cert_chain_ders()}]}],
    http_client:post(HeartbeatUrl, #{}, <<>>, Opts),

    timer:sleep(timer:seconds(12)),
    #{<<"resultsBatch">> => ArgsBatch};

exec(?ECHO_WITH_PAUSE_DOCKER_IMAGE_ID, #{
    <<"ctx">> := #{<<"atmWorkflowExecutionId">> := AtmWorkflowExecutionId},
    <<"argsBatch">> := ArgsBatch
}) ->
    atm_workflow_execution_api:init_pause(user_ctx:new(?ROOT_SESS_ID), AtmWorkflowExecutionId),
    #{<<"resultsBatch">> => ArgsBatch};

exec(?ECHO_WITH_EXCEPTION_ON_EVEN_NUMBERS, #{<<"argsBatch">> := ArgsBatch}) ->
    #{<<"resultsBatch">> => lists:map(fun
        (JobArgs = #{?ECHO_ARG_NAME := Num}) when Num rem 2 == 1 ->
            JobArgs;
        (_) ->
            #{<<"exception">> => <<"even number">>}
    end, ArgsBatch)};

exec(?ECHO_CONFIG_DOCKER_ID, #{<<"argsBatch">> := ArgsBatch, <<"ctx">> := #{
    <<"config">> := AtmLambdaExecutionConfig
}}) ->
    #{<<"resultsBatch">> => lists:map(fun(_) ->
        #{?ECHO_ARG_NAME => AtmLambdaExecutionConfig}
    end, ArgsBatch)};

exec(?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_1, #{<<"argsBatch">> := ArgsBatch}) ->
    #{<<"resultsBatch">> => lists:map(fun
        (#{<<"value">> := #{<<"tsName">> := <<"size">>}}) ->
            #{
                <<"schrodinger_cat">> => <<"dead">>,
                <<"schrodinger_dog">> => <<"dead">>
            };
        (Arg) ->
            Arg
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
    ?FAILING_ECHO_MEASUREMENTS_DOCKER_IMAGE_ID_4_ERROR_MSG.
