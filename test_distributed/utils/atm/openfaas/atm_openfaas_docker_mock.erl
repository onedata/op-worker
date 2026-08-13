%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022-2026 Onedata (onedata.org)
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

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_models.hrl").
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
    Opts = [{ssl_options, [{cacerts, https_listener:get_cert_chain_ders()}]}],

    % Report that job processing has started and then go silent, so that the job is
    % timed out. The heartbeat is dropped if it outruns the job's registration in the
    % workflow engine (TODO VFS-10550) - it is the finite enqueuing timeout set up by
    % atm_workflow_execution_test_mocks that makes the job time out either way.
    timer:sleep(timer:seconds(1)),
    http_client:post(HeartbeatUrl, #{}, <<>>, Opts),

    timer:sleep(timer:seconds(?ECHO_WITH_SLEEP_SILENCE_SEC)),
    #{<<"resultsBatch">> => ArgsBatch};

exec(?ECHO_UNTIL_STOPPING_DOCKER_IMAGE_ID, #{
    <<"ctx">> := #{
        <<"heartbeatUrl">> := HeartbeatUrl,
        <<"atmWorkflowExecutionId">> := AtmWorkflowExecutionId
    },
    <<"argsBatch">> := ArgsBatch
}) ->
    % NOTE: once op_worker has begun stopping there is no way for this answer to be
    % delivered - the https listener serving both the heartbeats and the job answers
    % is brought down before atm executions are asked to stop (see
    % node_manager_plugin:after_listeners_stop/0). Such a job is left to be timed out.
    heartbeat_until_workflow_execution_is_stopping(AtmWorkflowExecutionId, HeartbeatUrl),

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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec heartbeat_until_workflow_execution_is_stopping(atm_workflow_execution:id(), binary()) ->
    ok.
heartbeat_until_workflow_execution_is_stopping(AtmWorkflowExecutionId, HeartbeatUrl) ->
    timer:sleep(?ECHO_HEARTBEAT_INTERVAL_MILLIS),

    http_client:post(HeartbeatUrl, #{}, <<>>, [
        {ssl_options, [{cacerts, https_listener:get_cert_chain_ders()}]}
    ]),

    case is_workflow_execution_stopping(AtmWorkflowExecutionId) of
        true -> ok;
        false -> heartbeat_until_workflow_execution_is_stopping(AtmWorkflowExecutionId, HeartbeatUrl)
    end.


%% @private
-spec is_workflow_execution_stopping(atm_workflow_execution:id()) -> boolean().
is_workflow_execution_stopping(AtmWorkflowExecutionId) ->
    case atm_workflow_execution:get(AtmWorkflowExecutionId) of
        {ok, #document{value = #atm_workflow_execution{status = Status}}} ->
            Status =:= ?STOPPING_STATUS;
        {error, _} ->
            % the execution is gone - there is nothing left to answer to
            true
    end.
