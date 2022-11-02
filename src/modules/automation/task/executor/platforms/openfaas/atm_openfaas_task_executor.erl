%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements `atm_task_executor` functionality for `openfaas`
%%% lambda operation engine.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_task_executor).
-author("Bartosz Walkowicz").

-behaviour(atm_task_executor).
-behaviour(persistent_record).

-include("http/gui_paths.hrl").
-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([get_pod_status_registry_id/1]).

%% atm_task_executor callbacks
-export([
    create/4,
    initiate/2,
    abort/2,
    teardown/2,
    delete/1,
    is_in_readonly_mode/1,
    run/3
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_openfaas_task_executor, {
    function_name :: function_name(),
    operation_spec :: atm_openfaas_operation_spec:record(),
    pod_status_registry_id :: atm_openfaas_function_pod_status_registry:id()
}).
-type record() :: #atm_openfaas_task_executor{}.

-record(initiation_ctx, {
    task_executor_initiation_ctx :: atm_task_executor:initiation_ctx(),
    resource_spec :: atm_resource_spec:record(),
    openfaas_config :: atm_openfaas_config:record(),
    executor :: record()
}).
-type initiation_ctx() :: #initiation_ctx{}.

-export_type([record/0]).


% function name submitted to OpenFaaS, used also as function's unique identifier
-type function_name() :: binary().
-export_type([function_name/0]).


-define(AWAIT_RETRIES, 300).
-define(AWAIT_INTERVAL_SEC, 1).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_pod_status_registry_id(record()) -> atm_openfaas_function_pod_status_registry:id().
get_pod_status_registry_id(#atm_openfaas_task_executor{pod_status_registry_id = PodStatusRegistryId}) ->
    PodStatusRegistryId.


%%%===================================================================
%%% atm_task_executor callbacks
%%%===================================================================


-spec create(
    atm_workflow_execution_ctx:record(),
    atm_lane_execution:index(),
    atm_task_schema:record(),
    atm_lambda_revision:record()
) ->
    record() | no_return().
create(AtmWorkflowExecutionCtx, _AtmLaneIndex, _AtmTaskSchema, AtmLambdaRevision) ->
    atm_openfaas_monitor:assert_openfaas_healthy(),

    FunctionName = build_function_name(AtmWorkflowExecutionCtx, AtmLambdaRevision),
    {ok, PodStatusRegistryId} = atm_openfaas_function_pod_status_registry:create_for_function(FunctionName),

    #atm_openfaas_task_executor{
        function_name = FunctionName,
        operation_spec = AtmLambdaRevision#atm_lambda_revision.operation_spec,
        pod_status_registry_id = PodStatusRegistryId
    }.


-spec initiate(atm_task_executor:initiation_ctx(), record()) ->
    workflow_engine:task_spec() | no_return().
initiate(AtmTaskExecutorInitiationCtx = #atm_task_executor_initiation_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    task_schema = AtmTaskSchema,
    lambda_revision = AtmLambdaRevision,
    uncorrelated_results = AtmTaskExecutionUncorrelatedResultNames
}, AtmTaskExecutor) ->
    InitiationCtx = #initiation_ctx{
        task_executor_initiation_ctx = AtmTaskExecutorInitiationCtx,
        resource_spec = select_resource_spec(AtmTaskSchema, AtmLambdaRevision),
        openfaas_config = atm_openfaas_config:get(),
        executor = AtmTaskExecutor
    },

    % Ensure there is no function with specified name registered in OpenFaaS service
    % (may be e.g. remnant of previous executions)
    remove_function(AtmWorkflowExecutionCtx, AtmTaskExecutor),
    case await_function_removal(InitiationCtx, ?AWAIT_RETRIES) of
        true -> ok;
        false -> throw(?ERROR_ATM_OPENFAAS_FUNCTION_REGISTRATION_FAILED)
    end,

    register_function(InitiationCtx),
    await_function_readiness(InitiationCtx),

    #{
        type => async,
        data_stream_enabled => not lists_utils:is_empty(AtmTaskExecutionUncorrelatedResultNames)
    }.


-spec abort(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().
abort(AtmWorkflowExecutionCtx, AtmTaskExecutor) ->
    remove_function(AtmWorkflowExecutionCtx, AtmTaskExecutor).


-spec teardown(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().
teardown(AtmWorkflowExecutionCtx, AtmTaskExecutor) ->
    remove_function(AtmWorkflowExecutionCtx, AtmTaskExecutor).


-spec delete(record()) -> ok | no_return().
delete(#atm_openfaas_task_executor{
    pod_status_registry_id = PodStatusRegistryId
}) ->
    ok = atm_openfaas_function_pod_status_registry:delete(PodStatusRegistryId).


-spec is_in_readonly_mode(record()) -> boolean().
is_in_readonly_mode(#atm_openfaas_task_executor{operation_spec = #atm_openfaas_operation_spec{
    docker_execution_options = #atm_docker_execution_options{readonly = Readonly}
}}) ->
    Readonly.


-spec run(atm_run_job_batch_ctx:record(), atm_task_executor:lambda_input(), record()) ->
    ok | no_return().
run(AtmRunJobBatchCtx, LambdaInput, AtmTaskExecutor) ->
    schedule_function_execution(AtmRunJobBatchCtx, LambdaInput, AtmTaskExecutor).


%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================


-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_openfaas_task_executor{
    function_name = FunctionName,
    operation_spec = OperationSpec,
    pod_status_registry_id = PodStatusRegistryId
}, NestedRecordEncoder) ->
    #{
        <<"functionName">> => FunctionName,
        <<"operationSpec">> => NestedRecordEncoder(OperationSpec, atm_openfaas_operation_spec),
        <<"podStatusRegistryId">> => PodStatusRegistryId
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"functionName">> := FunctionName,
    <<"operationSpec">> := OperationSpecJson
} = RecordJson, NestedRecordDecoder) ->
    #atm_openfaas_task_executor{
        function_name = FunctionName,
        operation_spec = NestedRecordDecoder(OperationSpecJson, atm_openfaas_operation_spec),
        pod_status_registry_id = maps:get(<<"podStatusRegistryId">>, RecordJson, <<"unknown">>)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec select_resource_spec(atm_task_schema:record(), atm_lambda_revision:record()) ->
    atm_resource_spec:record().
select_resource_spec(#atm_task_schema{resource_spec_override = undefined}, AtmLambdaRevision) ->
    AtmLambdaRevision#atm_lambda_revision.resource_spec;
select_resource_spec(#atm_task_schema{resource_spec_override = ResourceSpec}, _AtmLambdaRevision) ->
    ResourceSpec.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Build name for function under which it will be registered in OpenFaaS service.
%% To be accepted the name must consist of lower case alphanumeric characters
%% or '-', start with an alphabetic character, and end with an alphanumeric
%% character (e.g. 'my-name',  or 'abc-123', regex used for validation by
%% OpenFaaS service is '[a-z]([-a-z0-9]{0,61}[a-z0-9])?').
%% @end
%%--------------------------------------------------------------------
-spec build_function_name(atm_workflow_execution_ctx:record(), atm_lambda_revision:record()) ->
    binary().
build_function_name(AtmWorkflowExecutionCtx, #atm_lambda_revision{name = AtmLambdaRevisionName}) ->
    AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
        AtmWorkflowExecutionCtx
    ),
    % NOTE: end name with '-l' (for lambda) as a function name end marker.
    % It is used by openfaas-pod-monitor when inferring function name from pod name
    % (k8s adds its own suffix when generating pod name).
    Name = str_utils:format_bin("w~s-s~s-~s-l", [
        binary:part(AtmWorkflowExecutionId, 0, 10),
        % Generate random substring to ensure functions registered in OpenFaaS
        % are unique for each task despite e.g. using the same lambda
        str_utils:rand_hex(5),
        binary:part(AtmLambdaRevisionName, 0, min(size(AtmLambdaRevisionName), 37))
    ]),
    << <<(sanitize_character(Char))/integer>> || <<Char>> <= Name>>.


%% @private
-spec sanitize_character(integer()) -> integer().
sanitize_character(Char) when Char >= $a, Char =< $z -> Char;
sanitize_character(Char) when Char >= $A, Char =< $Z -> Char + 32;
sanitize_character(Char) when Char >= $0, Char =< $9 -> Char;
sanitize_character(_) -> $-.


%% @private
-spec is_function_registered(initiation_ctx()) -> boolean() | no_return().
is_function_registered(#initiation_ctx{
    openfaas_config = OpenfaasConfig,
    executor = #atm_openfaas_task_executor{function_name = FunctionName}
}) ->
    Endpoint = atm_openfaas_config:get_endpoint(
        OpenfaasConfig, <<"/system/function/", FunctionName/binary>>
    ),
    Headers = atm_openfaas_config:get_basic_auth_header(OpenfaasConfig),

    case http_client:get(Endpoint, Headers) of
        {ok, ?HTTP_200_OK, _RespHeaders, _RespBody} ->
            true;
        {ok, ?HTTP_404_NOT_FOUND, _RespHeaders, _RespBody} ->
            false;
        {ok, ?HTTP_500_INTERNAL_SERVER_ERROR, _RespHeaders, ErrorReason} ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED(ErrorReason));
        _ ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED)
    end.


%% @private
-spec register_function(initiation_ctx()) -> ok | no_return().
register_function(#initiation_ctx{openfaas_config = OpenfaasConfig} = InitiationCtx) ->
    log_function_registering(InitiationCtx),

    Endpoint = atm_openfaas_config:get_endpoint(OpenfaasConfig, <<"/system/functions">>),
    AuthHeaders = atm_openfaas_config:get_basic_auth_header(OpenfaasConfig),
    Payload = json_utils:encode(prepare_function_definition(InitiationCtx)),

    case http_client:post(Endpoint, AuthHeaders, Payload) of
        {ok, ?HTTP_202_ACCEPTED, _RespHeaders, _RespBody} ->
            log_function_registered(InitiationCtx);
        {ok, ?HTTP_400_BAD_REQUEST, _RespHeaders, ErrorReason} ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED(ErrorReason));
        {ok, ?HTTP_500_INTERNAL_SERVER_ERROR, _RespHeaders, ErrorReason} ->
            % Possible race with other task registering function
            % (Openfaas returns 500 if function already exists)
            case is_function_registered(InitiationCtx) of
                true -> ok;
                false -> throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED(ErrorReason))
            end;
        _ ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED)
    end.


%% @private
-spec log_function_registering(initiation_ctx()) -> ok.
log_function_registering(#initiation_ctx{
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx
    },
    executor = #atm_openfaas_task_executor{
        function_name = FunctionName,
        operation_spec = #atm_openfaas_operation_spec{docker_image = DockerImage}
    }
}) ->
    AtmWorkflowExecutionLogger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    atm_workflow_execution_logger:workflow_info(
        "Registering docker '~ts' as function '~ts' in OpenFaaS.", [DockerImage, FunctionName],
        AtmWorkflowExecutionLogger
    ).


%% @private
-spec log_function_registered(initiation_ctx()) -> ok.
log_function_registered(#initiation_ctx{
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx
    },
    executor = #atm_openfaas_task_executor{function_name = FunctionName}
}) ->
    AtmWorkflowExecutionLogger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    atm_workflow_execution_logger:workflow_info(
        "Function '~ts' registered in OpenFaaS.", [FunctionName], AtmWorkflowExecutionLogger
    ).


%% @private
-spec prepare_function_definition(initiation_ctx()) -> json_utils:json_map().
prepare_function_definition(InitiationCtx = #initiation_ctx{
    openfaas_config = OpenfaasConfig,
    executor = #atm_openfaas_task_executor{
        function_name = FunctionName,
        operation_spec = #atm_openfaas_operation_spec{docker_image = DockerImage}
    }
}) ->
    BaseDefinition = #{
        <<"service">> => FunctionName,
        <<"image">> => DockerImage,
        <<"namespace">> => atm_openfaas_config:get_function_namespace(OpenfaasConfig)
    },
    FullDefinition1 = add_default_properties(BaseDefinition),
    FullDefinition2 = add_resources_properties(FullDefinition1, InitiationCtx),
    FullDefinition3 = add_function_ctx_annotations(FullDefinition2, InitiationCtx),
    FullDefinition4 = add_data_stream_annotations_if_required(FullDefinition3, InitiationCtx),
    add_oneclient_annotations_if_required(FullDefinition4, InitiationCtx).


%% @private
-spec add_default_properties(json_utils:json_map()) -> json_utils:json_map().
add_default_properties(FunctionDefinition) ->
    lists:foldl(fun({Property, EnvVar}, Acc) ->
        case op_worker:get_env(EnvVar, undefined) of
            undefined ->
                Acc;
            Array when is_list(Array) ->
                Acc#{Property => lists:map(fun str_utils:to_binary/1, Array)};
            Map when is_map(Map) ->
                Acc#{Property => lists:foldl(fun({Key, Value}, Values) ->
                    Values#{str_utils:to_binary(Key) => str_utils:to_binary(Value)}
                end, #{}, maps:to_list(Map))};
            Value ->
                Acc#{Property => str_utils:to_binary(Value)}
        end
    end, FunctionDefinition, [
        {<<"envVars">>, openfaas_function_env},
        {<<"constraints">>, openfaas_function_constraints},
        {<<"labels">>, openfaas_function_labels},
        {<<"annotations">>, openfaas_function_annotations}
    ]).


%% @private
-spec add_resources_properties(json_utils:json_map(), initiation_ctx()) ->
    json_utils:json_map().
add_resources_properties(FunctionDefinition, #initiation_ctx{resource_spec = #atm_resource_spec{
    cpu_requested = CpuRequested,
    cpu_limit = CpuLimit,
    memory_requested = MemoryRequested,
    memory_limit = MemoryLimit,
    ephemeral_storage_requested = EphemeralStorageRequested,
    ephemeral_storage_limit = EphemeralStorageLimit
}}) ->
    Requests = #{
        <<"cpu">> => str_utils:to_binary(CpuRequested),
        <<"memory">> => str_utils:to_binary(MemoryRequested)
    },

    Limits1 = maps_utils:put_if_defined(#{}, <<"cpu">>, encode_if_defined(CpuLimit)),
    Limits2 = maps_utils:put_if_defined(Limits1, <<"memory">>, encode_if_defined(MemoryLimit)),

    EphemeralStorageAnnotations = maps_utils:put_if_defined(
        #{<<"function.openfaas.onedata.org/ephemeral_storage_requested">> => str_utils:to_binary(
            EphemeralStorageRequested
        )},
        <<"function.openfaas.onedata.org/ephemeral_storage_limit">>,
        encode_if_defined(EphemeralStorageLimit)
    ),

    insert_function_annotations(
        FunctionDefinition#{<<"requests">> => Requests, <<"limits">> => Limits2},
        EphemeralStorageAnnotations
    ).


%% @private
-spec encode_if_defined(undefined | term()) -> undefined | binary().
encode_if_defined(undefined) -> undefined;
encode_if_defined(Value) -> str_utils:to_binary(Value).


%% @private
-spec add_function_ctx_annotations(json_utils:json_map(), initiation_ctx()) ->
    json_utils:json_map().
add_function_ctx_annotations(FunctionDefinition, #initiation_ctx{
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        task_execution_id = AtmTaskExecutionId
    },
    executor = #atm_openfaas_task_executor{function_name = FunctionName}
}) ->
    AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
        AtmWorkflowExecutionCtx
    ),
    insert_function_annotations(FunctionDefinition, #{
        <<"function.openfaas.onedata.org/workflow_execution_id">> => AtmWorkflowExecutionId,
        <<"function.openfaas.onedata.org/task_execution_id">> => AtmTaskExecutionId,
        <<"function.openfaas.onedata.org/name">> => FunctionName
    }).


%% @private
-spec add_data_stream_annotations_if_required(json_utils:json_map(), initiation_ctx()) ->
    json_utils:json_map().
add_data_stream_annotations_if_required(FunctionDefinition, #initiation_ctx{
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        uncorrelated_results = []
    }
}) ->
    FunctionDefinition;

add_data_stream_annotations_if_required(FunctionDefinition, #initiation_ctx{
    openfaas_config = OpenfaasConfig,
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        uncorrelated_results = AtmTaskExecutionUncorrelatedResultNames
    }
}) ->
    Endpoint = oneprovider:build_url(wss, string:replace(
        ?OPENFAAS_ACTIVITY_FEED_WS_COWBOY_ROUTE, ":client_type", "result_streamer"
    )),
    ResultStreamerImage = atm_openfaas_config:get_result_streamer_image(OpenfaasConfig),
    ActivityFeedSecret = atm_openfaas_config:get_activity_feed_secret(OpenfaasConfig),

    insert_function_annotations(FunctionDefinition, #{
        <<"resultstream.openfaas.onedata.org/inject">> => <<"enabled">>,
        <<"resultstream.openfaas.onedata.org/image">> => ResultStreamerImage,
        <<"resultstream.openfaas.onedata.org/result_names">> => str_utils:join_binary(
            AtmTaskExecutionUncorrelatedResultNames, <<",">>
        ),
        <<"resultstream.openfaas.onedata.org/secret">> => ActivityFeedSecret,
        <<"resultstream.openfaas.onedata.org/server_websocket_endpoint">> => Endpoint
    }).


%% @private
-spec add_oneclient_annotations_if_required(json_utils:json_map(), initiation_ctx()) ->
    json_utils:json_map().
add_oneclient_annotations_if_required(FunctionDefinition, #initiation_ctx{
    executor = #atm_openfaas_task_executor{operation_spec = #atm_openfaas_operation_spec{
        docker_execution_options = #atm_docker_execution_options{mount_oneclient = false}
    }}}
) ->
    FunctionDefinition;

add_oneclient_annotations_if_required(FunctionDefinition, #initiation_ctx{
    openfaas_config = OpenfaasConfig,
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx
    },
    executor = AtmTaskExecutor = #atm_openfaas_task_executor{
        operation_spec = #atm_openfaas_operation_spec{
            docker_execution_options = #atm_docker_execution_options{
                mount_oneclient = true,
                oneclient_mount_point = MountPoint,
                oneclient_options = LambdaSpecificOneclientOptions
            }
        }
    }
}) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),
    SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
    AccessToken = atm_workflow_execution_auth:get_access_token(AtmWorkflowExecutionAuth),
    {ok, OpDomain} = provider_logic:get_domain(),

    OneclientImage = atm_openfaas_config:get_oneclient_image(OpenfaasConfig),
    EnvSpecificOneclientOptions = atm_openfaas_config:get_oneclient_options(OpenfaasConfig),

    insert_function_annotations(FunctionDefinition, #{
        <<"oneclient.openfaas.onedata.org/inject">> => <<"enabled">>,
        <<"oneclient.openfaas.onedata.org/image">> => OneclientImage,
        <<"oneclient.openfaas.onedata.org/space_id">> => SpaceId,
        <<"oneclient.openfaas.onedata.org/mount_point">> => MountPoint,
        <<"oneclient.openfaas.onedata.org/options">> => <<
            EnvSpecificOneclientOptions/binary, " ", LambdaSpecificOneclientOptions/binary
        >>,
        <<"oneclient.openfaas.onedata.org/oneprovider_host">> => OpDomain,
        <<"oneclient.openfaas.onedata.org/token">> => case is_in_readonly_mode(AtmTaskExecutor) of
            true -> tokens:confine(AccessToken, #cv_data_readonly{});
            false -> AccessToken
        end
    }).


%% @private
-spec insert_function_annotations(json_utils:json_map(), json_utils:json_map()) ->
    json_utils:json_map().
insert_function_annotations(FunctionDefinition, NewFunctionAnnotations) ->
    maps:update_with(
        <<"annotations">>,
        fun(Annotations) -> maps:merge(Annotations, NewFunctionAnnotations) end,
        NewFunctionAnnotations,
        FunctionDefinition
    ).


%% @private
-spec await_function_readiness(initiation_ctx()) -> ok | no_return().
await_function_readiness(InitiationCtx) ->
    await_function_readiness(InitiationCtx, ?AWAIT_RETRIES).


%% @private
-spec await_function_readiness(initiation_ctx(), non_neg_integer()) -> ok | no_return().
await_function_readiness(_InitiationCtx, 0) ->
    throw(?ERROR_ATM_OPENFAAS_FUNCTION_REGISTRATION_FAILED);

await_function_readiness(#initiation_ctx{
    openfaas_config = OpenfaasConfig,
    executor = #atm_openfaas_task_executor{function_name = FunctionName}
} = InitiationCtx, RetriesLeft) ->
    Endpoint = atm_openfaas_config:get_endpoint(
        OpenfaasConfig, <<"/system/function/", FunctionName/binary>>
    ),
    Headers = atm_openfaas_config:get_basic_auth_header(OpenfaasConfig),

    Result = case http_client:get(Endpoint, Headers) of
        {ok, ?HTTP_200_OK, _RespHeaders, EncodedRespBody} ->
            RespBody = json_utils:decode(EncodedRespBody),

            case maps:get(<<"availableReplicas">>, RespBody, 0) > 0 of
                true -> ready;
                false -> not_ready
            end;
        _ ->
            not_ready
    end,

    case Result of
        ready ->
            log_function_ready(InitiationCtx);
        not_ready ->
            assert_atm_workflow_execution_is_not_stopping(InitiationCtx),
            timer:sleep(timer:seconds(?AWAIT_INTERVAL_SEC)),
            await_function_readiness(InitiationCtx, RetriesLeft - 1)
    end.


%% @private
-spec assert_atm_workflow_execution_is_not_stopping(initiation_ctx()) -> ok | no_return().
assert_atm_workflow_execution_is_not_stopping(#initiation_ctx{
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx
    }
}) ->
    AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
        AtmWorkflowExecutionCtx
    ),
    case atm_workflow_execution:get(AtmWorkflowExecutionId) of
        {ok, #document{value = #atm_workflow_execution{status = ?STOPPING_STATUS}}} ->
            throw(?ERROR_ATM_WORKFLOW_EXECUTION_STOPPING);
        _ ->
            ok
    end.


%% @private
-spec log_function_ready(initiation_ctx()) -> ok.
log_function_ready(#initiation_ctx{
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx
    },
    executor = #atm_openfaas_task_executor{function_name = FunctionName}
}) ->
    AtmWorkflowExecutionLogger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    atm_workflow_execution_logger:workflow_info(
        "Function '~ts' ready to use in OpenFaaS.", [FunctionName], AtmWorkflowExecutionLogger
    ).


%% @private
-spec schedule_function_execution(
    atm_run_job_batch_ctx:record(),
    atm_task_executor:lambda_input(),
    record()
) ->
    ok | no_return().
schedule_function_execution(AtmRunJobBatchCtx, LambdaInput, #atm_openfaas_task_executor{
    function_name = FunctionName
}) ->
    OpenfaasConfig = atm_openfaas_config:get(),
    Endpoint = atm_openfaas_config:get_endpoint(
        OpenfaasConfig, <<"/async-function/", FunctionName/binary>>
    ),
    AuthHeaders = atm_openfaas_config:get_basic_auth_header(OpenfaasConfig),
    AllHeaders = AuthHeaders#{<<"X-Callback-Url">> => build_job_output_url(LambdaInput)},

    Body = json_utils:encode(#{
        <<"ctx">> => #{
            <<"heartbeatUrl">> => build_job_heartbeat_url(LambdaInput),
            <<"timeoutSeconds">> => op_worker:get_env(atm_workflow_job_timeout_sec, 1800),
            <<"oneproviderDomain">> => oneprovider:get_domain(),
            <<"accessToken">> => atm_run_job_batch_ctx:get_access_token(AtmRunJobBatchCtx)
        },
        <<"argsBatch">> => LambdaInput#atm_lambda_input.args_batch
    }),
    case http_client:post(Endpoint, AllHeaders, Body) of
        {ok, ?HTTP_202_ACCEPTED, _, _} ->
            ok;
        {ok, ?HTTP_500_INTERNAL_SERVER_ERROR, _RespHeaders, ErrorReason} ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED(ErrorReason));
        _ ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED)
    end.


%% @private
-spec build_job_output_url(atm_task_executor:lambda_input()) -> binary().
build_job_output_url(#atm_lambda_input{
    workflow_execution_id = AtmWorkflowExecutionId,
    job_batch_id = AtmJobBatchId
}) ->
    atm_openfaas_task_callback_handler:build_job_batch_output_url(AtmWorkflowExecutionId, AtmJobBatchId).


%% @private
-spec build_job_heartbeat_url(atm_task_executor:lambda_input()) -> binary().
build_job_heartbeat_url(#atm_lambda_input{
    workflow_execution_id = AtmWorkflowExecutionId,
    job_batch_id = AtmJobBatchId
}) ->
    atm_openfaas_task_callback_handler:build_job_batch_heartbeat_url(AtmWorkflowExecutionId, AtmJobBatchId).


%% @private
-spec remove_function(atm_workflow_execution_ctx:record(), record()) ->
    ok | no_return().
remove_function(AtmWorkflowExecutionCtx, #atm_openfaas_task_executor{
    function_name = FunctionName
}) ->
    OpenfaasConfig = atm_openfaas_config:get(),

    Endpoint = atm_openfaas_config:get_endpoint(OpenfaasConfig, <<"/system/functions">>),
    AuthHeaders = atm_openfaas_config:get_basic_auth_header(OpenfaasConfig),
    Payload = json_utils:encode(#{<<"functionName">> => FunctionName}),

    case http_client:delete(Endpoint, AuthHeaders, Payload) of
        {ok, ?HTTP_202_ACCEPTED, _, _} ->
            log_function_removed(AtmWorkflowExecutionCtx, FunctionName);
        {ok, ?HTTP_404_NOT_FOUND, _, _} ->
            ok;
        {ok, ?HTTP_400_BAD_REQUEST, _RespHeaders, ErrorReason} ->
            Error = ?ERROR_ATM_OPENFAAS_QUERY_FAILED(ErrorReason),
            log_function_removal_failed(AtmWorkflowExecutionCtx, FunctionName, Error);
        {ok, ?HTTP_500_INTERNAL_SERVER_ERROR, _RespHeaders, ErrorReason} ->
            Error = ?ERROR_ATM_OPENFAAS_QUERY_FAILED(ErrorReason),
            log_function_removal_failed(AtmWorkflowExecutionCtx, FunctionName, Error)
    end.


%% @private
-spec log_function_removed(atm_workflow_execution_ctx:record(), function_name()) ->
    ok.
log_function_removed(AtmWorkflowExecutionCtx, FunctionName) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    atm_workflow_execution_logger:workflow_info(
        "Function '~ts' removed from OpenFaaS.", [FunctionName], Logger
    ).


%% @private
-spec log_function_removal_failed(
    atm_workflow_execution_ctx:record(),
    function_name(),
    errors:error()
) ->
    ok.
log_function_removal_failed(AtmWorkflowExecutionCtx, FunctionName, Error) ->
    LogContent = #{
        <<"description">> => str_utils:format_bin(
            "Failed to remove function '~ts' from OpenFaaS.",
            [FunctionName]
        ),
        <<"reason">> => errors:to_json(Error)
    },
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    atm_workflow_execution_logger:workflow_warning(LogContent, Logger).


%% @private
-spec await_function_removal(initiation_ctx(), non_neg_integer()) -> boolean().
await_function_removal(_InitiationCtx, 0) ->
    false;

await_function_removal(InitiationCtx, RetriesLeft) ->
    case is_function_registered(InitiationCtx) of
        true ->
            assert_atm_workflow_execution_is_not_stopping(InitiationCtx),
            timer:sleep(timer:seconds(?AWAIT_INTERVAL_SEC)),
            await_function_removal(InitiationCtx, RetriesLeft - 1);
        false ->
            true
    end.
