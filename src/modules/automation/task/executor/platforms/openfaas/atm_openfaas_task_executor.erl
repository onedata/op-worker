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
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([get_pod_status_registry_id/1]).

%% atm_task_executor callbacks
-export([
    create/1,
    initiate/2,
    abort/2,
    teardown/2,
    delete/1,
    is_in_readonly_mode/1,
    run/3,
    trigger_stream_conclusion/2
]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type function_id() :: binary().  % <<_:23>>
% function name submitted to OpenFaaS
-type function_name() :: binary().

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

-export_type([function_id/0, record/0]).


-define(ANNOTATIONS_OMITTED_WHEN_LOGGING, [
    <<"oneclient.openfaas.onedata.org/token">>,
    <<"resultstream.openfaas.onedata.org/secret">>
]).

-define(FUNCTION_REMOVAL_STRATEGY, op_worker:get_env(
    atm_openfaas_function_removal_strategy, always  %% possible values: always | upon_success | never
)).

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


-spec create(atm_task_executor:creation_args()) -> record() | no_return().
create(#atm_task_executor_creation_args{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    lambda_revision = AtmLambdaRevision
}) ->
    atm_openfaas_monitor:assert_openfaas_healthy(),

    FunctionId = build_function_id(AtmWorkflowExecutionCtx),
    FunctionName = build_function_name(FunctionId, AtmLambdaRevision),
    {ok, PodStatusRegistryId} = atm_openfaas_function_pod_status_registry:create_for_function(FunctionId),

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
    case ?FUNCTION_REMOVAL_STRATEGY of
        always -> remove_function(AtmWorkflowExecutionCtx, AtmTaskExecutor);
        _ -> ok
    end.


-spec teardown(atm_workflow_execution_ctx:record(), atm_task_execution:doc()) ->
    ok | no_return().
teardown(AtmWorkflowExecutionCtx, #document{value = #atm_task_execution{
    status = AtmTaskExecutionStatus,
    executor = AtmTaskExecutor
}}) ->
    case ?FUNCTION_REMOVAL_STRATEGY of
        always ->
            remove_function(AtmWorkflowExecutionCtx, AtmTaskExecutor);
        upon_success when
            AtmTaskExecutionStatus =:= ?FINISHED_STATUS;
            AtmTaskExecutionStatus =:= ?SKIPPED_STATUS
        ->
            remove_function(AtmWorkflowExecutionCtx, AtmTaskExecutor);
        _ ->
            ok
    end.


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


-spec trigger_stream_conclusion(atm_workflow_execution_ctx:record(), record()) ->
    ok | no_return().
trigger_stream_conclusion(AtmWorkflowExecutionCtx, _AtmTaskExecutor) ->
    atm_openfaas_result_stream_handler:trigger_conclusion(
        atm_workflow_execution_ctx:get_workflow_execution_id(AtmWorkflowExecutionCtx),
        atm_workflow_execution_ctx:get_task_execution_id(AtmWorkflowExecutionCtx)
    ).


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


%% @private
-spec build_function_id(atm_workflow_execution_ctx:record()) -> function_id().
build_function_id(AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionId = atm_workflow_execution_ctx:get_workflow_execution_id(
        AtmWorkflowExecutionCtx
    ),
    sanitize_binary(str_utils:format_bin("w~ts-s~ts", [
        binary:part(AtmWorkflowExecutionId, 0, 10),
        % Generate random substring to ensure functions are unique for each task
        % despite e.g. using the same lambda
        str_utils:rand_hex(5)
    ])).


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
-spec build_function_name(function_id(), atm_lambda_revision:record()) ->
    function_name().
build_function_name(FunctionId, #atm_lambda_revision{name = AtmLambdaRevisionName}) ->
    str_utils:format_bin("~ts-~ts", [
        FunctionId,
        sanitize_binary(binary:part(AtmLambdaRevisionName, 0, min(size(AtmLambdaRevisionName), 39)))
    ]).


%% @private
-spec sanitize_binary(binary()) -> binary().
sanitize_binary(Bin) ->
    << <<(sanitize_character(Char))/integer>> || <<Char>> <= Bin>>.


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
    Endpoint = atm_openfaas_config:get_endpoint(OpenfaasConfig, <<"/system/functions">>),
    AuthHeaders = atm_openfaas_config:get_basic_auth_header(OpenfaasConfig),
    FunctionDefinition = prepare_function_definition(InitiationCtx),

    log_function_registering(InitiationCtx, FunctionDefinition),

    case http_client:post(Endpoint, AuthHeaders, json_utils:encode(FunctionDefinition)) of
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
-spec log_function_registering(initiation_ctx(), json_utils:json_map()) -> ok.
log_function_registering(#initiation_ctx{
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx
    },
    executor = #atm_openfaas_task_executor{
        function_name = FunctionName,
        operation_spec = #atm_openfaas_operation_spec{docker_image = DockerImage}
    }
}, FunctionDefinition) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),

    ?atm_task_info(Logger, #{
        <<"description">> => ?fmt_bin(
            "Registering docker '~ts' as function '~ts' in OpenFaaS...",
            [DockerImage, FunctionName]
        ),
        <<"details">> => #{
            <<"functionDefinition">> => maps:update_with(<<"annotations">>, fun(Annotations) ->
                maps:without(?ANNOTATIONS_OMITTED_WHEN_LOGGING, Annotations)
            end, FunctionDefinition)
        }
    }).


%% @private
-spec log_function_registered(initiation_ctx()) -> ok.
log_function_registered(#initiation_ctx{
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx
    },
    executor = #atm_openfaas_task_executor{function_name = FunctionName}
}) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_task_debug(Logger, "Function '~ts' registered in OpenFaaS.", [FunctionName]).


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
    FullDefinition1 = add_default_properties(BaseDefinition, InitiationCtx),
    FullDefinition2 = add_resources_properties(FullDefinition1, InitiationCtx),
    FullDefinition3 = add_function_ctx_annotations(FullDefinition2, InitiationCtx),
    FullDefinition4 = add_data_stream_annotations_if_required(FullDefinition3, InitiationCtx),
    add_oneclient_annotations_if_required(FullDefinition4, InitiationCtx).


%% @private
-spec add_default_properties(json_utils:json_map(), initiation_ctx()) ->
    json_utils:json_map().
add_default_properties(FunctionDefinition, #initiation_ctx{openfaas_config = OpenfaasConfig}) ->
    DefaultProperties = lists:foldl(fun({Property, EnvVar}, Acc) ->
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
    ]),

    case atm_openfaas_config:should_disable_tls_verification(OpenfaasConfig) of
        true ->
            EnvVars = maps:get(<<"envVars">>, DefaultProperties, #{}),
            DefaultProperties#{<<"envVars">> => EnvVars#{
                <<"VERIFY_SSL_CERTIFICATES">> => <<"false">>
            }};
        false ->
            DefaultProperties
    end.


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

add_oneclient_annotations_if_required(FunctionDefinition, InitiationCtx = #initiation_ctx{
    openfaas_config = OpenfaasConfig,
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx
    },
    executor = AtmTaskExecutor = #atm_openfaas_task_executor{
        operation_spec = #atm_openfaas_operation_spec{
            docker_execution_options = #atm_docker_execution_options{
                mount_oneclient = true,
                oneclient_mount_point = MountPoint
            }
        }
    }
}) ->
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),
    SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
    AccessToken = atm_workflow_execution_auth:get_access_token(AtmWorkflowExecutionAuth),
    {ok, OpDomain} = provider_logic:get_domain(),

    OneclientImage = atm_openfaas_config:get_oneclient_image(OpenfaasConfig),

    insert_function_annotations(FunctionDefinition, #{
        <<"oneclient.openfaas.onedata.org/inject">> => <<"enabled">>,
        <<"oneclient.openfaas.onedata.org/image">> => OneclientImage,
        <<"oneclient.openfaas.onedata.org/input_spaces_ids">> => SpaceId,
        <<"oneclient.openfaas.onedata.org/output_space_id">> => SpaceId,
        <<"oneclient.openfaas.onedata.org/mount_point">> => MountPoint,
        <<"oneclient.openfaas.onedata.org/options">> => build_oneclient_options(InitiationCtx),
        <<"oneclient.openfaas.onedata.org/oneprovider_host">> => OpDomain,
        <<"oneclient.openfaas.onedata.org/token">> => case is_in_readonly_mode(AtmTaskExecutor) of
            true -> tokens:confine(AccessToken, #cv_data_readonly{});
            false -> AccessToken
        end
    }).


%% @private
-spec build_oneclient_options(initiation_ctx()) -> binary().
build_oneclient_options(InitiationCtx = #initiation_ctx{
    openfaas_config = OpenfaasConfig,
    executor = #atm_openfaas_task_executor{
        operation_spec = #atm_openfaas_operation_spec{
            docker_execution_options = #atm_docker_execution_options{
                oneclient_options = LambdaSpecificOneclientOptions
            }
        }
    }
}) ->
    DefaultOneclientOptions = atm_openfaas_config:get_oneclient_default_options(OpenfaasConfig),

    Opts0 = <<DefaultOneclientOptions/binary, " ", LambdaSpecificOneclientOptions/binary>>,
    Opts1 = ensure_oneclient_io_mode_specified(Opts0, InitiationCtx),
    ensure_insecure_if_tls_verification_disabled(Opts1, InitiationCtx).


%% @private
-spec ensure_oneclient_io_mode_specified(binary(), initiation_ctx()) -> binary().
ensure_oneclient_io_mode_specified(Options, #initiation_ctx{
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx
    }}
) ->
    case re:run(Options, <<"--force-(direct|proxy)-io">>) of
        nomatch ->
            IoMode = infer_oneclient_io_mode(AtmWorkflowExecutionCtx),
            <<Options/binary, " ", IoMode/binary>>;
        _ ->
            Options
    end.


%% @private
-spec infer_oneclient_io_mode(atm_workflow_execution_ctx:record()) -> binary().
infer_oneclient_io_mode(AtmWorkflowExecutionCtx) ->
    AtmWorkflowExecutionEnv = atm_workflow_execution_ctx:get_env(AtmWorkflowExecutionCtx),
    SpaceId = atm_workflow_execution_env:get_space_id(AtmWorkflowExecutionEnv),
    {ok, StorageId} = space_logic:get_local_supporting_storage(SpaceId),

    case storage:get_helper_name(StorageId) of
        ?POSIX_HELPER_NAME -> <<"--force-proxy-io">>;
        _ -> <<"--force-direct-io">>
    end.


%% @private
-spec ensure_insecure_if_tls_verification_disabled(binary(), initiation_ctx()) -> binary().
ensure_insecure_if_tls_verification_disabled(Options, #initiation_ctx{
    openfaas_config = OpenfaasConfig
}) ->
    TlsDisabled = atm_openfaas_config:should_disable_tls_verification(OpenfaasConfig),

    case re:run(Options, <<"(?:^|\\s)(-\\pL*i\\pL*|--insecure)\\b">>) of
        nomatch when TlsDisabled ->
            <<Options/binary, " --insecure">>;
        _ ->
            Options
    end.


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
-spec log_function_ready(initiation_ctx()) -> ok.
log_function_ready(#initiation_ctx{
    task_executor_initiation_ctx = #atm_task_executor_initiation_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx
    },
    executor = #atm_openfaas_task_executor{
        function_name = FunctionName,
        operation_spec = #atm_openfaas_operation_spec{docker_image = DockerImage}
    }
}) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_task_info(Logger, #{
        <<"description">> => ?fmt_bin("Function '~ts' ready to use in OpenFaaS.", [
            FunctionName
        ]),
        <<"details">> => #{
            <<"functionName">> => FunctionName,
            <<"dockerImage">> => DockerImage
        }
    }).


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

    AtmWorkflowExecutionCtx = atm_run_job_batch_ctx:get_workflow_execution_ctx(AtmRunJobBatchCtx),
    AtmWorkflowExecutionAuth = atm_workflow_execution_ctx:get_auth(AtmWorkflowExecutionCtx),
    AtmWorkflowExecutionEnv = atm_workflow_execution_ctx:get_env(AtmWorkflowExecutionCtx),

    Body = json_utils:encode(#{
        <<"ctx">> => #{
            <<"userId">> => atm_workflow_execution_auth:get_user_id(AtmWorkflowExecutionAuth),
            <<"spaceId">> => atm_workflow_execution_env:get_space_id(AtmWorkflowExecutionEnv),
            <<"atmWorkflowExecutionId">> => LambdaInput#atm_lambda_input.workflow_execution_id,
            <<"logLevel">> => audit_log:severity_from_int(LambdaInput#atm_lambda_input.log_level),
            <<"timeoutSeconds">> => op_worker:get_env(atm_workflow_job_timeout_sec, 1800),
            <<"oneproviderDomain">> => oneprovider:get_domain(),
            <<"onezoneDomain">> => oneprovider:get_oz_domain(),
            <<"oneproviderId">> => oneprovider:get_id(),
            <<"heartbeatUrl">> => build_job_heartbeat_url(LambdaInput),
            <<"accessToken">> => atm_run_job_batch_ctx:get_access_token(AtmRunJobBatchCtx),
            <<"config">> => LambdaInput#atm_lambda_input.config
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
    ?atm_task_info(Logger, "Function '~ts' removed from OpenFaaS.", [FunctionName]).


%% @private
-spec log_function_removal_failed(
    atm_workflow_execution_ctx:record(),
    function_name(),
    errors:error()
) ->
    ok.
log_function_removal_failed(AtmWorkflowExecutionCtx, FunctionName, Error) ->
    Logger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    ?atm_task_warning(Logger, #{
        <<"description">> => ?fmt_bin(
            "Failed to remove function '~ts' from OpenFaaS.",
            [FunctionName]
        ),
        <<"details">> => #{<<"reason">> => errors:to_json(Error)}
    }).


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
