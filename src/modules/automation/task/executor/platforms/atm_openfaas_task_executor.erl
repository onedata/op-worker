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

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([is_openfaas_available/0, assert_openfaas_available/0]).

%% atm_task_executor callbacks
-export([create/2, prepare/2, clean/1, get_spec/1, in_readonly_mode/1, run/3]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-record(atm_openfaas_task_executor, {
    function_name :: binary(),
    operation_spec :: atm_openfaas_operation_spec:record()
}).
-type record() :: #atm_openfaas_task_executor{}.

-record(openfaas_config, {
    url :: binary(),
    basic_auth :: binary(),
    function_namespace :: binary()
}).
-type openfaas_config() :: #openfaas_config{}.

-record(prepare_ctx, {
    workflow_execution_ctx :: atm_workflow_execution_ctx:record(),
    openfaas_config :: openfaas_config(),
    executor :: record()
}).
-type prepare_ctx() :: #prepare_ctx{}.

-export_type([record/0]).


-define(HEALTHCHECK_CACHE_TTL_SECONDS, 15).

-define(AWAIT_READINESS_RETRIES, 300).
-define(AWAIT_READINESS_INTERVAL_SEC, 1).


%%%===================================================================
%%% API
%%%===================================================================


-spec is_openfaas_available() -> boolean().
is_openfaas_available() ->
    case check_openfaas_availability() of
        ok -> true;
        {error, _} -> false
    end.


-spec assert_openfaas_available() -> ok | no_return().
assert_openfaas_available() ->
    case check_openfaas_availability() of
        ok -> ok;
        {error, _} = Error -> throw(Error)
    end.


%%%===================================================================
%%% atm_task_executor callbacks
%%%===================================================================


-spec create(atm_workflow_execution:id(), od_atm_lambda:doc()) ->
    record() | no_return().
create(AtmWorkflowExecutionId, AtmLambdaDoc = #document{value = #od_atm_lambda{
    operation_spec = AtmLambadaOperationSpec
}}) ->
    assert_openfaas_available(),

    #atm_openfaas_task_executor{
        function_name = build_function_name(AtmWorkflowExecutionId, AtmLambdaDoc),
        operation_spec = AtmLambadaOperationSpec
    }.


-spec prepare(atm_workflow_execution_ctx:record(), record()) -> ok | no_return().
prepare(AtmWorkflowExecutionCtx, AtmTaskExecutor) ->
    PrepareCtx = #prepare_ctx{
        workflow_execution_ctx = AtmWorkflowExecutionCtx,
        openfaas_config = get_openfaas_config(),
        executor = AtmTaskExecutor
    },
    case is_function_registered(PrepareCtx) of
        true -> ok;
        false -> register_function(PrepareCtx)
    end,
    await_function_readiness(PrepareCtx).


-spec clean(record()) -> ok | no_return().
clean(AtmTaskExecutor) ->
    remove_function(AtmTaskExecutor).


-spec get_spec(record()) -> workflow_engine:task_spec().
get_spec(_AtmTaskExecutor) ->
    #{type => async}.


-spec in_readonly_mode(record()) -> boolean().
in_readonly_mode(#atm_openfaas_task_executor{operation_spec = #atm_openfaas_operation_spec{
    docker_execution_options = #atm_docker_execution_options{readonly = Readonly}
}}) ->
    Readonly.


-spec run(atm_job_ctx:record(), json_utils:json_map(), record()) ->
    ok | no_return().
run(AtmJobCtx, Data, AtmTaskExecutor) ->
    schedule_function_execution(AtmJobCtx, Data, AtmTaskExecutor).


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
    operation_spec = OperationSpec
}, NestedRecordEncoder) ->
    #{
        <<"functionName">> => FunctionName,
        <<"operationSpec">> => NestedRecordEncoder(OperationSpec, atm_openfaas_operation_spec)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"functionName">> := FunctionName,
    <<"operationSpec">> := OperationSpecJson
}, NestedRecordDecoder) ->
    #atm_openfaas_task_executor{
        function_name = FunctionName,
        operation_spec = NestedRecordDecoder(OperationSpecJson, atm_openfaas_operation_spec)
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_openfaas_availability() -> ok | {error, term()}.
check_openfaas_availability() ->
    {ok, Result} = node_cache:acquire(?FUNCTION_NAME, fun() ->
        HealthcheckResult = try
            OpenfaasConfig = get_openfaas_config(),

            % /healthz is proper Openfaas endpoint defined in their swagger:
            % https://raw.githubusercontent.com/openfaas/faas/master/api-docs/swagger.yml
            Endpoint = get_openfaas_endpoint(OpenfaasConfig, <<"/healthz">>),
            Headers = get_basic_auth_header(OpenfaasConfig),

            case http_client:get(Endpoint, Headers) of
                {ok, ?HTTP_200_OK, _RespHeaders, _RespBody} ->
                    ok;
                {ok, ?HTTP_500_INTERNAL_SERVER_ERROR, _RespHeaders, ErrorReason} ->
                    ?ERROR_ATM_OPENFAAS_QUERY_FAILED(ErrorReason);
                _ ->
                    ?ERROR_ATM_OPENFAAS_UNREACHABLE
            end
        catch
            throw:{error, _} = Error ->
                Error;
            Class:Reason:Stacktrace ->
                ?error_stacktrace(
                    "Unexpected error during OpenFaaS healthcheck - ~w:~p",
                    [Class, Reason],
                    Stacktrace
                ),
                ?ERROR_ATM_INTERNAL_SERVER_ERROR
        end,
        {ok, HealthcheckResult, ?HEALTHCHECK_CACHE_TTL_SECONDS}
    end),
    Result.


%% @private
-spec build_function_name(atm_workflow_execution:id(), od_atm_lambda:doc()) ->
    binary().
build_function_name(AtmWorkflowExecutionId, #document{key = AtmLambdaId}) ->
    str_utils:format_bin("wf-~s-signature-~s", [
        binary:part(AtmWorkflowExecutionId, 0, 10),
        str_utils:md5_digest([AtmWorkflowExecutionId, AtmLambdaId])
    ]).


%% @private
-spec is_function_registered(prepare_ctx()) -> boolean() | no_return().
is_function_registered(#prepare_ctx{
    openfaas_config = OpenfaasConfig,
    executor = #atm_openfaas_task_executor{function_name = FunctionName}
}) ->
    Endpoint = get_openfaas_endpoint(
        OpenfaasConfig, <<"/system/function/", FunctionName/binary>>
    ),
    Headers = get_basic_auth_header(OpenfaasConfig),

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
-spec register_function(prepare_ctx()) -> ok | no_return().
register_function(#prepare_ctx{openfaas_config = OpenfaasConfig} = PrepareCtx) ->
    log_function_registering(PrepareCtx),

    Endpoint = get_openfaas_endpoint(OpenfaasConfig, <<"/system/functions">>),
    AuthHeaders = get_basic_auth_header(OpenfaasConfig),
    Payload = json_utils:encode(prepare_function_definition(PrepareCtx)),

    case http_client:post(Endpoint, AuthHeaders, Payload) of
        {ok, ?HTTP_202_ACCEPTED, _RespHeaders, _RespBody} ->
            log_function_registered(PrepareCtx);
        {ok, ?HTTP_400_BAD_REQUEST, _RespHeaders, ErrorReason} ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED(ErrorReason));
        {ok, ?HTTP_500_INTERNAL_SERVER_ERROR, _RespHeaders, ErrorReason} ->
            % Possible race with other task registering function
            % (Openfaas returns 500 if function already exists)
            case is_function_registered(PrepareCtx) of
                true -> ok;
                false -> throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED(ErrorReason))
            end;
        _ ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED)
    end.


%% @private
-spec log_function_registering(prepare_ctx()) -> ok.
log_function_registering(#prepare_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    executor = #atm_openfaas_task_executor{
        function_name = FunctionName,
        operation_spec = #atm_openfaas_operation_spec{docker_image = DockerImage}
    }
}) ->
    AtmWorkflowExecutionLogger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    atm_workflow_execution_logger:workflow_info(
        "Registering docker '~ts' as function '~ts' in OpenFaas", [DockerImage, FunctionName],
        AtmWorkflowExecutionLogger
    ).


%% @private
-spec log_function_registered(prepare_ctx()) -> ok.
log_function_registered(#prepare_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    executor = #atm_openfaas_task_executor{function_name = FunctionName}
}) ->
    AtmWorkflowExecutionLogger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    atm_workflow_execution_logger:workflow_info(
        "Function '~ts' registered in OpenFaas", [FunctionName], AtmWorkflowExecutionLogger
    ).


%% @private
-spec prepare_function_definition(prepare_ctx()) -> json_utils:json_map().
prepare_function_definition(PrepareCtx = #prepare_ctx{
    openfaas_config = #openfaas_config{
        function_namespace = FunctionNamespace
    },
    executor = #atm_openfaas_task_executor{
        function_name = FunctionName,
        operation_spec = #atm_openfaas_operation_spec{docker_image = DockerImage}
    }
}) ->
    RequiredProperties = #{
        <<"service">> => FunctionName,
        <<"image">> => DockerImage,
        <<"namespace">> => FunctionNamespace
    },

    AllProperties = lists:foldl(fun({Property, EnvVar}, Acc) ->
        case get_env(EnvVar, undefined) of
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
    end, RequiredProperties, [
        {<<"envVars">>, openfaas_function_env},
        {<<"constraints">>, openfaas_function_constraints},
        {<<"labels">>, openfaas_function_labels},
        {<<"annotations">>, openfaas_function_annotations},
        {<<"limits">>, openfaas_function_limits},
        {<<"requests">>, openfaas_function_requests}
    ]),

    add_mount_oneclient_function_annotations(AllProperties, PrepareCtx).


%% @private
-spec add_mount_oneclient_function_annotations(json_utils:json_map(), prepare_ctx()) ->
    json_utils:json_map().
add_mount_oneclient_function_annotations(FunctionDefinition, #prepare_ctx{
    executor = #atm_openfaas_task_executor{operation_spec = #atm_openfaas_operation_spec{
        docker_execution_options = #atm_docker_execution_options{mount_oneclient = false}
    }}}
) ->
    FunctionDefinition;
add_mount_oneclient_function_annotations(FunctionDefinition, #prepare_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
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

    EnvSpecificOneclientOptions = str_utils:to_binary(get_env(
        openfaas_oneclient_options, <<"">>
    )),

    OneclientMountRelatedAnnotations = #{
        % TODO VFS-8141 rm deprecated oneclient.openfass.*
        <<"oneclient.openfass.onedata.org/inject">> => <<"enabled">>,
        <<"oneclient.openfass.onedata.org/image">> => get_oneclient_image(),
        <<"oneclient.openfaas.onedata.org/inject">> => <<"enabled">>,
        <<"oneclient.openfaas.onedata.org/image">> => get_oneclient_image(),
        <<"oneclient.openfaas.onedata.org/space_id">> => SpaceId,
        <<"oneclient.openfaas.onedata.org/mount_point">> => MountPoint,
        <<"oneclient.openfaas.onedata.org/options">> => <<
            EnvSpecificOneclientOptions/binary, " ", LambdaSpecificOneclientOptions/binary
        >>,
        <<"oneclient.openfaas.onedata.org/oneprovider_host">> => OpDomain,
        <<"oneclient.openfaas.onedata.org/token">> => case in_readonly_mode(AtmTaskExecutor) of
            true -> tokens:confine(AccessToken, #cv_data_readonly{});
            false -> AccessToken
        end
    },

    maps:update_with(<<"annotations">>, fun(Annotations) ->
        json_utils:merge([Annotations, OneclientMountRelatedAnnotations])
    end, OneclientMountRelatedAnnotations, FunctionDefinition).


%% @private
-spec get_oneclient_image() -> binary().
get_oneclient_image() ->
    case get_env(openfaas_oneclient_image, undefined) of
        undefined ->
            ReleaseVersion = op_worker:get_release_version(),
            <<"onedata/oneclient:", ReleaseVersion/binary>>;
        OneclientImage ->
            str_utils:to_binary(OneclientImage)
    end.


%% @private
-spec await_function_readiness(prepare_ctx()) -> ok | no_return().
await_function_readiness(PrepareCtx) ->
    await_function_readiness(PrepareCtx, ?AWAIT_READINESS_RETRIES).


%% @private
-spec await_function_readiness(prepare_ctx(), non_neg_integer()) -> ok | no_return().
await_function_readiness(_PrepareCtx, 0) ->
    throw(?ERROR_ATM_OPENFAAS_FUNCTION_REGISTRATION_FAILED);

await_function_readiness(#prepare_ctx{
    openfaas_config = OpenfaasConfig,
    executor = #atm_openfaas_task_executor{function_name = FunctionName}
} = PrepareCtx, RetriesLeft) ->
    Endpoint = get_openfaas_endpoint(
        OpenfaasConfig, <<"/system/function/", FunctionName/binary>>
    ),
    Headers = get_basic_auth_header(OpenfaasConfig),

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
            log_function_ready(PrepareCtx);
        not_ready ->
            timer:sleep(timer:seconds(?AWAIT_READINESS_INTERVAL_SEC)),
            await_function_readiness(PrepareCtx, RetriesLeft - 1)
    end.


%% @private
-spec log_function_ready(prepare_ctx()) -> ok.
log_function_ready(#prepare_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    executor = #atm_openfaas_task_executor{function_name = FunctionName}
}) ->
    AtmWorkflowExecutionLogger = atm_workflow_execution_ctx:get_logger(AtmWorkflowExecutionCtx),
    atm_workflow_execution_logger:workflow_info(
        "Function '~ts' ready to use in OpenFaas", [FunctionName], AtmWorkflowExecutionLogger
    ).


%% @private
-spec schedule_function_execution(atm_job_ctx:record(), json_utils:json_map(), record()) ->
    ok | no_return().
schedule_function_execution(AtmJobCtx, Data, #atm_openfaas_task_executor{
    function_name = FunctionName
}) ->
    OpenfaasConfig = get_openfaas_config(),
    Endpoint = get_openfaas_endpoint(
        OpenfaasConfig, <<"/async-function/", FunctionName/binary>>
    ),
    AuthHeaders = get_basic_auth_header(OpenfaasConfig),
    AllHeaders = AuthHeaders#{
        <<"X-Callback-Url">> => atm_job_ctx:get_report_result_url(AtmJobCtx)
    },

    case http_client:post(Endpoint, AllHeaders, json_utils:encode(Data)) of
        {ok, ?HTTP_202_ACCEPTED, _, _} ->
            ok;
        {ok, ?HTTP_500_INTERNAL_SERVER_ERROR, _RespHeaders, ErrorReason} ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED(ErrorReason));
        _ ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED)
    end.


%% @private
-spec remove_function(record()) -> ok | no_return().
remove_function(#atm_openfaas_task_executor{function_name = FunctionName}) ->
    OpenfaasConfig = get_openfaas_config(),

    Endpoint = get_openfaas_endpoint(OpenfaasConfig, <<"/system/functions">>),
    AuthHeaders = get_basic_auth_header(OpenfaasConfig),
    Payload = json_utils:encode(#{<<"functionName">> => FunctionName}),

    % TODO VFS-8273 log warning in audit log if function removal failed
    http_client:delete(Endpoint, AuthHeaders, Payload),
    ok.


%% @private
-spec get_openfaas_config() -> openfaas_config() | no_return().
get_openfaas_config() ->
    Host = get_env(openfaas_host),
    Port = get_env(openfaas_port),

    AdminUsername = get_env(openfaas_admin_username),
    AdminPassword = get_env(openfaas_admin_password),
    Hash = base64:encode(str_utils:format_bin("~s:~s", [AdminUsername, AdminPassword])),

    #openfaas_config{
        url = str_utils:format_bin("http://~s:~B", [Host, Port]),
        basic_auth = <<"Basic ", Hash/binary>>,
        function_namespace = str_utils:to_binary(get_env(openfaas_function_namespace))
    }.


%% @private
-spec get_env(atom()) -> term() | no_return().
get_env(Key) ->
    case get_env(Key, undefined) of
        undefined -> throw(?ERROR_ATM_OPENFAAS_NOT_CONFIGURED);
        Value -> Value
    end.


%% @private
-spec get_env(atom(), term()) -> term().
get_env(Key, Default) ->
    op_worker:get_env(Key, Default).


%% @private
-spec get_openfaas_endpoint(openfaas_config(), binary()) -> binary().
get_openfaas_endpoint(#openfaas_config{url = OpenfaasUrl}, Path) ->
    str_utils:format_bin("~s~s", [OpenfaasUrl, Path]).


%% @private
-spec get_basic_auth_header(openfaas_config()) -> map().
get_basic_auth_header(#openfaas_config{basic_auth = BasicAuth}) ->
    #{?HDR_AUTHORIZATION => BasicAuth}.
