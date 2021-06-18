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


%% atm_task_executor callbacks
-export([create/2, prepare/2, get_spec/1, in_readonly_mode/1, run/3]).

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


-define(AWAIT_READINESS_RETRIES, 300).
-define(AWAIT_READINESS_INTERVAL_SEC, 1).


%%%===================================================================
%%% atm_task_executor callbacks
%%%===================================================================


-spec create(atm_workflow_execution:id(), atm_openfaas_operation_spec:record()) ->
    record() | no_return().
create(AtmWorkflowExecutionId, #atm_openfaas_operation_spec{} = OperationSpec) ->
    assert_openfaas_configured(),

    #atm_openfaas_task_executor{
        function_name = build_function_name(AtmWorkflowExecutionId, OperationSpec),
        operation_spec = OperationSpec
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


-spec get_spec(record()) -> workflow_engine:task_spec().
get_spec(_AtmTaskExecutor) ->
    #{type => async}.


-spec in_readonly_mode(record()) -> boolean().
in_readonly_mode(#atm_openfaas_task_executor{operation_spec = #atm_openfaas_operation_spec{
    docker_execution_options = #atm_docker_execution_options{readonly = Readonly}
}}) ->
    Readonly.


-spec run(atm_job_execution_ctx:record(), json_utils:json_map(), record()) ->
    ok | no_return().
run(AtmJobExecutionCtx, Data, AtmTaskExecutor) ->
    schedule_function_execution(AtmJobExecutionCtx, Data, AtmTaskExecutor).


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


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Generates name under which docker image with specific configuration
%% will be registered in Openfaas. As long as oneclient is not mounted
%% the function can be reused by various tasks from various workflow
%% executions. To that end simple digest from docker image is made.
%% Otherwise, the function cannot be reused (mounting requires specifying
%% token which in turn is unique for each workflow execution) and the name
%% must be unique.
%% @end
%%-------------------------------------------------------------------
-spec build_function_name(atm_workflow_execution:id(), atm_openfaas_operation_spec:record()) ->
    binary().
build_function_name(_AtmWorkflowExecutionId, #atm_openfaas_operation_spec{
    docker_image = DockerImage,
    docker_execution_options = #atm_docker_execution_options{mount_oneclient = false}
}) ->
    <<"fun-", (datastore_key:new_from_digest([DockerImage]))/binary>>;

build_function_name(AtmWorkflowExecutionId, #atm_openfaas_operation_spec{
    docker_image = DockerImage,
    docker_execution_options = #atm_docker_execution_options{
        mount_oneclient = true,
        oneclient_mount_point = MountPoint,
        oneclient_options = OneclientOptions
    }
}) ->
    <<"fun-", (datastore_key:new_from_digest([
        AtmWorkflowExecutionId, DockerImage, MountPoint, OneclientOptions
    ]))/binary>>.


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
        _ ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED)
    end.


%% @private
-spec register_function(prepare_ctx()) -> ok | no_return().
register_function(#prepare_ctx{
    openfaas_config = OpenfaasConfig,
    executor = #atm_openfaas_task_executor{
        function_name = FunctionName,
        operation_spec = #atm_openfaas_operation_spec{docker_image = DockerImage}
    }
} = PrepareCtx) ->
    Endpoint = get_openfaas_endpoint(OpenfaasConfig, <<"/system/functions">>),
    AuthHeaders = get_basic_auth_header(OpenfaasConfig),
    Payload = json_utils:encode(maps:merge(
        #{
            <<"service">> => FunctionName,
            <<"image">> => DockerImage,
            <<"namespace">> => OpenfaasConfig#openfaas_config.function_namespace,
            <<"envVars">> => prepare_function_timeouts()
        },
        prepare_function_annotations(PrepareCtx)
    )),

    case http_client:post(Endpoint, AuthHeaders, Payload) of
        {ok, ?HTTP_202_ACCEPTED, _RespHeaders, _RespBody} ->
            ok;
        {ok, ?HTTP_500_INTERNAL_SERVER_ERROR, _RespHeaders, _RespBody} ->
            % Possible race with other task registering function
            % (Openfaas returns 500 if function already exists)
            ok;
        {ok, ?HTTP_400_BAD_REQUEST, _RespHeaders, ErrorReason} ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED(ErrorReason));
        _ ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED)
    end.


%% @private
-spec prepare_function_timeouts() -> json_utils:json_map().
prepare_function_timeouts() ->
    lists:foldl(fun({Key, EnvVar}, Acc) ->
        TimeoutSeconds = op_worker:get_env(EnvVar),
        TimeoutSecondsBin = integer_to_binary(TimeoutSeconds),

        Acc#{Key => <<TimeoutSecondsBin/binary, "s">>}
    end, #{}, [
        {<<"read_timeout">>, openfaas_read_timeout_seconds},
        {<<"write_timeout">>, openfaas_write_timeout_seconds},
        {<<"exec_timeout">>, openfaas_exec_timeout_seconds}
    ]).


%% @private
-spec prepare_function_annotations(prepare_ctx()) -> json_utils:json_map().
prepare_function_annotations(#prepare_ctx{executor = #atm_openfaas_task_executor{
    operation_spec = #atm_openfaas_operation_spec{
        docker_execution_options = #atm_docker_execution_options{mount_oneclient = false}
    }}
}) ->
    #{};
prepare_function_annotations(#prepare_ctx{
    workflow_execution_ctx = AtmWorkflowExecutionCtx,
    executor = AtmTaskExecutor = #atm_openfaas_task_executor{
        operation_spec = #atm_openfaas_operation_spec{
            docker_execution_options = #atm_docker_execution_options{
                mount_oneclient = true,
                oneclient_mount_point = MountPoint,
                oneclient_options = OneclientOptions
            }
        }
    }
}) ->
    SpaceId = atm_workflow_execution_ctx:get_space_id(AtmWorkflowExecutionCtx),
    AccessToken = atm_workflow_execution_ctx:get_access_token(AtmWorkflowExecutionCtx),
    {ok, OpDomain} = provider_logic:get_domain(),

    #{<<"annotations">> => #{
        <<"oneclient.openfass.onedata.org/inject">> => <<"enabled">>,
        <<"oneclient.openfass.onedata.org/image">> => get_oneclient_image(),
        <<"oneclient.openfaas.onedata.org/space_id">> => SpaceId,
        <<"oneclient.openfaas.onedata.org/mount_point">> => MountPoint,
        <<"oneclient.openfaas.onedata.org/options">> => OneclientOptions,
        <<"oneclient.openfaas.onedata.org/oneprovider_host">> => OpDomain,
        <<"oneclient.openfaas.onedata.org/token">> => case in_readonly_mode(AtmTaskExecutor) of
            true -> tokens:confine(AccessToken, #cv_data_readonly{});
            false -> AccessToken
        end
    }}.


%% @private
-spec get_oneclient_image() -> binary().
get_oneclient_image() ->
    case op_worker:get_env(openfaas_oneclient_image, undefined) of
        undefined ->
            ReleaseVersion = op_worker:get_release_version(),
            <<"onedata/oneclient:", ReleaseVersion/binary>>;
        OneclientImage ->
            list_to_binary(OneclientImage)
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
            ok;
        not_ready ->
            timer:sleep(timer:seconds(?AWAIT_READINESS_INTERVAL_SEC)),
            await_function_readiness(PrepareCtx, RetriesLeft - 1)
    end.


%% @private
-spec schedule_function_execution(atm_job_execution_ctx:record(), json_utils:json_map(), record()) ->
    ok | no_return().
schedule_function_execution(AtmJobExecutionCtx, Data, #atm_openfaas_task_executor{
    function_name = FunctionName
}) ->
    OpenfaasConfig = get_openfaas_config(),
    Endpoint = get_openfaas_endpoint(
        OpenfaasConfig, <<"/async-function/", FunctionName/binary>>
    ),
    AuthHeaders = get_basic_auth_header(OpenfaasConfig),
    AllHeaders = AuthHeaders#{
        <<"X-Callback-Url">> => atm_job_execution_ctx:get_report_result_url(AtmJobExecutionCtx)
    },

    case http_client:post(Endpoint, AllHeaders, json_utils:encode(Data)) of
        {ok, ?HTTP_202_ACCEPTED, _, _} ->
            ok;
        _ ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED)
    end.


%% @private
-spec assert_openfaas_configured() -> ok | no_return().
assert_openfaas_configured() ->
    get_openfaas_config(),
    ok.


%% @private
-spec get_openfaas_config() -> openfaas_config() | no_return().
get_openfaas_config() ->
    try
        Host = op_worker:get_env(openfaas_host),
        Port = op_worker:get_env(openfaas_port),

        AdminUsername = op_worker:get_env(openfaas_admin_username),
        AdminPassword = op_worker:get_env(openfaas_admin_password),
        Hash = base64:encode(<<AdminUsername/binary, ":", AdminPassword/binary>>),

        #openfaas_config{
            url = str_utils:format_bin("http://~s:~B", [Host, Port]),
            basic_auth = <<"Basic ", Hash/binary>>,
            function_namespace = op_worker:get_env(openfaas_function_namespace)
        }
    catch error:{missing_env_variable, _} ->
        throw(?ERROR_ATM_OPENFAAS_NOT_CONFIGURED)
    end.


%% @private
-spec get_openfaas_endpoint(openfaas_config(), binary()) -> binary().
get_openfaas_endpoint(#openfaas_config{url = OpenfaasUrl}, Path) ->
    str_utils:format_bin("~s~s", [OpenfaasUrl, Path]).


%% @private
-spec get_basic_auth_header(openfaas_config()) -> map().
get_basic_auth_header(#openfaas_config{basic_auth = BasicAuth}) ->
    #{?HDR_AUTHORIZATION => BasicAuth}.
