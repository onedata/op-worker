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
-include("modules/automation/atm_tmp.hrl").
-include_lib("ctool/include/automation/automation.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").


%% atm_task_executor callbacks
-export([create/2, prepare/1, run/2]).

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
    openfaas_config :: openfaas_config(),
    executor :: record()
}).
-type prepare_ctx() :: #prepare_ctx{}.

-export_type([record/0]).


-define(AWAIT_READINESS_RETRIES, 150).
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


-spec prepare(record()) -> ok | no_return().
prepare(AtmTaskExecutor) ->
    PrepareCtx = #prepare_ctx{
        openfaas_config = get_openfaas_config(),
        executor = AtmTaskExecutor
    },
    case is_function_registered(PrepareCtx) of
        true -> ok;
        false -> register_function(PrepareCtx)
    end,
    await_function_readiness(PrepareCtx).


-spec run(json_utils:json_map(), record()) ->
    {ok, atm_task_execution_api:task_id()} | no_return().
run(Data, AtmTaskExecutor) ->
    schedule_function_execution(Data, AtmTaskExecutor).


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
    datastore_key:new_from_digest([DockerImage]);

build_function_name(AtmWorkflowExecutionId, #atm_openfaas_operation_spec{
    docker_image = DockerImage,
    docker_execution_options = #atm_docker_execution_options{
        mount_oneclient = true,
        oneclient_mount_point = MountPoint,
        oneclient_options = OneclientOptions
    }
}) ->
    datastore_key:new_from_digest([
        AtmWorkflowExecutionId, DockerImage, MountPoint, OneclientOptions
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
            <<"namespace">> => OpenfaasConfig#openfaas_config.function_namespace
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
        _ ->
            throw(?ERROR_ATM_OPENFAAS_QUERY_FAILED)
    end.


%% @private
-spec prepare_function_annotations(prepare_ctx()) -> json_utils:json_map().
prepare_function_annotations(#prepare_ctx{executor = #atm_openfaas_task_executor{
    operation_spec = #atm_openfaas_operation_spec{
        docker_execution_options = #atm_docker_execution_options{mount_oneclient = false}
    }}
}) ->
    #{};
prepare_function_annotations(#prepare_ctx{executor = #atm_openfaas_task_executor{
    operation_spec = #atm_openfaas_operation_spec{
        docker_execution_options = #atm_docker_execution_options{
            mount_oneclient = true,
            oneclient_mount_point = MountPoint,
            oneclient_options = OneclientOptions
        }
    }}
}) ->
    #{<<"annotations">> => #{
        % TODO VFS-7627 set proper annotation for oneclient mounts
        <<"com.mountpoint">> => MountPoint,
        <<"com.options">> => OneclientOptions
    }}.


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
-spec schedule_function_execution(json_utils:json_map(), record()) ->
    {ok, atm_task_execution_api:task_id()} | no_return().
schedule_function_execution(Data, #atm_openfaas_task_executor{
    function_name = FunctionName
}) ->
    TaskId = str_utils:rand_hex(32),

    OpenfaasConfig = get_openfaas_config(),
    Endpoint = get_openfaas_endpoint(
        OpenfaasConfig, <<"/async-function/", FunctionName/binary>>
    ),
    AuthHeaders = get_basic_auth_header(OpenfaasConfig),
    AllHeaders = AuthHeaders#{<<"X-Callback-Url">> => build_op_callback_url(TaskId)},

    case http_client:post(Endpoint, AllHeaders, json_utils:encode(Data)) of
        {ok, ?HTTP_202_ACCEPTED, _, _} ->
            {ok, TaskId};
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


%% @private
-spec build_op_callback_url(atm_task_execution_api:task_id()) -> binary().
build_op_callback_url(TaskId) ->
    Port = http_listener:port(),
    Host = oneprovider:get_domain(),

    % TODO VFS-7628 make openfaas respond to https
    str_utils:format_bin("http://~s:~B~s~s", [
        Host, Port, ?ATM_TASK_FINISHED_CALLBACK_PATH, TaskId
    ]).
