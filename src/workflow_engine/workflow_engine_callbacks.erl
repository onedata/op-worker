%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for workflow_engine to handle callback from
%%% task execution engines.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_engine_callbacks).
-author("Michal Wrzeszcz").

-behaviour(cowboy_handler).

-include("workflow_engine.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% Cowboy callback
-export([init/2]).
%% API
-export([prepare_finish_callback_id/4, prepare_heartbeat_callback_id/3, handle_callback/2]).
%% Test API
-export([decode_callback_id/1]).

-define(FINISH_CALLBACK_TYPE, finish_callback).
-define(HEARTBEAT_CALLBACK_TYPE, heartbeat_callback).
-type callback_type() :: ?FINISH_CALLBACK_TYPE | ?HEARTBEAT_CALLBACK_TYPE.
-type callback() :: workflow_handler:finished_callback_id() | workflow_handler:heartbeat_callback_id().

-define(SEPARATOR, "___").

%%%===================================================================
%%% Cowboy callback
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), any()) -> {ok, cowboy_req:req(), any()}.
init(Req, State) ->
    Path = cowboy_req:path(Req),
    {ok, Body, _} = cowboy_req:read_body(Req),
    <<"/tasks/", CallbackId/binary>> = Path,
    ?MODULE:handle_callback(CallbackId, Body), % Call via ?MODULE for tests

    {ok, cowboy_req:reply(?HTTP_200_OK, Req), State}.

%%%===================================================================
%%% API
%%%===================================================================

-spec prepare_finish_callback_id(
    workflow_engine:execution_id(),
    workflow_engine:id(),
    workflow_jobs:job_identifier(),
    workflow_engine:task_spec()
) -> workflow_handler:finished_callback_id().
prepare_finish_callback_id(ExecutionId, EngineId, JobIdentifier, TaskSpec) ->
    CallPools = maps:get(async_call_pools, TaskSpec, [?DEFAULT_ASYNC_CALL_POOL_ID]),
    encode_callback_id(?FINISH_CALLBACK_TYPE, ExecutionId, EngineId, JobIdentifier, CallPools).

-spec prepare_heartbeat_callback_id(
    workflow_engine:execution_id(),
    workflow_engine:id(),
    workflow_jobs:job_identifier()
) -> workflow_handler:heartbeat_callback_id().
prepare_heartbeat_callback_id(ExecutionId, EngineId, JobIdentifier) ->
    encode_callback_id(?HEARTBEAT_CALLBACK_TYPE, ExecutionId, EngineId, JobIdentifier, undefined).

-spec handle_callback(
    workflow_handler:finished_callback_id() | workflow_handler:heartbeat_callback_id(),
    workflow_handler:task_processing_result()
) -> ok.
handle_callback(CallbackId, Result) ->
    {CallbackType, ExecutionId, EngineId, JobIdentifier, CallPools} = decode_callback_id(CallbackId),
    case CallbackType of
        ?FINISH_CALLBACK_TYPE ->
            % TODO VFS-7551 - process result and get CallPools from state
            {Handler, Context, TaskId} = workflow_execution_state:get_result_processing_data(ExecutionId, JobIdentifier),
            ProcessedResult = Handler:process_result(ExecutionId, Context, TaskId, Result),
            workflow_engine:report_execution_status_update(
                ExecutionId, EngineId, ?ASYNC_CALL_FINISHED, JobIdentifier, CallPools, ProcessedResult);
        ?HEARTBEAT_CALLBACK_TYPE ->
            workflow_timeout_monitor:report_heartbeat(ExecutionId, JobIdentifier)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec encode_callback_id(
    callback_type(),
    workflow_engine:execution_id(),
    workflow_engine:id(),
    workflow_jobs:job_identifier(),
    [workflow_async_call_pool:id()] | undefined
) -> callback().
encode_callback_id(CallbackType, ExecutionId, EngineId, JobIdentifier, CallPools) ->
    <<(atom_to_binary(CallbackType, utf8))/binary, ?SEPARATOR,
        ExecutionId/binary, ?SEPARATOR,
        EngineId/binary, ?SEPARATOR,
        (workflow_jobs:job_identifier_to_binary(JobIdentifier))/binary, ?SEPARATOR,
        (call_pools_to_binary(CallPools))/binary>>.

-spec decode_callback_id(callback()) -> {
    callback_type(),
    workflow_engine:execution_id(),
    workflow_engine:id(),
    workflow_jobs:job_identifier(),
    [workflow_async_call_pool:id()] | undefined
}.
decode_callback_id(CallbackId) ->
    [CallbackTypeBin, ExecutionId, EngineId, JobIdentifierBin, CallPoolsBin] =
        binary:split(CallbackId, <<?SEPARATOR>>, [global, trim_all]),
    {
        binary_to_atom(CallbackTypeBin, utf8),
        ExecutionId,
        EngineId,
        workflow_jobs:binary_to_job_identifier(JobIdentifierBin),
        binary_to_calls_pool(CallPoolsBin)
    }.

-spec call_pools_to_binary([workflow_async_call_pool:id()] | undefined) -> binary().
call_pools_to_binary([CallPools]) ->
    CallPools; % TODO VFS-7551 - support multiple pools
call_pools_to_binary(undefined) ->
    <<"undefined">>.

-spec binary_to_calls_pool(binary()) -> [workflow_async_call_pool:id()] | undefined.
binary_to_calls_pool(<<"undefined">>) ->
    undefined;
binary_to_calls_pool(CallPools) ->
    CallPools.