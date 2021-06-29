%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for workflow_engine to handle callback from
%%% task execution platforms.
%%% @end
%%%-------------------------------------------------------------------
-module(workflow_engine_callback_handler).
-author("Michal Wrzeszcz").

-behaviour(cowboy_handler).

-include("workflow_engine.hrl").
-include("http/gui_paths.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").

%% Cowboy callback
-export([init/2]).
%% API
-export([prepare_finish_callback_id/3, prepare_heartbeat_callback_id/3, handle_callback/2]).
%% Test API
-export([decode_callback_id/1]).

-define(FINISH_CALLBACK_TYPE, finish_callback).
-define(HEARTBEAT_CALLBACK_TYPE, heartbeat_callback).
-type callback_type() :: ?FINISH_CALLBACK_TYPE | ?HEARTBEAT_CALLBACK_TYPE.
-type callback() :: workflow_handler:finished_callback_id() | workflow_handler:heartbeat_callback_id().

-define(SEPARATOR, "___").
-define(DOMAIN_SEPARATOR, "/").

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
    ParsedBody = try
        {ok, Body, _} = cowboy_req:read_body(Req),
        json_utils:decode(Body)
    catch _:_ ->
        ?WF_ERROR_MALFORMED_REQUEST
    end,

    Path = cowboy_req:path(Req),
    ?MODULE:handle_callback(Path, ParsedBody), % Call via ?MODULE for tests

    {ok, cowboy_req:reply(?HTTP_204_NO_CONTENT, Req), State}.

%%%===================================================================
%%% API
%%%===================================================================

-spec prepare_finish_callback_id(
    workflow_engine:execution_id(),
    workflow_engine:id(),
    workflow_jobs:job_identifier()
) -> workflow_handler:finished_callback_id().
prepare_finish_callback_id(ExecutionId, EngineId, JobIdentifier) ->
    encode_callback_id(?FINISH_CALLBACK_TYPE, ExecutionId, EngineId, JobIdentifier).

-spec prepare_heartbeat_callback_id(
    workflow_engine:execution_id(),
    workflow_engine:id(),
    workflow_jobs:job_identifier()
) -> workflow_handler:heartbeat_callback_id().
prepare_heartbeat_callback_id(ExecutionId, EngineId, JobIdentifier) ->
    encode_callback_id(?HEARTBEAT_CALLBACK_TYPE, ExecutionId, EngineId, JobIdentifier).

-spec handle_callback(callback(), workflow_handler:task_processing_result() | undefined) -> ok.
handle_callback(CallbackId, Message) ->
    {CallbackType, ExecutionId, EngineId, JobIdentifier} = decode_callback_id(CallbackId),
    case CallbackType of
        ?FINISH_CALLBACK_TYPE ->
            workflow_engine:report_execution_status_update(
                ExecutionId, EngineId, ?ASYNC_CALL_FINISHED, JobIdentifier, Message);
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
    workflow_jobs:job_identifier()
) -> callback().
encode_callback_id(CallbackType, ExecutionId, EngineId, JobIdentifier) ->
    <<"http://",
        (oneprovider:get_domain())/binary,
        ?DOMAIN_SEPARATOR,
        ?ATM_TASK_FINISHED_CALLBACK_PATH,
        (atom_to_binary(CallbackType, utf8))/binary, ?SEPARATOR,
        ExecutionId/binary, ?SEPARATOR,
        EngineId/binary, ?SEPARATOR,
        (workflow_jobs:job_identifier_to_binary(JobIdentifier))/binary>>.

-spec decode_callback_id(callback()) -> {
    callback_type(),
    workflow_engine:execution_id(),
    workflow_engine:id(),
    workflow_jobs:job_identifier()
}.
decode_callback_id(<<"http://", Tail/binary>>) ->
    [_Domain, Tail2] = binary:split(Tail, <<?DOMAIN_SEPARATOR>>),
    decode_callback_id(Tail2);
decode_callback_id(<<?ATM_TASK_FINISHED_CALLBACK_PATH, Tail/binary>>) ->
    decode_callback_id(Tail);
decode_callback_id(CallbackId) ->
    [CallbackTypeBin, ExecutionId, EngineId, JobIdentifierBin] =
        binary:split(CallbackId, <<?SEPARATOR>>, [global, trim_all]),
    {
        binary_to_atom(CallbackTypeBin, utf8),
        ExecutionId,
        EngineId,
        workflow_jobs:binary_to_job_identifier(JobIdentifierBin)
    }.