%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module handling heartbeats and job batch results sent by tasks
%%% executed by OpenFaaS service.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_task_callback_handler).
-author("Bartosz Walkowicz").

-behaviour(cowboy_handler).

-include("modules/automation/atm_execution.hrl").
-include("workflow_engine.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% API
-export([
    build_job_batch_output_url/2,
    build_job_batch_heartbeat_url/2
]).
%% Cowboy callback
-export([init/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_job_batch_output_url(
    atm_workflow_execution:id(),
    workflow_jobs:encoded_job_identifier()
) ->
    binary().
build_job_batch_output_url(AtmWorkflowExecutionId, AtmJobBatchId) ->
    build_url(AtmWorkflowExecutionId, AtmJobBatchId, <<"output">>).


-spec build_job_batch_heartbeat_url(
    atm_workflow_execution:id(),
    workflow_jobs:encoded_job_identifier()
) ->
    binary().
build_job_batch_heartbeat_url(AtmWorkflowExecutionId, AtmJobBatchId) ->
    build_url(AtmWorkflowExecutionId, AtmJobBatchId, <<"heartbeat">>).


%%%===================================================================
%%% Cowboy callback
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link dynamic_page_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec init(cowboy_req:req(), any()) -> {ok, cowboy_req:req(), any()}.
init(Req0, State = #{type := output}) ->
    {Body, Req1} = read_body(Req0),
    Result = case cowboy_req:header(<<"x-function-status">>, Req1) of
        <<"200">> -> decode_lambda_output(Body);
        <<"404">> -> ?ERROR_ATM_JOB_BATCH_WITHDRAWN(trim_body(Body));
        <<"500">> -> ?ERROR_ATM_JOB_BATCH_CRASHED(trim_body(Body))
    end,

    workflow_engine:report_async_task_result(
        cowboy_req:binding(wf_exec_id, Req1),
        cowboy_req:binding(jid, Req1),
        Result
    ),
    {ok, cowboy_req:reply(?HTTP_204_NO_CONTENT, Req1), State};

init(Req, State = #{type := heartbeat}) ->
    workflow_engine:report_async_task_heartbeat(
        cowboy_req:binding(wf_exec_id, Req),
        cowboy_req:binding(jid, Req)
    ),
    {ok, cowboy_req:reply(?HTTP_204_NO_CONTENT, Req), State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec build_url(
    atm_workflow_execution:id(),
    atm_task_executor:job_batch_id(),
    binary()
) ->
    binary().
build_url(AtmWorkflowExecutionId, AtmJobBatchId, TypeBin) ->
    str_utils:format_bin("http://~s:~B/automation/workflow/executions/~s/jobs/~s/~s", [
        oneprovider:get_domain(),
        http_listener:port(),
        AtmWorkflowExecutionId,
        AtmJobBatchId,
        TypeBin
    ]).


%% @private
-spec decode_lambda_output(binary()) -> atm_task_executor:job_batch_result().
decode_lambda_output(Body) ->
    try json_utils:decode(Body) of
        #{<<"resultsBatch">> := ResultsBatch} when is_list(ResultsBatch) ->
            {ok, #atm_lambda_output{results_batch = ResultsBatch}};
        _ ->
            ?ERROR_BAD_DATA(<<"lambdaOutput">>, str_utils:format_bin(
                "Expected '{\"resultsBatch\": [$LAMBDA_RESULTS_FOR_ITEM, ...]}' with "
                "$LAMBDA_RESULTS_FOR_ITEM object for each item in 'argsBatch' "
                "provided to lambda. Instead got: ~s",
                [json_utils:encode(trim_body(Body))]
            ))
    catch _:_ ->
        ?ERROR_BAD_DATA(<<"lambdaOutput">>, ?ERROR_BAD_MESSAGE(trim_body(Body)))
    end.


%% @private
-spec read_body(cowboy_req:req()) -> {binary(), cowboy_req:req()}.
read_body(Req) ->
    try
        read_body_insecure(<<>>, Req)
    catch _:_ ->
        {<<>>, Req}
    end.


%% @private
-spec read_body_insecure(binary(), cowboy_req:req()) -> {binary(), cowboy_req:req()}.
read_body_insecure(Acc, Req0) ->
    case cowboy_req:read_body(Req0) of
        {ok, Data, Req1} ->
            {<<Acc/binary, Data/binary>>, Req1};
        {more, Data, Req1} ->
            read_body_insecure(<<Acc/binary, Data/binary>>, Req1)
    end.


%% @private
-spec trim_body(binary()) -> binary().
trim_body(<<Data:1024/binary, _/binary>>) ->
    <<Data/binary, "...">>;
trim_body(Body) ->
    Body.
