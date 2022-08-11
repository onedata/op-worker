%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_task_callback_handler).
-author("Bartosz Walkowicz").

-behaviour(cowboy_handler).

-include("workflow_engine.hrl").
-include("http/gui_paths.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/http/headers.hrl").

%% API
-export([
    build_job_output_url/2,
    build_job_heartbeat_url/2
]).
%% Cowboy callback
-export([init/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_job_output_url(
    atm_workflow_execution:id(),
    workflow_jobs:job_identifier()
) ->
    binary().
build_job_output_url(AtmWorkflowExecutionId, AtmJobBatchId) ->
    build_url(AtmWorkflowExecutionId, AtmJobBatchId, <<"output">>).


-spec build_job_heartbeat_url(
    atm_workflow_execution:id(),
    workflow_jobs:job_identifier()
) ->
    binary().
build_job_heartbeat_url(AtmWorkflowExecutionId, AtmJobBatchId) ->
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
    {Req1, Body} = read_body(Req0),

    Result = try
        json_utils:decode(Body)
    catch _:_ ->
        ?ERROR_BAD_MESSAGE(Body)
    end,

    workflow_engine:report_execution_status_update(
        cowboy_req:binding(wf_exec_id, Req1),
        <<"todo">>,  %% TODO rm ?
        ?ASYNC_CALL_ENDED,
        cowboy_req:binding(jid, Req1),
        Result
    ),
    {ok, cowboy_req:reply(?HTTP_204_NO_CONTENT, Req1), State};

init(Req, State = #{type := heartbeat}) ->
    workflow_timeout_monitor:report_heartbeat(
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
-spec read_body(cowboy_req:req()) -> {cowboy_req:req(), binary()}.
read_body(Req) ->
    try
        read_body_insecure(Req, <<>>)
    catch _:_ ->
        {Req, <<>>}
    end.


%% @private
-spec read_body_insecure(cowboy_req:req(), binary()) -> {cowboy_req:req(), binary()}.
read_body_insecure(Req0, Acc) ->
    case cowboy_req:read_body(Req0) of
        {ok, Data, Req1} ->
            {Req1, <<Acc/binary, Data/binary>>};
        {more, Data, Req1} ->
            read_body_insecure(Req1, <<Acc/binary, Data/binary>>)
    end.
