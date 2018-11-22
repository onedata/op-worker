%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functionality of async messages handling.
%%% @end
%%%-------------------------------------------------------------------
-module(async_request_manager).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/elements/worker_host/worker_protocol.hrl").

%% API
-export([route_and_supervise/2, delegate/2, save_delegation/3, process_ans/3,
    check_processes/4, get_processes_check_interval/0,
    get_heartbeat_msg/1, get_error_msg/1]).

-define(TIMEOUT, timer:seconds(10)).

-type delegation() :: {message_id:id(), pid(), reference()}.
-type delegate_ans() :: {wait, delegation()}.

-export_type([delegate_ans/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Executes function that handles message and asynchronously wait for answer.
%% @end
%%--------------------------------------------------------------------
-spec route_and_supervise(fun(() -> {pid(), reference()}), message_id:id()) ->
    delegate_ans() | {ok, #server_message{}}.
route_and_supervise(Fun, Id) ->
    Fun2 = fun() ->
        Master = self(),
        Ref = make_ref(),
        Pid = spawn(fun() ->
            try
                Ans = Fun(),
                Master ! {slave_ans, Ref, Ans}
            catch
                _:E ->
                    ?error_stacktrace("Route_and_supervise error: ~p for "
                    "message id ~p", [E, Id]),
                    Master ! {slave_ans, Ref, #processing_status{code = 'ERROR'}}
            end
        end),
        {Pid, Ref}
    end,
    delegate(Fun2, Id).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes function that handles message returns information needed for
%% asynchronous waiting for answer.
%% @end
%%--------------------------------------------------------------------
-spec delegate(fun(() -> {pid(), reference()}), message_id:id()) ->
    delegate_ans() | {ok, #server_message{}}.
delegate(Fun, Id) ->
    try
        {Pid, Ref} = Fun(),
        case is_pid(Pid) of
            true ->
                {wait, {Id, Pid, Ref}};
            ErrorPid ->
                ?error("Router error: ~p for message id ~p", [ErrorPid, Id]),
                {ok, get_error_msg(Id)}
        end
    catch
        _:E ->
            ?error_stacktrace("Router error: ~p for message id ~p", [E, Id]),
            {ok, get_error_msg(Id)}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves informantion about asynchronous waiting for answer.
%% @end
%%--------------------------------------------------------------------
-spec save_delegation(delegation(), map(), map()) -> {map(), map()}.
save_delegation(Delegation, WaitMap, Pids) ->
    {Id, Pid, Ref} = Delegation,
    WaitMap2 = maps:put(Ref, Id, WaitMap),
    Pids2 = maps:put(Ref, Pid, Pids),
    {WaitMap2, Pids2}.

%%--------------------------------------------------------------------
%% @doc
%% Processes answer and returns message to be sent.
%% @end
%%--------------------------------------------------------------------
-spec process_ans(term(), map(), map()) ->
    {#server_message{}, map(), map()} | wrong_message.
process_ans(ReceivedAns, WaitMap, Pids) ->
    case ReceivedAns of
        {slave_ans, Ref, Ans} ->
            process_ans(Ref, Ans, WaitMap, Pids);
        #worker_answer{id = Ref, response = {ok, Ans}} ->
            process_ans(Ref, Ans, WaitMap, Pids);
        #worker_answer{id = Ref, response = ErrorAns} ->
            process_ans(Ref, {process_error, ErrorAns}, WaitMap, Pids);
        _ ->
            wrong_message
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks processes that handle requests and sends heartbeats.
%% @end
%%--------------------------------------------------------------------
-spec check_processes(map(), map(), fun((message_id:id()) -> ok),
    fun((message_id:id()) -> ok)) -> {map(), map()}.
check_processes(Pids, WaitMap, TimeoutFun, ErrorFun) ->
    {Pids2, Errors} = maps:fold(fun
        (Ref, {Pid, not_alive}, {Acc1, Acc2}) ->
            ?error("Router: process ~p connected with ref ~p is not alive",
                [Pid, Ref]),
            ErrorFun(maps:get(Ref, WaitMap)),
            {Acc1, [Ref | Acc2]};
        (Ref, Pid, {Acc1, Acc2}) ->
            case rpc:call(node(Pid), erlang, is_process_alive, [Pid]) of
                true ->
                    TimeoutFun(maps:get(Ref, WaitMap)),
                    {maps:put(Ref, Pid, Acc1), Acc2};
                _ ->
                    % Wait with error for another heartbeat
                    % (possible race heartbeat/answer)
                    TimeoutFun(maps:get(Ref, WaitMap)),
                    {maps:put(Ref, {Pid, not_alive}, Acc1), Acc2}
            end
    end, {#{}, []}, Pids),
    WaitMap2 = lists:foldl(fun(Ref, Acc) ->
        maps:remove(Ref, Acc)
    end, WaitMap, Errors),
    {Pids2, WaitMap2}.

%%--------------------------------------------------------------------
%% @doc
%% Returns interval between checking of processes that handle requests.
%% @end
%%--------------------------------------------------------------------
-spec get_processes_check_interval() -> non_neg_integer().
get_processes_check_interval() ->
    application:get_env(?APP_NAME, router_processes_check_interval, ?TIMEOUT).

%%--------------------------------------------------------------------
%% @doc
%% Provides heartbeat message.
%% @end
%%--------------------------------------------------------------------
-spec get_heartbeat_msg(message_id:id()) -> #server_message{}.
get_heartbeat_msg(MsgId) ->
    #server_message{message_id = MsgId,
        message_body = #processing_status{code = 'IN_PROGRESS'}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Provides error message.
%% @end
%%--------------------------------------------------------------------
-spec get_error_msg(message_id:id()) -> #server_message{}.
get_error_msg(MsgId) ->
    #server_message{message_id = MsgId,
        message_body = #processing_status{code = 'ERROR'}
    }.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes answer and returns message to be sent.
%% @end
%%--------------------------------------------------------------------
-spec process_ans(reference(), term(), map(), map()) ->
    {#server_message{}, map(), map()} | wrong_message.
process_ans(Ref, {process_error, ErrorAns}, WaitMap, Pids) ->
    case maps:get(Ref, WaitMap, undefined) of
        undefined ->
            wrong_message;
        Id ->
            ?error("Router wrong answer: ~p for message id ~p", [ErrorAns, Id]),
            WaitMap2 = maps:remove(Ref, WaitMap),
            Pids2 = maps:remove(Ref, Pids),
            {get_error_msg(Id), WaitMap2, Pids2}
    end;
process_ans(Ref, Ans, WaitMap, Pids) ->
    case maps:get(Ref, WaitMap, undefined) of
        undefined ->
            wrong_message;
        Id ->
            Return = #server_message{message_id = Id, message_body = Ans},
            WaitMap2 = maps:remove(Ref, WaitMap),
            Pids2 = maps:remove(Ref, Pids),
            {Return, WaitMap2, Pids2}
    end.
