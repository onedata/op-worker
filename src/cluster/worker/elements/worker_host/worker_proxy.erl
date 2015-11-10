%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is fasade of worker_host gen_server api,
%%% It simply translates arguments into apropriate #worker_request,
%%% and sends it to worker.
%%% @end
%%%-------------------------------------------------------------------
-module(worker_proxy).
-author("Tomasz Lichon").
-author("Krzysztof Trzepla").

-include("cluster/worker/elements/worker_host/worker_protocol.hrl").
-include_lib("ctool/include/logging.hrl").

-define(DEFAULT_REQUEST_TIMEOUT, timer:seconds(10)).

%% API
-export([call/2, call/3, multicall/2, multicall/3,
    cast/2, cast/3, cast/4, multicast/2, multicast/3, multicast/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Synchronously send request to worker with default timeout.
%% @equiv call(WorkerName, Request, ?DEFAULT_REQUEST_TIMEOUT)
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerRef :: request_dispatcher:worker_ref(), Request :: term()) ->
    Result :: term() | {error, term()}.
call(WorkerRef, Request) ->
    call(WorkerRef, Request, ?DEFAULT_REQUEST_TIMEOUT).

%%--------------------------------------------------------------------
%% @doc
%% Synchronously send request to worker with given timeout.
%% @equiv call(WorkerName, Request, Timeout, ?DEFAULT_WORKER_SELECTION_TYPE)
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerRef :: request_dispatcher:worker_ref(), Request :: term(), Timeout :: timeout()) ->
    Result :: term() | {error, term()}.
call(WorkerRef, Request, Timeout) ->
    MsgId = make_ref(),
    case choose_node(WorkerRef) of
        {ok, Name, Node} ->
            spawn(Node, worker_host, proc_request,
                [Name, #worker_request{req = Request, id = MsgId, reply_to = {proc, self()}}]),
            receive
                #worker_answer{id = MsgId, response = Response} -> Response
            after Timeout ->
                ?error("Worker: ~p, request: ~p exceeded timeout of ~p ms",
                    [WorkerRef, Request, Timeout]),
                {error, timeout}
            end;
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Synchronously send request to all workers with default timeout.
%% @equiv multicall(WorkerName, Request, ?DEFAULT_REQUEST_TIMEOUT)
%% @end
%%--------------------------------------------------------------------
-spec multicall(WorkerName :: request_dispatcher:worker_name(), Request :: term()) ->
    [{Node :: node(), ok | {ok, term()} | {error, term()}}].
multicall(WorkerName, Request) ->
    multicall(WorkerName, Request, ?DEFAULT_REQUEST_TIMEOUT).

%%--------------------------------------------------------------------
%% @doc
%% Synchronously send request to all workers with given timeout.
%% Returns list of pairs: node and associated answer.
%% @end
%%--------------------------------------------------------------------
-spec multicall(WorkerName :: request_dispatcher:worker_name(), Request :: term(), Timeout :: timeout()) ->
    [{Node :: node(), ok | {ok, term()} | {error, term()}}].
multicall(WorkerName, Request, Timeout) ->
    {ok, Nodes} = request_dispatcher:get_worker_nodes(WorkerName),
    utils:pmap(fun(Node) ->
        {Node, call({WorkerName, Node}, Request, Timeout)}
    end, Nodes).


%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker.
%% @equiv cast(WorkerRef, Request, undefined)
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerRef :: request_dispatcher:worker_ref(), Request :: term()) -> ok | {error, term()}.
cast(WorkerRef, Request) ->
    cast(WorkerRef, Request, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker, answer is expected at ReplyTo
%% process/gen_server.
%% @equiv cast(WorkerName, Request, ReplyTo, undefined)
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerRef :: request_dispatcher:worker_ref(), Request :: term(), ReplyTo :: process_ref()) ->
    ok | {error, term()}.
cast(WorkerRef, Request, ReplyTo) ->
    cast(WorkerRef, Request, ReplyTo, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker, answer with given MsgId is
%% expected at ReplyTo process/gen_server. The answer will be
%% 'worker_answer' record.
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerRef :: request_dispatcher:worker_ref(), Request :: term(), ReplyTo :: process_ref(),
    MsgId :: term() | undefined) -> ok | {error, term()}.
cast(WorkerRef, Request, ReplyTo, MsgId) ->
    case choose_node(WorkerRef) of
        {ok, Name, Node} ->
            spawn(Node, worker_host, proc_request,
                [Name, #worker_request{req = Request, id = MsgId, reply_to = ReplyTo}]),
            ok;
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to all workers.
%% @equiv multicast(WorkerName, Request, undefined)
%% @end
%%--------------------------------------------------------------------
-spec multicast(WorkerName :: request_dispatcher:worker_name(), Request :: term()) -> ok.
multicast(WorkerName, Request) ->
    multicast(WorkerName, Request, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to all workers, answer is expected at
%% ReplyTo process/gen_server.
%% @equiv multicast(WorkerName, Request, ReplyTo, undefined)
%% @end
%%--------------------------------------------------------------------
-spec multicast(WorkerName :: request_dispatcher:worker_name(), Request :: term(),
    ReplyTo :: process_ref()) -> ok.
multicast(WorkerName, Request, ReplyTo) ->
    multicast(WorkerName, Request, ReplyTo, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to all workers, answers with given MsgId
%% are expected at ReplyTo process/gen_server. The answers will be
%% 'worker_answer' records.
%% @end
%%--------------------------------------------------------------------
-spec multicast(WorkerName :: request_dispatcher:worker_name(), Request :: term(),
    ReplyTo :: process_ref(), MsgId :: term() | undefined) -> ok.
multicast(WorkerName, Request, ReplyTo, MsgId) ->
    {ok, Nodes} = request_dispatcher:get_worker_nodes(WorkerName),
    utils:pforeach(fun(Node) ->
        cast({WorkerName, Node}, Request, ReplyTo, MsgId)
    end, Nodes).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Chooses a node to send a worker request to.
%% @end
%%--------------------------------------------------------------------
-spec choose_node(WorkerRef :: request_dispatcher:worker_ref()) ->
    {ok, WorkerName :: request_dispatcher:worker_name(), WorkerNode :: atom()} | {error, term()}.
choose_node(WorkerRef) ->
    case WorkerRef of
        {WName, WNode} ->
            {ok, WName, WNode};
        WName ->
            case request_dispatcher:get_worker_node(WName) of
                {ok, WNode} ->
                    {ok, WName, WNode};
                {error, Error} ->
                    {error, Error}
            end
    end.
