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

-include("cluster_elements/worker_host/worker_protocol.hrl").
-include("cluster_elements/request_dispatcher/worker_map.hrl").
-include_lib("ctool/include/logging.hrl").

-define(DEFAULT_REQUEST_TIMEOUT, timer:seconds(10)).

%% API
-export([call/2, call/3, call/4, multicall/2, multicall/3,
    cast/2, cast/3, cast/4, cast/5, multicast/2, multicast/3, multicast/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Synchronously send request to worker with default timeout.
%% @equiv call(WorkerName, Request, ?DEFAULT_REQUEST_TIMEOUT)
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerName :: atom(), Request :: term()) ->
    ok | {ok, term()} | {error, term()}.
call(WorkerName, Request) ->
    call(WorkerName, Request, ?DEFAULT_REQUEST_TIMEOUT).

%%--------------------------------------------------------------------
%% @doc
%% Synchronously send request to worker with default worker selection type.
%% @equiv call(WorkerName, Request, Timeout, ?DEFAULT_WORKER_SELECTION_TYPE)
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerName :: atom(), Request :: term(), Timeout :: timeout()) ->
    ok | {ok, term()} | {error, term()}.
call(WorkerName, Request, Timeout) ->
    call(WorkerName, Request, Timeout, ?DEFAULT_WORKER_SELECTION_TYPE).

%%--------------------------------------------------------------------
%% @doc
%% Synchronously send request to worker selected by given 'SelectionType'
%% algorithm, with given timeout.
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerName :: atom(), Request :: term(), Timeout :: timeout(),
    SelectionType :: selection_type()) -> ok | {ok, term()} | {error, term()}.
call(WorkerName, Request, Timeout, SelectionType) ->
    MsgId = make_ref(),
    case worker_map:get_worker_node(WorkerName, SelectionType) of
        {ok, Node} ->
            gen_server:cast({WorkerName, Node}, #worker_request{req = Request,
                id = MsgId, reply_to = {proc, self()}}),
            receive
                #worker_answer{id = MsgId, response = Response} -> Response
            after Timeout ->
                ?error("Worker: ~p, request: ~p exceeded timeout of ~p ms",
                    [WorkerName, Request, Timeout]),
                {error, timeout}
            end;
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Synchronously send request to all workers with default timeout.
%% @equiv multicall(WorkerName, Request, ?DEFAULT_REQUEST_TIMEOUT)
%% @end
%%--------------------------------------------------------------------
-spec multicall(WorkerName :: atom(), Request :: term()) ->
    [{Node :: node(), ok | {ok, term()} | {error, term()}}].
multicall(WorkerName, Request) ->
    multicall(WorkerName, Request, ?DEFAULT_REQUEST_TIMEOUT).

%%--------------------------------------------------------------------
%% @doc
%% Synchronously send request to all workers with given timeout.
%% @end
%%--------------------------------------------------------------------
-spec multicall(WorkerName :: atom(), Request :: term(), Timeout :: timeout()) ->
    [{Node :: node(), ok | {ok, term()} | {error, term()}}].
multicall(WorkerName, Request, Timeout) ->
    {ok, Nodes} = worker_map:get_worker_nodes(WorkerName),
    utils:pmap(fun(Node) ->
        {Node, call(WorkerName, Request, Timeout, {node, Node})}
    end, Nodes).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker.
%% @equiv cast(WorkerName, Request, undefined)
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerName :: atom(), Request :: term()) -> ok | {error, term()}.
cast(WorkerName, Request) ->
    cast(WorkerName, Request, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker, answer is expected at ReplyTo
%% process/gen_server.
%% @equiv cast(WorkerName, Request, ReplyTo, undefined)
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerName :: atom(), Request :: term(), ReplyTo :: {proc, pid()}
    | {gen_serv, atom() | pid()}) -> ok | {error, term()}.
cast(WorkerName, Request, ReplyTo) ->
    cast(WorkerName, Request, ReplyTo, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker, answer with given MsgId is
%% expected at ReplyTo process/gen_server. The answer would be
%% 'worker_answer' record.
%% @equiv cast(WorkerName, Request, ReplyTo, MsgId, ?DEFAULT_WORKER_SELECTION_TYPE)
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerName :: atom(), Request :: term(), ReplyTo :: {proc, pid()}
    | {gen_serv, atom() | pid()}, MsgId :: term() | undefined) -> ok | {error, term()}.
cast(WorkerName, Request, ReplyTo, MsgId) ->
    cast(WorkerName, Request, ReplyTo, MsgId, ?DEFAULT_WORKER_SELECTION_TYPE).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker (selected according to given
%% 'SelectionType' algorithm), answer with given MsgId is
%% expected at ReplyTo process/gen_server. The answer would be
%% 'worker_answer' record.
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerName :: atom(), Request :: term(), ReplyTo :: {proc, pid()}
    | {gen_serv, atom() | pid()}, MsgId :: term() | undefined,
    SelectionType :: selection_type()) -> ok | {error, term()}.
cast(WorkerName, Request, ReplyTo, MsgId, SelectionType) ->
    case worker_map:get_worker_node(WorkerName, SelectionType) of
        {ok, Node} ->
            gen_server:cast({WorkerName, Node}, #worker_request{req = Request,
                id = MsgId, reply_to = ReplyTo});
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to all workers.
%% @equiv multicast(WorkerName, Request, undefined)
%% @end
%%--------------------------------------------------------------------
-spec multicast(WorkerName :: atom(), Request :: term()) ->
    [{Node :: node(), ok | {error, term()}}].
multicast(WorkerName, Request) ->
    multicast(WorkerName, Request, undefiend).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to all workers, answer is expected at
%% ReplyTo process/gen_server.
%% @equiv multicast(WorkerName, Request, ReplyTo, undefined)
%% @end
%%--------------------------------------------------------------------
-spec multicast(WorkerName :: atom(), Request :: term(), ReplyTo :: {proc, pid()}
    | {gen_serv, atom() | pid()}) -> [{Node :: node(), ok | {error, term()}}].
multicast(WorkerName, Request, ReplyTo) ->
    multicast(WorkerName, Request, ReplyTo, undefiend).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to all workers, answer with given MsgId
%% is expected at ReplyTo process/gen_server. The answer would be
%% list of pairs: node and associated 'worker_answer' record.
%% @end
%%--------------------------------------------------------------------
-spec multicast(WorkerName :: atom(), Request :: term(), ReplyTo :: {proc, pid()}
    | {gen_serv, atom() | pid()}, MsgId :: term() | undefined) ->
    [{Node :: node(), ok | {error, term()}}].
multicast(WorkerName, Request, ReplyTo, MsgId) ->
    {ok, Nodes} = worker_map:get_worker_nodes(WorkerName),
    utils:pmap(fun(Node) ->
        {Node, cast(WorkerName, Request, ReplyTo, MsgId, {node, Node})}
    end, Nodes).

%%%===================================================================
%%% Internal functions
%%%===================================================================
