%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
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

-include("cluster_elements/worker_host/worker_protocol.hrl").
-include("cluster_elements/request_dispatcher/worker_map.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([call/2, call/3, call/4, cast/2, cast/3, cast/4, cast/5]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Call with default timeout
%% @equiv call(WorkerRef, Request, 10000)
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerRef :: worker_ref(), Request :: term()) -> ok | {ok, term()} | {error, term()}.
call(WorkerRef, Request) ->
    call(WorkerRef, Request, 10000).

%%--------------------------------------------------------------------
%% @doc
%% Call with default worker selection type
%% @equiv call(WorkerRef, Request, Timeout, ?default_worker_selection_type)
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerRef :: worker_ref(), Request :: term(), Timeout :: integer()) -> ok | {ok, term()} | {error, term()}.
call(WorkerRef, Request, Timeout) ->
    call(WorkerRef, Request, Timeout, ?default_worker_selection_type).

%%--------------------------------------------------------------------
%% @doc
%% Call worker node, selected by given 'SelectionType' algorithm, with given timeout
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerRef :: worker_ref(), Request :: term(), Timeout :: integer(), SelectionType :: selection_type()) ->
    ok | {ok, term()} | {error, term()}.
call(WorkerRef, Request, Timeout, SelectionType) ->
    MsgId = make_ref(),
    case choose_node(WorkerRef, SelectionType) of
        {ok, Name, Node} ->
            gen_server:cast({Name, Node}, #worker_request{req = Request, id = MsgId, reply_to = {proc, self()}}),
            receive
                #worker_answer{id = MsgId, response = Response} -> Response
            after Timeout ->
                ?error("Worker: ~p, request: ~p exceeded timeout of ~p ms", [WorkerRef, Request, Timeout]),
                {error, timeout}
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker
%% @equiv cast(WorkerRef, Request, undefined).
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerRef :: worker_ref(), Request :: term()) -> ok | {error, term()}.
cast(WorkerRef, Request) ->
    cast(WorkerRef, Request, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker, answer is expected at ReplyTo
%% process/gen_server
%% @equiv cast(WorkerRef, Request, ReplyTo, undefined).
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerRef :: worker_ref(), Request :: term(), ReplyTo :: {proc, pid()} | {gen_serv, atom() | pid()}) ->
    ok | {error, term()}.
cast(WorkerRef, Request, ReplyTo) ->
    cast(WorkerRef, Request, ReplyTo, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker, answer with given MsgId is
%% expected at ReplyTo process/gen_server. The answer would be
%% 'worker_answer' record
%% @equiv cast(WorkerRef, Request, ReplyTo, MsgId, ?default_worker_selection_type).
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerRef :: worker_ref(), Request :: term(), ReplyTo :: {proc, pid()} | {gen_serv, atom() | pid()}, MsgId :: term() | undefined) ->
    ok | {error, term()}.
cast(WorkerRef, Request, ReplyTo, MsgId) ->
    cast(WorkerRef, Request, ReplyTo, MsgId, ?default_worker_selection_type).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker (selected according to given
%% 'SelectionType' algorithm), answer with given MsgId is
%% expected at ReplyTo process/gen_server. The answer would be
%% 'worker_answer' record
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerRef :: worker_ref(), Request :: term(), ReplyTo :: {proc, pid()} | {gen_serv, atom() | pid()},
    MsgId :: term() | undefined, SelectionType :: selection_type()) ->
    ok | {error, term()}.
cast(WorkerRef, Request, ReplyTo, MsgId, SelectionType) ->
    MsgId = make_ref(),
    case choose_node(WorkerRef, SelectionType) of
        {ok, Name, Node} ->
            gen_server:cast({Name, Node}, #worker_request{req = Request, id = MsgId, reply_to = ReplyTo});
        Error ->
            Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Chooses a node to send a worker request to.
%% @end
%%--------------------------------------------------------------------
-spec choose_node(WorkerRef :: worker_ref(), SelectionType :: selection_type()) -> {ok, WorkerName :: atom(), WorkerNode :: atom()} | {error, term()}.
choose_node(WorkerRef, SelectionType) ->
    case WorkerRef of
        {WName, WNode} ->
            {ok, WName, WNode};
        WName ->
            case worker_map:get_worker_node(WName, SelectionType) of
                {ok, WNode} ->
                    {ok, WName, WNode};
                {error, Error} ->
                    {error, Error}
            end
    end.