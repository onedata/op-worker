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

-include("cluster_elements/worker_host/worker_proxy.hrl").
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
%% @equiv call(WorkerName, Request, 10000)
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerName :: atom(), Request :: term()) -> ok | {ok, term()} | {error, term()}.
call(WorkerName, Request) ->
    call(WorkerName, Request, 10000).

%%--------------------------------------------------------------------
%% @doc
%% Call with default worker selection type
%% @equiv call(WorkerName, Request, Timeout, ?default_worker_selection_type)
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerName :: atom(), Request :: term(), Timeout :: integer()) -> ok | {ok, term()} | {error, term()}.
call(WorkerName, Request, Timeout) ->
    call(WorkerName, Request, Timeout, ?default_worker_selection_type).

%%--------------------------------------------------------------------
%% @doc
%% Call worker node, selected by given 'SelectionType' algorithm, with given timeout
%% @end
%%--------------------------------------------------------------------
-spec call(WorkerName :: atom(), Request :: term(), Timeout :: integer(), SelectionType :: selection_type()) ->
    ok | {ok, term()} | {error, term()}.
call(WorkerName, Request, Timeout, SelectionType) ->
    MsgId = make_ref(),
    case worker_map:get_worker_node(WorkerName, SelectionType) of
        {ok, Node} ->
            gen_server:cast({WorkerName, Node}, #worker_request{req = Request, id = MsgId, reply_to = {proc, self()}}),
            receive
                #worker_answer{id = MsgId, response = Response} -> Response
            after Timeout ->
                ?error("Worker: ~p, request: ~p exceeded timeout of ~p ms",[WorkerName, Request, Timeout]),
                {error, timeout}
            end;
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker
%% @equiv cast(WorkerName, Request, undefined).
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerName :: atom(), Request :: term()) -> ok | {error, term()}.
cast(WorkerName, Request) ->
    cast(WorkerName, Request, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker, answer is expected at ReplyTo
%% process/gen_server
%% @equiv cast(WorkerName, Request, ReplyTo, undefined).
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerName :: atom(), Request :: term(), ReplyTo :: {proc, pid()} | {gen_serv, atom() | pid()}) ->
    ok | {error, term()}.
cast(WorkerName, Request, ReplyTo) ->
    cast(WorkerName, Request, ReplyTo, undefined).


%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker, answer with given MsgId is
%% expected at ReplyTo process/gen_server. The answer would be
%% 'worker_answer' record
%% @equiv cast(WorkerName, Request, ReplyTo, MsgId, ?default_worker_selection_type).
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerName :: atom(), Request :: term(), ReplyTo :: {proc, pid()} | {gen_serv, atom() | pid()}, MsgId :: term() | undefined) ->
    ok | {error, term()}.
cast(WorkerName, Request, ReplyTo, MsgId) ->
    cast(WorkerName, Request, ReplyTo, MsgId, ?default_worker_selection_type).

%%--------------------------------------------------------------------
%% @doc
%% Asynchronously send request to worker (selected according to given
%% 'SelectionType' algorithm), answer with given MsgId is
%% expected at ReplyTo process/gen_server. The answer would be
%% 'worker_answer' record
%% @end
%%--------------------------------------------------------------------
-spec cast(WorkerName :: atom(), Request :: term(), ReplyTo :: {proc, pid()} | {gen_serv, atom() | pid()},
    MsgId :: term() | undefined, SelectionType :: selection_type()) ->
    ok | {error, term()}.
cast(WorkerName, Request, ReplyTo, MsgId, SelectionType) ->
    case worker_map:get_worker_node(WorkerName, SelectionType) of
        {ok, Node} ->
            gen_server:cast({WorkerName, Node}, #worker_request{req = Request, id = MsgId, reply_to = ReplyTo});
        Error -> Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
