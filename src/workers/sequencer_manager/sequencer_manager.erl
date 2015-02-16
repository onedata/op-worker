%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for creating and forwarding requests to
%%% sequencer manager worker.
%%% @end
%%%-------------------------------------------------------------------
-module(sequencer_manager).
-author("Krzysztof Trzepla").

%% API
-export([get_or_create_sequencer_dispatcher/1, remove_sequencer_dispatcher/1]).

-define(TIMEOUT, timer:seconds(5)).
-define(SEQUENCER_MANAGER_WORKER, sequencer_manager_worker).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns pid of sequencer dispatcher for given session. If sequencer
%% dispatcher does not exist it is instantiated.
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_sequencer_dispatcher(SessId :: session:id()) ->
    {ok, SeqDisp :: pid()} | {error, Reason :: term()}.
get_or_create_sequencer_dispatcher(SessId) ->
    worker_proxy:call(
        ?SEQUENCER_MANAGER_WORKER,
        {get_or_create_sequencer_dispatcher, SessId},
        ?TIMEOUT,
        prefer_local
    ).

%%--------------------------------------------------------------------
%% @doc
%% Removes sequencer dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec remove_sequencer_dispatcher(SessId :: session:id()) ->
    ok | {error, Reason :: term()}.
remove_sequencer_dispatcher(SessId) ->
    worker_proxy:call(
        ?SEQUENCER_MANAGER_WORKER,
        {remove_sequencer_dispatcher, SessId},
        ?TIMEOUT,
        prefer_local
    ).

%%%===================================================================
%%% Internal functions
%%%===================================================================

