%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% notify_state_change annotation implementation, that tracks state changes
%%% of dispatcher and ccm
%%% @end
%%%-------------------------------------------------------------------
-module(notify_state_change).
-author("Tomasz Lichon").

-annotation('function').

-include("cluster_elements/cluster_manager/cluster_manager_state.hrl").
-include("cluster_elements/request_dispatcher/request_dispatcher_state.hrl").
-include("global_definitions.hrl").
-include_lib("annotations/include/types.hrl").
-include_lib("ctool/include/logging.hrl").

-compile(export_all).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Function executed after handling casts in ccm and dispatcher, it tracks
%% cluster state changes and notifies 'cluster_state_notifier'
%% @end
%%--------------------------------------------------------------------
-spec after_advice(#annotation{}, M :: atom(), F :: atom(), _Inputs :: list(), Result :: term()) -> NewResult :: term().
after_advice(#annotation{data=ccm}, _M, _F, _Inputs, Result = {noreply, #cm_state{nodes = Nodes, state_num = StateNum}}) ->
    cluster_state_notifier:cast({ccm_state_updated, [node() | Nodes], StateNum}),
    Result;
after_advice(#annotation{data=dispatcher}, _M, _F, _Inputs, Result = {noreply, #dispatcher_state{state_num = StateNum}}) ->
    cluster_state_notifier:cast({dispatcher_state_updated, node(), StateNum}),
    Result;
after_advice(_, M, F, Inputs, Result) ->
    ?warning("Ignoring state change after call: ~p:~p(~p) -> ~p", [M, F, Inputs, Result]),
    Result.

%%%===================================================================
%%% Internal functions
%%%===================================================================