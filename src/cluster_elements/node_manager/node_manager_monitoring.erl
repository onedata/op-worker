%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions for gathering and calculating
%%% statistical data
%%% @end
%%%-------------------------------------------------------------------
-module(node_manager_monitoring).
-author("Tomasz Lichon").

%% API
-export([node_load/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @todo implement
%% @doc
%% Calculates load on node
%% @end
%%--------------------------------------------------------------------
-spec node_load(Node :: node()) -> float().
node_load(_Node) -> 1.

%%%===================================================================
%%% Internal functions
%%%===================================================================