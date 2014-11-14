%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% gateway functionality (transfer of files between data centers).
%% @end
%% ===================================================================

-module(gateway).
-behaviour(worker_plugin_behaviour).

-include_lib("ctool/include/logging.hrl").

-ifdef(TEST).
-define(NOTIFICATION_STATE, notification_state).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0]).

-ifdef(TEST).
init(_Args) ->
  ets:new(?NOTIFICATION_STATE, [named_table, public, set]),
  [].
-else.
init(_Args) ->
  [].
-endif.

handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, healthcheck) ->
	ok;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();


handle(_ProtocolVersion, {node_lifecycle_notification, Node, Module, Action, Pid}) ->
  handle_node_lifecycle_notification(Node, Module, Action, Pid),
  ok;

handle(_ProtocolVersion, node_lifecycle_get_notification) ->
  node_lifecycle_get_notification();


handle(_ProtocolVersion, _Msg) ->
  ?warning("Wrong request: ~p", [_Msg]),
	ok.


%% handle_node_lifecycle_notification/4
%% ====================================================================
%% @doc Handles lifecycle calls
-spec handle_node_lifecycle_notification(Node :: list(), Module :: atom(), Action :: atom(), Pid :: pid()) -> ok.
%% ====================================================================
-ifdef(TEST).
handle_node_lifecycle_notification(Node, Module, Action, Pid) ->
  case ets:lookup(?NOTIFICATION_STATE, node_lifecycle_notification) of
    [{_, L}] -> ets:insert(?NOTIFICATION_STATE, {node_lifecycle_notification, [{Node, Module, Action, Pid}|L]});
    _ -> ets:insert(?NOTIFICATION_STATE, {node_lifecycle_notification, [{Node, Module, Action, Pid}]})
  end,
  ok.
-else.
handle_node_lifecycle_notification(_Node, _Module, _Action, _Pid) ->
  ok.
-endif.

%% node_lifecycle_get_notification/0
%% ====================================================================
%% @doc Handles test calls.
-spec node_lifecycle_get_notification() -> ok | term().
%% ====================================================================
-ifdef(TEST).
node_lifecycle_get_notification() ->
  Notification = ets:lookup(?NOTIFICATION_STATE, node_lifecycle_notification),
  {ok, {node_lifecycle, Notification}}.
-else.
node_lifecycle_get_notification() ->
  ok.
-endif.

cleanup() ->
	ok.
