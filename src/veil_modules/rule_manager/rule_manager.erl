%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour to provide
%% functionality of rule engines manager (update of rules in all types
%% of rule engines).
%% @end
%% ===================================================================

-module(rule_manager).
-behaviour(worker_plugin_behaviour).
-include("logging.hrl").
-include("registered_names.hrl").
-include("veil_modules/dao/dao_helper.hrl").
-include("veil_modules/dao/dao.hrl").
-include("communication_protocol_pb.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0, send_push_msg/1]).

init(_Args) ->
  ets:new(?MSG_ID_TO_HANDLER_ID, [named_table, set, public]),
  ets:new(?ACK_HANDLERS, [named_table, set, public]),
	[].

handle(_ProtocolVersion, ping) ->
  pong;

handle(_ProtocolVersion, healthcheck) ->
	ok;

handle(_ProtocolVersion, get_version) ->
  node_manager:check_vsn();

handle(_ProtocolVersion, _Msg) ->
	ok.

on_complete(Message, SuccessFuseIds, FailFuseIds) ->
  ?info("oncomplete called"),
  case FailFuseIds of
    [] -> ?info("------- ack success --------");
    _ -> ?info("-------- ack fail ---------")
  end.

send_push_msg(ProtocolVersion) ->

  TestAtom11 = #atom{value = "test_atom11"},
  TestAtom = #atom{value = "test_atom2"},

  Rows = fetch_rows(?FUSE_CONNECTIONS_VIEW, #view_query_args{}),
  FuseIds = lists:map(fun(#view_row{key = FuseId}) -> FuseId end, Rows),
  UniqueFuseIds = sets:to_list(sets:from_list(FuseIds)),
  ?info("----- bazinga 345 ---"),

  lists:foreach(fun(FuseId) -> request_dispatcher:send_to_fuse(FuseId, TestAtom11, "communication_protocol") end, UniqueFuseIds),
  ?info("----- bazinga 346 ---"),

  OnComplete = fun(SuccessFuseIds, FailFuseIds) -> on_complete(TestAtom, SuccessFuseIds, FailFuseIds) end,
  worker_host:send_to_user({uuid, "20000"}, TestAtom, "communication_protocol", OnComplete, ProtocolVersion),
  ?info("---- bazinga sent to user ---").

cleanup() ->
	ok.

fetch_rows(ViewName, QueryArgs) ->
  case dao:list_records(ViewName, QueryArgs) of
    {ok, #view_result{rows = Rows}} ->
      Rows;
    Error ->
      ?error("Invalid view response: ~p", [Error]),
      throw(invalid_data)
  end.
