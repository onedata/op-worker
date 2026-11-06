%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API for space file event monitoring.
%%% 
%%% Can be used by any protocol: SSE, WebSocket, GraphQL, gRPC, etc.
%%% The client (handler) is responsible for protocol-specific formatting.
%%% @end
%%%-------------------------------------------------------------------
-module(space_files_monitoring_api).
-author("Bartosz Walkowicz").

-include("http/space_file_events_stream.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([subscribe/4, handle_exit/3]).

%% Exported for rpc
-export([do_subscribe/2]).


%%%===================================================================
%%% API functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Subscribes a client to file events in a space.
%% 
%% This is the single entry point for both initial connections and reconnections.
%% This function decides whether to route the client to main monitor or start
%% a catching monitor based on Last-Event-Id.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(
    od_space:id(), 
    session:id(), 
    space_files_monitoring_spec:t(),
    undefined | couchbase_changes:seq()
) -> 
    {ok, files_monitoring_manager:subscription()} | {error, term()}.
subscribe(SpaceId, SessionId, FilesMonitoringSpec, SinceSeq) ->
    SubscribeReq = #subscribe_req{
        observer_pid = self(),
        session_id = SessionId,
        files_monitoring_spec = FilesMonitoringSpec,
        since_seq = SinceSeq
    },

    Node = datastore_key:any_responsible_node(SpaceId),

    case node() of
        Node -> do_subscribe(SpaceId, SubscribeReq);
        _ -> erpc:call(Node, ?MODULE, do_subscribe, [SpaceId, SubscribeReq])
    end.


-spec do_subscribe(od_space:id(), space_files_monitor_common:subscribe_req()) ->
    {ok, files_monitoring_manager:subscription()} | errors:error().
do_subscribe(SpaceId, SubscribeReq) ->
    files_monitoring_manager:try_subscribe(SpaceId, SubscribeReq).


-spec handle_exit(pid(), term(), files_monitoring_manager:subscription()) ->
    {ok, files_monitoring_manager:subscription()} | stop.
handle_exit(ExitPid, Reason, Subscription) ->
    files_monitoring_manager:handle_monitor_exit(ExitPid, Reason, Subscription).
