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
%%% Responsibilities:
%%%   - Supervisor tree initialization
%%%   - Routing clients to main or catching monitors
%%%   - Maintaining opaque connection state
%%%   - Interpreting EXIT signals from monitors
%%% 
%%% This module hides all internal details (PIDs, monitor types, supervisors)
%%% from clients. Clients work only with opaque subscription() type.
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


-record(subscription, {
    monitor_type :: main | catching,
    main_pid :: pid(),
    catching_pid :: pid() | undefined
}).
-opaque subscription() :: #subscription{}.

-export_type([subscription/0]).


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
    {ok, subscription()} | {error, term()}.
subscribe(SpaceId, SessionId, FilesMonitoringSpec, SinceSeq) ->
    InitialSubscribeReq = #subscribe_req{
        observer_pid = self(),
        session_id = SessionId,
        files_monitoring_spec = FilesMonitoringSpec,
        since_seq = SinceSeq
    },

    Node = datastore_key:any_responsible_node(SpaceId),

    case node() of
        Node -> do_subscribe(SpaceId, InitialSubscribeReq);
        _ -> erpc:call(Node, ?MODULE, do_subscribe, [SpaceId, InitialSubscribeReq])
    end.


-spec do_subscribe(od_space:id(), space_files_monitor_common:subscribe_req()) ->
    {ok, subscription()} | {error, term()}.
do_subscribe(SpaceId, InitialSubscribeReq) ->
    %% Ensure supervisor tree exists for this space
    {ok, SpaceSupPid} = files_monitoring_sup:ensure_monitoring_tree_for_space(SpaceId),
    MainMonitorPid = space_files_monitoring_sup:get_main_monitor_pid(SpaceSupPid),

    case space_files_main_monitor:try_subscribe(MainMonitorPid, InitialSubscribeReq) of
        ok ->
            %% Client is caught up - connected to main
            {ok, #subscription{
                monitor_type = main,
                main_pid = MainMonitorPid,
                catching_pid = undefined
            }};

        {error, {main_ahead, UntilSeq}} ->
            %% Client is behind - start catching monitor
            SubscribeReq = InitialSubscribeReq#subscribe_req{until_seq = UntilSeq},

            CatchingSupPid = space_files_monitoring_sup:get_catching_monitors_sup_pid(SpaceSupPid),
            {ok, CatchingPid} = space_files_catching_monitors_sup:start_catching_monitor(
                CatchingSupPid, MainMonitorPid, SubscribeReq
            ),

            {ok, #subscription{
                monitor_type = catching,
                main_pid = MainMonitorPid,
                catching_pid = CatchingPid
            }};

        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Handles EXIT signal from a monitor process.
%% 
%% Interprets the exit reason and updates connection state or signals
%% that the handler should terminate.
%% @end
%%--------------------------------------------------------------------
-spec handle_exit(pid(), term(), subscription()) ->
    {ok, subscription()} | stop.
handle_exit(ExitPid, _Reason, #subscription{main_pid = ExitPid}) ->
    %% EXIT from main monitor - always fatal
    stop;

handle_exit(
    ExitPid,
    {shutdown, caught_up},
    Subscription = #subscription{monitor_type = catching, catching_pid = ExitPid}
) ->
    %% EXIT from catching monitor after successful takeover
    {ok, Subscription#subscription{monitor_type = main, catching_pid = undefined}};

handle_exit(ExitPid, _Reason, #subscription{catching_pid = ExitPid}) ->
    %% EXIT from catching monitor before catching to main monitor
    stop.
