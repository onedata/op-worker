%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for creating and forwarding requests to
%%% event manager worker.
%%% @end
%%%-------------------------------------------------------------------
-module(event_manager).
-author("Krzysztof Trzepla").

-include("workers/event_manager/read_event.hrl").
-include("workers/event_manager/write_event.hrl").
-include("cluster_elements/protocol_handler/credentials.hrl").

%% API
-export([get_or_create_event_dispatcher/1, remove_event_dispatcher/1]).

-export_type([event/0, event_subscription/0, event_producer/0]).

-type event() :: #read_event{} | #write_event{}.
-type event_subscription() :: #read_event_subscription{}
| #write_event_subscription{}.
-type event_producer() :: fuse.

-define(TIMEOUT, timer:seconds(5)).
-define(EVENT_MANAGER_WORKER, event_manager_worker).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns pid of event dispatcher for given session. If event
%% dispatcher does not exist it is instantiated.
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_event_dispatcher(SessionId :: session_id()) ->
    {ok, SeqDisp :: pid()} | {error, Reason :: term()}.
get_or_create_event_dispatcher(SessionId) ->
    worker_proxy:call(
        ?EVENT_MANAGER_WORKER,
        {get_or_create_event_dispatcher, SessionId},
        ?TIMEOUT,
        prefer_local
    ).

%%--------------------------------------------------------------------
%% @doc
%% Removes event dispatcher for client session.
%% @end
%%--------------------------------------------------------------------
-spec remove_event_dispatcher(SessionId :: session_id()) ->
    ok | {error, Reason :: term()}.
remove_event_dispatcher(SessionId) ->
    worker_proxy:call(
        ?EVENT_MANAGER_WORKER,
        {remove_event_dispatcher, SessionId},
        ?TIMEOUT,
        prefer_local
    ).

%%%===================================================================
%%% Internal functions
%%%===================================================================

