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

-include("workers/event_manager/events.hrl").
-include("workers/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit/2, subscribe/1, unsubscribe/1]).
-export([get_or_create_event_dispatcher/1, remove_event_dispatcher/1]).

-export_type([event/0, subscription/0, subscription_id/0, producer/0]).

-type event() :: #read_event{} | #write_event{}.
-type subscription() :: #read_event_subscription{}
| #write_event_subscription{}.
-type subscription_id() :: non_neg_integer().
-type producer() :: fuse | gui.

-define(TIMEOUT, timer:seconds(5)).
-define(EVENT_MANAGER_WORKER, event_manager_worker).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Emits an event to event manager associated with given session.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: event(), SessionId :: session:id()) ->
    ok | {error, Reason :: term()}.
emit(Evt, SessionId) ->
    worker_proxy:call(
        ?EVENT_MANAGER_WORKER,
        {emit, Evt, SessionId},
        ?TIMEOUT,
        prefer_local
    ).

%%--------------------------------------------------------------------
%% @doc
%% Creates subscription for events.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Sub :: subscription()) ->
    {ok, SubId :: subscription_id()} | {error, Reason :: term()}.
subscribe(Sub) ->
    worker_proxy:call(
        ?EVENT_MANAGER_WORKER,
        {subscribe, Sub},
        ?TIMEOUT,
        prefer_local
    ).

%%--------------------------------------------------------------------
%% @doc
%% Removes subscription for events.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(SubId :: subscription_id()) ->
    ok | {error, Reason :: term()}.
unsubscribe(SubId) ->
    worker_proxy:call(
        ?EVENT_MANAGER_WORKER,
        {unsubscribe, SubId},
        ?TIMEOUT,
        prefer_local
    ).

%%--------------------------------------------------------------------
%% @doc
%% Returns pid of event dispatcher for given session. If event
%% dispatcher does not exist it is instantiated.
%% @end
%%--------------------------------------------------------------------
-spec get_or_create_event_dispatcher(SessionId :: session:id()) ->
    {ok, EvtDisp :: pid()} | {error, Reason :: term()}.
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
-spec remove_event_dispatcher(SessionId :: session:id()) ->
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

