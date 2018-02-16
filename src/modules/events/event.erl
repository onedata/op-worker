%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides API for event processing.
%%% @end
%%%-------------------------------------------------------------------
-module(event).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/exometer_utils.hrl").

%% API
-export([emit/1, emit/2, flush/2, flush/5, subscribe/2, unsubscribe/2]).
-export([get_event_managers/1]).

-export([init_counters/0, init_report/0]).

-export_type([key/0, base/0, type/0, stream/0, manager_ref/0]).

-type key() :: term().
-type base() :: #event{}.
-type type() :: #file_read_event{} | #file_written_event{} |
                #file_attr_changed_event{} | #file_location_changed_event{} |
                #file_perm_changed_event{} | #file_removed_event{} |
                #quota_exceeded_event{} | #file_renamed_event{} |
                #monitoring_event{}.
-type stream() :: #event_stream{}.
-type manager_ref() :: pid() | session:id() | [pid() | session:id()] |
% reference all event managers except one provided
{exclude, pid() | session:id()} |
% reference all event managers except those provided in list
{exclude, [pid() | session:id()]}.

-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, Param)).
-define(EXOMETER_COUNTERS, [emit]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends an event to all subscribed event managers.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: base() | type()) -> ok | {error, Reason :: term()}.
emit(Evt) ->
    case event_router:get_subscribers(Evt) of
        {ok, SessIds} -> emit(Evt, SessIds);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends an event to selected event managers.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: base() | type(), MgrRef :: manager_ref()) ->
    ok | {error, Reason :: term()}.
emit(Evt, {exclude, MgrRef}) ->
    case event_router:get_subscribers(Evt) of
        {ok, SessIds} ->
            Excluded = get_event_managers(MgrRef),
            Subscribed = get_event_managers(SessIds),
            emit(Evt, subtract_unique(Subscribed, Excluded));
        {error, Reason} ->
            {error, Reason}
    end;

emit(#event{} = Evt, MgrRef) ->
    ?update_counter(?EXOMETER_NAME(emit)),
    send_to_event_managers(Evt, get_event_managers(MgrRef));

emit(Evt, MgrRef) ->
    emit(#event{type = Evt}, MgrRef).

%%--------------------------------------------------------------------
%% @doc
%% Forwards flush request to the selected event streams.
%% @end
%%--------------------------------------------------------------------
-spec flush(Req :: #flush_events{}, MgrRef :: manager_ref()) -> ok.
flush(#flush_events{} = Req, MgrRef) ->
    send_to_event_managers(Req, get_event_managers(MgrRef)).

%%--------------------------------------------------------------------
%% @doc
%% Flushes selected event streams associated with a subscription. Injects
%% PID of a process, which should be notified when operation completes, to the
%% event handler context.
%% IMPORTANT! Event handler is responsible for notifying the awaiting process.
%% @end
%%--------------------------------------------------------------------
-spec flush(ProviderId :: oneprovider:id(), Context :: term(),
    SubId :: subscription:id(), Notify :: pid(), MgrRef :: manager_ref()) ->
    RecvRef :: reference().
flush(ProviderId, Context, SubId, Notify, MgrRef) ->
    RecvRef = make_ref(),
    flush(#flush_events{
        provider_id = ProviderId,
        context = Context,
        subscription_id = SubId,
        notify = fun
            (#server_message{message_body = #status{code = ?OK}}) ->
                Notify ! {RecvRef, ok};
            (#server_message{message_body = #status{code = Code}}) ->
                Notify ! {RecvRef, {error, Code}}
        end
    }, MgrRef),
    RecvRef.

%%--------------------------------------------------------------------
%% @doc
%% Sends a subscription to selected event managers.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Sub :: subscription:base() | subscription:type(),
    MgrRef :: manager_ref()) -> SubId :: subscription:id().
subscribe(#subscription{id = undefined} = Sub, MgrRef) ->
    subscribe(Sub#subscription{id = subscription:generate_id()}, MgrRef);

subscribe(#subscription{id = SubId} = Sub, MgrRef) ->
    send_to_event_managers(Sub, get_event_managers(MgrRef)),
    SubId;

subscribe(Sub, MgrRef) ->
    subscribe(#subscription{type = Sub}, MgrRef).

%%--------------------------------------------------------------------
%% @doc
%% Sends a subscription cancellation to selected event managers.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(SubId :: subscription:id() | subscription:cancellation(),
    MgrRef :: manager_ref()) -> ok.
unsubscribe(#subscription_cancellation{} = SubCan, MgrRef) ->
    send_to_event_managers(SubCan, get_event_managers(MgrRef));

unsubscribe(SubId, MgrRef) ->
    unsubscribe(#subscription_cancellation{id = SubId}, MgrRef).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of event managers associated with provided references.
%% A reference can be either an event manager pids or a session IDs.
%% @end
%%--------------------------------------------------------------------
-spec get_event_managers(MgrRef :: manager_ref()) -> Mgrs :: [pid()].
get_event_managers([]) ->
    [];

get_event_managers([_ | _] = MgrRefs) ->
    lists:foldl(fun(MgrRef, Mgrs) ->
        case get_event_manager(MgrRef) of
            {ok, Mgr} -> [Mgr | Mgrs];
            {error, _} -> Mgrs
        end
    end, [], MgrRefs);

get_event_managers(MgrRef) ->
    get_event_managers([MgrRef]).

%%%===================================================================
%%% Exometer API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes all counters.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), counter}
    end, ?EXOMETER_COUNTERS),
    ?init_counters(Counters).

%%--------------------------------------------------------------------
%% @doc
%% Subscribe for reports for all parameters.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    Reports = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), [value]}
    end, ?EXOMETER_COUNTERS),
    ?init_reports(Reports).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns pid of an event manager associated with provided reference.
%% A reference can be either an event manager pid or a session ID.
%% @end
%%--------------------------------------------------------------------
-spec get_event_manager(MgrRef :: manager_ref()) ->
    {ok, Mgr :: pid()} | {error, Reason :: term()}.
get_event_manager(MgrRef) when is_pid(MgrRef) ->
    {ok, MgrRef};

get_event_manager(MgrRef) ->
    case session:get_event_manager(MgrRef) of
        {ok, Mgr} ->
            {ok, Mgr};
        {error, not_found = Reason} ->
            {error, Reason};
        {error, Reason} ->
            ?warning("Cannot get event manager for session ~p due to: ~p",
                [MgrRef, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private  @doc
%% Sends message to event managers.
%% @end
%%--------------------------------------------------------------------
-spec send_to_event_managers(Msg :: term(), Mgrs :: [pid()]) ->
    ok.
send_to_event_managers(Msg, Mgrs) ->
    lists:foreach(fun(Mgr) ->
        gen_server2:cast(Mgr, Msg)
    end, Mgrs).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a list consisting of unique elements that occurs in the ListA
%% but not in the ListB.
%% @end
%%--------------------------------------------------------------------
-spec subtract_unique(ListA :: list(), ListB :: list()) -> Diff :: list().
subtract_unique(ListA, ListB) ->
    SetA = gb_sets:from_list(ListA),
    SetB = gb_sets:from_list(ListB),
    gb_sets:to_list(gb_sets:subtract(SetA, SetB)).
