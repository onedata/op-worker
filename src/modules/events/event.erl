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

-include("modules/events/routing.hrl").
-include("modules/events/definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("cluster_worker/include/exometer_utils.hrl").

%% API
-export([emit/1, emit/2, emit_to_filtered_subscribers/3, flush/2, flush/5, subscribe/2, unsubscribe/2]).
-export([get_event_managers/1]).

-export([init_counters/0, init_report/0]).

-export_type([key/0, base/0, aggregated/0, type/0, stream/0, manager_ref/0]).

-type key() :: term().
-type base() :: #event{}.
-type aggregated() :: {aggregated, [base()]}.
-type type() :: #file_read_event{} | #file_written_event{} |
                #file_attr_changed_event{} | #file_location_changed_event{} |
                #file_perm_changed_event{} | #file_removed_event{} |
                #quota_exceeded_event{} | #helper_params_changed_event{} |
                #file_renamed_event{} | #monitoring_event{}.
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
    emit_to_filtered_subscribers(Evt, undefined, []).

%%--------------------------------------------------------------------
%% @doc
%% Sends an event to selected event managers.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: base() | aggregated() | type(), MgrRef :: manager_ref()) ->
    ok | {error, Reason :: term()}.
emit(Evt, {exclude, MgrRef}) ->
    emit_to_filtered_subscribers(Evt, undefined, MgrRef);

emit(#event{} = Evt, MgrRef) ->
    ?update_counter(?EXOMETER_NAME(emit)),
    case event_type:get_context(Evt) of
        {file, Guid} ->
            % Filter events connected with trash (oneclient should not see trash) and
            % tmp dir (it is accessed via guid - it does not appear in listing results)
            case fslogic_file_id:is_trash_dir_guid(Guid) orelse fslogic_file_id:is_tmp_dir_guid(Guid) of
                true -> ok;
                false -> send_to_event_managers(Evt, get_event_managers(MgrRef))
            end;
        _ ->
            send_to_event_managers(Evt, get_event_managers(MgrRef))
    end;

emit({aggregated, [#event{} | _]} = Evts, MgrRef) ->
    ?update_counter(?EXOMETER_NAME(emit)),
    % Aggregated events are produced only for regular files - trash dir does not have to be filtered
    send_to_event_managers(Evts, get_event_managers(MgrRef));

emit({aggregated, Evts}, MgrRef) ->
    WrappedEvents = lists:map(fun(Evt) -> #event{type = Evt} end, Evts),
    emit({aggregated, WrappedEvents}, MgrRef);

emit(Evt, MgrRef) ->
    emit(#event{type = Evt}, MgrRef).

%%--------------------------------------------------------------------
%% @doc
%% Gets subscribers and sends an event to event managers that represent subscribers.
%% Filters subscribers list if needed.
%% @end
%%--------------------------------------------------------------------
-spec emit_to_filtered_subscribers(Evt :: base() | aggregated() | type(), subscription_manager:routing_info(),
    ExcludedRef :: pid() | session:id() | [pid() | session:id()]) ->
    ok | {error, Reason :: term()}.
emit_to_filtered_subscribers(Evt, RoutingInfo, ExcludedRef) ->
    case subscription_manager:get_subscribers(Evt, RoutingInfo) of
        #event_subscribers{subscribers = SessIds} = EventSubscribers ->
            SubscribedMap = map_sessions_to_managers(SessIds, get_event_managers(ExcludedRef)),
            EventsMap = extend_event_for_space_dir(Evt, maps:keys(SubscribedMap)),
            maps:fold(fun(FinalEvt, Sessions, _) ->
                emit(FinalEvt, [maps:get(S, SubscribedMap) || S <- Sessions])
            end, ok, EventsMap),
            emit_for_file_links(Evt, EventSubscribers);
        {error, Reason} ->
            {error, Reason}
    end.


-spec map_sessions_to_managers([session:id()], [pid()]) -> #{session:id() => pid()}.
map_sessions_to_managers(SessionIds, Excluded) ->
    lists:foldl(fun(Session, Acc) ->
        case get_event_manager(Session) of
            {ok, Mgr} ->
                case lists:member(Mgr, Excluded) of
                    true ->
                        Acc;
                    false ->
                        Acc#{Session => Mgr}
                end;
            {error, _} ->
                Acc
        end
    end, #{}, SessionIds).


-spec emit_for_file_links(base() | aggregated() | type(), subscription_manager:event_subscribers()) -> ok.
emit_for_file_links(Evt, #event_subscribers{subscribers_for_links = SessIdsForLinks}) ->
    lists:foreach(fun({Context, AdditionalSessIds}) ->
        try
            emit(fslogic_event_emitter:clone_event(Evt, Context), AdditionalSessIds)
        catch
            Class:Reason:Stacktrace ->
                % Race with file/link deletion can result in error logged here
                ?warning_exception("Error emitting event for additional guid ~s",
                    [?autoformat([Context, Evt])], Class, Reason, Stacktrace)
        end
    end, SessIdsForLinks).


-spec extend_event_for_space_dir(base() | aggregated() | type(), [session:id()]) -> #{type() => [session:id()]}.
extend_event_for_space_dir(#file_attr_changed_event{file_attr = #file_attr{guid = Guid} = Attr} = Evt, Sessions) ->
    case fslogic_file_id:is_space_dir_guid(Guid) of
        true ->
            SpaceId = file_id:guid_to_space_id(Guid),
            lists:foldl(fun(Session, Acc) ->
                % file_attr can contain invalid values when fetched with root sess id for space dir. Fill proper values here.
                case get_space_dir_event_details(SpaceId, Session) of
                    {ok, Name, ParentGuid} ->
                        FilledAttr = Attr#file_attr{name = Name, parent_guid = ParentGuid},
                        FinalEvent = Evt#file_attr_changed_event{file_attr = FilledAttr},
                        Acc#{FinalEvent => [Session | maps:get(FinalEvent, Acc, [])]};
                    not_applicable ->
                        Acc
                end
            end, #{}, Sessions);
        false ->
            #{Evt => Sessions}
    end;
extend_event_for_space_dir(#event{type = Type}, Sessions) ->
    extend_event_for_space_dir(Type, Sessions);
extend_event_for_space_dir({aggregated, Evts}, Sessions) ->
    lists:foldl(fun(Evt, Acc) ->
        maps:merge(Acc, extend_event_for_space_dir(Evt, Sessions))
    end, #{}, Evts);
extend_event_for_space_dir(Evt, Sessions) ->
    #{Evt => Sessions}.


-spec get_space_dir_event_details(od_space:id(), session:id()) ->
    {ok, file_meta:name(), file_id:file_guid()} | not_applicable.
get_space_dir_event_details(SpaceId, Session) ->
    case space_logic:get_protected_data(Session, SpaceId) of
        {ok, #document{value = #od_space{name = Name, providers = Providers}}} when map_size(Providers) > 0 ->
            case session:get_user_id(Session) of
                {ok, UserId} ->
                    {FinalName, _} = file_attr:get_space_name_and_conflicts(user_ctx:new(Session), Name, SpaceId),
                    {ok, FinalName, fslogic_file_id:user_root_dir_guid(UserId)};
                {error, not_found} ->
                    not_applicable
            end;
        {ok, #document{value = #od_space{providers = Providers}}} when map_size(Providers) == 0 ->
            not_applicable;
        ?ERROR_FORBIDDEN ->
            not_applicable
    end.
    

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

get_event_manager(SessId) ->
    case session:get_event_manager(SessId) of
        {ok, Mgr} ->
            {ok, Mgr};
        {error, not_found = Reason} ->
            {error, Reason};
        {error, Reason} ->
            ?warning("Cannot get event manager for session ~p due to: ~p",
                [SessId, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends message to event managers.
%% @end
%%--------------------------------------------------------------------
-spec send_to_event_managers(Message :: term(), Managers :: [pid()]) -> ok.
send_to_event_managers({aggregated, Messages}, Managers) ->
    lists:foreach(fun(Message) ->
        send_to_event_managers(Message, Managers)
    end, Messages);
send_to_event_managers(Message, Managers) ->
    lists:foreach(fun(Manager) ->
        send_to_event_manager(Manager, Message, 1)
    end, Managers).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends message to event manager.
%% @end
%%--------------------------------------------------------------------
-spec send_to_event_manager(Manager :: pid(), Message :: term(), RetryCounter :: non_neg_integer()) -> ok.
send_to_event_manager(Manager, Message, 0) ->
    ok = event_manager:handle(Manager, Message);
send_to_event_manager(Manager, Message, RetryCounter) ->
    try
        ok = event_manager:handle(Manager, Message)
    catch
        exit:{timeout, _} ->
            ?debug("Timeout of event manager for message ~p, retry", [Message]),
            send_to_event_manager(Manager, Message, RetryCounter - 1);
        Reason1:Reason2:Stacktrace ->
            ?error_stacktrace("Cannot process event ~p due to: ~p", [Message, {Reason1, Reason2}], Stacktrace),
            send_to_event_manager(Manager, Message, RetryCounter - 1)
    end.
