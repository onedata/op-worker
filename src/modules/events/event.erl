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

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/events/definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include("proto/oneclient/server_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit/1, emit/2, flush/2, flush/4, flush/5, subscribe/1, subscribe/2,
    unsubscribe/1, unsubscribe/2]).

-export_type([key/0, object/0, update_object/0, counter/0, subscription/0,
    manager_ref/0, event/0]).

-type event() :: #event{}.
-type key() :: term().
-type object() :: #read_event{} | #update_event{} | #write_event{}
| #permission_changed_event{} | #file_removal_event{} | #quota_exeeded_event{}
| #file_renamed_event{} | #file_accessed_event{} | #storage_used_updated{}
| #space_info_updated{} | #file_operations_statistics{} | #rtransfer_statistics{}.
-type update_object() :: #file_attr{} | #file_location{}.
-type counter() :: non_neg_integer().
-type subscription() :: #subscription{}.
-type manager_ref() :: pid() | session:id() | [pid() | session:id()] |
% reference all event managers except one provided
{exclude, pid() | session:id()} |
% reference all event managers except those provided in list
{exclude, [pid() | session:id()]}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends an event to all event managers.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: event() | object()) -> ok | {error, Reason :: term()}.
emit(Evt) ->
    emit(Evt, get_event_managers_for_event(Evt)).

%%--------------------------------------------------------------------
%% @doc
%% Sends an event to the event manager associated with a session.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: event() | object(), Ref :: event:manager_ref()) ->
    ok | {error, Reason :: term()}.
emit(Evt, {exclude, Ref}) ->
    ExcludedEvtMans = sets:from_list(get_event_managers(as_list(Ref))),
    emit(Evt, filter_event_managers(get_event_managers_for_event(Evt), ExcludedEvtMans));

emit(#event{key = undefined} = Evt, Ref) ->
    emit(set_key(Evt), Ref);

emit(#event{} = Evt, Ref) ->
    send_to_event_managers(set_stream_id(Evt), get_event_managers(as_list(Ref)));

emit(EvtObject, Ref) ->
    emit(#event{object = EvtObject}, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Flushes all event streams associated with a subscription. Injects PID of a process,
%% which should be notified when operation completes, to the event handler context.
%% IMPORTANT! Event handler is responsible for notifying the awaiting process.
%% @end
%%--------------------------------------------------------------------
-spec flush(#flush_events{}, Ref :: event:manager_ref()) -> ok.
flush(#flush_events{} = FlushMsg, Ref) ->
    send_to_event_managers(FlushMsg, get_event_managers(as_list(Ref))).

%%--------------------------------------------------------------------
%% @doc
%% @equiv flush(ProviderId, Context, SubId, Notify, get_event_managers())
%% @end
%%--------------------------------------------------------------------
-spec flush(ProviderId :: oneprovider:id(), Context :: term(), SubId :: subscription:id(),
    Notify :: pid()) -> term().
flush(ProviderId, Context, SubId, Notify) ->
    flush(ProviderId, Context, SubId, Notify, get_event_managers()).

%%--------------------------------------------------------------------
%% @doc
%% Flushes event streams associated with a subscription for given session. Injects
%% PID of a process, which should be notified when operation completes, to the
%% event handler context.
%% IMPORTANT! Event handler is responsible for notifying the awaiting process.
%% @end
%%--------------------------------------------------------------------
-spec flush(ProviderId :: oneprovider:id(), Context :: term(), SubId :: subscription:id(),
    Notify :: pid(), Ref :: event:manager_ref()) -> reference().
flush(ProviderId, Context, SubId, Notify, Ref) ->
    RecvRef = make_ref(),
    ok = send_to_event_managers(#flush_events{
        provider_id = ProviderId, subscription_id = SubId,
        context = Context,
        notify = fun
            (#server_message{message_body = #status{code = ?OK}}) ->
                Notify ! {RecvRef, ok};
            (#server_message{message_body = #status{code = Code}}) ->
                Notify ! {RecvRef, {error, Code}}
        end
    }, get_event_managers(as_list(Ref))),
    RecvRef.

%%--------------------------------------------------------------------
%% @doc
%% Creates durable event subscription and notifies all event managers.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Sub :: subscription()) ->
    {ok, SubId :: subscription:id()} | {error, Reason :: term()}.
subscribe(#subscription{id = undefined} = Sub) ->
    subscribe(Sub#subscription{id = subscription:generate_id()});

subscribe(#subscription{id = SubId} = Sub) ->
    case subscription:create(#document{key = SubId, value = Sub}) of
        {ok, SubId} ->
            send_to_event_managers(Sub, get_event_managers()),
            {ok, SubId};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates event subscription associated with a session and notifies appropriate
%% event manager.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Sub :: subscription(), Ref :: event:manager_ref()) ->
    {ok, SubId :: subscription:id()} | {error, Reason :: term()}.
subscribe(#subscription{id = undefined} = Sub, Ref) ->
    subscribe(Sub#subscription{id = subscription:generate_id()}, Ref);

subscribe(#subscription{id = SubId} = Sub, Ref) ->
    send_to_event_managers(Sub, get_event_managers(as_list(Ref))),
    {ok, SubId}.

%%--------------------------------------------------------------------
%% @doc
%% Removes durable event subscription and notifies all event managers.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(SubId :: subscription:id() | subscription:cancellation()) ->
    ok | {error, Reason :: term()}.
unsubscribe(#subscription_cancellation{id = Id} = SubCan) ->
    case subscription:delete(Id) of
        ok -> send_to_event_managers(SubCan, get_event_managers());
        {error, Reason} -> {error, Reason}
    end;

unsubscribe(SubId) ->
    unsubscribe(#subscription_cancellation{id = SubId}).

%%--------------------------------------------------------------------
%% @doc
%% Removes subscription associated with a session.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(SubId :: subscription:id() | subscription:cancellation(),
    Ref :: event:manager_ref()) -> ok | {error, Reason :: term()}.
unsubscribe(#subscription_cancellation{} = SubCan, Ref) ->
    send_to_event_managers(SubCan, get_event_managers(as_list(Ref)));

unsubscribe(SubId, Ref) ->
    unsubscribe(#subscription_cancellation{id = SubId}, Ref).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets event key based on its type. This key will be later used by event stream
%% in aggregation process. Events with the same key will be aggregated.
%% @end
%%--------------------------------------------------------------------
-spec set_key(Evt :: event()) -> NewEvt :: event().
set_key(#event{object = #read_event{file_uuid = FileUuid}} = Evt) ->
    Evt#event{key = FileUuid};
set_key(#event{object = #write_event{file_uuid = FileUuid}} = Evt) ->
    Evt#event{key = FileUuid};
set_key(#event{object = #update_event{object = #file_attr{uuid = Uuid}}} = Evt) ->
    Evt#event{key = Uuid, stream_key = <<"file_attr.", Uuid/binary>>};
set_key(#event{object = #update_event{object = #file_location{uuid = Uuid}}} = Evt) ->
    Evt#event{key = Uuid, stream_key = <<"file_location.", Uuid/binary>>};
set_key(#event{object = #permission_changed_event{file_uuid = Uuid}} = Evt) ->
    Evt#event{key = Uuid, stream_key = <<"permission_changed.", Uuid/binary>>};
set_key(#event{object = #file_removal_event{file_uuid = Uuid}} = Evt) ->
    Evt#event{key = Uuid, stream_key = <<"file_removal.", Uuid/binary>>};
set_key(#event{object = #quota_exeeded_event{}} = Evt) ->
    Evt#event{key = <<"quota_exeeded">>};
set_key(#event{object = #file_renamed_event{top_entry = #file_renamed_entry{old_uuid = Uuid}}} = Evt) ->
    Evt#event{key = Uuid, stream_key = <<"file_renamed.", Uuid/binary>>};
set_key(#event{object = #file_accessed_event{file_uuid = Uuid}} = Evt) ->
    Evt#event{key = Uuid};
set_key(#event{object = #storage_used_updated{space_id = SpaceId, user_id = UserId}} = Evt) ->
    Evt#event{key = {SpaceId, UserId, <<"storage_used_updated">>}};
set_key(#event{object = #space_info_updated{space_id = SpaceId}} = Evt) ->
    Evt#event{key = {SpaceId, <<"space_info_updated">>}};
set_key(#event{object = #file_operations_statistics{space_id = SpaceId, user_id = UserId}} = Evt) ->
    Evt#event{key = {SpaceId, UserId, <<"file_operations_statistics">>}};
set_key(#event{object = #rtransfer_statistics{space_id = SpaceId, user_id = UserId}} = Evt) ->
    Evt#event{key = {SpaceId, UserId, <<"rtransfer_statistics">>}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets ID of a stream that is responsible for handling the event.
%% @end
%%--------------------------------------------------------------------
-spec set_stream_id(Evt :: event()) -> NewEvt :: event().
set_stream_id(#event{stream_id = StmId} = Evt) when StmId =/= undefined ->
    Evt;
set_stream_id(#event{object = #read_event{}} = Evt) ->
    Evt#event{stream_id = read_event_stream};
set_stream_id(#event{object = #write_event{}} = Evt) ->
    Evt#event{stream_id = write_event_stream};
set_stream_id(#event{object = #update_event{object = #file_attr{}}} = Evt) ->
    Evt#event{stream_id = file_attr_event_stream};
set_stream_id(#event{object = #update_event{object = #file_location{}}} = Evt) ->
    Evt#event{stream_id = file_location_event_stream};
set_stream_id(#event{object = #permission_changed_event{}} = Evt) ->
    Evt#event{stream_id = permission_changed_event_stream};
set_stream_id(#event{object = #file_removal_event{}} = Evt) ->
    Evt#event{stream_id = file_removal_event_stream};
set_stream_id(#event{object = #quota_exeeded_event{}} = Evt) ->
    Evt#event{stream_id = quota_exceeded_event_stream};
set_stream_id(#event{object = #file_renamed_event{}} = Evt) ->
    Evt#event{stream_id = file_renamed_event_stream};
set_stream_id(#event{object = #file_accessed_event{}} = Evt) ->
    Evt#event{stream_id = file_accessed_event_stream};
set_stream_id(#event{object = #storage_used_updated{}} = Evt) ->
    Evt#event{stream_id = monitoring_event_stream};
set_stream_id(#event{object = #space_info_updated{}} = Evt) ->
    Evt#event{stream_id = monitoring_event_stream};
set_stream_id(#event{object = #file_operations_statistics{}} = Evt) ->
    Evt#event{stream_id = monitoring_event_stream};
set_stream_id(#event{object = #rtransfer_statistics{}} = Evt) ->
    Evt#event{stream_id = monitoring_event_stream}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns event manager for reference, either event manager pid or session ID.
%% @end
%%--------------------------------------------------------------------
-spec get_event_manager(Ref :: event:manager_ref()) ->
    {ok, EvtMan :: pid()} | {error, Reason :: term()}.
get_event_manager(Ref) when is_pid(Ref) ->
    {ok, Ref};
get_event_manager(Ref) ->
    case session:get_event_manager(Ref) of
        {ok, EvtMan} ->
            {ok, EvtMan};
        {error, Reason} ->
            ?warning("Cannot get event manager for session ~p due to: ~p", [Ref, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns event manager for references, either event manager pids or session IDs.
%% @end
%%--------------------------------------------------------------------
-spec get_event_managers(Refs :: [event:manager_ref()]) -> [EvtMan :: pid()].
get_event_managers(Refs) ->
    lists:foldl(fun(Ref, EvtMans) ->
        case get_event_manager(Ref) of
            {ok, EvtMan} -> [EvtMan | EvtMans];
            {error, _} -> EvtMans
        end
    end, [], Refs).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns event manager for all sessions.
%% @end
%%--------------------------------------------------------------------
-spec get_event_managers() -> [EvtMan :: pid()].
get_event_managers() ->
    case session:get_event_managers() of
        {ok, Refs} ->
            lists:foldl(fun
                ({ok, EvtMan}, EvtMans) ->
                    [EvtMan | EvtMans];
                ({error, {not_found, _}}, EvtMans) ->
                    EvtMans
            end, [], Refs);
        {error, Reason} ->
            ?error("Cannot get event managers due to: ~p", [Reason]),
            []
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns list of event managers that are dedicated for the event.
%% @end
%%--------------------------------------------------------------------
-spec get_event_managers_for_event(Evt :: event() | object()) -> [EvtMan :: pid()].
get_event_managers_for_event(#event{key = undefined} = Evt) ->
    get_event_managers_for_event(set_key(Evt));
get_event_managers_for_event(#event{} = Evt) ->
    case file_subscription:get(Evt) of
        {ok, #document{value = #file_subscription{sessions = SessIds}}} ->
            get_event_managers(gb_sets:to_list(SessIds));
        _ ->
            get_event_managers()
    end;
get_event_managers_for_event(EvtObject) ->
    get_event_managers_for_event(#event{object = EvtObject}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns event managers that do not appear in excluded set of event managers.
%% @end
%%--------------------------------------------------------------------
-spec filter_event_managers(EvtMans :: [event:manager_ref()],
    ExcludedEvtMans :: sets:set(event:manager_ref())) -> [EvtMan :: pid()].
filter_event_managers(EvtMans, ExcludedEvtMans) ->
    lists:filter(fun(EvtMan) ->
        not sets:is_element(EvtMan, ExcludedEvtMans)
    end, EvtMans).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends message to event managers.
%% @end
%%--------------------------------------------------------------------
-spec send_to_event_managers(Msg :: term(), EvtMans :: [EvtMan :: pid()]) ->
    ok.
send_to_event_managers(Msg, EvtMans) ->
    lists:foreach(fun(EvtMan) ->
        gen_server2:cast(EvtMan, Msg)
    end, EvtMans).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Wraps any object in list unless it is a list already.
%% @end
%%--------------------------------------------------------------------
-spec as_list(Object :: term()) -> List :: list().
as_list(Object) when is_list(Object) ->
    Object;
as_list(Object) ->
    [Object].
