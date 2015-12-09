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

-include("modules/datastore/datastore.hrl").
-include("modules/events/definitions.hrl").
-include("proto/oneclient/client_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit/1, emit/2, flush/2, flush/3, subscribe/1, subscribe/2,
    unsubscribe/1, unsubscribe/2]).

-export_type([key/0, object/0, update_object/0, counter/0, subscription/0]).

-type key() :: term().
-type object() :: #read_event{} | #update_event{} | #write_event{}.
-type update_object() :: #file_attr{} | #file_location{}.
-type counter() :: non_neg_integer().
-type subscription() :: #subscription{}.
-type event_manager_ref() :: pid() | session:id().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends an event to all event managers.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: #event{} | object()) -> ok | {error, Reason :: term()}.
emit(#event{key = undefined} = Evt) ->
    emit(set_key(Evt));

emit(#event{} = Evt) ->
    send_to_event_managers(Evt);

emit(EvtObject) ->
    emit(#event{object = EvtObject}).

%%--------------------------------------------------------------------
%% @doc
%% Sends an event to the event manager associated with a session.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: #event{} | object(), Ref :: event_manager_ref()) ->
    ok | {error, Reason :: term()}.
emit(#event{key = undefined} = Evt, Ref) ->
    emit(set_key(Evt), Ref);

emit(#event{} = Evt, Ref) ->
    send_to_event_manager(Evt, Ref);

emit(EvtObject, Ref) ->
    emit(#event{object = EvtObject}, Ref).

%%--------------------------------------------------------------------
%% @doc
%% Flushes all event streams associated with a subscription. Injects PID of a process,
%% which should be notified when operation completes, to the event handler context.
%% IMPORTANT! Event handler is responsible for notifying the awaiting process.
%% @end
%%--------------------------------------------------------------------
-spec flush(SubId :: subscription:id(), Notify :: pid()) ->
    ok | {error, Reason :: term()}.
flush(SubId, Notify) ->
    send_to_event_managers({flush_stream, SubId, Notify}).

%%--------------------------------------------------------------------
%% @doc
%% Flushes event streams associated with a subscription for given session. Injects
%% PID of a process, which should be notified when operation completes, to the
%% event handler context.
%% IMPORTANT! Event handler is responsible for notifying the awaiting process.
%% @end
%%--------------------------------------------------------------------
-spec flush(SubId :: subscription:id(), Notify :: pid(), Ref :: event_manager_ref()) ->
    ok | {error, Reason :: term()}.
flush(SubId, Notify, Ref) ->
    send_to_event_manager({flush_stream, SubId, Notify}, Ref).

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
            send_to_event_managers(Sub),
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
-spec subscribe(Sub :: subscription(), Ref :: event_manager_ref()) ->
    {ok, SubId :: subscription:id()} | {error, Reason :: term()}.
subscribe(#subscription{id = undefined} = Sub, Ref) ->
    subscribe(Sub#subscription{id = subscription:generate_id()}, Ref);

subscribe(#subscription{id = SubId} = Sub, Ref) ->
    send_to_event_manager(Sub, Ref),
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
        ok -> send_to_event_managers(SubCan);
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
    Ref :: event_manager_ref()) -> ok | {error, Reason :: term()}.
unsubscribe(#subscription_cancellation{} = SubCan, Ref) ->
    send_to_event_manager(SubCan, Ref);

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
-spec set_key(Evt :: #event{}) -> NewEvt :: #event{}.
set_key(#event{object = #read_event{file_uuid = FileUuid}} = Evt) ->
    Evt#event{key = FileUuid};

set_key(#event{object = #write_event{file_uuid = FileUuid}} = Evt) ->
    Evt#event{key = FileUuid};

set_key(#event{object = #update_event{object = #file_attr{uuid = Uuid}}} = Evt) ->
    Evt#event{key = Uuid};

set_key(#event{object = #update_event{object = #file_location{uuid = Uuid}}} = Evt) ->
    Evt#event{key = Uuid}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends a message to the event manager referenced by pid or session ID.
%% @end
%%--------------------------------------------------------------------
-spec send_to_event_manager(Msg :: term(), Ref :: event_manager_ref()) ->
    ok | {error, Reason :: term()}.
send_to_event_manager(Msg, Ref) when is_pid(Ref) ->
    gen_server:cast(Ref, Msg);

send_to_event_manager(Msg, Ref) ->
    case session:get_event_manager(Ref) of
        {ok, EvtMan} ->
            send_to_event_manager(Msg, EvtMan);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends a message to all event managers.
%% @end
%%--------------------------------------------------------------------
-spec send_to_event_managers(Msg :: term()) -> ok | {error, Reason :: term()}.
send_to_event_managers(Msg) ->
    case session:get_event_managers() of
        {ok, EvtMans} ->
            utils:pforeach(fun
                ({ok, EvtMan}) ->
                    send_to_event_manager(Msg, EvtMan);
                ({error, {not_found, SessId}}) ->
                    ?warning("Event manager not found for session ~p while "
                    "sending message: ~p", [SessId, Msg])
            end, EvtMans);
        {error, Reason} ->
            {error, Reason}
    end.