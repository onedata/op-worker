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
-export([emit/1, emit/2, subscribe/1, subscribe/2, unsubscribe/1, unsubscribe/2]).

-export_type([key/0, type/0, update_type/0, counter/0, subscription/0]).

-type key() :: term().
-type type() :: #read_event{} | #update_event{} | #write_event{}.
-type update_type() :: #file_attr{} | #file_location{}.
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
-spec emit(Evt :: #event{} | type()) -> ok | {error, Reason :: term()}.
emit(#event{key = undefined} = Evt) ->
    emit(set_key(Evt));

emit(#event{} = Evt) ->
    send_to_event_managers(Evt);

emit(EvtType) ->
    emit(#event{type = EvtType}).

%%--------------------------------------------------------------------
%% @doc
%% Sends an event to the event manager associated with a session.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: #event{} | type(), Ref :: event_manager_ref()) ->
    ok | {error, Reason :: term()}.
emit(#event{key = undefined} = Evt, Ref) ->
    emit(set_key(Evt), Ref);

emit(#event{} = Evt, Ref) ->
    send_to_event_manager(Evt, Ref);

emit(EvtType, Ref) ->
    emit(#event{type = EvtType}, Ref).

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
-spec unsubscribe(SubId :: subscription:id() | subscription:cancellaton()) ->
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
-spec unsubscribe(SubId :: subscription:id() | subscription:cancellaton(),
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
set_key(#event{type = #read_event{file_uuid = FileUuid}} = Evt) ->
    Evt#event{key = FileUuid};

set_key(#event{type = #write_event{file_uuid = FileUuid}} = Evt) ->
    Evt#event{key = FileUuid};

set_key(#event{type = #update_event{type = #file_attr{uuid = Uuid}}} = Evt) ->
    Evt#event{key = Uuid};

set_key(#event{type = #update_event{type = #file_location{uuid = Uuid}}} = Evt) ->
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