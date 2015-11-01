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

-export_type([key/0, type/0, counter/0, subscription/0]).

-type key() :: term().
-type type() :: #read_event{} | #update_event{} | #write_event{}.
-type counter() :: non_neg_integer().
-type subscription() :: #subscription{}.

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
-spec emit(Evt :: #event{} | type(), SessId :: session:id()) ->
    ok | {error, Reason :: term()}.
emit(#event{key = undefined} = Evt, SessId) ->
    emit(set_key(Evt), SessId);

emit(#event{} = Evt, SessId) ->
    send_to_event_manager(Evt, SessId);

emit(EvtType, SessId) ->
    emit(#event{type = EvtType}, SessId).

%%--------------------------------------------------------------------
%% @doc
%% Creates durable event subscription and notifies all event managers.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Sub :: subscription()) ->
    {ok, SubId :: subscription:id()} | {error, Reason :: term()}.
subscribe(Sub) ->
    SubId = subscription:generate_id(),
    NewSub = Sub#subscription{id = SubId},
    case subscription:create(#document{key = SubId, value = NewSub}) of
        {ok, SubId} ->
            send_to_event_managers(NewSub),
            {ok, SubId};
        {error, already_exists} ->
            subscribe(Sub);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates event subscription associated with a session and notifies appropriate
%% event manager.
%% @end
%%--------------------------------------------------------------------
-spec subscribe(Sub :: subscription(), SessId :: session:id()) ->
    {ok, SubId :: subscription:id()} | {error, Reason :: term()}.
subscribe(Sub, SessId) ->
    send_to_event_manager(Sub, SessId).


%%--------------------------------------------------------------------
%% @doc
%% Removes subscription associated with a session.
%% @end
%%--------------------------------------------------------------------
-spec unsubscribe(SubId :: subscription:id() | subscription:cancellaton(),
    SessId :: session:id()) -> ok | {error, Reason :: term()}.
unsubscribe(#subscription_cancellation{} = SubCan, SessId) ->
    send_to_event_manager(SubCan, SessId);

unsubscribe(SubId, SessId) ->
    unsubscribe(#subscription_cancellation{id = SubId}, SessId).

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
    Evt#event{key = FileUuid}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends a message to the event manager referenced by pid or session ID.
%% @end
%%--------------------------------------------------------------------
-spec send_to_event_manager(Msg :: term(), Ref :: pid() | session:id()) ->
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