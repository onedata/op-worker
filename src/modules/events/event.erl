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

-export_type([key/0, object/0, update_object/0, counter/0, subscription/0]).

-type key() :: term().
-type object() :: #read_event{} | #update_event{} | #write_event{}.
-type update_object() :: #file_attr{} | #file_location{}.
-type counter() :: non_neg_integer().
-type subscription() :: #subscription{}.
-type event_manager_ref() :: pid() | session:id() | [pid() | session:id()] |
                             {exclude, pid() | session:id()} |
                             {exclude, [pid() | session:id()]}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends an event to all event managers.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: #event{} | object()) -> ok | {error, Reason :: term()}.
emit(Evt) ->
    emit(Evt, get_event_managers()).

%%--------------------------------------------------------------------
%% @doc
%% Sends an event to the event manager associated with a session.
%% @end
%%--------------------------------------------------------------------
-spec emit(Evt :: #event{} | object(), Ref :: event_manager_ref()) ->
    ok | {error, Reason :: term()}.
emit(Evt, {exclude, Ref}) ->
    ExcludedEvtMans = get_excluded_event_managers(as_list(Ref)),
    emit(Evt, filter_event_managers(get_event_managers(), ExcludedEvtMans));

emit(#event{key = undefined} = Evt, Ref) ->
    emit(set_key(Evt), Ref);

emit(#event{} = Evt, Ref) ->
    send_to_event_managers(Evt, get_event_managers(as_list(Ref)));

emit(EvtObject, Ref) ->
    emit(#event{object = EvtObject}, Ref).

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
-spec subscribe(Sub :: subscription(), Ref :: event_manager_ref()) ->
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
    Ref :: event_manager_ref()) -> ok | {error, Reason :: term()}.
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
-spec set_key(Evt :: #event{}) -> NewEvt :: #event{}.
set_key(#event{object = #read_event{file_uuid = FileUuid}} = Evt) ->
    Evt#event{key = FileUuid};

set_key(#event{object = #write_event{file_uuid = FileUuid}} = Evt) ->
    Evt#event{key = FileUuid};

set_key(#event{object = #update_event{object = #file_attr{uuid = Uuid}}} = Evt) ->
    Evt#event{key = Uuid};

set_key(#event{object = #update_event{object = #file_location{uuid = Uuid}}} = Evt) ->
    Evt#event{key = Uuid}.


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

get_event_managers(Refs) ->
    lists:foldl(fun(Ref, EvtMans) ->
        case get_event_manager(Ref) of
            {ok, EvtMan} -> [EvtMan | EvtMans];
            {error, _} -> EvtMans
        end
    end, [], Refs).

get_event_managers() ->
    case session:get_event_managers() of
        {ok, Refs} ->
            lists:foldl(fun
                ({ok, EvtMan}, EvtMans) ->
                    [EvtMan | EvtMans];
                ({error, {not_found, SessId}}, EvtMans) ->
                    ?warning("Cannot get event manager for session ~p due to: missing", [SessId]),
                    EvtMans
            end, [], Refs);
        {error, Reason} ->
            ?warning("Cannot get event managers due to: ~p", [Reason]),
            []
    end.

get_excluded_event_managers(Refs) ->
    lists:foldl(fun(Ref, ExcludedEvtMans) ->
        case get_event_manager(Ref) of
            {ok, EvtMan} -> sets:add_element(EvtMan, ExcludedEvtMans);
            {error, _} -> ExcludedEvtMans
        end
    end, sets:new(), Refs).

filter_event_managers(EvtMans, ExcludedEvtMans) ->
    lists:filter(fun(EvtMan) ->
        not sets:is_element(EvtMan, ExcludedEvtMans)
    end, EvtMans).


send_to_event_managers(Msg, EvtMans) ->
    lists:foreach(fun(EvtMan) ->
        gen_server:cast(EvtMan, Msg)
    end, EvtMans).

as_list(Object) when is_list(Object) ->
    Object;
as_list(Object) ->
    [Object].
