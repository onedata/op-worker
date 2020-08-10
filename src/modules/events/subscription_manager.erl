%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for events routing table management.
%%% @end
%%%-------------------------------------------------------------------
-module(subscription_manager).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").

%% API
-export([add_subscriber/2, get_subscribers/2, get_attr_event_subscribers/2, remove_subscriber/2]).

-type key() :: binary().
% routing can require connection of several contexts, e.g., old and new parent when moving file
-type routing_info() :: event_type:routing_ctx() | [event_type:routing_ctx()].

-export_type([key/0, routing_info/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds subscriber to the globally cached events routing table.
%% @end
%%--------------------------------------------------------------------
-spec add_subscriber(Key :: key() | subscription:base() | subscription:type(),
    SessId :: session:id()) -> {ok, Key :: key()} | {error, Reason :: term()}.
add_subscriber(<<_/binary>> = Key, SessId) ->
    Diff = fun(#file_subscription{sessions = SessIds} = Sub) ->
        {ok, Sub#file_subscription{sessions = gb_sets:add_element(SessId, SessIds)}}
    end,
    case file_subscription:update(Key, Diff) of
        {ok, #document{key = Key}} -> {ok, Key};
        {error, not_found} ->
            Doc = #document{key = Key, value = #file_subscription{
                sessions = gb_sets:from_list([SessId])
            }},
            case file_subscription:create(Doc) of
                {ok, _} -> {ok, Key};
                {error, already_exists} -> add_subscriber(Key, SessId);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} -> {error, Reason}
    end;

add_subscriber(Sub, SessId) ->
    case subscription_type:get_routing_key(Sub) of
        {ok, Key} -> add_subscriber(Key, SessId);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of event subscribers.
%% @end
%%--------------------------------------------------------------------
-spec get_subscribers(Key :: key() | event:base() | event:aggregated() | event:type(), routing_info()) ->
    {ok, SessIds :: [session:id()]} | {error, Reason :: term()}.
get_subscribers(<<_/binary>> = Key, _) ->
    case file_subscription:get(Key) of
        {ok, #document{value = #file_subscription{sessions = SessIds}}} ->
            {ok, gb_sets:to_list(SessIds)};
        {error, not_found} ->
            {ok, []};
        {error, Reason} ->
            {error, Reason}
    end;

get_subscribers({aggregated, [Evt | _]}, RoutingInfo) ->
    get_subscribers(Evt, RoutingInfo);
get_subscribers(_Evt, []) ->
    {ok, []};
get_subscribers(Evt, [RoutingCtx | RoutingInfo]) ->
    case get_subscribers(Evt, RoutingCtx) of
        {ok, SessIds} ->
            case get_subscribers(Evt, RoutingInfo) of
                {ok, SessIds2} ->
                    {ok, SessIds2 ++ (SessIds -- SessIds2)};
                Other2 ->
                    Other2
            end;
        Other ->
            Other
    end;
get_subscribers(Evt, RoutingCtx) ->
    case event_type:get_routing_key(Evt, RoutingCtx) of
        {ok, Key} ->
            get_subscribers(Key, RoutingCtx);
        {ok, Key, SpaceIDFilter} ->
            case get_subscribers(Key, RoutingCtx) of
                {ok, SessIds} ->
                    {ok, apply_space_id_filter(SessIds, SpaceIDFilter)};
                Other ->
                    Other
            end;
        {error, session_only} ->
            {ok, []}
    end.

-spec get_attr_event_subscribers(fslogic_worker:file_guid(), event_type:routing_ctx()) ->
    [{ok, SessIds :: [session:id()]} | {error, Reason :: term()}].
get_attr_event_subscribers(Guid, RoutingCtx) ->
    lists:map(fun
        ({ok, Key}) ->
            subscription_manager:get_subscribers(Key, RoutingCtx);
        ({ok, Key, SpaceIDFilter}) ->
            case get_subscribers(Key, RoutingCtx) of
                {ok, SessIds} ->
                    {ok, apply_space_id_filter(SessIds, SpaceIDFilter)};
                Other ->
                    Other
            end
    end, event_type:get_attr_routing_keys(Guid, RoutingCtx)).

%%--------------------------------------------------------------------
%% @doc
%% Filter sessions when information about space dirs is broadcast
%% (not all clients are allowed to see particular space).
%% @end
%%--------------------------------------------------------------------
-spec apply_space_id_filter([session:id()], od_space:id()) -> [session:id()].
apply_space_id_filter(SessIds, SpaceIDFilter) ->
    lists:filter(fun(SessId) ->
        UserCtx = user_ctx:new(SessId),
        Spaces = user_ctx:get_eff_spaces(UserCtx),
        lists:member(SpaceIDFilter, Spaces)
    end, SessIds).

%%--------------------------------------------------------------------
%% @doc
%% Removes subscriber from the globally cached events routing table.
%% @end
%%--------------------------------------------------------------------
-spec remove_subscriber(Key :: key(), SessId :: session:id()) ->
    ok | {error, Reason :: term()}.
remove_subscriber(Key, SessId) ->
    Diff = fun(#file_subscription{sessions = SessIds} = Sub) ->
        {ok, Sub#file_subscription{sessions = gb_sets:del_element(SessId, SessIds)}}
    end,
    case file_subscription:update(Key, Diff) of
        {ok, #document{value = #file_subscription{sessions = SIds}}} ->
            case gb_sets:is_empty(SIds) of
                true ->
                    Pred = fun(#file_subscription{sessions = SIds2}) ->
                        gb_sets:is_empty(SIds2)
                    end,
                    file_subscription:delete(Key, Pred);
                false ->
                    ok
            end;
        {error, not_found} -> ok;
        {error, Reason} -> {error, Reason}
    end.