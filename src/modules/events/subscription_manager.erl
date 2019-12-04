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
-export([add_subscriber/2, get_subscribers/2, remove_subscriber/2]).

-export_type([key/0]).

-type key() :: binary().

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
-spec get_subscribers(Key :: key() | event:base() | event:aggregated() | event:type(),
    RoutingBase :: fslogic_worker:file_guid() | [fslogic_worker:file_guid()] | undefined) ->
    {ok, SessIds :: [session:id()]} | {error, Reason :: term()}.
get_subscribers(<<_/binary>> = Key, _RoutingBase) ->
    case file_subscription:get(Key) of
        {ok, #document{value = #file_subscription{sessions = SessIds}}} ->
            {ok, gb_sets:to_list(SessIds)};
        {error, not_found} ->
            {ok, []};
        {error, Reason} ->
            {error, Reason}
    end;

get_subscribers({aggregated, [Evt | _]}, RoutingBase) ->
    get_subscribers(Evt, RoutingBase);
get_subscribers(_Evt, []) ->
    {ok, []};
get_subscribers(Evt, [RoutingBase | RoutingList]) ->
    case get_subscribers(Evt, RoutingBase) of
        {ok, SessIds} ->
            case get_subscribers(Evt, RoutingList) of
                {ok, SessIds2} ->
                    {ok, SessIds2 ++ (SessIds -- SessIds2)};
                Other2 ->
                    Other2
            end;
        Other ->
            Other
    end;
get_subscribers(Evt, RoutingBase) ->
    case event_type:get_routing_key(Evt, RoutingBase) of
        {ok, Key} ->
            get_subscribers(Key, RoutingBase);
        {ok, Key, SpaceIDFilter} ->
            case get_subscribers(Key, RoutingBase) of
                {ok, SessIds} ->
                    {ok, lists:filter(fun(SessId) ->
                        UserCtx = user_ctx:new(SessId),
                        #document{value = #od_user{eff_spaces = Spaces}} = user_ctx:get_user(UserCtx),
                        lists:member(SpaceIDFilter, Spaces)
                    end, SessIds)};
                Other ->
                    Other
            end;
        {error, session_only} ->
            {ok, []}
    end.

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