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
-module(event_router).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").

%% API
-export([add_subscriber/2, get_subscribers/1, remove_subscriber/2]).

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
        {ok, Key} -> {ok, Key};
        {error, {not_found, _}} ->
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
-spec get_subscribers(Key :: key() | event:base() | event:type()) ->
    {ok, SessIds :: [session:id()]} | {error, Reason :: term()}.
get_subscribers(<<_/binary>> = Key) ->
    case file_subscription:get(Key) of
        {ok, #document{value = #file_subscription{sessions = SessIds}}} ->
            {ok, gb_sets:to_list(SessIds)};
        {error, {not_found, _}} ->
            {ok, []};
        {error, Reason} ->
            {error, Reason}
    end;

get_subscribers(Evt) ->
    case event_type:get_routing_key(Evt) of
        {ok, Key} -> get_subscribers(Key);
        {error, session_only} -> {ok, []}
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
        {ok, _} -> ok;
        {error, {not_found, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.