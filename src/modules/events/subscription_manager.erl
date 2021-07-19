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

-include("modules/events/routing.hrl").
-include("modules/events/definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([add_subscriber/2, get_subscribers/2, get_attr_event_subscribers/3, remove_subscriber/2]).
%% For tests
-export([get_subscribers/1]).

-type key() :: binary().
% routing can require connection of several contexts, e.g., old and new parent when moving file
-type routing_info() :: event_type:routing_ctx() | [event_type:routing_ctx()].
-type event_routing_keys() :: #event_routing_keys{}.
-type event_subscribers() :: #event_subscribers{}.

-export_type([key/0, routing_info/0, event_routing_keys/0, event_subscribers/0]).

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
        {ok, #document{key = Key}} ->
            file_subscription_counter:subscription_added(Key),
            {ok, Key};
        {error, not_found} ->
            Doc = #document{key = Key, value = #file_subscription{
                sessions = gb_sets:from_list([SessId])
            }},
            case file_subscription:create(Doc) of
                {ok, _} ->
                    file_subscription_counter:subscription_added(Key),
                    {ok, Key};
                {error, already_exists} ->
                    add_subscriber(Key, SessId);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} -> {error, Reason}
    end;

add_subscriber(Sub, SessId) ->
    case subscription_type:get_routing_key(Sub) of
        {ok, Key} -> add_subscriber(Key, SessId);
        {error, Reason} -> {error, Reason}
    end.

-spec get_subscribers(Key :: key()) -> {ok, SessIds :: [session:id()]} | {error, Reason :: term()}.
get_subscribers(Key) ->
    case file_subscription:get(Key) of
        {ok, #document{value = #file_subscription{sessions = SessIds}}} ->
            {ok, gb_sets:to_list(SessIds)};
        {error, not_found} ->
            {ok, []};
        {error, Reason} ->
            {error, Reason}
    end.

-spec get_subscribers(event:base() | event:aggregated() | event:type(), routing_info()) ->
    event_subscribers() | {error, Reason :: term()}.
get_subscribers({aggregated, [Evt | _]}, RoutingInfo) ->
    get_subscribers(Evt, RoutingInfo);
get_subscribers(_Evt, []) ->
    #event_subscribers{};
get_subscribers(Evt, [RoutingCtx | RoutingInfo]) ->
    case get_subscribers(Evt, RoutingCtx) of
        #event_subscribers{subscribers = SessIds} = Subscribers ->
            case get_subscribers(Evt, RoutingInfo) of
                #event_subscribers{subscribers = SessIds2} ->
                    % Note that list of routing ctxs is only used for #file_renamed_event{} that cannot
                    % produce any subscribers for links
                    Subscribers#event_subscribers{subscribers = SessIds2 ++ (SessIds -- SessIds2)};
                Other2 ->
                    Other2
            end;
        Other ->
            Other
    end;
get_subscribers(Evt, RoutingCtx) ->
    case file_subscription_counter:has_subscriptions(Evt) of
        false ->
            % This is hack for rest-based tests with large amount of links to single file
            % It cannot be handled with good performance until event's subsystem architecture is changed
            #event_subscribers{};
        _ ->
            case event_type:get_routing_key(Evt, RoutingCtx) of
                {ok, Keys} -> process_event_routing_keys(Keys);
                {error, session_only} -> #event_subscribers{}
            end
    end.

-spec get_attr_event_subscribers(fslogic_worker:file_guid(), event_type:routing_ctx(), boolean()) ->
    {event_subscribers() | {error, Reason :: term()}, event_subscribers() | {error, Reason :: term()}}.
get_attr_event_subscribers(Guid, RoutingCtx, SizeChanged) ->
    HasAttrSubscriptions = file_subscription_counter:has_subscriptions(
        event_type:get_attr_changed_reference_based_prefix()),
    HasReplicaSubscriptions = file_subscription_counter:has_subscriptions(
        event_type:get_replica_status_reference_based_prefix()),

    case {(HasAttrSubscriptions =/= false) andalso SizeChanged, HasReplicaSubscriptions} of
        {true, true} ->
            {AttrChangedKeys, StatusChangedKeys} = event_type:get_attr_routing_keys(Guid, RoutingCtx),
            {
                process_event_routing_keys(AttrChangedKeys),
                process_event_routing_keys(StatusChangedKeys)
            };
        {true, false} ->
            {
                process_event_routing_keys(event_type:get_attr_routing_keys_without_replica_status_changes(Guid, RoutingCtx)),
                #event_subscribers{}
            };
        {false, true} ->
            {
                #event_subscribers{},
                process_event_routing_keys(event_type:get_replica_status_routing_keys(Guid, RoutingCtx))
            };
        {false, false} ->
            {
                #event_subscribers{},
                #event_subscribers{}
            }
    end.

%%--------------------------------------------------------------------
%% @doc
%% Filter sessions when information about space dirs is broadcast
%% (not all clients are allowed to see particular space).
%% @end
%%--------------------------------------------------------------------
-spec apply_space_id_filter([session:id()], undefined | od_space:id()) -> [session:id()].
apply_space_id_filter(SessIds, undefined) ->
    SessIds;
apply_space_id_filter(SessIds, SpaceIDFilter) ->
    lists:filter(fun(SessId) ->
        UserCtx = user_ctx:new(SessId),
        Spaces = user_ctx:get_eff_spaces(UserCtx),
        lists:member(SpaceIDFilter, Spaces)
    end, SessIds).

-spec process_event_routing_keys(event_routing_keys()) -> event_subscribers() | {error, Reason :: term()}.
process_event_routing_keys(
    #event_routing_keys{main_key = MainKey, filter = Filter, additional_keys = AdditionalKeys} = Record) ->
    try
        SubscribersForLinks = lists:foldl(fun({Context, AdditionalKey}, Acc) ->
            case get_subscribers(AdditionalKey) of
                {ok, []} -> Acc;
                {ok, KeySessIds} -> [{Context, apply_space_id_filter(KeySessIds, Filter)} | Acc]
            end
        end, [], AdditionalKeys),
        {ok, SessIds} = get_subscribers(MainKey),
        #event_subscribers{
            subscribers = apply_space_id_filter(SessIds, Filter),
            subscribers_for_links = SubscribersForLinks
        }
    catch
        Error:Reason:Stacktrace ->
            ?error_stacktrace("Processing event routing keys error ~p~p for keys ~p", [Error, Reason, Record], Stacktrace),
            {error, processing_event_routing_keys_failed}
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
            file_subscription_counter:subscription_deleted(Key),
            case gb_sets:is_empty(SIds) of
                true ->
                    Pred = fun(#file_subscription{sessions = SIds2}) ->
                        gb_sets:is_empty(SIds2)
                    end,
                    file_subscription:delete(Key, Pred);
                false ->
                    ok
            end;
        {error, not_found} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.