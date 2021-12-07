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
-include("modules/events/routing.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([add_subscriber/2, get_subscribers/2, get_attr_event_subscribers/3, remove_subscriber/2]).
%% For tests
-export([get_subscribers/1]).

-type key() :: binary().
% routing can require connection of several contexts, e.g., old and new parent when moving file
-type routing_info() :: event_type:routing_ctx() | [event_type:routing_ctx()].
-type event_routing_key() :: #event_routing_key{}.

-export_type([key/0, routing_info/0, event_routing_key/0]).

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
    {ok, SessIds :: [session:id()]} | {error, Reason :: term()}.
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
            process_event_routing_key(Key);
        {error, session_only} ->
            {ok, []}
    end.

-spec get_attr_event_subscribers(fslogic_worker:file_guid(), event_type:routing_ctx(), boolean()) ->
    [{ok, SessIds :: [session:id()]} | {error, Reason :: term()}].
get_attr_event_subscribers(Guid, RoutingCtx, SizeChanged) ->
    Keys = case SizeChanged of
        true -> event_type:get_attr_routing_keys(Guid, RoutingCtx);
        false -> [event_type:get_replica_status_routing_keys(Guid, RoutingCtx)]
    end,

    lists:map(fun(Key) -> process_event_routing_key(Key) end, Keys).

-spec process_event_routing_key(event_routing_key()) -> {ok, SessIds :: [session:id()]} | {error, Reason :: term()}.
process_event_routing_key(#event_routing_key{
    file_ctx = FileCtx,
    manager_key = ManagerKey,
    space_id_filter = SpaceIdFilter,
    auth_check_type = AuthCheckType
} = Record) ->
    try
        {ok, SessIds} = get_subscribers(ManagerKey),
        {ok, apply_auth_filter(apply_space_id_filter(SessIds, SpaceIdFilter), AuthCheckType, FileCtx)}
    catch
        Error:Reason ->
            ?error_stacktrace("Processing event routing key error ~p~p for ~p", [Error, Reason, Record]),
            {error, processing_event_routing_key_failed}
    end.

-spec apply_auth_filter([session:id()], event_type:auth_check_type(), undefined | file_ctx:ctx()) -> [session:id()].
apply_auth_filter(SessIds, _AuthCheckType, undefined) ->
    SessIds;
apply_auth_filter(SessIds, AuthCheckType, FileCtx) ->
    lists:filter(fun(SessId) ->
        try
            ensure_authorized(SessId, AuthCheckType, FileCtx)
        catch
            _:_ -> false
        end
    end, SessIds).

-spec ensure_authorized(session:id(), event_type:auth_check_type(), file_ctx:ctx()) -> boolean().
ensure_authorized(SessId, attrs, FileCtx) ->
    data_constraints:inspect(user_ctx:new(SessId), FileCtx, allow_ancestors, [traverse_ancestors]),
    true;
ensure_authorized(SessId, location, FileCtx) ->
    data_constraints:inspect(user_ctx:new(SessId), FileCtx, disallow_ancestors, [traverse_ancestors]),
    true;
ensure_authorized(SessId, rename, FileCtx) ->
    UserCtx = user_ctx:new(SessId),
    try
        % Reset file_ctx before usage as it can cache old parent or document
        % (cached data cannot be used to check if file was visible to client before rename
        % because there is no guarantee that this data is cached)
        data_constraints:inspect(UserCtx, file_ctx:reset(FileCtx), disallow_ancestors, [traverse_ancestors]),
        true
    catch
        _:_ ->
            % TODO VFS-8717 - This is hack as client does not understand that file should not be visible after rename
            % There is no possibility to check if file was visible to client before rename so #file_removed_event{}
            % is always sent and client ignores it if the file was not visible for him
            spawn(fun() ->
                event:emit(#file_removed_event{file_guid = file_ctx:get_guid_const(FileCtx)}, [SessId])
            end),
            false
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