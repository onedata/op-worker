%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for sending direct and broadcast DBSync messages
%%% to providers.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_communicator).
-author("Krzysztof Trzepla").

-include("proto/oneprovider/dbsync_messages2.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([send/2, forward/1, broadcast/4]).
-export([request_changes/4, send_changes/5, broadcast_changes/4]).

-type changes_batch() :: #changes_batch{}.
-type changes_request() :: #changes_request2{}.
-type tree_broadcast() :: #tree_broadcast2{}.
-type msg_id() :: binary().
-type msg() :: changes_batch() | changes_request() | tree_broadcast().
-type broadcast_opt() :: {src_provider_id, od_provider:id()} |
                         {low_provider_id, od_provider:id()} |
                         {high_provider_id, od_provider:id()} |
                         {low_open, boolean()} |
                         {high_open, boolean()}.

-export_type([changes_batch/0, changes_request/0, tree_broadcast/0]).
-export_type([msg_id/0, msg/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends direct message to a provider.
%% @end
%%--------------------------------------------------------------------
-spec send(od_provider:id(), msg()) -> ok | {error, Reason :: term()}.
send(ProviderId, Msg) ->
    SessId = session_manager:get_provider_session_id(outgoing, ProviderId),
    provider_communicator:send(#dbsync_message{message_body = Msg}, SessId, 3).

%%--------------------------------------------------------------------
%% @doc
%% Forwards broadcast message to suitable providers.
%% @end
%%--------------------------------------------------------------------
-spec forward(tree_broadcast()) -> ok.
forward(#tree_broadcast2{
    src_provider_id = SrcProviderId,
    low_provider_id = LowProviderId,
    high_provider_id = HighProviderId,
    message_id = MsgId,
    message_body = #changes_batch{space_id = SpaceId} = Msg
}) ->
    case oneprovider:get_provider_id() of
        LowProviderId ->
            broadcast(SpaceId, MsgId, Msg, [
                {src_provider_id, SrcProviderId},
                {low_provider_id, LowProviderId},
                {high_provider_id, HighProviderId},
                {low_open, true}
            ]);
        HighProviderId ->
            broadcast(SpaceId, MsgId, Msg, [
                {src_provider_id, SrcProviderId},
                {low_provider_id, LowProviderId},
                {high_provider_id, HighProviderId},
                {high_open, true}
            ])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Broadcast message to all providers supporting given space.
%% The broadcast algorithm is organised as follows:
%% 1. a list L = [P1, P2, ..., Pn] of a space supporting providers is generated
%% 2. provider that is broadcasting message (source provider) is removed
%%    from L creating new list L'
%% 3. L' is sorted in as ascending order and split randomly into two lists
%%    L1 = [P1, P2, ..., Pk] and L2 = [Pk+1, Pk+2, ..., Pn], where 0 <= k <= n
%% 4. message is sent to providers Pk and Pk+1, which are responsible for
%%    broadcasting this message further to providers [P1, ..., Pk-1] 
%%    and [Pk+2, ..., Pn] respectively
%% @end
%%--------------------------------------------------------------------
-spec broadcast(od_space:id(), msg_id(), changes_batch(), [broadcast_opt()]) ->
    ok.
broadcast(SpaceId, MsgId, Msg, Opts) ->
    Hops = get_next_broadcast_hops(SpaceId, Opts),
    ?info("dddd ~p", [{SpaceId, Hops}]),
    lists:foreach(fun({ProviderId, {LowProviderId, HighProviderId}}) ->
        Result = dbsync_communicator:send(ProviderId, #tree_broadcast2{
            src_provider_id = proplists:get_value(src_provider_id,
                Opts, oneprovider:get_provider_id()),
            low_provider_id = LowProviderId,
            high_provider_id = HighProviderId,
            message_id = MsgId,
            message_body = Msg
        }),
%%        ?info("dddd2 ~p", [{SpaceId, Msg, ProviderId, Result}]),
        case Result of
            ok -> ok;
            {error, Reason} ->
                ?warning("Cannot broadcast changes batch to provider ~p "
                "due to: ~p", [ProviderId, Reason])
        end
    end, Hops).

%%--------------------------------------------------------------------
%% @doc
%% Sends direct message to a provider requesting changes from a space
%% in given range.
%% @end
%%--------------------------------------------------------------------
-spec request_changes(od_provider:id(), od_space:id(),
    couchbase_changes:since(), couchbase_changes:until()) ->
    ok | {error, Reason :: term()}.
request_changes(ProviderId, SpaceId, Since, Until) ->
    dbsync_communicator:send(ProviderId, #changes_request2{
        space_id = SpaceId,
        since = Since,
        until = Until
    }).

%%--------------------------------------------------------------------
%% @doc
%% Sends direct message to a provider containing changes from a space
%% in given range.
%% @end
%%--------------------------------------------------------------------
-spec send_changes(od_provider:id(), od_space:id(), couchbase_changes:since(),
    couchbase_changes:until(), [datastore:doc()]) ->
    ok | {error, Reason :: term()}.
send_changes(ProviderId, SpaceId, Since, Until, Docs) ->
    dbsync_communicator:send(ProviderId, #changes_batch{
        space_id = SpaceId,
        since = Since,
        until = Until,
        compressed_docs = dbsync_utils2:compress(Docs)
    }).

%%--------------------------------------------------------------------
%% @doc
%% Broadcast changes from a space in given range to all providers supporting
%% this space.
%% @end
%%--------------------------------------------------------------------
-spec broadcast_changes(od_space:id(), couchbase_changes:since(),
    couchbase_changes:until(), [datastore:doc()]) -> ok.
broadcast_changes(SpaceId, Since, Until, Docs) ->
    MsgId = dbsync_utils2:gen_request_id(),
    Msg = #changes_batch{
        space_id = SpaceId,
        since = Since,
        until = Until,
        compressed_docs = dbsync_utils2:compress(Docs)
    },
    broadcast(SpaceId, MsgId, Msg, []),
    broadcast(SpaceId, MsgId, Msg, []).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns a list of next broadcast hops. Broadcast hop consist of:
%% * DestProviderId - provider to which message will be sent,
%%                    provider responsible for further broadcast
%% * LowProviderId  - lower bound for range of receiving providers
%% * HighProviderId - upper bound for range of receiving providers
%% @end
%%--------------------------------------------------------------------
-spec get_next_broadcast_hops(od_space:id(), [broadcast_opt()]) ->
    [{DestProviderId, {LowProviderId, HighProviderId}}] when
    DestProviderId :: od_provider:id(),
    LowProviderId :: od_provider:id(),
    HighProviderId :: od_provider:id().
get_next_broadcast_hops(SpaceId, Opts) ->
    ProviderIds = dbsync_utils2:get_providers(SpaceId),
    ProviderIds2 = select_receiving_providers(ProviderIds, Opts),
    Pivot = rand:uniform(length(ProviderIds2) + 1) - 1,
    {LowProviderIds, HighProviderIds} = lists:split(Pivot, ProviderIds2),
    Hops = case LowProviderIds of
        [] -> [];
        [LowProviderId | _] ->
            HighProviderId = lists:last(LowProviderIds),
            [{HighProviderId, {LowProviderId, HighProviderId}}]
    end,
    case HighProviderIds of
        [] -> Hops;
        [LowProviderId2 | _] ->
            HighProviderId2 = lists:last(HighProviderIds),
            [{LowProviderId2, {LowProviderId2, HighProviderId2}} | Hops]
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Selects receiving providers from a list according to provided options.
%% @end
%%--------------------------------------------------------------------
-spec select_receiving_providers([od_provider:id()], [broadcast_opt()]) -> 
    [od_provider:id()].
select_receiving_providers([], _Opts) ->
    [];
select_receiving_providers(ProviderIds, Opts) ->
    ProviderIds2 = lists:usort(ProviderIds),
    ThisProviderId = oneprovider:get_provider_id(),
    LastProviderId = lists:last(ProviderIds2),
    SrcProviderId = proplists:get_value(src_provider_id, Opts, ThisProviderId),
    LowProviderId = proplists:get_value(low_provider_id, Opts, hd(ProviderIds2)),
    HighProviderId = proplists:get_value(high_provider_id, Opts, LastProviderId),
    LowOpen = proplists:get_value(low_open, Opts, false),
    HighOpen = proplists:get_value(high_open, Opts, false),

    ProviderIds3 = lists:delete(SrcProviderId, ProviderIds2),

    ProviderIds4 = lists:dropwhile(fun(ProviderId) ->
        ProviderId < LowProviderId
    end, ProviderIds3),
    ProviderIds6 = case {LowOpen, ProviderIds4} of
        {true, [LowProviderId | ProviderIds5]} -> ProviderIds5;
        _ -> ProviderIds4
    end,

    ProviderIds7 = lists:takewhile(fun(ProviderId) ->
        ProviderId =< HighProviderId
    end, ProviderIds6),
    case {HighOpen, lists:reverse(ProviderIds7)} of
        {true, [HighProviderId | ProviderIds8]} -> lists:reverse(ProviderIds8);
        _ -> ProviderIds7
    end.