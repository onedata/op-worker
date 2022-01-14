%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for manipulating transfers in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_transfers).
-author("Bartosz Walkowicz").

%% API
-export([
    schedule_file_replication/4, schedule_replication_by_view/6,
    schedule_file_replica_eviction/5, schedule_replica_eviction_by_view/7,

    schedule_file_transfer/6, schedule_view_transfer/8
]).

-define(CALL(NodeSelector, Args),
    try opw_test_rpc:call(NodeSelector, mi_transfers, ?FUNCTION_NAME, Args) of
        ok -> ok;
        __RESULT -> {ok, __RESULT}
    catch throw:__ERROR ->
        __ERROR
    end
).


%%%===================================================================
%%% API
%%%===================================================================


-spec schedule_file_replication(
    oct_background:node_selector(),
    session:id(),
    lfm:file_key(),
    oneprovider:id()
) ->
    {ok, transfer:id()} | errors:error().
schedule_file_replication(NodeSelector, SessionId, FileKey, ProviderId) ->
    schedule_file_transfer(NodeSelector, SessionId, FileKey, ProviderId, undefined, undefined).


-spec schedule_replication_by_view(
    oct_background:node_selector(),
    session:id(),
    ProviderId :: oneprovider:id(),
    SpaceId :: od_space:id(),
    ViewName :: transfer:view_name(),
    transfer:query_view_params()
) ->
    {ok, transfer:id()} | errors:error().
schedule_replication_by_view(NodeSelector, SessionId, ProviderId, SpaceId, ViewName, QueryViewParams) ->
    schedule_view_transfer(
        NodeSelector, SessionId, SpaceId, ViewName, QueryViewParams,
        ProviderId, undefined, undefined
    ).


-spec schedule_file_replica_eviction(
    oct_background:node_selector(),
    session:id(),
    lfm:file_key(),
    ProviderId :: oneprovider:id(),
    MigrationProviderId :: undefined | oneprovider:id()
) ->
    {ok, transfer:id()} | errors:error().
schedule_file_replica_eviction(NodeSelector, SessionId, FileKey, ProviderId, MigrationProviderId) ->
    schedule_file_transfer(
        NodeSelector, SessionId, FileKey, MigrationProviderId, ProviderId, undefined
    ).


-spec schedule_replica_eviction_by_view(
    oct_background:node_selector(),
    session:id(),
    ProviderId :: oneprovider:id(),
    MigrationProviderId :: undefined | oneprovider:id(),
    od_space:id(),
    transfer:view_name(),
    transfer:query_view_params()
) ->
    {ok, transfer:id()} | errors:error().
schedule_replica_eviction_by_view(
    NodeSelector, SessionId, ProviderId, MigrationProviderId,
    SpaceId, ViewName, QueryViewParams
) ->
    schedule_view_transfer(
        NodeSelector, SessionId, SpaceId, ViewName, QueryViewParams, 
        MigrationProviderId, ProviderId, undefined
    ).


-spec schedule_file_transfer(
    oct_background:node_selector(),
    session:id(),
    lfm:file_key(),
    ReplicatingProviderId :: undefined | od_provider:id(),
    EvictingProviderId :: undefined | od_provider:id(),
    transfer:callback()
) ->
    {ok, transfer:id()} | errors:error().
schedule_file_transfer(
    NodeSelector, SessionId, FileKey, ReplicatingProviderId, EvictingProviderId, Callback
) ->
    ?CALL(NodeSelector, [
        SessionId, FileKey, ReplicatingProviderId, EvictingProviderId, Callback
    ]).


-spec schedule_view_transfer(
    oct_background:node_selector(),
    session:id(),
    od_space:id(),
    transfer:view_name(),
    transfer:query_view_params(),
    ReplicatingProviderId :: undefined | od_provider:id(),
    EvictingProviderId :: undefined | od_provider:id(),
    transfer:callback()
) ->
    {ok, transfer:id()} | errors:error().
schedule_view_transfer(
    NodeSelector, SessionId, SpaceId, ViewName, QueryViewParams,
    ReplicatingProviderId, EvictingProviderId, Callback
) ->
    ?CALL(NodeSelector, [
        SessionId, SpaceId, ViewName, QueryViewParams,
        ReplicatingProviderId, EvictingProviderId, Callback
    ]).
