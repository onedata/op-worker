%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Persistent state of DBSync worker. For each space it holds mapping from
%%% provider to a sequence number of the beginning of expected changes range.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_state).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([delete/1, get_seq/2, set_seq/3]).

%% datastore_model callbacks
-export([get_record_struct/1]).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes dbsync state for a space.
%% @end
%%--------------------------------------------------------------------
-spec delete(od_space:id()) -> ok | {error, term()}.
delete(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId).

%%--------------------------------------------------------------------
%% @doc
%% Returns sequence number of the beginning of expected changes range
%% from given space and provider.
%% @end
%%--------------------------------------------------------------------
-spec get_seq(od_space:id(), od_provider:id()) -> couchbase_changes:seq().
get_seq(SpaceId, ProviderId) ->
    case datastore_model:get(?CTX, SpaceId) of
        {ok, #document{value = #dbsync_state{seq = Seq}}} ->
            maps:get(ProviderId, Seq, 1);
        {error, not_found} ->
            1
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets sequence number of the beginning of expected changes range
%% from given space and provider.
%% @end
%%--------------------------------------------------------------------
-spec set_seq(od_space:id(), od_provider:id(), couchbase_changes:seq()) ->
    ok | {error, Reason :: term()}.
set_seq(SpaceId, ProviderId, ProviderSeq) ->
    Diff = fun(#dbsync_state{seq = Seq} = State) ->
        {ok, State#dbsync_state{seq = maps:put(ProviderId, ProviderSeq, Seq)}}
    end,
    Default = #dbsync_state{seq = #{ProviderId => ProviderSeq}},
    case datastore_model:update(?CTX, SpaceId, Diff, Default) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {seq, #{string => integer}}
    ]}.