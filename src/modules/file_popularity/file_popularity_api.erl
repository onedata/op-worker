%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% API for file-popularity management
%%% @end
%%%-------------------------------------------------------------------
-module(file_popularity_api).
-author("Jakub Kudzia").


%% API
-export([enable/1, disable/1, is_enabled/1, get_configuration/1, query/2,
    query/3, delete_config/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec enable(file_popularity_config:id()) -> ok | {error, term()}.
enable(SpaceId) ->
    % ensure that view is created
    % todo get weight from app.config or from file_popularity_config
    % todo ensure that weights are numbers
    TimestampWeight = 1,
    AvgOpenCountPerDayWeight = 0.75,
    file_popularity_view:create(SpaceId, TimestampWeight, AvgOpenCountPerDayWeight),
    file_popularity_config:enable(SpaceId).

-spec disable(file_popularity_config:id()) -> ok | {error, term()}.
disable(SpaceId) ->
    autocleaning_api:disable(SpaceId),
    file_popularity_config:disable(SpaceId),
    file_popularity_view:delete(SpaceId).

-spec delete_config(file_popularity_config:id()) -> ok.
delete_config(SpaceId) ->
    file_popularity_config:delete(SpaceId).

-spec is_enabled(file_popularity_config:id()) -> boolean().
is_enabled(SpaceId) ->
    file_popularity_config:is_enabled(SpaceId).

-spec get_configuration(file_popularity_config:id()) -> maps:map().
get_configuration(SpaceId) -> #{
    enabled => is_enabled(SpaceId),
    rest_url => file_popularity_view:rest_url(SpaceId)
}.

-spec query(od_space:id(), non_neg_integer()) ->
    {[cdmi_id:objectid()], file_popularity_view:index_token() | undefined} | {error, term()}.
query(SpaceId, Limit) ->
    file_popularity_view:query(SpaceId, undefined, Limit).

-spec query(od_space:id(), file_popularity_view:index_token() | undefined, non_neg_integer()) ->
    {[cdmi_id:objectid()], file_popularity_view:index_token()} | {error, term()}.
query(SpaceId, IndexToken, Limit) ->
    file_popularity_view:query(SpaceId, IndexToken, Limit).