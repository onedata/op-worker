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
-export([enable/1, disable/1, is_enabled/1, get_configuration/1, query/3, initial_token/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec enable(file_popularity_config:id()) -> ok | {error, term()}.
enable(SpaceId) ->
    % ensure that view is created
    file_popularity_view:create(SpaceId),
    file_popularity_config:enable(SpaceId).

-spec disable(file_popularity_config:id()) -> ok | {error, term()}.
disable(SpaceId) ->
    % todo should we delete the view?
    autocleaning_api:disable(SpaceId),
    file_popularity_config:disable(SpaceId).

-spec is_enabled(file_popularity_config:id()) -> boolean().
is_enabled(SpaceId) ->
    file_popularity_config:is_enabled(SpaceId).

-spec get_configuration(file_popularity_config:id()) -> maps:map().
get_configuration(SpaceId) -> #{
    enabled => is_enabled(SpaceId),
    rest_url => file_popularity_view:rest_url(SpaceId)
}.

-spec query(od_space:id(), file_popularity_view:token(), non_neg_integer()) ->
    {[file_ctx:ctx()], file_popularity_view:token()} | {error, term()}.
query(SpaceId, Token, Limit) ->
    file_popularity_view:query(SpaceId, Token, Limit).

-spec initial_token(binary() | [non_neg_integer()], binary() | [non_neg_integer()]) ->
    file_popularity_view:token().
initial_token(StartKey, EndKey) ->
    file_popularity_view:initial_token(StartKey, EndKey).