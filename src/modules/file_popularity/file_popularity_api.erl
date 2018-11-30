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
-export([enable/1, disable/1, is_enabled/1, get_configuration/1]).

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
