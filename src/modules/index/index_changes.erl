%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling changes on index documents. The
%%% callback is called for all changes - remote (dbsync) and local (posthook).
%%% @end
%%%-------------------------------------------------------------------
-module(index_changes).
-author("Bartosz Walkowicz").
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle/1]).

-define(run_if_is_on_providers_list(Doc, Fun),
    ProviderIds = Doc#document.value#index.providers,
    case oneprovider:get_id_or_undefined() of
        undefined ->
            ok;
        ProviderId ->
            case lists:member(ProviderId, ProviderIds) of
                true ->
                    Fun(Doc);
                _ ->
                    ok
            end
    end
).

handle(Doc) ->
    ?run_if_is_on_providers_list(Doc, fun handle_internal/1).

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_internal(#document{
    key = Id,
    value = #index{
        name = IndexName,
        space_id = SpaceId,
        spatial = Spatial,
        map_function = MapFunction,
        index_options = Options
}}) ->
    case provider_logic:supports_space(SpaceId) of
        true ->
            ok = index:save_db_view(Id, SpaceId, MapFunction, Spatial, Options);
        false ->
            ?warning("Creation of index ~p with id ~p requested within not supported space ~p",
                [IndexName, Id, SpaceId])
    end.
