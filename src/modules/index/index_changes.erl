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

-spec handle(index:doc()) -> ok.
handle(Doc = #document{value = #index{providers = ProviderIds}}) ->
    case oneprovider:get_id_or_undefined() of
        undefined ->
            ok;
        ProviderId ->
            case lists:member(ProviderId, ProviderIds) of
                true ->
                    create_or_update_db_view(Doc);
                false ->
                    remove_db_view(Doc)
            end
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec create_or_update_db_view(index:doc()) -> ok.
create_or_update_db_view(#document{
    key = Id,
    value = #index{
        name = IndexName,
        space_id = SpaceId,
        spatial = Spatial,
        map_function = MapFunction,
        reduce_function = ReduceFunction,
        index_options = Options
    }
}) ->
    case provider_logic:supports_space(SpaceId) of
        true ->
            ok = index:save_db_view(Id, SpaceId, MapFunction, ReduceFunction, Spatial, Options);
        false ->
            ?warning("Creation of index ~p with id ~p requested within not supported space ~p",
                [IndexName, Id, SpaceId])
    end.

-spec remove_db_view(index:doc()) -> ok.
remove_db_view(#document{key = IndexId}) ->
    case index:delete_db_view(IndexId) of
        ok ->
            ok;
        {error, {<<"not_found">>, <<"missing">>}} ->
            ok;
        {error, {<<"not_found">>, <<"deleted">>}} ->
            ok;
        {error, Error} = Err ->
            ?error("Removal of db view ~p from provider failed due to ~p", [IndexId, Error]),
            Err
    end.
