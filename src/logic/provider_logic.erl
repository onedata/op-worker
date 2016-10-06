%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Interface to provider's cache containing od_provider.
%%% Operations may involve interactions with OZ api
%%% or cached records from the datastore.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_logic).
-author("Michal Zmuda").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

-export([get/1, get_providers_with_common_support/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Retrieves group document as provider.
%% @end
%%--------------------------------------------------------------------
-spec get(ProviderID :: binary()) ->
    {ok, datastore:document()} | {error, Reason :: term()}.
get(ProviderID) ->
    od_provider:get(ProviderID).

%%--------------------------------------------------------------------
%% @doc
%% Retrieves provider info docs for providers that support at least
%% one space supported also by this provider. Returns error if any piece
%% of information needed is unavailable.
%% @end
%%--------------------------------------------------------------------
-spec get_providers_with_common_support() ->
    {ok, [datastore:document()]} | {error, Reason :: term()}.
get_providers_with_common_support() ->
    GetIdsOfProvidersWithCommonSupport = get_ids_of_providers_with_common_support(),
    case GetIdsOfProvidersWithCommonSupport of
        {ok, ProviderIDs} -> get_providers(ProviderIDs);
        Error -> Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_providers(ProviderIDs :: [binary()]) ->
    {ok, [datastore:document()]} | {error, Reason :: term()}.
get_providers(ProviderIDs) ->
    Results = utils:pmap(fun od_provider:get/1, ProviderIDs),
    case lists:any(fun
        ({error, Reason}) ->
            ?warning("Unable to read provider info due to ~p", [Reason]),
            true;
        (_) -> false
    end, Results) of
        true -> {error, no_public_provider_info};
        false -> {ok, lists:map(fun({ok, Doc}) -> Doc end, Results)}
    end.

-spec get_ids_of_providers_with_common_support() ->
    {ok, [ProviderID :: binary()]} | {error, Reason :: term()}.
get_ids_of_providers_with_common_support() ->
    ProviderID = oneprovider:get_provider_id(),
    case provider_logic:get(ProviderID) of
        {ok, #document{value = Provider}} ->
            case Provider of
                #od_provider{public_only = true} ->
                    ?error("Own provider info contains public only data"),
                    {error, no_private_info};
                #od_provider{spaces = SIDs} ->
                    ger_supporting_providers(SIDs)
            end;
        {error, Reason} ->
            ?error("Could not read own provider info due to ~p", [Reason]),
            {error, no_info}
    end.

-spec ger_supporting_providers(SpaceIDs :: [binary()]) ->
    {ok, [ProviderID :: binary()]} | {error, Reason :: term()}.
ger_supporting_providers(SpaceIDs) ->
    ProviderIDs = lists:flatten(utils:pmap(fun(SpaceID) ->
        {ok, #document{value = #od_space{providers_supports = Supports}}}
            = od_space:get(SpaceID),
        {SupportingProviderIDs, _} = lists:unzip(Supports),
        SupportingProviderIDs
    end, SpaceIDs)),

    case lists:all(fun erlang:is_binary/1, ProviderIDs) of
        false ->
            ?error("Unable to read space info"),
            {error, no_od_space};
        true -> {ok, ProviderIDs}
    end.