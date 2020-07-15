%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% space entities into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(space_rest_translator).
-author("Bartosz Walkowicz").

-include("http/rest.hrl").
-include("middleware/middleware.hrl").

-export([get_response/2]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(#gri{id = SpaceId, aspect = instance, scope = private}, {#od_space{
    name = Name,
    providers = ProvidersIds
}, _}) ->
    Providers = lists:map(fun(ProviderId) ->
        {ok, ProviderName} = provider_logic:get_name(ProviderId),
        #{
            <<"providerId">> => ProviderId,
            <<"providerName">> => ProviderName
        }
    end, maps:keys(ProvidersIds)),
    ?OK_REPLY(#{
        <<"name">> => Name,
        <<"providers">> => Providers,
        <<"spaceId">> => SpaceId
    });
get_response(_, SpaceData) ->
    ?OK_REPLY(SpaceData).
