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

-export([create_response/4, get_response/2]).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback create_response/4.
%% @end
%%--------------------------------------------------------------------
-spec create_response(gri:gri(), middleware:auth_hint(),
    middleware:data_format(), Result :: term() | {gri:gri(), term()} |
    {gri:gri(), middleware:auth_hint(), term()}) -> #rest_resp{}.
create_response(#gri{aspect = evaluate_qos_expression}, _, value, Result) ->
    ?OK_REPLY(maps:get(<<"matchingStorages">>, Result)).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(#gri{id = SpaceId, aspect = instance, scope = private}, #od_space{
    name = Name,
    providers = ProvidersIds
}) ->
    SpaceDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    {ok, SpaceDirObjectId} = file_id:guid_to_objectid(SpaceDirGuid),

    Providers = lists:map(fun(ProviderId) ->
        {ok, ProviderName} = provider_logic:get_name(ProviderId),
        #{
            <<"providerId">> => ProviderId,
            <<"providerName">> => ProviderName
        }
    end, maps:keys(ProvidersIds)),

    ?OK_REPLY(#{
        <<"name">> => Name,
        <<"spaceId">> => SpaceId,
        <<"fileId">> => SpaceDirObjectId,
        <<"providers">> => Providers
    });
get_response(_, SpaceData) ->
    ?OK_REPLY(SpaceData).
