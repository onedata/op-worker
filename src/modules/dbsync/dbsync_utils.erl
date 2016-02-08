%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_utils).
-author("Rafal Slota").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").


%% API
-export([get_providers_for_space/1]).
-export([get_spaces_for_provider/0, get_spaces_for_provider/1]).
-export([get_provider_url/1, encode_term/1, decode_term/1, gen_request_id/0]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_providers_for_space(SpaceId :: binary()) ->
    [oneprovider:id()].
get_providers_for_space(SpaceId) ->
    try
        {ok, ProviderIds} = gr_spaces:get_providers(provider, SpaceId),
        ProviderIds
    catch
        _:{_, {error, 'No such file or directory'}} ->
            []
    end .

-spec get_spaces_for_provider() ->
    [SpaceId :: binary()].
get_spaces_for_provider() ->
    get_spaces_for_provider(oneprovider:get_provider_id()).

-spec get_spaces_for_provider(oneprovider:id()) ->
    [SpaceId :: binary()].
get_spaces_for_provider(ProviderId) ->
    {ok, SpaceIds} = gr_providers:get_spaces(provider),
    lists:foldl(
        fun(SpaceId, Acc) ->
            {ok, Providers} = gr_spaces:get_providers(provider, SpaceId),
            case lists:member(ProviderId, Providers) of
                true -> [SpaceId | Acc];
                false -> Acc
            end
        end, [], SpaceIds).


%%--------------------------------------------------------------------
%% @doc Selects URL of the provider
%% @end
%%--------------------------------------------------------------------
-spec get_provider_url(ProviderId :: oneprovider:id()) -> URL :: binary() | no_return().
get_provider_url(ProviderId) ->
    {ok, #provider_details{urls = URLs}} = gr_providers:get_details(provider, ProviderId),
    _URL = lists:nth(crypto:rand_uniform(1, length(URLs) + 1), URLs).


%%--------------------------------------------------------------------
%% @doc Encodes given erlang term to binary
%% @end
%%--------------------------------------------------------------------
-spec encode_term(term()) -> binary().
encode_term(Doc) ->
    term_to_binary(Doc).


%%--------------------------------------------------------------------
%% @doc Decodes given binary to erlang term (reverses encode_term/1)
%% @end
%%--------------------------------------------------------------------
-spec decode_term(binary()) -> term().
decode_term(Doc) ->
    binary_to_term(Doc).


%%--------------------------------------------------------------------
%% @doc Generates UUID for inter-provider dbsync's requests
%% @end
%%--------------------------------------------------------------------
-spec gen_request_id() -> binary().
gen_request_id() ->
    base64:encode(crypto:rand_bytes(32)).
