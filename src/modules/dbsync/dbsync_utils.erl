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
-export([get_provider_url/1, encode_term/1, decode_term/1, gen_request_id/0, get_spaces_for_provider/1]).

%%%===================================================================
%%% API
%%%===================================================================

get_providers_for_space(SpaceId) ->
    {ok, ProviderIds} = gr_spaces:get_providers(provider, SpaceId),
    ProviderIds.

get_spaces_for_provider(ProviderId) ->
    {ok, SpaceIds} = gr_providers:get_spaces( ProviderId),
    SpaceIds.


%% ====================================================================
%% @doc Selects URL of the provider
%% ====================================================================
-spec get_provider_url(ProviderId :: binary()) -> URL :: string() | no_return().
get_provider_url(ProviderId) ->
    {ok, #provider_details{urls = URLs}} = gr_providers:get_details(provider, ProviderId),
    _URL = lists:nth(crypto:rand_uniform(1, length(URLs) + 1), URLs).


%%%% ====================================================================
%%%% @doc Normalizes sequence info format
%%%% ====================================================================
%%-spec normalize_seq_info(term()) -> {SeqNum :: integer(), SeqHash :: binary()}.
%%normalize_seq_info(SeqNum) when is_integer(SeqNum) ->
%%    normalize_seq_info({SeqNum, <<>>});
%%normalize_seq_info({SeqNum, SeqHash}) when is_integer(SeqNum), is_binary(SeqHash) ->
%%    {SeqNum, SeqHash};
%%normalize_seq_info({SeqNum, [SeqHash | _]}) when is_integer(SeqNum), is_binary(SeqHash) ->
%%    normalize_seq_info({SeqNum, SeqHash});
%%normalize_seq_info([SeqNum, SeqHash]) ->
%%    normalize_seq_info({SeqNum, SeqHash}).



%% ====================================================================
%% @doc Encodes given erlang term to binary
%% ====================================================================
-spec encode_term(term()) -> binary().
encode_term(Doc) ->
    term_to_binary(Doc).


%% ====================================================================
%% @doc Decodes given binary to erlang term (reverses encode_term/1)
%% ====================================================================
-spec decode_term(binary()) -> term().
decode_term(Doc) ->
    binary_to_term(Doc).


%% ====================================================================
%% @doc Generates UUID for inter-provider dbsync's requests
%% ====================================================================
-spec gen_request_id() -> binary().
gen_request_id() ->
    base64:encode(crypto:rand_bytes(32)).

%%%===================================================================
%%% Internal functions
%%%===================================================================