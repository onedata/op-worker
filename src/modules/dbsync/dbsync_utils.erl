%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Utility functions for DBSync
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_utils).
-author("Rafal Slota").

-include("proto/oneprovider/dbsync_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_providers.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").


%% API
-export([get_providers_for_space/1]).
-export([get_spaces_for_provider/0, get_spaces_for_provider/1]).
-export([get_provider_url/1, get_provider_urls/1, encode_term/1, decode_term/1, gen_request_id/0]).
-export([communicate/2]).
-export([temp_get/1, temp_put/3, temp_clear/1]).
-export([validate_space_access/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if given provider supports given space.
%% @end
%%--------------------------------------------------------------------
-spec validate_space_access(ProviderId :: oneprovider:id(), SpaceId :: space_info:id()) ->
    ok | {error, space_not_supported_locally}.
validate_space_access(ProviderId, SpaceId) ->
    IsMember = lists:member(ProviderId, get_providers_for_space(SpaceId)),
    ?debug("validate_space_access ~p ~p: ~p", [ProviderId, SpaceId, IsMember]),
    case IsMember of
        true -> ok;
        false ->

            {error, space_not_supported_locally}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Puts given Value in datastore worker's temporary state
%% @end
%%--------------------------------------------------------------------
-spec temp_put(Key :: term(), Value :: term(), Timeout :: non_neg_integer()) -> ok.
temp_put(Key, Value, 0) ->
    worker_host:state_put(dbsync_worker, Key, Value);
temp_put(Key, Value, Timeout) ->
    timer:send_after(Timeout, whereis(dbsync_worker), {timer, {clear_temp, Key}}),
    worker_host:state_put(dbsync_worker, Key, Value).


%%--------------------------------------------------------------------
%% @doc
%% Clears given Key in datastore worker's temporary state
%% @end
%%--------------------------------------------------------------------
-spec temp_clear(Key :: term()) -> ok.
temp_clear(Key) ->
    worker_host:state_delete(dbsync_worker, Key).

%%--------------------------------------------------------------------
%% @doc
%% Puts Value from datastore worker's temporary state
%% @end
%%--------------------------------------------------------------------
-spec temp_get(Key :: term()) -> Value :: term().
temp_get(Key) ->
    worker_host:state_get(dbsync_worker, Key).


%%--------------------------------------------------------------------
%% @doc Returns list of providers that supports given space. If provider is nor properly configured to work
%%      as part of onedata system, empty list is returned.
%% @end
%%--------------------------------------------------------------------
-spec get_providers_for_space(SpaceId :: binary()) ->
    [oneprovider:id()].
get_providers_for_space(SpaceId) ->
    try
        {ok, #document{value = #space_info{providers = ProviderIds}}} = space_info:get_or_fetch(?ROOT_SESS_ID, SpaceId),
        ProviderIds
    catch
        _:{_, {error, 'No such file or directory'}} ->
            []
    end .


%%--------------------------------------------------------------------
%% @doc Returns list of spaces supported by this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces_for_provider() ->
    [SpaceId :: binary()].
get_spaces_for_provider() ->
    get_spaces_for_provider(oneprovider:get_provider_id()).


%%--------------------------------------------------------------------
%% @doc Returns list of spaces supported by both given provider and this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces_for_provider(oneprovider:id()) ->
    [SpaceId :: binary()].
get_spaces_for_provider(ProviderId) ->
    Key = {spaces_for, ProviderId},
    case temp_get(Key) of
        SpaceIds0 when is_list(SpaceIds0) ->
            SpaceIds0;
        _ ->
            {ok, SpaceIds} = oz_providers:get_spaces(provider),
            SpaceIds1 = lists:foldl(
                fun(SpaceId, Acc) ->
                    {ok, Providers} = oz_spaces:get_providers(provider, SpaceId),
                    case lists:member(ProviderId, Providers) of
                        true -> [SpaceId | Acc];
                        false -> Acc
                    end
                end, [], SpaceIds),
            temp_put(Key, SpaceIds1, timer:seconds(15)),
            SpaceIds1
    end.


%%--------------------------------------------------------------------
%% @doc Get list of URLs for the provider
%% @end
%%--------------------------------------------------------------------
-spec get_provider_urls(ProviderId :: oneprovider:id()) -> [URL :: binary()] | no_return().
get_provider_urls(ProviderId) ->
    Key = {urls_for, ProviderId},
    case temp_get(Key) of
       [_ | _] = URLs0 ->
           URLs0;
       _ ->
           {ok, #provider_details{urls = URLs}} = oz_providers:get_details(provider, ProviderId),
           temp_put(Key, URLs, timer:seconds(15)),
           URLs
   end.

%%--------------------------------------------------------------------
%% @doc Selects random URL of the provider
%% @end
%%--------------------------------------------------------------------
-spec get_provider_url(ProviderId :: oneprovider:id()) -> URL :: binary() | no_return().
get_provider_url(ProviderId) ->
    URLs = get_provider_urls(ProviderId),
    _URL = lists:nth(crypto:rand_uniform(1, length(URLs) + 1), URLs).


%%--------------------------------------------------------------------
%% @doc Encodes given erlang term to binary
%% @end
%%--------------------------------------------------------------------
-spec encode_term(term()) -> binary().
encode_term(Doc) ->
    Data = term_to_binary(Doc),
    Compressed = zlib:compress(Data),
    ?debug("[DBSync] Data compression ratio ~p", [size(Compressed) / size(Data)]),
    Compressed.


%%--------------------------------------------------------------------
%% @doc Decodes given binary to erlang term (reverses encode_term/1)
%% @end
%%--------------------------------------------------------------------
-spec decode_term(binary()) -> term().
decode_term(Data) ->
    binary_to_term(zlib:uncompress(Data)).


%%--------------------------------------------------------------------
%% @doc Generates UUID for inter-provider dbsync's requests
%% @end
%%--------------------------------------------------------------------
-spec gen_request_id() -> binary().
gen_request_id() ->
    http_utils:base64url_encode(crypto:rand_bytes(32)).


%%--------------------------------------------------------------------
%% @doc
%% Send given protocol record to given provider asynchronously.
%% @end
%%--------------------------------------------------------------------
-spec communicate(oneprovider:id(), Message :: #tree_broadcast{} | #changes_request{} | #status_request{}) ->
    {ok, MsgId :: term()} | {error, Reason :: term()}.
communicate(ProviderId, Message) ->
    SessId = session_manager:get_provider_session_id(outgoing, ProviderId),
    provider_communicator:communicate_async(#dbsync_request{message_body = Message}, SessId, self()).
