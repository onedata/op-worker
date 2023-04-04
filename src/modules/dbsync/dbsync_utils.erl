%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains DBSync utility functions.
%%% @end
%%%-------------------------------------------------------------------
-module(dbsync_utils).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([get_bucket/0]).
-export([get_spaces/0, get_provider/1, get_providers/1, is_supported/2]).
-export([gen_request_id/0]).
-export([encode/2, decode/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a bucket for which synchronization should be enabled.
%% @end
%%--------------------------------------------------------------------
-spec get_bucket() -> couchbase_config:bucket().
get_bucket() ->
    <<"onedata">>.

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of spaces supported by this provider.
%% @end
%%--------------------------------------------------------------------
-spec get_spaces() -> [od_space:id()].
get_spaces() ->
    try
        case oneprovider:get_id_or_undefined() of
            undefined ->
                [];
            ProviderId ->
                case provider_logic:get_spaces(ProviderId) of
                    {ok, Spaces} -> Spaces;
                    {error, _Reason} -> []
                end
        end
    catch
        _:Reason:Stacktrace ->
            ?error_stacktrace(
                "Cannot resolve spaces of provider due to ~p", [Reason], Stacktrace
            ),
            []
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns a provider associated with given session.
%% @end
%%--------------------------------------------------------------------
-spec get_provider(session:id()) -> od_provider:id().
get_provider(SessId) ->
    {ok, #document{value = #session{
        identity = ?SUB(?ONEPROVIDER, ProviderId)
    }}} = session:get(SessId),
    ProviderId.

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of providers supporting given space.
%% @end
%%--------------------------------------------------------------------
-spec get_providers(od_space:id()) -> [od_provider:id()].
get_providers(SpaceId) ->
    case oneprovider:get_id_or_undefined() of
        undefined ->
            [];
        _ ->
            case space_logic:get_provider_ids(?ROOT_SESS_ID, SpaceId) of
                {ok, ProvIds} ->
                    ProvIds;
                _ ->
                    []
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check whether a space is supported by all given providers.
%% @end
%%--------------------------------------------------------------------
-spec is_supported(od_space:id(), [od_provider:id()]) -> boolean().
is_supported(SpaceId, ProviderIds) ->
    ValidProviderIds = gb_sets:from_list(dbsync_utils:get_providers(SpaceId)),
    lists:all(fun(ProviderId) ->
        gb_sets:is_element(ProviderId, ValidProviderIds)
    end, ProviderIds).

%%--------------------------------------------------------------------
%% @doc
%% Generates random request ID.
%% @end
%%--------------------------------------------------------------------
-spec gen_request_id() -> binary().
gen_request_id() ->
    base64:encode(crypto:strong_rand_bytes(16)).

%%--------------------------------------------------------------------
%% @doc
%% Returns a encoded datastore documents.
%% @end
%%--------------------------------------------------------------------
-spec encode([datastore:doc()], boolean()) -> binary().
encode(Docs, Compress) ->
    EncodedDocs = jiffy:encode([datastore_json:encode(Doc) || Doc <- Docs]),

    case Compress of
        true -> zlib:compress(EncodedDocs);
        false -> EncodedDocs
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns a list of decoded datastore documents.
%% @end
%%--------------------------------------------------------------------
-spec decode(binary(), boolean()) -> [datastore:doc()].
decode(EncodedDocs0, Compressed) ->
    EncodedDocs1 = case Compressed of
        true -> zlib:uncompress(EncodedDocs0);
        false -> EncodedDocs0
    end,
    Docs = jiffy:decode(EncodedDocs1, [copy_strings]),
    [datastore_json:decode(Doc) || Doc <- Docs].
