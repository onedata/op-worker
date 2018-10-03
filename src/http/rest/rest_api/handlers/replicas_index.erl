%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler allowing for managing file replicas
%%% @end
%%%--------------------------------------------------------------------
-module(replicas_index).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("http/rest/http_status.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([terminate/3, allowed_methods/2, is_authorized/2,
    content_types_accepted/2, delete_resource/2]).

%% resource functions
-export([replicate_files_from_index/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), maps:map()) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), maps:map() | {error, term()}) -> {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"POST">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | stop, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{atom() | binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {'*', replicate_files_from_index}
    ], Req, State}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/replicas/{path}'
%% @doc This method evicts file or dir replicas
%%
%% HTTP method: DELETE
%%
%% @param path File path (e.g. &#39;/My Private Space/testfiles/file1.txt&#39;)
%% @param provider_id The ID of the provider in which the replica should be evicted.
%%    By default the file will be replicated to the provider handling this REST call.\n
%% @param migration_provider_id The ID of the provider to which the replica should be
%%    synchronized before replica eviction.
%%    By default the file will be migrated to random provider.\n
%% @end
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, State) ->
    {State1, Req1} = validator:parse_index_name(Req, State),
    {State2, Req2} = validator:parse_query_space_id(Req1, State1),
    {State3, Req3} = validator:parse_provider_id(Req2, State2),
    {State4, Req4} = validator:parse_migration_provider_id(Req3, State3),

    % parse options
    {StateWithOptions, ReqWithOptions} = parse_options(Req4, State4),

    evict_file_replica_internal(ReqWithOptions, StateWithOptions).

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/replicas-index/{index_name}'
%% @doc Replicates a file to a specified provider. This operation is asynchronous
%% as it can  take a long time depending on the size of the data to move.
%% If the &#x60;path&#x60; parameter specifies a folder, entire folder is
%% replicated to  requested provider.
%%
%% HTTP method: POST
%%
%% @param path File path (e.g. &#39;/My Private Space/testfiles/file1.txt&#39;)
%% @param provider_id The ID of the provider to which the file should be replicated.
%%    By default the file will be replicated to the provided handling this REST call.\n
%% @param callback This parameter allows the user to specify a REST callback URL,
%%    which will be called when the transfer is complete\n
%% @end
%%--------------------------------------------------------------------
-spec replicate_files_from_index(req(), maps:map()) -> {term(), req(), maps:map()}.
replicate_files_from_index(Req, State) ->
    {State1, Req1} = validator:parse_index_name(Req, State),
    {State2, Req2} = validator:parse_query_space_id(Req1, State1),
    {State3, Req3} = validator:parse_provider_id(Req2, State2),
    {State4, Req4} = validator:parse_callback(Req3, State3),

    % parse options
    {StateWithOptions, ReqWithOptions} = parse_options(Req4, State4),

    replicate_files_from_index_internal(ReqWithOptions, StateWithOptions).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc parse supported query options
%% @end
%%--------------------------------------------------------------------
parse_options(Req, State) ->
    {StateWithBbox, ReqWithBbox} = validator:parse_bbox(Req, State),
    {StateWithDescending, ReqWithDescending} = validator:parse_descending(ReqWithBbox, StateWithBbox),
    {StateWithEndkey, ReqWithEndkey} = validator:parse_endkey(ReqWithDescending, StateWithDescending),
    {StateWithInclusiveEnd, ReqWithInclusiveEnd} = validator:parse_inclusive_end(ReqWithEndkey, StateWithEndkey),
    {StateWithKey, ReqWithKey} = validator:parse_key(ReqWithInclusiveEnd, StateWithInclusiveEnd),
    {StateWithLimit, ReqWithLimit} = validator:parse_limit(ReqWithKey, StateWithKey),
    {StateWithSkip, ReqWithSkip} = validator:parse_skip(ReqWithLimit, StateWithLimit),
    {StateWithStale, ReqWithStale} = validator:parse_stale(ReqWithSkip, StateWithSkip),
    {StateWithStartkey, ReqWithStartkey} = validator:parse_startkey(ReqWithStale, StateWithStale),
    {StateWithStartRange, ReqWithStartRange} = validator:parse_start_range(ReqWithStartkey, StateWithStartkey),
    {StateWithEndRange, ReqWithEndRange} = validator:parse_end_range(ReqWithStartRange, StateWithStartRange),
    validator:parse_spatial(ReqWithEndRange, StateWithEndRange).

%%--------------------------------------------------------------------
%% @private
%% @doc internal version of delete_resource/2
%% @end
%%--------------------------------------------------------------------
-spec evict_file_replica_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
evict_file_replica_internal(Req, State = #{
    auth := Auth,
    space_id := SpaceId,
    index_name := IndexName,
    provider_id := SourceProviderId,
    migration_provider_id := MigrationProviderId
}) ->
    throw_if_non_local_space(SpaceId),
    throw_if_nonexistent_provider(SpaceId, MigrationProviderId),
    throw_if_index_not_supported(SpaceId, IndexName, SourceProviderId),
    throw_if_index_not_supported(SpaceId, IndexName, MigrationProviderId),

    QueryViewParams = index_utils:sanitize_query_options(State),

    {ok, TransferId} = onedata_file_api:schedule_replica_eviction_by_index(
        Auth, SourceProviderId, MigrationProviderId, SpaceId, IndexName,
        QueryViewParams
    ),

    Response = json_utils:encode(#{<<"transferId">> => TransferId}),
    Req2 = cowboy_req:reply(?HTTP_OK, #{}, Response, Req),
    {stop, Req2, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc internal version of replicate_files_from_index/2
%% @end
%%--------------------------------------------------------------------
-spec replicate_files_from_index_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
replicate_files_from_index_internal(Req, #{
    auth := Auth,
    space_id := SpaceId,
    index_name := IndexName,
    provider_id := ProviderId,
    callback := Callback
} = State) ->
    throw_if_non_local_space(SpaceId),
    throw_if_nonexistent_provider(SpaceId, ProviderId),
    throw_if_index_not_supported(SpaceId, IndexName, ProviderId),

    QueryViewParams = index_utils:sanitize_query_options(State),

    {ok, TransferId} = onedata_file_api:schedule_replication_by_index(
        Auth, ProviderId, Callback, SpaceId, IndexName, QueryViewParams
    ),

    Response = json_utils:encode(#{<<"transferId">> => TransferId}),
    Req2 = cowboy_req:reply(?HTTP_OK, #{}, Response, Req),
    {stop, Req2, State}.

%%--------------------------------------------------------------------
%% @doc
%% Throws error if given space is not locally supported
%% @end
%%--------------------------------------------------------------------
-spec throw_if_non_local_space(od_space:id()) -> ok.
throw_if_non_local_space(SpaceId) ->
    case provider_logic:supports_space(SpaceId) of
        true ->
            ok;
        false ->
            throw(?ERROR_SPACE_NOT_SUPPORTED)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Throws error if given provider does not support given space
%% @end
%%--------------------------------------------------------------------
-spec throw_if_nonexistent_provider(od_space:id(), od_provider:id()) -> ok.
throw_if_nonexistent_provider(_SpaceId, undefined) ->
    ok;
throw_if_nonexistent_provider(SpaceId, ProviderId) ->
    case space_logic:is_supported(?ROOT_SESS_ID, SpaceId, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_PROVIDER_NOT_SUPPORTING_SPACE(ProviderId))
    end.

%%--------------------------------------------------------------------
%% @doc
%% Throws error if given provider does not support given index
%% @end
%%--------------------------------------------------------------------
-spec throw_if_index_not_supported(od_space:id(), binary(), od_provider:id()) ->
    ok.
throw_if_index_not_supported(_SpaceId, _IndexName, undefined) ->
    ok;
throw_if_index_not_supported(SpaceId, IndexName, ProviderId) ->
    case index:is_supported(SpaceId, IndexName, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_PROVIDER_NOT_SUPPORTING_INDEX(ProviderId))
    end.
