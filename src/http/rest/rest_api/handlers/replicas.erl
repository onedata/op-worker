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
-module(replicas).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("http/rest/http_status.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_accepted/2, content_types_provided/2, delete_resource/2]).

%% resource functions
-export([replicate_file/2, get_file_replicas/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, State) ->
    {ok, Req, State}.

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
    {[<<"GET">>, <<"POST">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | halt, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{atom() | binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {'*', replicate_file}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_file_replicas}
    ], Req, State}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/replicas/{path}'
%% @doc This method invalidates file or dir replicas
%%
%% HTTP method: DELETE
%%
%% @param path File path (e.g. &#39;/My Private Space/testfiles/file1.txt&#39;)
%% @param provider_id The ID of the provider in which the replica should be invalidated.
%%    By default the file will be replicated to the provider handling this REST call.\n
%% @param migration_provider_id The ID of the provider to which the replica should be
%%    synchronized before invalidation. By default the file will be migrated to random provider.\n
%% @end
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, State = #{resource_type := id}) ->
    {State2, Req2} = validator:parse_objectid(Req, State),
    {State3, Req3} = validator:parse_provider_id(Req2, State2),
    {State4, Req4} = validator:parse_migration_provider_id(Req3, State3),

    invalidate_file_replica_internal(Req4, State4);
delete_resource(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),
    {State3, Req3} = validator:parse_provider_id(Req2, State2),
    {State4, Req4} = validator:parse_migration_provider_id(Req3, State3),

    invalidate_file_replica_internal(Req4, State4).

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/replicas/{path}'
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
-spec replicate_file(req(), maps:map()) -> {term(), req(), maps:map()}.
replicate_file(Req, State = #{resource_type := id}) ->
    {State2, Req2} = validator:parse_objectid(Req, State),
    {State3, Req3} = validator:parse_provider_id(Req2, State2),
    {State4, Req4} = validator:parse_callback(Req3, State3),

    replicate_file_internal(Req4, State4);
replicate_file(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),
    {State3, Req3} = validator:parse_provider_id(Req2, State2),
    {State4, Req4} = validator:parse_callback(Req3, State3),

    replicate_file_internal(Req4, State4).

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/replicas/{path}'
%% @doc Returns file distribution information about a specific file replicated at this provider.\n
%%
%% HTTP method: GET
%%
%% @param path File path (e.g. &#39;/My Private Space/testfiles/file1.txt&#39;)
%% @end
%%--------------------------------------------------------------------
-spec get_file_replicas(req(), maps:map()) -> {term(), req(), maps:map()}.
get_file_replicas(Req, State = #{resource_type := id}) ->
    {State2, Req2} = validator:parse_objectid(Req, State),

    get_file_replicas_internal(Req2, State2);
get_file_replicas(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),

    get_file_replicas_internal(Req2, State2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc internal version of delete_resource/2
%% @end
%%--------------------------------------------------------------------
-spec invalidate_file_replica_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
invalidate_file_replica_internal(Req, State = #{
    auth := Auth,
    provider_id := ProviderId,
    migration_provider_id := MigrationProviderId
}) ->
    FileGuid = get_file_guid(State),
    FilePath = get_file_path(State),
    SpaceId = fslogic_uuid:guid_to_space_id(FileGuid),

    throw_if_non_local_space(SpaceId),
    throw_if_non_local_provider(ProviderId),
    throw_if_nonexistent_provider(SpaceId, MigrationProviderId),
    {ok, _} = onedata_file_api:stat(Auth, {guid, FileGuid}),
    {ok, TransferId} = transfer:start(Auth, FileGuid, FilePath, MigrationProviderId, undefined, true),

    Response = json_utils:encode_map(#{<<"transferId">> => TransferId}),
    {ok, Req2} = cowboy_req:reply(?HTTP_OK, [], Response, Req),
    {halt, Req2, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc internal version of replicate_file/2
%% @end
%%--------------------------------------------------------------------
-spec replicate_file_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
replicate_file_internal(Req, #{auth := Auth, provider_id := ProviderId, callback := Callback} = State) ->
    FileGuid = get_file_guid(State),
    FilePath = get_file_path(State),
    SpaceId = fslogic_uuid:guid_to_space_id(FileGuid),

    throw_if_non_local_space(SpaceId),
    throw_if_nonexistent_provider(SpaceId, ProviderId),
    {ok, _} = onedata_file_api:stat(Auth, {guid, FileGuid}),
    {ok, TransferId} = transfer:start(Auth, FileGuid, FilePath, ProviderId, Callback, false),

    Response = json_utils:encode_map(#{<<"transferId">> => TransferId}),
    {ok, Req2} = cowboy_req:reply(?HTTP_OK, [], Response, Req),
    {halt, Req2, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc internal version of get_file_replicas/2
%% @end
%%--------------------------------------------------------------------
-spec get_file_replicas_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
get_file_replicas_internal(Req, #{auth := Auth} = State) ->
    FileGuid = get_file_guid(State),

    {ok, Distribution} = onedata_file_api:get_file_distribution(Auth, {guid, FileGuid}),
    Response = json_utils:encode_map(Distribution),
    {Response, Req, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get file path from state
%% @end
%%--------------------------------------------------------------------
-spec get_file_path(maps:map()) -> file_meta:path().
get_file_path(#{path := Path}) ->
    Path;
get_file_path(#{auth := Auth, id := Id}) ->
    {ok, Path} = logical_file_manager:get_file_path(Auth, Id),
    Path.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get file guid from state
%% @end
%%--------------------------------------------------------------------
-spec get_file_guid(maps:map()) -> fslogic_worker:file_guid().
get_file_guid(#{id := Guid}) ->
    Guid;
get_file_guid(#{auth := Auth, path := Path}) ->
    {ok, Guid} = logical_file_manager:get_file_guid(Auth, Path),
    Guid.

%%--------------------------------------------------------------------
%% @doc
%% Throws error if given space is not locally supported
%% @end
%%--------------------------------------------------------------------
-spec throw_if_non_local_space(od_space:id()) -> ok.
throw_if_non_local_space(SpaceId) ->
    {ok, Provider} = od_provider:get(oneprovider:get_provider_id()),
    case lists:member(SpaceId, Provider#document.value#od_provider.spaces) of
        true ->
            ok;
        false ->
            throw(?ERROR_SPACE_NOT_SUPPORTED)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Throws error if given provide_id is not local
%% @end
%%--------------------------------------------------------------------
-spec throw_if_non_local_provider(od_provider:id()) -> ok.
throw_if_non_local_provider(ProviderId) ->
    case ProviderId =:= oneprovider:get_provider_id() of
        true ->
            ok;
        false ->
            throw(?ERROR_NON_LOCAL_PROVIDER)
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
    case od_provider:get(ProviderId) of
        {ok, _} ->
            {ok, #document{value = #od_space{providers = Providers}}} = od_space:get(SpaceId),
            case lists:member(ProviderId, Providers) of
                true ->
                    ok;
                false ->
                    throw(?ERROR_PROVIDER_NOT_FOUND)
            end;
        _Error ->
            throw(?ERROR_PROVIDER_NOT_SUPPORTING_SPACE)
    end.
