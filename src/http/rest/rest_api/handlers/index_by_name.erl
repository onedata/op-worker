%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for creating, getting, modifying and deleting indexes.
%%% @end
%%%--------------------------------------------------------------------
-module(index_by_name).
-author("Jakub Kudzia").

-include("http/http_common.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2, content_types_accepted/2, delete_resource/2]).

%% resource functions
-export([get_index/2, create_or_modify_index/2]).

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
-spec allowed_methods(req(), maps:map() | {error, term()}) ->
    {[binary()], req(), maps:map()}.
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"PUT">>, <<"PATCH">>, <<"DELETE">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) ->
    {true | {false, binary()} | stop, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_index}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/javascript">>, create_or_modify_index}
    ], Req, State}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/spaces/{sid}/indexes/{name}'
%% @doc This method removes index
%%
%% HTTP method: DELETE
%%
%% @param sid Id of the space within which index exist.
%% @param name Name of the index.
%%--------------------------------------------------------------------
-spec delete_resource(req(), maps:map()) -> {term(), req(), maps:map()}.
delete_resource(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),
    {State3, Req3} = validator:parse_index_name(Req2, State2),

    #{space_id := SpaceId, index_name := IndexName} = State3,

    ok = index:delete(SpaceId, IndexName),
    {true, Req3, State3}.

%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/spaces/{sid}/indexes/{name}'
%% @doc This method returns a JSON containing index options and functions.
%%
%% The indexes are defined as JavaScript functions which are executed
%% on the database backend.
%%
%% ***Example cURL requests***
%%
%% **Get list of indexes for space**
%% &#x60;&#x60;&#x60;bash
%% curl --tlsv1.2 -H \&quot;X-Auth-Token: $TOKEN\&quot; -X GET \\
%% https://$HOST:443/api/v1/oneprovider/spaces/$SPACE_ID/indexes/$INDEX_NAME
%%
%% {
%%   "name": $INDEX_NAME,
%%   "spatial": false,
%%   "mapFunction": "function(x){...}",
%%   "indexOptions": {}
%% }
%% &#x60;&#x60;&#x60;
%%
%% HTTP method: GET
%%
%% @param sid Id of the space within which index exist.
%% @param name Name of the index.
%%--------------------------------------------------------------------
-spec get_index(req(), maps:map()) -> {term(), req(), maps:map()}.
get_index(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),
    {State3, Req3} = validator:parse_index_name(Req2, State2),

    #{space_id := SpaceId, index_name := IndexName} = State3,

    case index:get_json(SpaceId, IndexName) of
        {ok, JSON} ->
            {json_utils:encode(JSON), Req3, State3};
        {error, not_found} ->
            throw(?ERROR_INDEX_NOT_FOUND)
    end.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/spaces/{sid}/indexes/{name}'
%% @doc This method creates or replaces an existing index definition with
%% request body content and options specified in query string.
%%
%% HTTP method: PUT/PATCH
%%
%% @param sid Id of the space within which index exist.
%% @param name Name of the index.
%%--------------------------------------------------------------------
-spec create_or_modify_index(req(), maps:map()) -> {term(), req(), maps:map()}.
create_or_modify_index(#{method := <<"PUT">>} = Req, State) ->
    create_index(Req, State);
create_or_modify_index(#{method := <<"PATCH">>} = Req, State) ->
    update_index(Req, State).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates index.
%% @end
%%--------------------------------------------------------------------
-spec create_index(req(), maps:map()) -> {term(), req(), maps:map()}.
create_index(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),
    {State3, Req3} = validator:parse_index_name(Req2, State2),
    {State4, Req4} = validator:parse_spatial(Req3, State3),
    {State5, Req5} = validator:parse_function(Req4, State4),
    {State6, Req6} = validator:parse_update_min_changes(Req5, State5),
    {State7, Req7} = validator:parse_replica_update_min_changes(Req6, State6),
    {State8, Req8} = validator:parse_index_providers(Req7, State7),

    #{
        space_id := SpaceId,
        index_name := IndexName,
        spatial := Spatial,
        function := MapFunction,
        providers := Providers
    } = State8,
    Options = prepare_options(State8),

    lists:foreach(fun(ProviderId) ->
        throw_if_provider_does_not_support_space(SpaceId, ProviderId)
    end, Providers),

    case index:save(
        SpaceId, IndexName, MapFunction, undefined,
        Options, Spatial, Providers
    ) of
        ok ->
            {stop, cowboy_req:reply(?HTTP_OK, Req8), State8};
        {error, already_exists} ->
            throw(?ERROR_INDEX_ALREADY_EXISTS)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates index definition. This operation does not require specifying all
%% index create parameters. In case when some parameters are not specified,
%% old values will be kept.
%% @end
%%--------------------------------------------------------------------
-spec update_index(req(), maps:map()) -> {term(), req(), maps:map()}.
update_index(Req, State) ->
    {State2, Req2} = validator:parse_space_id(Req, State),
    {State3, Req3} = validator:parse_index_name(Req2, State2),
    {State4, Req4} = validator:parse_spatial(Req3, State3, undefined),
    {State5, Req5} = validator:parse_function(Req4, State4),
    {State6, Req6} = validator:parse_update_min_changes(Req5, State5),
    {State7, Req7} = validator:parse_replica_update_min_changes(Req6, State6),
    {State8, Req8} = validator:parse_index_providers(Req7, State7, undefined),

    #{
        space_id := SpaceId,
        index_name := IndexName,
        spatial := Spatial,
        function := MapFunctionRaw,
        providers := ProvidersRaw
    } = State8,
    Options = prepare_options(State8),
    MapFunction = utils:ensure_defined(MapFunctionRaw, <<>>, undefined),
    Providers = case ProvidersRaw of
        undefined ->
            undefined;
        _ ->
            lists:foreach(fun(ProviderId) ->
                throw_if_provider_does_not_support_space(SpaceId, ProviderId)
            end, ProvidersRaw),
            ProvidersRaw
    end,

    case index:update(SpaceId, IndexName, MapFunction, Options, Spatial, Providers) of
        ok ->
            {stop, cowboy_req:reply(?HTTP_OK, Req8), State8};
        {error, not_found} ->
            throw(?ERROR_INDEX_NOT_FOUND)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Parses index options.
%% @end
%%--------------------------------------------------------------------
-spec prepare_options(maps:map() | list()) -> list().
prepare_options(Map) when is_map(Map) ->
    #{
        update_min_changes := UpdateMinChanges,
        replica_update_min_changes := ReplicaUpdateMinChanges
    } = Map,

    RawList = [
        {update_min_changes, UpdateMinChanges},
        {replica_update_min_changes, ReplicaUpdateMinChanges}
    ],
    prepare_options(RawList);
prepare_options([]) ->
    [];
prepare_options([{update_min_changes, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{update_min_changes, UpdateMinChanges} | Rest]) ->
    case catch binary_to_integer(UpdateMinChanges) of
        N when is_integer(N), N > 0 ->
            [{update_min_changes, N} | prepare_options(Rest)];
        _Error ->
            throw(?ERROR_INVALID_UPDATE_MIN_CHANGES)
    end;

prepare_options([{replica_update_min_changes, undefined} | Rest]) ->
    prepare_options(Rest);
prepare_options([{replica_update_min_changes, ReplicaUpdateMinChanges} | Rest]) ->
    case catch binary_to_integer(ReplicaUpdateMinChanges) of
        N when is_integer(N), N > 0 ->
            [{replica_update_min_changes, N} | prepare_options(Rest)];
        _Error ->
            throw(?ERROR_INVALID_REPLICA_UPDATE_MIN_CHANGES)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Throws error if given provider does not support given space
%% @end
%%--------------------------------------------------------------------
-spec throw_if_provider_does_not_support_space(od_space:id(), od_provider:id()) ->
    ok.
throw_if_provider_does_not_support_space(_SpaceId, undefined) ->
    ok;
throw_if_provider_does_not_support_space(SpaceId, ProviderId) ->
    case space_logic:is_supported(?ROOT_SESS_ID, SpaceId, ProviderId) of
        true ->
            ok;
        false ->
            throw(?ERROR_PROVIDER_NOT_SUPPORTING_SPACE(ProviderId))
    end.
