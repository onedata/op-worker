%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for file metadata.
%%% @end
%%%--------------------------------------------------------------------
-module(metadata).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

%% API
-export([terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2, content_types_accepted/2]).

%% resource functions
-export([get_json/2, set_json/2, get_rdf/2, set_rdf/2]).

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
    {[<<"GET">>, <<"PUT">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), maps:map()) -> {true | {false, binary()} | stop, req(), maps:map()}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), maps:map()) -> {[{binary(), atom()}], req(), maps:map()}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_json},
        {<<"application/rdf+xml">>, get_rdf}

    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), maps:map()) ->
    {[{binary(), atom()}], req(), maps:map()}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, set_json},
        {<<"application/rdf+xml">>, set_rdf}
    ], Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/metadata/{path}'
%% @doc Gets file's metadata
%%--------------------------------------------------------------------
-spec get_json(req(), maps:map()) -> {term(), req(), maps:map()}.
get_json(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_objectid(Req, State),
    get_json_internal(ReqWithId, StateWithId);
get_json(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    get_json_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of get_json/2
%%--------------------------------------------------------------------
-spec get_json_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
get_json_internal(Req, State) ->
    {StateWithMetadataType, ReqWithMetadataType} = validator:parse_metadata_type(Req, State),
    {StateWithFilterType, ReqWithFilterType} = validator:parse_filter_type(ReqWithMetadataType, StateWithMetadataType),
    {StateWithFilter, ReqWithFilter} = validator:parse_filter(ReqWithFilterType, StateWithFilterType),
    {StateWithInherited, ReqWithInherited} = validator:parse_inherited(ReqWithFilter, StateWithFilter),

    #{auth := Auth, metadata_type := MetadataType, filter_type := FilterType,
        filter := Filter, inherited := Inherited} = StateWithInherited,
    DefinedMetadataType = validate_metadata_type(MetadataType, json),
    FilterList = get_filter_list(FilterType, Filter),

    {ok, Meta} = onedata_file_api:get_metadata(Auth, get_file(StateWithInherited),
        DefinedMetadataType, FilterList, Inherited),
    Response = json_utils:encode(Meta),
    {Response, ReqWithInherited, StateWithInherited}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/metadata/{path}'
%% @doc Gets file's metadata
%%--------------------------------------------------------------------
-spec get_rdf(req(), maps:map()) -> {term(), req(), maps:map()}.
get_rdf(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_objectid(Req, State),
    get_rdf_internal(ReqWithId, StateWithId);
get_rdf(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    get_rdf_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of get_rdf/2
%%--------------------------------------------------------------------
-spec get_rdf_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
get_rdf_internal(Req, State) ->
    {StateWithMetadataType, ReqWithMetadataType} = validator:parse_metadata_type(Req, State),

    #{auth := Auth, metadata_type := MetadataType} = StateWithMetadataType,
    DefinedMetadataType = validate_metadata_type(MetadataType, rdf),

    {ok, Meta} = onedata_file_api:get_metadata(Auth, get_file(StateWithMetadataType),
        DefinedMetadataType, [], false),
    {Meta, ReqWithMetadataType, StateWithMetadataType}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/metadata/{path}'
%% @doc Sets file's metadata
%%--------------------------------------------------------------------
-spec set_json(req(), maps:map()) -> {term(), req(), maps:map()}.
set_json(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_objectid(Req, State),
    set_json_internal(ReqWithId, StateWithId);
set_json(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    set_json_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of set_json/2
%%--------------------------------------------------------------------
-spec set_json_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
set_json_internal(Req, State) ->
    {StateWithMetadataType, ReqWithMetadataType} = validator:parse_metadata_type(Req, State),
    {StateWithFilterType, ReqWithFilterType} = validator:parse_filter_type(ReqWithMetadataType, StateWithMetadataType),
    {StateWithFilter, ReqWithFilter} = validator:parse_filter(ReqWithFilterType, StateWithFilterType),
    {ok, Body, FinalReq} = cowboy_req:read_body(ReqWithFilter),

    Json = json_utils:decode(Body),
    #{auth := Auth, metadata_type := MetadataType, filter_type := FilterType,
        filter := Filter} = StateWithFilter,
    DefinedMetadataType = validate_metadata_type(MetadataType, json),
    FilterList = get_filter_list(FilterType, Filter),

    ok = onedata_file_api:set_metadata(Auth, get_file(StateWithFilter), DefinedMetadataType, Json, FilterList),

    {true, FinalReq, StateWithFilter}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/metadata/{path}'
%% @doc Sets file's metadata
%%--------------------------------------------------------------------
-spec set_rdf(req(), maps:map()) -> {term(), req(), maps:map()}.
set_rdf(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_objectid(Req, State),
    set_rdf_internal(ReqWithId, StateWithId);
set_rdf(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    set_rdf_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of set_rdf/2
%%--------------------------------------------------------------------
-spec set_rdf_internal(req(), maps:map()) -> {term(), req(), maps:map()}.
set_rdf_internal(Req, State) ->
    {StateWithMetadataType, ReqWithMetadataType} = validator:parse_metadata_type(Req, State),
    {ok, Rdf, FinalReq} = cowboy_req:read_body(ReqWithMetadataType),

    #{auth := Auth, metadata_type := MetadataType} = StateWithMetadataType,
    DefinedMetadataType = validate_metadata_type(MetadataType, rdf),

    ok = onedata_file_api:set_metadata(Auth, get_file(StateWithMetadataType),
        DefinedMetadataType, Rdf, []),

    {true, FinalReq, StateWithMetadataType}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Validate metadata type according to provided default
%% @end
%%--------------------------------------------------------------------
-spec validate_metadata_type(onedata_file_api:metadata_type(), onedata_file_api:metadata_type()) -> onedata_file_api:metadata_type().
validate_metadata_type(undefined, Default) ->
    Default;
validate_metadata_type(MetadataType, MetadataType) ->
    MetadataType;
validate_metadata_type(_, _) ->
    throw(?ERROR_INVALID_METADATA_TYPE).

%%--------------------------------------------------------------------
%% @doc
%% Get list of metadata names from filter description
%% @end
%%--------------------------------------------------------------------
-spec get_filter_list(binary(), binary()) -> list().
get_filter_list(undefined, _) ->
    [];
get_filter_list(<<"keypath">>, undefined)->
    throw(?ERROR_MISSING_FILTER);
get_filter_list(<<"keypath">>, Filter) when not is_binary(Filter) ->
    throw(?ERROR_INVALID_FILTER);
get_filter_list(<<"keypath">>, Filter) ->
    binary:split(Filter, <<".">>, [global]);
get_filter_list(_, _) ->
    throw(?ERROR_INVALID_FILTER_TYPE).

%%--------------------------------------------------------------------
%% @doc
%% Get file entry from state
%% @end
%%--------------------------------------------------------------------
-spec get_file(maps:map()) -> {guid, binary()} | {path, binary()}.
get_file(#{id := Id}) ->
    {guid, Id};
get_file(#{path := Path}) ->
    {path, Path}.
