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
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("http/rest/http_status.hrl").
-include("http/rest/rest_api/rest_errors.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2, content_types_accepted/2]).

%% resource functions
-export([get_json/2, set_json/2, get_rdf/2, set_rdf/2]).

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
-spec terminate(Reason :: term(), req(), #{}) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), #{} | {error, term()}) -> {[binary()], req(), #{}}.
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"PUT">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {true | {false, binary()} | halt, req(), #{}}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), #{}) -> {[{binary(), atom()}], req(), #{}}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_json},
        {<<"application/rdf+xml">>, get_rdf}

    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), #{}) ->
    {[{binary(), atom()}], req(), #{}}.
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
-spec get_json(req(), #{}) -> {term(), req(), #{}}.
get_json(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_id(Req, State),
    get_json_internal(ReqWithId, StateWithId);
get_json(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    get_json_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of get_json/2
%%--------------------------------------------------------------------
-spec get_json_internal(req(), #{}) -> {term(), req(), #{}}.
get_json_internal(Req, State) ->
    {StateWithMetadataType, ReqWithMetadataType} = validator:parse_metadata_type(Req, State),
    {StateWithFilterType, ReqWithFilterType} = validator:parse_filter_type(ReqWithMetadataType, StateWithMetadataType),
    {StateWithFilter, ReqWithFilter} = validator:parse_filter(ReqWithFilterType, StateWithFilterType),
    {StateWithInherited, ReqWithInherited} = validator:parse_inherited(ReqWithFilter, StateWithFilter),

    #{auth := Auth, metadata_type := MetadataType, filter_type := FilterType,
        filter := Filter, inherited := Inherited} = StateWithInherited,
    DefinedMetadataType = validate_metadata_type(MetadataType, <<"json">>),
    FilterList = get_filter_list(FilterType, Filter),

    case onedata_file_api:get_metadata(Auth, get_file(StateWithInherited), DefinedMetadataType, FilterList, Inherited) of
        {ok, Meta} ->
            Response = jiffy:encode(Meta),
            {Response, ReqWithInherited, StateWithInherited};
        {error, ?ENOATTR} ->
            Response = jiffy:encode(#{}),
            {Response, ReqWithInherited, StateWithInherited}
    end.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/metadata/{path}'
%% @doc Gets file's metadata
%%--------------------------------------------------------------------
-spec get_rdf(req(), #{}) -> {term(), req(), #{}}.
get_rdf(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_id(Req, State),
    get_rdf_internal(ReqWithId, StateWithId);
get_rdf(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    get_rdf_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of get_rdf/2
%%--------------------------------------------------------------------
-spec get_rdf_internal(req(), #{}) -> {term(), req(), #{}}.
get_rdf_internal(Req, State) ->
    {StateWithMetadataType, ReqWithMetadataType} = validator:parse_metadata_type(Req, State),

    #{auth := Auth, metadata_type := MetadataType} = StateWithMetadataType,
    DefinedMetadataType = validate_metadata_type(MetadataType, <<"rdf">>),

    {ok, Meta} = onedata_file_api:get_metadata(Auth, get_file(StateWithMetadataType),
        DefinedMetadataType, [], false),
    {Meta, ReqWithMetadataType, StateWithMetadataType}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/metadata/{path}'
%% @doc Sets file's metadata
%%--------------------------------------------------------------------
-spec set_json(req(), #{}) -> {term(), req(), #{}}.
set_json(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_id(Req, State),
    set_json_internal(ReqWithId, StateWithId);
set_json(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    set_json_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of set_json/2
%%--------------------------------------------------------------------
-spec set_json_internal(req(), #{}) -> {term(), req(), #{}}.
set_json_internal(Req, State) ->
    {StateWithMetadataType, ReqWithMetadataType} = validator:parse_metadata_type(Req, State),
    {ok, Body, FinalReq} = cowboy_req:body(ReqWithMetadataType),

    Json = jiffy:decode(Body, [return_maps]),
    #{auth := Auth, metadata_type := MetadataType} = StateWithMetadataType,
    DefinedMetadataType = validate_metadata_type(MetadataType, <<"json">>),

    ok = onedata_file_api:set_metadata(Auth, get_file(StateWithMetadataType), DefinedMetadataType, Json, []),

    {true, FinalReq, StateWithMetadataType}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/metadata/{path}'
%% @doc Sets file's metadata
%%--------------------------------------------------------------------
-spec set_rdf(req(), #{}) -> {term(), req(), #{}}.
set_rdf(Req, State = #{resource_type := id}) ->
    {StateWithId, ReqWithId} = validator:parse_id(Req, State),
    set_rdf_internal(ReqWithId, StateWithId);
set_rdf(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    set_rdf_internal(ReqWithPath, StateWithPath).

%%--------------------------------------------------------------------
%% @doc Internal version of set_rdf/2
%%--------------------------------------------------------------------
-spec set_rdf_internal(req(), #{}) -> {term(), req(), #{}}.
set_rdf_internal(Req, State) ->
    {StateWithMetadataType, ReqWithMetadataType} = validator:parse_metadata_type(Req, State),
    {ok, Rdf, FinalReq} = cowboy_req:body(ReqWithMetadataType),

    #{auth := Auth, metadata_type := MetadataType} = StateWithMetadataType,
    DefinedMetadataType = validate_metadata_type(MetadataType, <<"rdf">>),

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
-spec validate_metadata_type(binary(), binary()) -> term().
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
get_filter_list(<<"keypath">>, Filter) ->
    binary:split(Filter, <<".">>, [global]);
get_filter_list(undefined, _) ->
    [];
get_filter_list(_, _) ->
    throw(?ERROR_INVALID_FILTER_TYPE).

%%--------------------------------------------------------------------
%% @doc
%% Get file entry from state
%% @end
%%--------------------------------------------------------------------
-spec get_file(#{}) -> {guid, binary()} | {path, binary()}.
get_file(#{id := Id}) ->
    {guid, Id};
get_file(#{path := Path}) ->
    {path, Path}.