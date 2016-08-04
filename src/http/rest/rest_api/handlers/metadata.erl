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
rest_init(Req, _Opts) ->
    {ok, Req, #{}}.

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
get_json(Req, State) ->
    {StateWithPath, ReqWithPath} = validator:parse_path(Req, State),
    {StateWithMetadataType, ReqWithMetadataType} = validator:parse_metadata_type(ReqWithPath, StateWithPath),
    {StateWithFilterType, ReqWithFilterType} = validator:parse_filter_type(ReqWithMetadataType, StateWithMetadataType),
    {StateWithFilter, ReqWithFilter} = validator:parse_filter(ReqWithFilterType, StateWithFilterType),

    #{auth := Auth, path := Path, metadata_type := MetadataType, filter_type := FilterType, filter := Filter} = StateWithFilter,
    DefinedMetadataType = validate_metadata_type(MetadataType, <<"json">>),
    FilterList = get_filter_list(FilterType, Filter),

    case onedata_file_api:get_metadata(Auth, {path, Path}, DefinedMetadataType, FilterList) of
        {ok, Meta} ->
            Response = jiffy:encode(Meta),
            {Response, ReqWithFilter, StateWithFilter};
        {error, ?ENOATTR} ->
            Response = jiffy:encode(#{}),
            {Response, ReqWithFilter, StateWithFilter}
    end.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/metadata/{path}'
%% @doc Gets file's metadata
%%--------------------------------------------------------------------
-spec get_rdf(req(), #{}) -> {term(), req(), #{}}.
get_rdf(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),
    {State3, Req3} = validator:parse_metadata_type(Req2, State2),

    #{auth := Auth, path := Path, metadata_type := MetadataType} = State3,
    DefinedMetadataType = validate_metadata_type(MetadataType, <<"rdf">>),

    {ok, Meta} = onedata_file_api:get_metadata(Auth, {path, Path}, DefinedMetadataType, []),
    {Meta, Req3, State3}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/metadata/{path}'
%% @doc Sets file's metadata
%%--------------------------------------------------------------------
-spec set_json(req(), #{}) -> {term(), req(), #{}}.
set_json(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),
    {State3, Req3} = validator:parse_metadata_type(Req2, State2),
    {ok, Body, Req4} = cowboy_req:body(Req3),

    Json = jiffy:decode(Body, [return_maps]),
    #{auth := Auth, path := Path, metadata_type := MetadataType} = State3,
    DefinedMetadataType = validate_metadata_type(MetadataType, <<"json">>),

    ok = onedata_file_api:set_metadata(Auth, {path, Path}, DefinedMetadataType, Json, []),

    {true, Req4, State3}.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/metadata/{path}'
%% @doc Sets file's metadata
%%--------------------------------------------------------------------
-spec set_rdf(req(), #{}) -> {term(), req(), #{}}.
set_rdf(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),
    {State3, Req3} = validator:parse_metadata_type(Req2, State2),
    {ok, Rdf, Req4} = cowboy_req:body(Req3),

    #{auth := Auth, path := Path, metadata_type := MetadataType} = State3,
    DefinedMetadataType = validate_metadata_type(MetadataType, <<"rdf">>),

    ok = onedata_file_api:set_metadata(Auth, {path, Path}, DefinedMetadataType, Rdf, []),

    {true, Req4, State3}.

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

