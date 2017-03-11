%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc API for files' rdf metadata.
%%% @end
%%%-------------------------------------------------------------------
-module(rdf_metadata).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/fslogic/metadata.hrl").

%% API
-export([get/2, set/2, remove/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Gets file's rdf metadata
%% @equiv get_xattr_metadata(FileUuid, ?RDF_METADATA_KEY).
%%--------------------------------------------------------------------
-spec get(file_ctx:ctx(), boolean()) -> {ok, custom_metadata:rdf()} | {error, term()}.
get(FileCtx, Inherited) ->
    custom_metadata:get_xattr_metadata(file_ctx:get_uuid_const(FileCtx), ?RDF_METADATA_KEY, Inherited).

%%--------------------------------------------------------------------
%% @doc Gets file's rdf metadata
%% @equiv get_xattr_metadata(FileCtx, ?RDF_METADATA_KEY).
%%--------------------------------------------------------------------
-spec set(file_ctx:ctx(), custom_metadata:rdf()) -> {ok, file_meta:uuid()} | {error, term()}.
set(FileCtx, Value) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    custom_metadata:set_xattr_metadata(FileUuid, SpaceId, ?RDF_METADATA_KEY, Value).

%%--------------------------------------------------------------------
%% @doc Removes file's rdf metadata
%% @equiv remove_xattr_metadata(FileCtx, ?RDF_METADATA_KEY).
%%--------------------------------------------------------------------
-spec remove(file_ctx:ctx()) -> ok | {error, term()}.
remove(FileCtx) ->
    custom_metadata:remove_xattr_metadata(file_ctx:get_uuid_const(FileCtx), ?RDF_METADATA_KEY).