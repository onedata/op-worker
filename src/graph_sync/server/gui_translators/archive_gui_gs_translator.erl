%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% archive entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_gui_gs_translator).
-author("Jakub Kudzia").

-include("middleware/middleware.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([translate_value/2, translate_resource/2]).
% Util functions
-export([translate_archive_info/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
translate_value(#gri{aspect = identify_file}, FileInfo) ->
    #{
        <<"sourceFile">> := SourceFileGuid,
        <<"archivedFile">> := ArchivedFileGuid
    } = FileInfo,
    #{
        <<"sourceFileId">> => utils:undefined_to_null(SourceFileGuid),
        <<"archivedFileId">> => utils:undefined_to_null(ArchivedFileGuid)
    };
translate_value(#gri{aspect = audit_log}, ListedEntries) ->
    ListedEntries;
translate_value(#gri{aspect = recall}, RootId) ->
    #{<<"rootFileId">> => RootId}.


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = private}, ArchiveInfo) ->
    translate_archive_info(ArchiveInfo).

%%%===================================================================
%%% Util functions
%%%===================================================================

-spec translate_archive_info(archive_api:info()) -> json_utils:json_map().
translate_archive_info(#archive_info{
    id = ArchiveId,
    dataset_id = DatasetId,
    archiving_provider = ProviderId,
    state = State,
    root_dir_guid = RootDirGuid,
    creation_time = CreationTime,
    config = Config,
    preserved_callback = PreservedCallback,
    deleted_callback = DeletedCallback,
    description = Description,
    index = Index,
    stats = Stats,
    parent_archive_id = ParentArchiveId,
    base_archive_id = BaseArchiveId,
    related_aip_id = RelatedAipId,
    related_dip_id = RelatedDipId
}) ->
    #{
        <<"gri">> => build_serialized_archive_instance_gri(ArchiveId),
        <<"dataset">> => build_serialized_instance_gri(op_dataset, DatasetId),
        <<"provider">> => build_serialized_instance_gri(op_provider, ProviderId, protected),
        <<"state">> => str_utils:to_binary(State),
        <<"rootDir">> => build_serialized_instance_gri(op_file, RootDirGuid),
        <<"creationTime">> => CreationTime,
        <<"config">> => archive_config:to_json(Config),
        <<"preservedCallback">> => utils:undefined_to_null(PreservedCallback),
        <<"deletedCallback">> => utils:undefined_to_null(DeletedCallback),
        <<"description">> => Description,
        <<"index">> => Index,
        <<"stats">> => archive_stats:to_json(Stats),
        <<"parentArchive">> => build_serialized_archive_instance_gri(ParentArchiveId),
        <<"baseArchive">> => build_serialized_archive_instance_gri(BaseArchiveId),
        <<"relatedAip">> => build_serialized_archive_instance_gri(RelatedAipId),
        <<"relatedDip">> => build_serialized_archive_instance_gri(RelatedDipId)
    }.


-spec build_serialized_archive_instance_gri(undefined | archive:id()) -> gri:serialized() | null.
build_serialized_archive_instance_gri(ArchiveId) ->
    build_serialized_instance_gri(op_archive, ArchiveId).


-spec build_serialized_instance_gri(gri:entity_type(), gri:entity_id()) -> gri:serialized() | null.
build_serialized_instance_gri(Type, Id) ->
    build_serialized_instance_gri(Type, Id, private).
    

-spec build_serialized_instance_gri(gri:entity_type(), gri:entity_id(), gri:scope()) -> 
    gri:serialized() | null.
build_serialized_instance_gri(_, undefined, _) ->
    null;
build_serialized_instance_gri(Type, Id, Scope) ->
    gri:serialize(#gri{
        type = Type, id = Id,
        aspect = instance, scope = Scope
    }).
