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
translate_value(#gri{aspect = file_info}, FileInfo) ->
    #{
        <<"sourceFile">> := SourceFileGuid,
        <<"archivedFile">> := ArchivedFile
    } = FileInfo,
    #{
        <<"sourceFile">> => prepare_instance_gri(op_file, SourceFileGuid),
        <<"archivedFile">> => prepare_instance_gri(op_file, ArchivedFile)
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
        <<"gri">> => prepare_archive_instance_gri(ArchiveId),
        <<"dataset">> => prepare_instance_gri(op_dataset, DatasetId),
        <<"state">> => str_utils:to_binary(State),
        <<"rootDir">> => prepare_instance_gri(op_file, RootDirGuid),
        <<"creationTime">> => CreationTime,
        <<"config">> => archive_config:to_json(Config),
        <<"preservedCallback">> => utils:undefined_to_null(PreservedCallback),
        <<"deletedCallback">> => utils:undefined_to_null(DeletedCallback),
        <<"description">> => Description,
        <<"index">> => Index,
        <<"stats">> => archive_stats:to_json(Stats),
        <<"parentArchive">> => prepare_archive_instance_gri(ParentArchiveId),
        <<"baseArchive">> => prepare_archive_instance_gri(BaseArchiveId),
        <<"relatedAip">> => prepare_archive_instance_gri(RelatedAipId),
        <<"relatedDip">> => prepare_archive_instance_gri(RelatedDipId)
    }.


-spec prepare_archive_instance_gri(undefined | archive:id()) -> gri:serialized() | null.
prepare_archive_instance_gri(ArchiveId) ->
    prepare_instance_gri(op_archive, ArchiveId).
    

-spec prepare_instance_gri(gri:entity_type(), gri:entity_id()) -> gri:serialized() | null.
prepare_instance_gri(_, undefined) ->
    null;
prepare_instance_gri(Type, Id) ->
    gri:serialize(#gri{
        type = Type, id = Id,
        aspect = instance, scope = private
    }).
