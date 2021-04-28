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
-export([translate_resource/2]).
% Util functions
-export([translate_archive_info/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = private}, ArchiveInfo) ->
    translate_archive_info(ArchiveInfo).

%%%===================================================================
%%% Util functions
%%%===================================================================

-spec translate_archive_info(lfm_datasets:archive_info()) -> json_utils:json_map().
translate_archive_info(#archive_info{
    id = ArchiveId,
    dataset_id = DatasetId,
    state = State,
    root_dir_guid = RootDirGuid,
    creation_time = CreationTime,
    type = Type,
    character = Character,
    data_structure = DataStructure,
    metadata_structure = MetadataStructure,
    description = Description,
    index = Index
}) ->
    #{
        <<"gri">> => gri:serialize(#gri{
            type = op_archive, id = ArchiveId,
            aspect = instance, scope = private
        }),
        <<"dataset">> => gri:serialize(#gri{
            type = op_dataset, id = DatasetId,
            aspect = instance, scope = private
        }),
        <<"rootDir">> => gri:serialize(#gri{
            type = op_file, id = RootDirGuid,
            aspect = instance, scope = private
        }),
        <<"state">> => atom_to_binary(State, utf8),
        <<"creationTime">> => CreationTime,
        <<"type">> => atom_to_binary(Type, utf8),
        <<"character">> => atom_to_binary(Character, utf8),
        <<"dataStructure">> => atom_to_binary(DataStructure, utf8),
        <<"metadataStructure">> => atom_to_binary(MetadataStructure, utf8),
        <<"description">> => Description,
        <<"index">> => Index
    }.