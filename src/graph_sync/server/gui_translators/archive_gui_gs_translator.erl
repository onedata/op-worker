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
    config = Config,
    preserved_callback = PreservedCallback,
    purged_callback = PurgedCallback,
    description = Description,
    index = Index,
    files_to_archive = FilesToArchive,
    files_archived = FilesArchived,
    files_failed = FilesFailed,
    bytes_archived = BytesArchived
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
        <<"state">> => str_utils:to_binary(State),
        <<"rootDir">> => case RootDirGuid =/= undefined of
            true -> gri:serialize(#gri{
                type = op_file, id = RootDirGuid,
                aspect = instance, scope = private
            });
            false ->
                null
        end,
        <<"creationTime">> => CreationTime,
        <<"config">> => archive_config:to_json(Config),
        <<"preservedCallback">> => utils:undefined_to_null(PreservedCallback),
        <<"purgedCallback">> => utils:undefined_to_null(PurgedCallback),
        <<"description">> => Description,
        <<"index">> => Index,
        <<"filesToArchive">> => FilesToArchive,
        <<"filesArchived">> => FilesArchived,
        <<"filesFailed">> => FilesFailed,
        <<"bytesArchived">> => BytesArchived
    }.