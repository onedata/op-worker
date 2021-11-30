%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module with utility functions concerning datasets.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_utils).
-author("Michal Stanisz").

-include("proto/oneprovider/provider_messages.hrl").

-export([dataset_info_to_json/1]).

%%%===================================================================
%%% API functions
%%%===================================================================


-spec dataset_info_to_json(opl_datasets:info()) -> json_utils:json_map().
dataset_info_to_json(#dataset_info{
    id = DatasetId,
    state = State,
    root_file_guid = RootFileGuid,
    root_file_path = RootFilePath,
    root_file_type = RootFileType,
    root_file_deleted = RootFileDeleted,
    creation_time = CreationTime,
    protection_flags = ProtectionFlags,
    eff_protection_flags = EffProtectionFlags,
    parent = ParentId,
    archive_count = ArchiveCount
}) ->
    {ok, RootFileObjectId} = file_id:guid_to_objectid(RootFileGuid),
    #{
        <<"state">> => State,
        <<"datasetId">> => DatasetId,
        <<"parentId">> => utils:undefined_to_null(ParentId),
        <<"rootFileId">> => RootFileObjectId,
        <<"rootFileType">> => str_utils:to_binary(RootFileType),
        <<"rootFileDeleted">> => RootFileDeleted,
        <<"rootFilePath">> => RootFilePath,
        <<"protectionFlags">> => file_meta:protection_flags_to_json(ProtectionFlags),
        <<"effectiveProtectionFlags">> => file_meta:protection_flags_to_json(EffProtectionFlags),
        <<"creationTime">> => CreationTime,
        <<"archiveCount">> => ArchiveCount
    }.
