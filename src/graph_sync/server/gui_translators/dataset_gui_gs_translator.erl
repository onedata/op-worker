%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% dataset entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([translate_value/2, translate_resource/2]).
% Util functions
-export([translate_dataset_info/1, translate_datasets_details_list/2]).

%%%===================================================================
%%% API
%%%===================================================================


-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
translate_value(#gri{aspect = children_details, scope = private}, {Datasets, IsLast}) ->
    translate_datasets_details_list(Datasets, IsLast).


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = private}, DatasetInfo) ->
    translate_dataset_info(DatasetInfo).

%%%===================================================================
%%% Util functions
%%%===================================================================

-spec translate_dataset_info(lfm_datasets:info()) -> json_utils:json_map().
translate_dataset_info(#dataset_info{
    id = DatasetId,
    state = State,
    root_file_guid = RootFileGuid,
    root_file_path = RootFilePath,
    root_file_type = RootFileType,
    creation_time = CreationTime,
    protection_flags = ProtectionFlags,
    eff_protection_flags = EffProtectionFlags,
    parent = ParentId,
    index = Index
}) ->
    #{
        <<"gri">> => gri:serialize(#gri{
            type = op_dataset, id = DatasetId,
            aspect = instance, scope = private
        }),
        <<"parent">> => case ParentId of
            undefined ->
                null;
            _ ->
                gri:serialize(#gri{
                    type = op_dataset, id = ParentId,
                    aspect = instance, scope = private
                })
        end,
        <<"rootFile">> => gri:serialize(#gri{
            type = op_file, id = RootFileGuid,
            aspect = instance, scope = private
        }),
        <<"rootFileType">> => str_utils:to_binary(RootFileType),
        <<"rootFilePath">> => RootFilePath,
        <<"state">> => atom_to_binary(State, utf8),
        <<"protectionFlags">> => file_meta:protection_flags_to_json(ProtectionFlags),
        <<"effProtectionFlags">> => file_meta:protection_flags_to_json(EffProtectionFlags),
        <<"creationTime">> => CreationTime,
        <<"index">> => Index
    }.


-spec translate_datasets_details_list([lfm_datasets:info()], boolean()) -> json_utils:json_map().
translate_datasets_details_list(Datasets, IsLast) ->
    TranslatedDatasets = lists:map(fun translate_dataset_info/1, Datasets),
    #{
        <<"datasets">> => TranslatedDatasets,
        <<"isLast">> => IsLast
    }.