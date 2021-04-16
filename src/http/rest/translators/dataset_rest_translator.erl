%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% dataset entities into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(dataset_rest_translator).
-author("Bartosz Walkowicz").

-include("http/rest.hrl").
-include("middleware/middleware.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([create_response/4, get_response/2]).
% Util functions
-export([translate_dataset_info/1, translate_datasets_list/2]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback create_response/4.
%% @end
%%--------------------------------------------------------------------
-spec create_response(gri:gri(), middleware:auth_hint(),
    middleware:data_format(), Result :: term() | {gri:gri(), term()} |
    {gri:gri(), middleware:auth_hint(), term()}) -> #rest_resp{}.
create_response(#gri{aspect = instance}, _, resource, {#gri{id = DatasetId}, _}) ->
    PathTokens = [<<"datasets">>, DatasetId],
    ?CREATED_REPLY(PathTokens, #{<<"datasetId">> => DatasetId}).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(#gri{aspect = instance}, #dataset_info{} = DatasetInfo) ->
    ?OK_REPLY(translate_dataset_info(DatasetInfo));

get_response(#gri{aspect = children}, {Datasets, IsLast}) ->
    ?OK_REPLY(translate_datasets_list(Datasets, IsLast)).

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
    parent = ParentId
}) ->
    {ok, RootFileObjectId} = file_id:guid_to_objectid(RootFileGuid),
    #{
        <<"state">> => State,
        <<"datasetId">> => DatasetId,
        <<"parentId">> => utils:undefined_to_null(ParentId),
        <<"rootFileId">> => RootFileObjectId,
        <<"rootFileType">> => str_utils:to_binary(RootFileType),
        <<"rootFilePath">> => RootFilePath,
        <<"protectionFlags">> => file_meta:protection_flags_to_json(ProtectionFlags),
        <<"effectiveProtectionFlags">> => file_meta:protection_flags_to_json(EffProtectionFlags),
        <<"creationTime">> => CreationTime
    }.


-spec translate_datasets_list([{dataset:id(), dataset:name(), datasets_structure:index()}], boolean()) -> json_utils:json_map().
translate_datasets_list(Datasets, IsLast) ->
    {TranslatedDatasetsReversed, NextPageToken} = lists:foldl(fun({DatasetId, DatasetName, Index}, {Acc, _}) ->
        {[#{<<"datasetId">> => DatasetId, <<"name">> => DatasetName} | Acc], Index}
    end, {[], undefined}, Datasets),
    #{
        <<"datasets">> => lists:reverse(TranslatedDatasetsReversed),
        <<"nextPageToken">> => case IsLast of
            true -> null;
            false -> base64url:encode(NextPageToken)
        end
    }.