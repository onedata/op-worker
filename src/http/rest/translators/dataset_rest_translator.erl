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

-export([create_response/4, get_response/2]).


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
get_response(#gri{aspect = instance}, #dataset_info{
    id = DatasetId,
    state = State,
    root_file_guid = RootFileGuid,
    root_file_path = RootFilePath,
    root_file_type = RootFileType,
    creation_time = CreationTime,
    protection_flags = ProtectionFlags,
    parent = ParentId
}) ->
    {ok, RootFileObjectId} = file_id:guid_to_objectid(RootFileGuid),

    ?OK_REPLY(#{
        <<"state">> => State,
        <<"datasetId">> => DatasetId,
        <<"parentId">> => utils:undefined_to_null(ParentId),
        <<"rootFileId">> => RootFileObjectId,
        <<"rootFileType">> => str_utils:to_binary(RootFileType),
        <<"rootFilePath">> => RootFilePath,
        <<"protectionFlags">> => file_meta:protection_flags_to_json(ProtectionFlags),
        <<"creationTime">> => CreationTime
    });

get_response(#gri{aspect = children}, Children) ->
    ?OK_REPLY(Children).
