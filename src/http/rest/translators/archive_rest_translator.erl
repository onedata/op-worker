%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% archive entities into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(archive_rest_translator).
-author("Jakub Kudzia").

-include("http/rest.hrl").
-include("middleware/middleware.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
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
create_response(#gri{aspect = instance}, _, resource, {#gri{id = ArchiveId}, _}) ->
    PathTokens = [<<"archives">>, ArchiveId],
    ?CREATED_REPLY(PathTokens, #{<<"archiveId">> => ArchiveId}).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(#gri{aspect = instance}, #archive_info{} = ArchiveInfo) ->
    ?OK_REPLY(translate_archive_info(ArchiveInfo)).

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
    purged_callback = PurgedCallback,
    description = Description,
    stats = Stats,
    base_archive_id = BaseArchive,
    related_aip = RelatedAip,
    related_dip = RelatedDip
}) ->
    #{
        <<"archiveId">> => ArchiveId,
        <<"datasetId">> => DatasetId,
        <<"state">> => str_utils:to_binary(State),
        <<"rootDirectoryId">> => case RootDirGuid =/= undefined of
            true ->
                {ok, DirObjectId} = file_id:guid_to_objectid(RootDirGuid),
                DirObjectId;
            false ->
                null
        end,
        <<"creationTime">> => CreationTime,
        <<"config">> => archive_config:to_json(Config),
        <<"preservedCallback">> => utils:undefined_to_null(PreservedCallback),
        <<"purgedCallback">> => utils:undefined_to_null(PurgedCallback),
        <<"description">> => Description,
        <<"stats">> => archive_stats:to_json(Stats),
        <<"baseArchiveId">> => utils:undefined_to_null(BaseArchive),
        <<"relatedAipId">> => utils:undefined_to_null(RelatedAip),
        <<"relatedDipId">> => utils:undefined_to_null(RelatedDip)
    }.