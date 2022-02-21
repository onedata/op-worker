%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% file entities into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(file_rest_translator).
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
create_response(#gri{aspect = object_id}, _, value, ObjectId) ->
    ?OK_REPLY(#{<<"fileId">> => ObjectId});
create_response(#gri{aspect = register_file}, _, value, ObjectId) ->
    ?CREATED_REPLY([<<"data">>, ObjectId],  #{<<"fileId">> => ObjectId}).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(#gri{aspect = As}, Result) when
    As =:= object_id
->
    ?OK_REPLY(Result);

get_response(#gri{aspect = As}, RdfMetadata) when
    As =:= rdf_metadata;
    As =:= symlink_value
->
    ?OK_REPLY({binary, RdfMetadata});

get_response(#gri{aspect = As}, Metadata) when
    As =:= attrs;
    As =:= distribution;
    As =:= qos_summary;
    As =:= xattrs;
    As =:= json_metadata
->
    ?OK_REPLY(Metadata);

get_response(#gri{aspect = dataset_summary}, #file_eff_dataset_summary{
    direct_dataset = DatasetId,
    eff_ancestor_datasets = EffAncestorDatasets,
    eff_protection_flags = EffProtectionFlags
}) ->
    ?OK_REPLY(#{
        <<"directDataset">> => utils:undefined_to_null(DatasetId),
        <<"effectiveAncestorDatasets">> => EffAncestorDatasets,
        <<"effectiveProtectionFlags">> => file_meta:protection_flags_to_json(EffProtectionFlags)
    });

get_response(#gri{aspect = children}, {Children, IsLast}) ->
    ?OK_REPLY(#{
        <<"children">> => lists:map(fun({Guid, Name}) ->
            {ok, ObjectId} = file_id:guid_to_objectid(Guid),
            #{
                <<"id">> => ObjectId,
                <<"name">> => Name
            }
        end, Children),
        <<"isLast">> => IsLast
    });

get_response(#gri{aspect = list_recursive}, {Files, NextPageToken, IsLast}) ->
    ?OK_REPLY(#{
        <<"files">> => Files,
        <<"nextPageToken">> => NextPageToken,
        <<"isLast">> => IsLast
    });

get_response(#gri{aspect = hardlinks}, Hardlinks) ->
    ?OK_REPLY(lists:map(fun(FileGuid) ->
        {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
        ObjectId
    end, Hardlinks));

get_response(#gri{aspect = {hardlinks, _}}, _Result) ->
    ?NO_CONTENT_REPLY;

get_response(#gri{aspect = archive_recall_details}, Result) ->
    ?OK_REPLY(translate_archive_recall_details(Result));

get_response(#gri{aspect = archive_recall_progress}, #{<<"lastError">> := LastError} = Result) ->
    ?OK_REPLY(Result#{<<"lastError">> => utils:undefined_to_null(LastError)}).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec translate_archive_recall_details(archive_recall:record()) -> map().
translate_archive_recall_details(#archive_recall_details{
    archive_id = ArchiveId,
    dataset_id = DatasetId,
    start_timestamp = StartTimestamp,
    finish_timestamp = FinishTimestamp,
    total_file_count = TotalFileCount,
    total_byte_size = TotalByteSize
}) ->
    #{
        <<"archiveId">> => ArchiveId,
        <<"datasetId">> => DatasetId,
        <<"startTime">> => utils:undefined_to_null(StartTimestamp),
        <<"finishTime">> => utils:undefined_to_null(FinishTimestamp),
        <<"totalFileCount">> => TotalFileCount,
        <<"totalByteSize">> => TotalByteSize
    }.
