%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% space entities into REST responses.
%%% @end
%%%-------------------------------------------------------------------
-module(space_rest_translator).
-author("Bartosz Walkowicz").

-include("http/rest.hrl").
-include("middleware/middleware.hrl").
-include("modules/dataset/archivisation_tree.hrl").

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
create_response(#gri{aspect = evaluate_qos_expression}, _, value, Result) ->
    ?OK_REPLY(maps:with([<<"matchingStorages">>], Result)).


%%--------------------------------------------------------------------
%% @doc
%% {@link rest_translator_behaviour} callback get_response/2.
%% @end
%%--------------------------------------------------------------------
-spec get_response(gri:gri(), Resource :: term()) -> #rest_resp{}.
get_response(#gri{id = SpaceId, aspect = instance, scope = private}, Space) ->
    ?OK_REPLY(translate_space(SpaceId, Space));

get_response(#gri{aspect = list}, Spaces) ->
    ?OK_REPLY([translate_space(SpaceId, Space) || {SpaceId, Space} <- Spaces]);

get_response(#gri{aspect = atm_workflow_executions}, {AtmWorkflowExecutionIds, NextPageToken, IsLast}) ->
    ?OK_REPLY(#{
        <<"atmWorkflowExecutions">> => AtmWorkflowExecutionIds,
        <<"nextPageToken">> => utils:undefined_to_null(NextPageToken),
        <<"isLast">> => IsLast
    });

get_response(#gri{aspect = datasets}, {Datasets, IsLast}) ->
    ?OK_REPLY(dataset_rest_translator:translate_datasets_list(Datasets, IsLast));

get_response(_, SpaceData) ->
    ?OK_REPLY(SpaceData).


%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @private
-spec translate_space(od_space:id(), od_space:record()) -> json_utils:json_map().
translate_space(SpaceId, #od_space{
    name = Name,
    providers = ProvidersIds
}) ->
    SpaceDirGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    {ok, SpaceDirObjectId} = file_id:guid_to_objectid(SpaceDirGuid),
    {ok, TrashRootDirObjectId} = file_id:guid_to_objectid(file_id:pack_guid(?TRASH_DIR_UUID(SpaceId), SpaceId)),
    {ok, ArchivesRootDirObjectId} = file_id:guid_to_objectid(file_id:pack_guid(?ARCHIVES_ROOT_DIR_UUID(SpaceId), SpaceId)),

    Providers = lists:map(fun(ProviderId) ->
        {ok, ProviderName} = provider_logic:get_name(ProviderId),
        #{
            <<"providerId">> => ProviderId,
            <<"providerName">> => ProviderName
        }
    end, maps:keys(ProvidersIds)),

    #{
        <<"name">> => Name,
        <<"spaceId">> => SpaceId,
        <<"fileId">> => SpaceDirObjectId, % deprecated, left for backward compatibility
        <<"dirId">> => SpaceDirObjectId,
        <<"trashDirId">> => TrashRootDirObjectId,
        <<"archivesDirId">> => ArchivesRootDirObjectId,
        <<"providers">> => Providers  %@fixme miejsce 1
        %@fixme testy na oba miejsca lolol
    }.
