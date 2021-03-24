%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_datasets).
-author("Jakub Kudzia").

-include("proto/oneprovider/provider_messages.hrl").


%% API
-export([
    establish/2, remove/2,
    detach/2, reattach/2,
    get_info/2, get_file_eff_summary/2,
    list_space/4, list_dataset/3
]).

-type attrs() :: #dataset_info{}.
-type file_eff_summary() :: #file_eff_dataset_summary{}.

-export_type([attrs/0, file_eff_summary/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec establish(session:id(), lfm:file_key()) ->
    {ok, dataset:id()} | lfm:error_reply().
establish(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid, #establish_dataset{},
        fun(#dataset_established{id = DatasetId}) ->
            {ok, DatasetId}
        end).


-spec detach(session:id(), dataset:id()) ->
    ok | lfm:error_reply().
detach(SessId, DatasetId) ->
    SpaceGuid = get_space_guid(DatasetId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #detach_dataset{id = DatasetId},
        fun(_) -> ok end).


-spec reattach(session:id(), dataset:id()) ->
    ok | lfm:error_reply().
reattach(SessId, DatasetId) ->
    SpaceGuid = get_space_guid(DatasetId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #reattach_dataset{id = DatasetId},
        fun(_) -> ok end).


-spec remove(session:id(), dataset:id()) ->
    ok | lfm:error_reply().
remove(SessId, DatasetId) ->
    SpaceGuid = get_space_guid(DatasetId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #remove_dataset{id = DatasetId},
        fun(_) -> ok end).


-spec get_info(session:id(), dataset:id()) ->
    {ok, attrs()} | lfm:error_reply().
get_info(SessId, DatasetId) ->
    SpaceGuid = get_space_guid(DatasetId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #get_dataset_info{id = DatasetId},
        fun(#dataset_info{} = Attrs) -> {ok, Attrs} end).


-spec get_file_eff_summary(session:id(), lfm:file_key()) ->
    {ok, file_eff_summary()} | lfm:error_reply().
get_file_eff_summary(SessId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid, #get_file_eff_dataset_summary{},
        fun(EffSummary = #file_eff_dataset_summary{}) ->
            {ok, EffSummary}
        end
    ).


-spec list_space(session:id(), od_space:id(), dataset:state(), datasets_structure:opts()) ->
    ok | lfm:error_reply().
list_space(SessId, SpaceId, State, Opts) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #list_space_datasets{state = State, opts = Opts},
        fun(#nested_datasets{datasets = Datasets, is_last = IsLast}) ->
            {ok, Datasets, IsLast}
        end).


-spec list_dataset(session:id(), dataset:id(), datasets_structure:opts()) ->
    ok | lfm:error_reply().
list_dataset(SessId, DatasetId, Opts) ->
    SpaceGuid = get_space_guid(DatasetId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #list_nested_datasets{id = DatasetId, opts = Opts},
        fun(#nested_datasets{datasets = Datasets, is_last = IsLast}) ->
            {ok, Datasets, IsLast}
        end).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_space_guid(dataset:id()) -> fslogic_worker:file_guid().
get_space_guid(DatasetId) ->
    {ok, SpaceId} = dataset:get_space_id(DatasetId),
    fslogic_uuid:spaceid_to_space_dir_guid(SpaceId).
