%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module performs dataset-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_datasets).
-author("Jakub Kudzia").

-include("proto/oneprovider/provider_messages.hrl").


%% API
-export([
    establish/3, remove/2, update/5,
    get_info/2, get_file_eff_summary/2,
    list_top_datasets/4, list_children_datasets/3
]).

-type attrs() :: #dataset_info{}.
-type file_eff_summary() :: #file_eff_dataset_summary{}.

-export_type([attrs/0, file_eff_summary/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec establish(session:id(), lfm:file_key(), data_access_control:bitmask()) ->
    {ok, dataset:id()} | lfm:error_reply().
establish(SessId, FileKey, ProtectionFlags) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid, #establish_dataset{protection_flags = ProtectionFlags},
        fun(#dataset_established{id = DatasetId}) ->
            {ok, DatasetId}
        end).


-spec update(session:id(), dataset:id(), undefined | dataset:state(), data_access_control:bitmask(),
    data_access_control:bitmask()) -> ok | lfm:error_reply().
update(SessId, DatasetId, NewState, FlagsToSet, FlagsToUnset) ->
    SpaceGuid = get_space_guid(DatasetId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #update_dataset{
            id = DatasetId,
            state = NewState,
            flags_to_set = FlagsToSet,
            flags_to_unset = FlagsToUnset
        },
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
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid, #get_file_eff_dataset_summary{},
        fun(EffSummary = #file_eff_dataset_summary{}) ->
            {ok, EffSummary}
        end
    ).


-spec list_top_datasets(session:id(), od_space:id(), dataset:state(), datasets_structure:opts()) ->
    {ok, [{dataset:id(), dataset:name()}], boolean()} | lfm:error_reply().
list_top_datasets(SessId, SpaceId, State, Opts) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #list_top_datasets{state = State, opts = Opts},
        fun(#datasets{datasets = Datasets, is_last = IsLast}) ->
            {ok, Datasets, IsLast}
        end).


-spec list_children_datasets(session:id(), dataset:id(), datasets_structure:opts()) ->
    {ok, [{dataset:id(), dataset:name()}], boolean()} | lfm:error_reply().
list_children_datasets(SessId, DatasetId, Opts) ->
    SpaceGuid = get_space_guid(DatasetId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #list_children_datasets{id = DatasetId, opts = Opts},
        fun(#datasets{datasets = Datasets, is_last = IsLast}) ->
            {ok, Datasets, IsLast}
        end).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_space_guid(dataset:id()) -> fslogic_worker:file_guid().
get_space_guid(DatasetId) ->
    {ok, SpaceId} = dataset:get_space_id(DatasetId),
    fslogic_uuid:spaceid_to_space_dir_guid(SpaceId).
