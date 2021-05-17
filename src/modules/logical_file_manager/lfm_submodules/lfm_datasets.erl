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


%% Datasets API
-export([
    establish/3, remove/2, update/5,
    get_info/2, get_file_eff_summary/2,
    list_top_datasets/5, list_children_datasets/4
]).

%% Archives API
-export([archive/6, update_archive/3, get_archive_info/2, list_archives/4, init_archive_purge/3]).

-type info() :: #dataset_info{}.
-type archive_info() :: #archive_info{}.
-type file_eff_summary() :: #file_eff_dataset_summary{}.

-export_type([info/0, file_eff_summary/0, archive_info/0]).

%%%===================================================================
%%% Datasets API functions
%%%===================================================================

-spec establish(session:id(), lfm:file_key(), data_access_control:bitmask()) ->
    {ok, dataset:id()} | lfm:error_reply().
establish(SessId, FileKey, ProtectionFlags) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid, #establish_dataset{protection_flags = ProtectionFlags},
        fun(#dataset_established{id = DatasetId}) ->
            {ok, DatasetId}
        end
    ).


-spec update(session:id(), dataset:id(), undefined | dataset:state(), data_access_control:bitmask(),
    data_access_control:bitmask()) -> ok | lfm:error_reply().
update(SessId, DatasetId, NewState, FlagsToSet, FlagsToUnset) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #update_dataset{
            id = DatasetId,
            state = NewState,
            flags_to_set = FlagsToSet,
            flags_to_unset = FlagsToUnset
        },
        fun(_) -> ok end
    ).


-spec remove(session:id(), dataset:id()) ->
    ok | lfm:error_reply().
remove(SessId, DatasetId) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #remove_dataset{id = DatasetId},
        fun(_) -> ok end
    ).


-spec get_info(session:id(), dataset:id()) ->
    {ok, info()} | lfm:error_reply().
get_info(SessId, DatasetId) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #get_dataset_info{id = DatasetId},
        fun(#dataset_info{} = Info) -> {ok, Info} end
    ).


-spec get_file_eff_summary(session:id(), lfm:file_key()) ->
    {ok, file_eff_summary()} | lfm:error_reply().
get_file_eff_summary(SessId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid, #get_file_eff_dataset_summary{},
        fun(EffSummary = #file_eff_dataset_summary{}) ->
            {ok, EffSummary}
        end
    ).


-spec list_top_datasets(session:id(), od_space:id(), dataset:state(), dataset_api:listing_opts(),
    dataset_api:listing_mode()) -> {ok, dataset_api:entries(), boolean()} | lfm:error_reply().
list_top_datasets(SessId, SpaceId, State, Opts, ListingMode) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    ListingMode2 = utils:ensure_defined(ListingMode, ?BASIC_INFO),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #list_top_datasets{state = State, opts = Opts, mode = ListingMode2},
        fun(#datasets{datasets = Datasets, is_last = IsLast}) ->
            {ok, Datasets, IsLast}
        end
    ).


-spec list_children_datasets(session:id(), dataset:id(), dataset_api:listing_opts(), dataset_api:listing_mode()) ->
    {ok, dataset_api:entries(), boolean()} | lfm:error_reply().
list_children_datasets(SessId, DatasetId, Opts, ListingMode) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),
    ListingMode2 = utils:ensure_defined(ListingMode, ?BASIC_INFO),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #list_children_datasets{id = DatasetId, opts = Opts, mode = ListingMode2},
        fun(#datasets{datasets = Datasets, is_last = IsLast}) ->
            {ok, Datasets, IsLast}
        end
    ).


%%%===================================================================
%%% Archives API functions
%%%===================================================================

-spec archive(session:id(), dataset:id(), archive:config(), archive:callback(), archive:callback(), archive:description()) ->
    {ok, archive:id()} | lfm:error_reply().
archive(SessId, DatasetId, Config, PreservedCallback, PurgedCallback, Description) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #archive_dataset{
            id = DatasetId,
            config = Config,
            description = Description,
            preserved_callback = PreservedCallback,
            purged_callback = PurgedCallback
        },
        fun(#dataset_archived{id = ArchiveId}) ->
            {ok, ArchiveId}
        end
    ).


-spec update_archive(session:id(), archive:id(), archive:diff()) ->
    ok | lfm:error_reply().
update_archive(SessId, ArchiveId, Diff) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #update_archive{id = ArchiveId, diff = Diff},
        fun(_) -> ok end

    ).


-spec get_archive_info(session:id(), archive:id()) ->
    {ok, archive_info()} | lfm:error_reply().
get_archive_info(SessId, ArchiveId) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #get_archive_info{id = ArchiveId},
        fun(#archive_info{} = Info) -> {ok, Info} end
    ).


-spec list_archives(session:id(), dataset:id(), dataset_api:listing_opts(), dataset_api:listing_mode()) ->
    {ok, [archive:id()], boolean()} | lfm:error_reply().
list_archives(SessId, DatasetId, Opts, ListingMode) ->
    SpaceGuid = dataset_id_to_space_guid(DatasetId),
    ListingMode2 = utils:ensure_defined(ListingMode, ?BASIC_INFO),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #list_archives{dataset_id = DatasetId, opts = Opts, mode = ListingMode2},
        fun(#archives{archives = Archives, is_last = IsLast}) ->
            {ok, Archives, IsLast}
        end
    ).


-spec init_archive_purge(session:id(), archive:id(), archive:callback()) ->
    ok | lfm:error_reply().
init_archive_purge(SessId, ArchiveId, CallbackUrl) ->
    SpaceGuid = archive_id_to_space_guid(ArchiveId),
    remote_utils:call_fslogic(SessId, provider_request, SpaceGuid,
        #init_archive_purge{id = ArchiveId, callback = CallbackUrl},
        fun(_) -> ok end
    ).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec dataset_id_to_space_guid(dataset:id()) -> fslogic_worker:file_guid().
dataset_id_to_space_guid(DatasetId) ->
    {ok, SpaceId} = dataset:get_space_id(DatasetId),
    fslogic_uuid:spaceid_to_space_dir_guid(SpaceId).


-spec archive_id_to_space_guid(archive:id()) -> fslogic_worker:file_guid().
archive_id_to_space_guid(ArchiveId) ->
    {ok, SpaceId} = archive:get_space_id(ArchiveId),
    fslogic_uuid:spaceid_to_space_dir_guid(SpaceId).
