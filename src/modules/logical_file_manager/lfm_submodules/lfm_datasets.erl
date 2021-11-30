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

%% Archives API
-export([archive/6, update_archive/3, get_archive_info/2, list_archives/4, init_archive_purge/3]).

-type archive_info() :: #archive_info{}.

-export_type([archive_info/0]).


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
