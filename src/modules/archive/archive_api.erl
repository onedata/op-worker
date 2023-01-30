%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for performing operations on archives.
%%%
%%% Archivisation mechanism uses the following helper modules:
%%%  * archivisation_tree - module for creating file structure, stored in
%%%    .__onedata__archive hidden directory, in which archived files are stored
%%%  * archivisation_callback - module for calling HTTP webhooks to notify users
%%%    about finished archivisation or deleting jobs
%%%  * archivisation_traverse - module that uses tree_traverse to archive a dataset.
%%%    It traverses the dataset and builds an archive (and nested archives if required
%%%    by create_nested_archives parameter).
%%%  * bagit_archive - module used by archivisation_traverse to archive single file,
%%%    according to bagit specification. It also contains functions for initializing/
%%%    and finalizing whole archive complying to bagit specification.
%%%  * plain_archive - module used by archivisation_traverse to archive single file to
%%%    a plain archive.
%%%  * archive - module that implements archive datastore model
%%%  * archive_config - module that implements persistent_record behaviour,
%%%    which is stored in archive record and is used to store configuration of an archive
%%%  * archive_stats - module that implements persistent_record behaviour,
%%%    which is stored in archive record and is used to store statistics of archivisation procedure
%%%  * incremental_archive - module that is used for creating incremental archives. It contains functions
%%%    for finding base archives and for determining whether file in the dataset has changed in comparison
%%%    to its archived version.
%%%  * archives_list - module that implements list structure which allows to track
%%%    archives associated with given dataset
%%%  * archives_forest - module that is used to track parent-nested archive relations
%%% @end
%%%-------------------------------------------------------------------
-module(archive_api).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/dataset/archive.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([start_archivisation/6, cancel_archivisation/3, recall/4, cancel_recall/1, update_archive/2, get_archive_info/1,
    list_archives/3, delete/2, get_nested_archives_stats/1, get_aggregated_stats/1]).
-export([delete_single_archive/2]).
-export([delete_archive_recursive/1]).


-type info() :: #archive_info{}.
-type basic_entries() :: [archives_list:entry()].
-type extended_entries() :: [info()].
-type entries() :: basic_entries() | extended_entries().
-type index() :: archives_list:index().
-type error() :: {error, term()}.

-type listing_opts() :: dataset_api:listing_opts().
-type listing_mode() :: dataset_api:listing_mode().

-export_type([info/0, basic_entries/0, entries/0, index/0, listing_mode/0, listing_opts/0]).


% TODO VFS-7718 improve deleting so that archive record is deleted when files are removed from storage
% TODO VFS-7613 use datastore function for getting number of links in forest to acquire number of archives per dataset
% TODO VFS-7616 refine archives' attributes
% TODO VFS-7619 add tests concerning archives to permissions test suites
% TODO VFS-7662 send precise error descriptions to archivisation webhook

-define(MAX_LIST_EXTENDED_DATASET_INFO_PROCS,
    op_worker:get_env(max_list_extended_dataset_info_procs, 20)).

-define(BATCH_SIZE, 1000).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_archivisation(
    dataset:id(), archive:config(), archive:callback(), archive:callback(),
    archive:description(), user_ctx:ctx()
) -> {ok, info()} | error().
start_archivisation(
    DatasetId, Config, PreservedCallback, DeletedCallback, Description, UserCtx
) ->
    {ok, DatasetDoc} = dataset:get(DatasetId),
    {ok, State} = dataset:get_state(DatasetDoc),
    case State of
        ?ATTACHED_DATASET ->
            {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
            UserId = user_ctx:get_user_id(UserCtx),
            BaseArchiveId = ensure_base_archive_is_set_if_applicable(Config, DatasetId),
            case archive:create(DatasetId, SpaceId, UserId, Config,
                PreservedCallback, DeletedCallback, Description, BaseArchiveId)
            of
                {ok, AipArchiveDoc} ->
                    {ok, AipArchiveId} = archive:get_id(AipArchiveDoc),
                    {ok, Timestamp} = archive:get_creation_time(AipArchiveDoc),
                    {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
                    {ok, FinalAipArchiveDoc} = case archive_config:should_include_dip(Config) of
                        true -> 
                            {ok, #document{key = DipArchiveId}} = archive:create_dip_archive(AipArchiveDoc),
                            archive:set_related_dip(AipArchiveDoc, DipArchiveId);
                        false -> 
                            {ok, AipArchiveDoc}
                    end, 
                    archives_list:add(DatasetId, SpaceId, AipArchiveId, Timestamp),
                    case archivisation_traverse:start(FinalAipArchiveDoc, DatasetDoc, UserCtx) of
                        ok ->
                            get_archive_info(FinalAipArchiveDoc, undefined);
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        ?DETACHED_DATASET ->
            throw(?ERROR_BAD_DATA(<<"datasetId">>, <<"Detached dataset cannot be modified.">>))
    end.


-spec cancel_archivisation(archive:doc(), archive:cancel_preservation_policy(), user_ctx:ctx()) ->
    ok | {error, term()}.
cancel_archivisation(ArchiveDoc = #document{value = #archive{related_dip = undefined, related_aip = RelatedAip}}, PP, UserCtx) ->
    cancel_archivisations(ArchiveDoc, RelatedAip, PP, UserCtx);
cancel_archivisation(ArchiveDoc = #document{value = #archive{related_aip = undefined, related_dip = RelatedDip}}, PP, UserCtx) ->
    cancel_archivisations(ArchiveDoc, RelatedDip, PP, UserCtx).


-spec recall(archive:id(), user_ctx:ctx(), file_id:file_guid(), file_meta:name() | default) -> 
    {ok, file_id:file_guid()} | error().
recall(ArchiveId, UserCtx, ParentGuid, TargetRootName) ->
    case archive:get(ArchiveId) of
        {ok, #document{value = #archive{state = ?ARCHIVE_PRESERVED}} = ArchiveDoc} ->
            archive_recall_traverse:start(ArchiveDoc, UserCtx, ParentGuid, TargetRootName);
        {ok, #document{value = #archive{state = State}}} ->
            ?ERROR_FORBIDDEN_FOR_CURRENT_ARCHIVE_STATE(State, [?ARCHIVE_PRESERVED]);
        {error, _} = Error ->
            Error
    end.


-spec cancel_recall(archive_recall:id()) -> ok | error().
cancel_recall(RecallId) ->
    archive_recall_traverse:cancel(RecallId).


-spec update_archive(archive:id(), archive:diff()) -> ok | error().
update_archive(ArchiveId, Diff) ->
    archive:modify_attrs(ArchiveId, Diff).


-spec get_archive_info(archive:id()) -> {ok, info()} | {error, term()}.
get_archive_info(ArchiveId) ->
    get_archive_info(ArchiveId, undefined).


%% @private
-spec get_archive_info(archive:id() | archive:doc(), index() | undefined) -> 
    {ok, info()} | {error, term()}.
get_archive_info(ArchiveDoc = #document{}, ArchiveIndex) ->
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    {ok, DatasetId} = archive:get_dataset_id(ArchiveDoc),
    {ok, ProviderId} = archive:get_archiving_provider_id(ArchiveDoc),
    {ok, Creator} = archive:get_creator(ArchiveDoc),
    {ok, Timestamp} = archive:get_creation_time(ArchiveDoc),
    {ok, State} = get_state(ArchiveDoc),
    {ok, Config} = archive:get_config(ArchiveDoc),
    {ok, ArchiveRootDirGuid} = archive:get_root_dir_guid(ArchiveDoc),
    {ok, ArchiveDataDirGuid} = archive:get_data_dir_guid(ArchiveDoc),
    {ok, PreservedCallback} = archive:get_preserved_callback(ArchiveDoc),
    {ok, DeletedCallback} = archive:get_deleted_callback(ArchiveDoc),
    {ok, Description} = archive:get_description(ArchiveDoc),
    {ok, ParentArchiveId} = archive:get_parent_id(ArchiveDoc),
    {ok, BaseArchiveId} = archive:get_base_archive_id(ArchiveDoc),
    {ok, RelatedAipId} = archive:get_related_aip_id(ArchiveDoc),
    {ok, RelatedDipId} = archive:get_related_dip_id(ArchiveDoc),
    {ok, Stats} = get_aggregated_stats(ArchiveDoc),
    {ok, #archive_info{
        id = ArchiveId,
        dataset_id = DatasetId,
        archiving_provider = ProviderId,
        creator = Creator,
        state = State,
        root_dir_guid = ArchiveRootDirGuid,
        data_dir_guid = ArchiveDataDirGuid,
        creation_time = Timestamp,
        config = Config,
        preserved_callback = PreservedCallback,
        deleted_callback = DeletedCallback,
        description = Description,
        index = case ArchiveIndex =:= undefined of
            true -> archives_list:index(ArchiveId, Timestamp);
            false -> ArchiveIndex
        end,
        stats = Stats,
        parent_archive_id = ParentArchiveId,
        base_archive_id = BaseArchiveId,
        related_aip_id = RelatedAipId,
        related_dip_id = RelatedDipId
    }};
get_archive_info(ArchiveId, ArchiveIndex) ->
    case archive:get(ArchiveId) of
        {ok, ArchiveDoc} ->
            get_archive_info(ArchiveDoc, ArchiveIndex);
        {error, _} = Error ->
            Error
    end.


-spec list_archives(dataset:id(), archives_list:opts(), listing_mode()) ->
    {ok, {entries(), IsLast :: boolean()}}.
list_archives(DatasetId, ListingOpts, ListingMode) ->
    ArchiveEntries = archives_list:list(DatasetId, ListingOpts),
    IsLast = maps:get(limit, ListingOpts) > length(ArchiveEntries),
    case ListingMode of
        ?BASIC_INFO ->
            {ok, {ArchiveEntries, IsLast}};
        ?EXTENDED_INFO ->
            {ok, {extend_with_archive_info(ArchiveEntries), IsLast}}
    end.


-spec delete(archive:id(), archive:callback()) -> ok | error().
delete(ArchiveId, CallbackUrl) ->
    case archive:mark_deleting(ArchiveId, CallbackUrl) of
        {ok, ArchiveDoc} ->
            {ok, DatasetId} = archive:get_dataset_id(ArchiveDoc),
            % TODO VFS-7718 removal of archive doc and callback should be executed when deleting from trash is finished
            % (now it's done before archive files are deleted from storage)
            ok = delete_archive_recursive(ArchiveDoc),
            archivisation_callback:notify_deleted(ArchiveId, DatasetId, CallbackUrl);
        {error, _} = Error ->
            Error
    end.


-spec delete_archive_recursive(archive:doc() | archive:id()) -> ok.
delete_archive_recursive(#document{} = ArchiveDoc) ->
    delete_archive_recursive(ArchiveDoc, #link_token{});
delete_archive_recursive(ArchiveId) ->
    case archive:get(ArchiveId) of
        {ok, ArchiveDoc} -> delete_archive_recursive(ArchiveDoc);
        ?ERROR_NOT_FOUND -> ok
    end.


-spec delete_single_archive(archive:id() | archive:doc(), user_ctx:ctx()) -> ok | error().
delete_single_archive(undefined, _UserCtx) ->
    ok;
delete_single_archive(ArchiveDoc = #document{}, UserCtx) ->
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    case archive:delete(ArchiveId) of
        ok ->
            ok = unblock_archive(ArchiveDoc),
            {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
            ArchiveDocCtx = file_ctx:new_by_uuid(?ARCHIVE_DIR_UUID(ArchiveId), SpaceId),
            % TODO VFS-7718 Should it be possible to register many callbacks in case of parallel delete requests?
            delete_req:delete_using_trash(UserCtx, ArchiveDocCtx, true),
            
            {ok, DatasetId} = archive:get_dataset_id(ArchiveDoc),
            {ok, Timestamp} = archive:get_creation_time(ArchiveDoc),
            {ok, ParentArchiveId} = archive:get_parent_id(ArchiveDoc),
            ParentArchiveId =/= undefined andalso archives_forest:delete(ParentArchiveId, SpaceId, ArchiveId),
            archives_list:delete(DatasetId, SpaceId, ArchiveId, Timestamp);
        ?ERROR_NOT_FOUND ->
            % there was race with other process removing the archive
            ok
    end;
delete_single_archive(ArchiveId, UserCtx) ->
    case archive:get(ArchiveId) of
        {ok, ArchiveDoc} -> delete_single_archive(ArchiveDoc, UserCtx);
        ?ERROR_NOT_FOUND -> ok
    end.


-spec get_nested_archives_stats(archive:id() | archive:doc()) -> archive_stats:record().
get_nested_archives_stats(ArchiveIdOrDoc) ->
    get_nested_archives_stats(ArchiveIdOrDoc, #link_token{}, archive_stats:empty()).

-spec get_nested_archives_stats(archive:id() | archive:doc(), archives_forest:token(), archive_stats:record()) ->
    archive_stats:record().
get_nested_archives_stats(#document{key = ArchiveId}, Token, NestedArchiveStatsAccIn) ->
    get_nested_archives_stats(ArchiveId, Token, NestedArchiveStatsAccIn);
get_nested_archives_stats(ArchiveId, Token, NestedArchiveStatsAccIn) when is_binary(ArchiveId) ->
    {ok, NestedArchives, Token2} = archives_forest:list(ArchiveId, Token, ?BATCH_SIZE),
    NestedArchiveStatsAcc = lists:foldl(fun(NestedArchiveId, Acc) ->
        {ok, NestedArchiveStats} = get_aggregated_stats(NestedArchiveId),
        archive_stats:sum(Acc, NestedArchiveStats)
    end, NestedArchiveStatsAccIn, NestedArchives),
    case Token2#link_token.is_last of
        true -> NestedArchiveStatsAcc;
        false -> get_nested_archives_stats(ArchiveId, Token2, NestedArchiveStatsAcc)
    end.


-spec get_aggregated_stats(archive:doc() | archive:id()) -> {ok, archive_stats:record()}.
get_aggregated_stats(ArchiveDoc = #document{}) ->
    {ok, ArchiveStats} = archive:get_stats(ArchiveDoc),
    case archive:is_building(ArchiveDoc) of
        false ->
            {ok, ArchiveStats};
        true ->
            {ok, ArchiveId} = archive:get_id(ArchiveDoc),
            NestedArchivesStats = get_nested_archives_stats(ArchiveId),
            {ok, archive_stats:sum(ArchiveStats, NestedArchivesStats)}
    end;
get_aggregated_stats(ArchiveId) ->
    {ok, ArchiveDoc} = archive:get(ArchiveId),
    get_aggregated_stats(ArchiveDoc).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec delete_archive_recursive(archive:doc(), archives_forest:token()) -> ok.
delete_archive_recursive(ArchiveDoc, Token) ->
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    case archives_forest:list(ArchiveId, Token, ?BATCH_SIZE) of
        {ok, ChildrenArchives, Token2} ->
            lists:foreach(fun(ChildArchiveId) ->
                delete_archive_recursive(ChildArchiveId)
            end, ChildrenArchives),
            case Token2#link_token.is_last of
                true ->
                    delete_archive(ArchiveDoc);
                false ->
                    delete_archive_recursive(ArchiveDoc, Token2)
            end;
        {error, not_found} ->
            ok
    end.
    

%% @private
-spec delete_archive(archive:doc()) -> ok | error().
delete_archive(ArchiveDoc = #document{value = #archive{related_dip = undefined, related_aip = RelatedAip}}) ->
    delete_archives(ArchiveDoc, RelatedAip);
delete_archive(ArchiveDoc = #document{value = #archive{related_aip = undefined, related_dip = RelatedDip}}) ->
    delete_archives(ArchiveDoc, RelatedDip).


%% @private
-spec delete_archives(archive:doc(), archive:id() | undefined) -> ok | error().
delete_archives(Archive, RelatedArchive) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    ok = delete_single_archive(Archive, UserCtx),
    ok = delete_single_archive(RelatedArchive, UserCtx).


%% @private
-spec unblock_archive(archive:doc() | archive:id() | undefined) -> ok.
unblock_archive(#document{} = ArchiveDoc) ->
    archive_verification_traverse:unblock_archive_modification(ArchiveDoc);
unblock_archive(ArchiveId) ->
    case archive:get(ArchiveId) of
        {ok, ArchiveDoc} -> unblock_archive(ArchiveDoc);
        ?ERROR_NOT_FOUND -> ok
    end.


%% @private
-spec cancel_archivisations(archive:doc(), archive:id(), archive:cancel_preservation_policy(), user_ctx:ctx()) ->
    ok | {error, term()}.
cancel_archivisations(ArchiveDoc, RelatedArchiveId, PreservationPolicy, UserCtx) ->
    RelatedArchiveId =/= undefined andalso cancel_single_archive(RelatedArchiveId, PreservationPolicy, UserCtx),
    cancel_single_archive(ArchiveDoc, PreservationPolicy, UserCtx).


%% @private
-spec cancel_single_archive(archive:doc() | archive:id(), archive:cancel_preservation_policy(), user_ctx:ctx()) ->
    ok | {error, term()}.
cancel_single_archive(ArchiveDocOrId, PreservationPolicy, UserCtx) ->
    case archive:mark_cancelling(ArchiveDocOrId, PreservationPolicy) of
        ok ->
            {ok, TaskId} = archive:get_id(ArchiveDocOrId),
            ok = ?ok_if_not_found(archive_verification_traverse:cancel(TaskId));
        {error, already_cancelled} ->
            case PreservationPolicy of
                retain -> ok;
                delete -> delete_single_archive(ArchiveDocOrId, UserCtx)
            end;
        {error, already_finished} ->
            ok;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec extend_with_archive_info(basic_entries()) -> extended_entries().
extend_with_archive_info(ArchiveEntries) ->
    FilterMapFun = fun({ArchiveIndex, ArchiveId}) ->
        try
            {ok, ArchiveInfo} = get_archive_info(ArchiveId, ArchiveIndex),
            {true, ArchiveInfo}
        catch _:_ ->
            % Archive can be not synchronized with other provider
            false
        end
    end,
    lists_utils:pfiltermap(FilterMapFun, ArchiveEntries, ?MAX_LIST_EXTENDED_DATASET_INFO_PROCS).


%% @private
-spec get_state(archive:doc()) -> {ok, archive:state()}.
get_state(ArchiveDoc = #document{}) ->
    archive:get_state(ArchiveDoc).


%% @private
-spec ensure_base_archive_is_set_if_applicable(archive:config(), dataset:id()) -> 
    archive:id() | undefined.
ensure_base_archive_is_set_if_applicable(Config, DatasetId) ->
    case archive_config:is_incremental(Config) of
        true ->
            case archive_config:get_incremental_based_on(Config) of
                undefined -> incremental_archive:find_base_archive_id(DatasetId);
                BaseArchiveId -> BaseArchiveId
            end;
        false ->
            undefined
    end.
