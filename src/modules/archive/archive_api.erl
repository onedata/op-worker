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
%%%    about finished archivisation or purging jobs
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
-include("modules/dataset/dataset.hrl").
-include("modules/dataset/archivisation_tree.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([start_archivisation/6, update_archive/2, get_archive_info/1,
    list_archives/3, init_archive_purge/3, get_nested_archives_stats/1]).

%% Exported for use in tests
-export([remove_archive_recursive/1]).


-type info() :: #archive_info{}.
-type basic_entries() :: [archives_list:entry()].
-type extended_entries() :: [info()].
-type entries() :: basic_entries() | extended_entries().
-type index() :: archives_list:index().
-type error() :: {error, term()}.

-type listing_opts() :: dataset_api:listing_opts().
-type listing_mode() :: dataset_api:listing_mode().

-export_type([info/0, basic_entries/0, entries/0, index/0, listing_mode/0, listing_opts/0]).


% TODO VFS-7617 implement recall operation of archives
% TODO VFS-7718 improve purging so that archive record is deleted when files are removed from storage
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
) -> {ok, archive:id()} | error().
start_archivisation(
    DatasetId, Config, PreservedCallback, PurgedCallback, Description, UserCtx
) ->
    {ok, DatasetDoc} = dataset:get(DatasetId),
    {ok, State} = dataset:get_state(DatasetDoc),
    case State of
        ?ATTACHED_DATASET ->
            {ok, SpaceId} = dataset:get_space_id(DatasetDoc),
            UserId = user_ctx:get_user_id(UserCtx),
            BaseArchiveId = ensure_base_archive_is_set_if_applicable(Config, DatasetId),
            case archive:create(DatasetId, SpaceId, UserId, Config,
                PreservedCallback, PurgedCallback, Description, BaseArchiveId)
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
                            {ok, AipArchiveId};
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        ?DETACHED_DATASET ->
            {error, ?EINVAL}
        %% TODO VFS-7208 uncomment after introducing API errors to fslogic
        % throw(?ERROR_BAD_DATA(<<"datasetId">>, <<"Detached dataset cannot be modified.">>));
    end.


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
    {ok, Timestamp} = archive:get_creation_time(ArchiveDoc),
    {ok, State} = get_state(ArchiveDoc),
    {ok, Config} = archive:get_config(ArchiveDoc),
    {ok, ArchiveRootDirGuid} = archive:get_root_dir_guid(ArchiveDoc),
    {ok, PreservedCallback} = archive:get_preserved_callback(ArchiveDoc),
    {ok, PurgedCallback} = archive:get_purged_callback(ArchiveDoc),
    {ok, Description} = archive:get_description(ArchiveDoc),
    {ok, BaseArchiveId} = archive:get_base_archive_id(ArchiveDoc),
    {ok, RelatedAip} = archive:get_related_aip(ArchiveDoc),
    {ok, RelatedDip} = archive:get_related_dip(ArchiveDoc),
    {ok, #archive_info{
        id = ArchiveId,
        dataset_id = DatasetId,
        state = State,
        root_dir_guid = ArchiveRootDirGuid,
        creation_time = Timestamp,
        config = Config,
        preserved_callback = PreservedCallback,
        purged_callback = PurgedCallback,
        description = Description,
        index = case ArchiveIndex =:= undefined of
            true -> archives_list:index(ArchiveId, Timestamp);
            false -> ArchiveIndex
        end,
        stats = get_aggregated_stats(ArchiveDoc),
        base_archive_id = BaseArchiveId,
        related_aip = RelatedAip,
        related_dip = RelatedDip
    }};
get_archive_info(ArchiveId, ArchiveIndex) ->
    case archive:get(ArchiveId) of
        {ok, ArchiveDoc} ->
            get_archive_info(ArchiveDoc, ArchiveIndex);
        {error, _} = Error ->
            Error
    end.


-spec list_archives(dataset:id(), archives_list:opts(), listing_mode()) ->
    {ok, entries(), IsLast :: boolean()}.
list_archives(DatasetId, ListingOpts, ListingMode) ->
    ArchiveEntries = archives_list:list(DatasetId, ListingOpts),
    IsLast = maps:get(limit, ListingOpts) > length(ArchiveEntries),
    case ListingMode of
        ?BASIC_INFO ->
            {ok, ArchiveEntries, IsLast};
        ?EXTENDED_INFO ->
            {ok, extend_with_archive_info(ArchiveEntries), IsLast}
    end.


-spec init_archive_purge(archive:id(), archive:callback(), user_ctx:ctx()) -> ok | error().
init_archive_purge(ArchiveId, CallbackUrl, _UserCtx) ->
    case archive:mark_purging(ArchiveId, CallbackUrl) of
        {ok, ArchiveDoc} ->
            {ok, DatasetId} = archive:get_dataset_id(ArchiveDoc),
            % TODO VFS-7718 removal of archive doc and callback should be executed when deleting from trash is finished
            % (now it's done before archive files are deleted from storage)
            ok = remove_archive_recursive(ArchiveDoc),
            archivisation_callback:notify_purged(ArchiveId, DatasetId, CallbackUrl);
        {error, _} = Error ->
            Error
    end.


-spec remove_archive_recursive(archive:doc() | archive:id()) -> ok.
remove_archive_recursive(ArchiveDocOrId) ->
    remove_archive_recursive(ArchiveDocOrId, #link_token{}).


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
        NestedArchiveStats = get_aggregated_stats(NestedArchiveId),
        archive_stats:sum(Acc, NestedArchiveStats)
    end, NestedArchiveStatsAccIn, NestedArchives),
    case Token2#link_token.is_last of
        true -> NestedArchiveStatsAcc;
        false -> get_nested_archives_stats(ArchiveId, Token2, NestedArchiveStatsAcc)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec remove_archive_recursive(archive:doc() | archive:id(), archives_forest:token()) -> ok.
remove_archive_recursive(ArchiveDocOrId, Token) ->
    {ok, ArchiveId} = case ArchiveDocOrId of
        #document{} = ArchiveDoc -> archive:get_id(ArchiveDoc);
        ArchiveId0 when is_binary(ArchiveId0) -> {ok, ArchiveId0}
    end,
    case archives_forest:list(ArchiveId, Token, ?BATCH_SIZE) of
        {ok, ChildrenArchives, Token2} ->
            lists:foreach(fun(ChildArchiveId) ->
                remove_archive_recursive(ChildArchiveId)
            end, ChildrenArchives),
            case Token2#link_token.is_last of
                true ->
                    remove_archive(ArchiveDocOrId);
                false ->
                    remove_archive_recursive(ArchiveDocOrId, Token2)
            end;
        {error, not_found} ->
            ok
    end.


%% @private
-spec remove_archive(archive:doc() | archive:id()) -> ok | error().
remove_archive(ArchiveDoc = #document{value = #archive{related_dip = undefined, related_aip = RelatedAip}}) ->
    remove_archives(ArchiveDoc, RelatedAip);
remove_archive(ArchiveDoc = #document{value = #archive{related_aip = undefined, related_dip = RelatedDip}}) ->
    remove_archives(ArchiveDoc, RelatedDip);
remove_archive(ArchiveId) ->
    case archive:get(ArchiveId) of
        {ok, ArchiveDoc} -> remove_archive(ArchiveDoc);
        ?ERROR_NOT_FOUND -> ok;
        {error, _} = Error -> Error
    end.


%% @private
-spec remove_archives(archive:id() | archive:doc(), archive:id() | undefined) -> ok | error().
remove_archives(Archive, RelatedArchive) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    ok = remove_single_archive(Archive, UserCtx),
    ok = remove_single_archive(RelatedArchive, UserCtx).


%% @private
-spec remove_single_archive(archive:id() | archive:doc(), user_ctx:ctx()) -> ok | error().
remove_single_archive(undefined, _UserCtx) ->
    ok;
remove_single_archive(ArchiveDoc = #document{}, UserCtx) ->
    {ok, ArchiveId} = archive:get_id(ArchiveDoc),
    case archive:delete(ArchiveId) of
        ok ->
            {ok, SpaceId} = archive:get_space_id(ArchiveDoc),
            ArchiveDocCtx = file_ctx:new_by_uuid(?ARCHIVE_DIR_UUID(ArchiveId), SpaceId),
            % TODO VFS-7718 Should it be possible to register many callbacks in case of parallel purge requests?
            delete_req:delete_using_trash(UserCtx, ArchiveDocCtx, true),
            
            {ok, DatasetId} = archive:get_dataset_id(ArchiveDoc),
            {ok, Timestamp} = archive:get_creation_time(ArchiveDoc),
            {ok, ParentArchiveId} = archive:get_parent(ArchiveDoc),
            ParentArchiveId =/= undefined andalso archives_forest:delete(ParentArchiveId, SpaceId, ArchiveId),
            archives_list:delete(DatasetId, SpaceId, ArchiveId, Timestamp);
        ?ERROR_NOT_FOUND ->
            % there was race with other process removing the archive
            ok
    end;
remove_single_archive(ArchiveId, UserCtx) ->
    case archive:get(ArchiveId) of
        {ok, ArchiveDoc} -> remove_single_archive(ArchiveDoc, UserCtx);
        ?ERROR_NOT_FOUND -> ok;
        {error, _} = Error -> Error
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
-spec get_aggregated_stats(archive:doc() | archive:id()) -> archive_stats:record().
get_aggregated_stats(ArchiveDoc = #document{}) ->
    {ok, ArchiveStats} = archive:get_stats(ArchiveDoc),
    case archive:is_finished(ArchiveDoc) of
        true ->
            ArchiveStats;
        false ->
            {ok, ArchiveId} = archive:get_id(ArchiveDoc),
            NestedArchivesStats = get_nested_archives_stats(ArchiveId),
            archive_stats:sum(ArchiveStats, NestedArchivesStats)
    end;
get_aggregated_stats(ArchiveId) ->
    {ok, ArchiveDoc} = archive:get(ArchiveId),
    get_aggregated_stats(ArchiveDoc).


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
