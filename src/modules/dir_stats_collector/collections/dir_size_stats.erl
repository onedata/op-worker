%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for counting children and data size inside a directory
%%% (recursively, i.e. including all its subdirectories).
%%% It provides following statistics for each directory:
%%%    - ?REG_FILE_AND_LINK_COUNT - total number of regular files, hardlinks and symlinks,
%%%    - ?DIR_COUNT - total number of nested directories,
%%%    - ?LOGICAL_SIZE - total size in case of download (hardlinks of same file are downloaded multiple times),
%%%    - ?VIRTUAL_SIZE - total byte size of the logical data (if file has multiple hardlinks,
%%%                    size is counted only for first reference),
%%%    - ?PHYSICAL_SIZE(StorageId) - physical byte size on a specific storage.
%%% NOTE: the virtual size is not a sum of sizes on different storages, as the blocks stored
%%%       on different storages may overlap.
%%% NOTE: all references have the same LOGICAL_SIZE, but only first has VIRTUAL_SIZE set
%%%       (it is equal to LOGICAL_SIZE for this reference).
%%%
%%% This module offers two types of statistics in its API:
%%%   * current_stats() - a collection with current values for each statistic,
%%%   * historical_stats() - time series collection slice showing the changes of stats in time.
%%% Internally, both collections are kept in the same underlying persistent
%%% time series collection - internal_stats(). The current statistics are stored
%%% in the special ?CURRENT_METRIC. Additionally, the internal_stats() hold dir stats
%%% incarnation info in a separate time series. The internal_stats() are properly
%%% trimmed into current_stats() and/or historical_stats() when these collections are retrieved.
%%%
%%% NOTE: Functions that report changes of file size have to be called from the inside of replica_synchronizer,
%%%       to prevent races between changes of size and references list.
%%%
%%% NOTE: Sizes of opened deleted files (files inside OPENED_DELETED_FILES_DIR) are counted differently. Their
%%%       ?LOGICAL_SIZE is always 0 as they should be used only via existing handles and downloading
%%%       require opening of file.
%%% @end
%%%-------------------------------------------------------------------
-module(dir_size_stats).
-author("Michal Wrzeszcz").


-behavior(dir_stats_collection_behaviour).


-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_time_series.hrl").
-include_lib("cluster_worker/include/time_series/browsing.hrl").
-include_lib("ctool/include/time_series/common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").


%% API - generic stats
-export([get_stats/1, get_stats/2, browse_historical_stats_collection/2, delete_stats/1]).
%% API - reporting file size changes
-export([report_virtual_size_changed/2, report_logical_size_changed/2, report_physical_size_changed/3]).
%% API - reporting file count changes
-export([report_file_created/2, report_file_created_without_state_check/2, report_file_deleted/2]).
%% API - hooks
-export([on_link_register/2, on_link_deregister/1, on_local_file_delete/1,
    report_remote_links_change/2, handle_references_list_changes/4,
    on_opened_file_delete/2, on_deleted_file_close/2]).

%% dir_stats_collection_behaviour callbacks
-export([
    acquire/1, consolidate/3, on_collection_move/2, save/3, delete/1, init_dir/1, init_child/2,
    compress/1, decompress/1
]).

%% datastore_model callbacks
-export([get_ctx/0]).


-type ctx() :: datastore:ctx().

%% see the module doc
-type current_stats() :: dir_stats_collection:collection().
-type historical_stats() :: time_series_collection:slice().
-type internal_stats() :: time_series_collection:slice().

-export_type([current_stats/0]).

-define(CTX, #{
    model => ?MODULE
}).

-define(NOW(), global_clock:timestamp_seconds()).
% Metric storing current value of statistic or incarnation (depending on time series)
-define(CURRENT_METRIC, <<"current">>).
% Time series storing incarnation - historical values are not required
% but usage of time series allows keeping everything in single structure
-define(INCARNATION_TIME_SERIES, <<"incarnation">>).

-define(ERROR_HANDLING_MODE, op_worker:get_env(dir_size_stats_init_errors_handling_mode, repeat)).


-define(IGNORE_LOCATION_MISSING(TO_EXECUTE, ON_LOCATION_MISSING), try
    TO_EXECUTE
catch
    _:{error, file_location_missing} ->
        % Do not log error - if file_location is missing no stats had been counted for file
        ON_LOCATION_MISSING;
    Class:Reason:Stacktrace  ->
        ?error_exception(Class, Reason, Stacktrace),
        ok
end).
-define(IGNORE_LOCATION_MISSING(TO_EXECUTE), ?IGNORE_LOCATION_MISSING(TO_EXECUTE, ok)).

-define(FLUSH(TO_EXECUTE),
    fslogic_cache:flush(),
    TO_EXECUTE
).

-record(reference_list_changes, {
    added = [] :: file_meta_hardlinks:references_list(),
    removed = [] :: file_meta_hardlinks:references_list(),
    % If main reference is removed, it is stored in this field as it has to be treated differently
    % (the field is list as this record can aggregate information about multiple operations).
    % The reason for this special handling is that only one reference per file can have VIRTUAL_SIZE greater than 0
    % (all references have the same LOGICAL_SIZE, but only first has VIRTUAL_SIZE set).
    removed_main_refs = [] :: file_meta_hardlinks:references_list()
}).

%%%===================================================================
%%% API - generic stats
%%%===================================================================

-spec get_stats(file_id:file_guid()) -> {ok, current_stats()} | dir_stats_collector:error().
get_stats(Guid) ->
    get_stats(Guid, all).


%%--------------------------------------------------------------------
%% @doc
%% Provides subset of collection's statistics.
%% @end
%%--------------------------------------------------------------------
-spec get_stats(file_id:file_guid(), dir_stats_collection:stats_selector()) ->
    {ok, current_stats()} | dir_stats_collector:error().
get_stats(Guid, StatNames) ->
    dir_stats_collector:get_stats(Guid, ?MODULE, StatNames).


-spec browse_historical_stats_collection(file_id:file_guid(), ts_browse_request:record()) -> 
    {ok, ts_browse_result:record()} | dir_stats_collector:collecting_status_error() | ?ERROR_INTERNAL_SERVER_ERROR.
browse_historical_stats_collection(Guid, BrowseRequest) ->
    case dir_stats_service_state:is_active(file_id:guid_to_space_id(Guid)) of
        true ->
            case dir_stats_collector:flush_stats(Guid, ?MODULE) of
                ok ->
                    Uuid = file_id:guid_to_uuid(Guid),
                    case datastore_time_series_collection:browse(?CTX, Uuid, BrowseRequest) of
                        {ok, BrowseResult} -> {ok, internal_to_historical_stats_browse_result(BrowseResult)};
                        {error, not_found} -> {ok, gen_empty_historical_stats_browse_result(BrowseRequest, Guid)};
                        {error, _} = Error2 -> Error2
                    end;
                {error, _} = Error ->
                    Error
            end;
        false ->
            ?ERROR_DIR_STATS_DISABLED_FOR_SPACE
    end.


-spec delete_stats(file_id:file_guid()) -> ok.
delete_stats(Guid) ->
    dir_stats_collector:delete_stats(Guid, ?MODULE).


%%%===================================================================
%%% API - reporting file size changes
%%%===================================================================


-spec report_virtual_size_changed(file_id:file_guid(), integer()) -> ok.
report_virtual_size_changed(_Guid, 0) ->
    ok;
report_virtual_size_changed(Guid, SizeDiff) ->
    {Uuid, SpaceId} = file_id:unpack_guid(Guid),
    case get_conflict_protected_reference_list(Uuid) of
        [?OPENED_DELETED_FILE_LINK_PATTERN = MainRef] ->
            ok = dir_stats_collector:update_stats_of_parent(file_id:pack_guid(MainRef, SpaceId),
                ?MODULE, #{?VIRTUAL_SIZE => SizeDiff});
        [MainRef | References] ->
            ok = dir_stats_collector:update_stats_of_parent(file_id:pack_guid(MainRef, SpaceId), ?MODULE,
                #{?VIRTUAL_SIZE => SizeDiff, ?LOGICAL_SIZE => SizeDiff}),
            lists:foreach(fun(Ref) ->
                ok = dir_stats_collector:update_stats_of_parent(file_id:pack_guid(Ref, SpaceId), ?MODULE,
                    #{?LOGICAL_SIZE => SizeDiff})
            end, References);
        [] ->
            ok = dir_stats_collector:update_stats_of_parent(Guid, ?MODULE, #{?VIRTUAL_SIZE => SizeDiff})
    end.


-spec report_logical_size_changed(file_id:file_guid(), integer()) -> ok.
report_logical_size_changed(_Guid, 0) ->
    ok;
report_logical_size_changed(Guid, SizeDiff) ->
    ok = dir_stats_collector:update_stats_of_parent(Guid, ?MODULE, #{?LOGICAL_SIZE => SizeDiff}).


-spec report_physical_size_changed(file_id:file_guid(), storage:id(), integer()) -> ok.
report_physical_size_changed(_Guid, _StorageId, 0) ->
    ok;
report_physical_size_changed(Guid, StorageId, SizeDiff) ->
    {Uuid, SpaceId} = file_id:unpack_guid(Guid),
    ok = case get_conflict_protected_reference_list(Uuid) of
        [MainRef | _] ->
            dir_stats_collector:update_stats_of_parent(
                file_id:pack_guid(MainRef, SpaceId), ?MODULE, #{?PHYSICAL_SIZE(StorageId) => SizeDiff});
        _ ->
            dir_stats_collector:update_stats_of_parent(Guid, ?MODULE, #{?PHYSICAL_SIZE(StorageId) => SizeDiff})
    end.


%%%===================================================================
%%% API - reporting file count changes
%%%===================================================================

-spec report_file_created(onedata_file:type(), file_id:file_guid()) -> ok.
report_file_created(?DIRECTORY_TYPE, Guid) ->
    update_stats(Guid, #{?DIR_COUNT => 1});
report_file_created(_, Guid) ->
    update_stats(Guid, #{?REG_FILE_AND_LINK_COUNT => 1}).


-spec report_file_created_without_state_check(onedata_file:type(), file_id:file_guid()) -> ok.
report_file_created_without_state_check(?DIRECTORY_TYPE, Guid) ->
    ok = dir_stats_collector:update_stats_of_dir_without_state_check(Guid, ?MODULE, #{?DIR_COUNT => 1});
report_file_created_without_state_check(_, Guid) ->
    ok = dir_stats_collector:update_stats_of_dir_without_state_check(Guid, ?MODULE, #{?REG_FILE_AND_LINK_COUNT => 1}).


-spec report_file_deleted(onedata_file:type(), file_id:file_guid()) -> ok.
report_file_deleted(?DIRECTORY_TYPE, Guid) ->
    update_stats(Guid, #{?DIR_COUNT => -1});
report_file_deleted(_, Guid) ->
    update_stats(Guid, #{?REG_FILE_AND_LINK_COUNT => -1}).


%%%===================================================================
%%% API - hooks
%%%===================================================================

-spec on_link_register(file_ctx:ctx(), file_id:file_guid()) -> ok.
on_link_register(TargetFileCtx, TargetParentGuid) ->
    case file_ctx:get_or_create_local_regular_file_location_doc(TargetFileCtx, true, true) of
        {#document{value = #file_location{size = undefined}} = FMDoc, _} ->
            case fslogic_blocks:upper(fslogic_location_cache:get_blocks(FMDoc)) of
                0 -> ok;
                Size -> update_stats(TargetParentGuid, #{?LOGICAL_SIZE => Size})
            end;
        {#document{value = #file_location{size = 0}}, _} ->
            ok;
        {#document{value = #file_location{size = Size}}, _} ->
            update_stats(TargetParentGuid, #{?LOGICAL_SIZE => Size})
    end.


-spec on_link_deregister(file_ctx:ctx()) -> ok.
on_link_deregister(FileCtx) ->
    LinkUuid = file_ctx:get_logical_uuid_const(FileCtx),
    LinkGuid = file_ctx:get_logical_guid_const(FileCtx),
    ReferencedFileCtx = file_ctx:ensure_based_on_referenced_guid(FileCtx),

    ?IGNORE_LOCATION_MISSING(?FLUSH(begin
        {FileSizes, _} = file_ctx:prepare_file_size_summary(ReferencedFileCtx, throw_on_missing_location),
        case file_meta_hardlinks:list_references(file_ctx:get_logical_uuid_const(ReferencedFileCtx)) of
            {ok, [LinkUuid]} ->
                update_using_size_summary(LinkGuid, FileSizes, true, subtract),
                update_using_size_summary(file_ctx:get_logical_guid_const(ReferencedFileCtx),
                    FileSizes, true, add);
            {ok, [LinkUuid, NextRef | _]} ->
                update_using_size_summary(LinkGuid, FileSizes, true, subtract),
                SpaceId = file_ctx:get_space_id_const(FileCtx),
                update_using_size_summary(file_id:pack_guid(NextRef, SpaceId), FileSizes, false, add);
            {ok, References} ->
                case lists:member(LinkUuid, References) of
                    true ->
                        report_logical_size_changed(LinkGuid,
                            -1 * proplists:get_value(virtual, FileSizes));
                    false ->
                        ok
                end;
            _ ->
                ok
        end
    end)).


-spec on_local_file_delete(file_ctx:ctx()) -> file_meta_hardlinks:references_presence().
on_local_file_delete(FileCtx) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    replica_synchronizer:apply(FileCtx, fun() ->
        ?IGNORE_LOCATION_MISSING(?FLUSH(begin
            {FileSizes, _} = file_ctx:prepare_file_size_summary(FileCtx, throw_on_missing_location),
            case file_meta_hardlinks:list_references(FileUuid) of
                {ok, []} ->
                    no_references_left;
                {ok, [NextRef | _]} ->
                    update_using_size_summary(file_ctx:get_logical_guid_const(FileCtx), FileSizes, true, subtract),
                    update_using_size_summary(file_id:pack_guid(NextRef, SpaceId), FileSizes, false, add),
                    has_at_least_one_reference
            end
        end), file_meta_hardlinks:inspect_references(FileUuid))
    end).


-spec report_remote_links_change(file_meta:uuid(), od_space:id()) -> ok.
report_remote_links_change(Uuid, SpaceId) ->
    % Check is uuid is dir space uuid to prevent its creation by file_meta:get_including_deleted/1
    case fslogic_file_id:is_space_dir_uuid(Uuid) of
        true ->
            % Send empty update to prevent race between links sync and initialization
            update_stats(file_id:pack_guid(Uuid, SpaceId), #{});
        false ->
            case file_meta:get_including_deleted(Uuid) of
                {ok, Doc} ->
                    case file_meta:get_type(Doc) of
                        ?DIRECTORY_TYPE ->
                            % Send empty update to prevent race between links sync and initialization
                            update_stats(file_id:pack_guid(Uuid, SpaceId), #{});
                        _ ->
                            ok
                    end;
                ?ERROR_NOT_FOUND ->
                    ok
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Handles changes of references list. Handling of reference changes has to be performed inside synchronizer but
%% call to synchronizer should not block reference conflicts resolving. Thus, replica_synchronizer:apply is spawned.
%% Arguments are passed by node_cache to handle possible races with file size changes (functions handling size changes
%% have access to list of changes that is being processed).
%% @end
%%--------------------------------------------------------------------
-spec handle_references_list_changes(file_id:file_guid(), file_meta_hardlinks:references_list(),
    file_meta_hardlinks:references_list(), file_meta_hardlinks:references_list()) -> ok.
handle_references_list_changes(Guid, AddedReferences, RemovedReferences, OldRefsList) ->
    FileCtx = file_ctx:new_by_guid(Guid),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    Uuid = file_ctx:get_referenced_uuid_const(FileCtx),
    node_cache:update({?MODULE, Uuid}, fun(NotProcessedReferenceChanges) ->
        NewRemovedMainRefs = case OldRefsList of
            [FirstOldRef | _] ->
                case lists:member(FirstOldRef, RemovedReferences) of
                    true -> NotProcessedReferenceChanges#reference_list_changes.removed_main_refs ++ [FirstOldRef];
                    false -> NotProcessedReferenceChanges#reference_list_changes.removed_main_refs
                end;
            _ ->
                NotProcessedReferenceChanges#reference_list_changes.removed_main_refs
        end,
        {ok, NotProcessedReferenceChanges#reference_list_changes{
            added = NotProcessedReferenceChanges#reference_list_changes.added ++ AddedReferences,
            removed = NotProcessedReferenceChanges#reference_list_changes.removed ++ RemovedReferences -- NewRemovedMainRefs,
            removed_main_refs = NewRemovedMainRefs
        }, infinity}
    end, #reference_list_changes{}),

    spawn(fun() ->
        replica_synchronizer:apply(FileCtx, fun() ->
            ReferenceListChanges = node_cache:get({?MODULE, Uuid}, #reference_list_changes{}),
            AddedList = ReferenceListChanges#reference_list_changes.added,
            RemovedList = ReferenceListChanges#reference_list_changes.removed,
            RemovedMainRefs = ReferenceListChanges#reference_list_changes.removed_main_refs,

            ?IGNORE_LOCATION_MISSING(?FLUSH(begin
                {FileSizes, _} = file_ctx:prepare_file_size_summary(FileCtx, throw_on_missing_location),
                Size = proplists:get_value(virtual, FileSizes),

                report_logical_size_changed_for_ref_list(AddedList -- RemovedList, SpaceId, Size),
                report_logical_size_changed_for_ref_list(RemovedList -- AddedList, SpaceId, -Size),

                case RemovedMainRefs of
                    [] ->
                        ok;
                    [MainRef | _] ->
                        update_using_size_summary(file_id:pack_guid(MainRef, SpaceId), FileSizes, true, subtract),

                        % check changes resolve has finished (it blocks updates on file)
                        file_meta:update(Uuid, fun(_) -> {error, do_nothing} end),

                        case file_meta_hardlinks:list_references(Uuid) of
                            {ok, [NewMainRef | _]} ->
                                update_using_size_summary(file_id:pack_guid(NewMainRef, SpaceId), FileSizes, false, add);
                            _ ->
                                update_using_size_summary(Guid, FileSizes, true, add)
                        end
                end
            end)),

            node_cache:update({?MODULE, Uuid}, fun(NewReferenceListChanges) ->
                NewRecord = #reference_list_changes{
                    added = NewReferenceListChanges#reference_list_changes.added -- AddedList,
                    removed = NewReferenceListChanges#reference_list_changes.removed -- RemovedList,
                    removed_main_refs = NewReferenceListChanges#reference_list_changes.removed_main_refs -- RemovedMainRefs
                },
                case NewRecord of
                    #reference_list_changes{added = [], removed = [], removed_main_refs = []} ->
                        clear;
                    _ ->
                        {ok, NewRecord, infinity}
                end
            end, #reference_list_changes{}),

            ok
        end)
    end),
    ok.


-spec on_opened_file_delete(file_ctx:ctx(), file_meta:uuid()) -> ok.
on_opened_file_delete(FileCtx, TmpLinkUuid) ->
    ReferencedFileCtx = file_ctx:ensure_based_on_referenced_guid(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    ?IGNORE_LOCATION_MISSING(?FLUSH(begin
        {FileSizes, _} = file_ctx:prepare_file_size_summary(ReferencedFileCtx, throw_on_missing_location),
        update_using_size_summary(file_ctx:get_logical_guid_const(ReferencedFileCtx), FileSizes, true, subtract),
        ok = dir_stats_collector:update_stats_of_parent(file_id:pack_guid(TmpLinkUuid, SpaceId), ?MODULE,
            size_summary_to_stats(FileSizes, #{?REG_FILE_AND_LINK_COUNT => 1}, false))
    end)).


-spec on_deleted_file_close(file_ctx:ctx(), file_meta:uuid()) -> ok.
on_deleted_file_close(FileCtx, TmpLinkUuid) ->
    ReferencedFileCtx = file_ctx:ensure_based_on_referenced_guid(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    ?IGNORE_LOCATION_MISSING(?FLUSH(begin
        {FileSizes, _} = file_ctx:prepare_file_size_summary(ReferencedFileCtx, throw_on_missing_location),
        ok = dir_stats_collector:update_stats_of_parent(file_id:pack_guid(TmpLinkUuid, SpaceId), ?MODULE,
            size_summary_to_stats(lists:map(fun({K, V}) -> {K, -V} end, FileSizes), #{?REG_FILE_AND_LINK_COUNT => -1}, false)),
        update_using_size_summary(file_ctx:get_logical_guid_const(ReferencedFileCtx), FileSizes, false, add)
    end)).


%%%===================================================================
%%% dir_stats_collection_behaviour callbacks
%%%===================================================================

-spec acquire(file_id:file_guid()) -> {dir_stats_collection:collection(), non_neg_integer()}.
acquire(Guid) ->
    Uuid = file_id:guid_to_uuid(Guid),
    SliceLayout = #{?ALL_TIME_SERIES => [?CURRENT_METRIC]},
    case datastore_time_series_collection:get_slice(?CTX, Uuid, SliceLayout, #{window_limit => 1}) of
        {ok, Slice} ->
            {internal_stats_to_current_stats(Slice), internal_stats_to_incarnation(Slice)};
        {error, not_found} ->
            {gen_empty_current_stats(Guid), 0}
    end.


-spec consolidate(dir_stats_collection:stat_name(), dir_stats_collection:stat_value(),
    dir_stats_collection:stat_value()) -> dir_stats_collection:stat_value().
consolidate(_, Value, Diff) ->
    Value + Diff.


-spec on_collection_move(dir_stats_collection:stat_name(), dir_stats_collection:stat_value()) ->
    {update_source_parent, dir_stats_collection:stat_value()}.
on_collection_move(_, Value) ->
    {update_source_parent, -Value}.



-spec save(file_id:file_guid(), dir_stats_collection:collection(), non_neg_integer() | current) -> ok.
save(Guid, Collection, Incarnation) ->
    Uuid = file_id:guid_to_uuid(Guid),
    Timestamp = ?NOW(),
    IncarnationConsumeSpec = case Incarnation of
        current -> #{};
        _ -> #{?INCARNATION_TIME_SERIES => #{?CURRENT_METRIC => [{Timestamp, Incarnation}]}}
    end,
    StatsConsumeSpec = maps:map(fun(_StatName, Value) -> #{?ALL_METRICS => [{Timestamp, Value}]} end, Collection),
    ConsumeSpec = maps:merge(StatsConsumeSpec, IncarnationConsumeSpec),
    case datastore_time_series_collection:consume_measurements(?CTX, Uuid, ConsumeSpec) of
        ok ->
            ok;
        {error, not_found} ->
            Config = internal_stats_config(Guid),
            % NOTE: single pes process is dedicated for each guid so race resulting in
            % {error, already_exists} is impossible - match create answer to ok
            ok = datastore_time_series_collection:create(?CTX, Uuid, Config),
            save(Guid, Collection, Incarnation);
        ?ERROR_TSC_MISSING_LAYOUT(MissingLayout) ->
            MissingConfig = maps:with(maps:keys(MissingLayout), internal_stats_config(Guid)),
            ok = datastore_time_series_collection:incorporate_config(?CTX, Uuid, MissingConfig),
            ok = datastore_time_series_collection:consume_measurements(?CTX, Uuid, ConsumeSpec)
    end.


-spec delete(file_id:file_guid()) -> ok.
delete(Guid) ->
    case datastore_time_series_collection:delete(?CTX, file_id:guid_to_uuid(Guid)) of
        ok -> ok;
        ?ERROR_NOT_FOUND -> ok
    end.


-spec init_dir(file_id:file_guid()) -> dir_stats_collection:collection().
init_dir(Guid) ->
    gen_empty_current_stats(Guid).


-spec init_child(file_id:file_guid(), boolean()) -> dir_stats_collection:collection().
init_child(Guid, IncludeDeleted) ->
    case file_meta:get_including_deleted(file_id:guid_to_uuid(Guid)) of
        {ok, Doc} ->
            case file_meta:is_deleted(Doc) andalso not IncludeDeleted of
                true ->
                    % Race with file deletion - stats will be invalidated by next update
                    gen_empty_current_stats_and_handle_errors(Guid);
                false ->
                    init_existing_child(Guid, Doc)
            end;
        ?ERROR_NOT_FOUND ->
            % Race with file deletion - stats will be invalidated by next update
            gen_empty_current_stats_and_handle_errors(Guid)
    end.


-spec compress(dir_stats_collection:collection()) -> term().
compress(Collection) ->
    maps:fold(fun(StatName, Values, Acc) ->
        Acc#{encode_stat_name(StatName) => Values}
    end, #{}, Collection).

-spec decompress(term()) -> dir_stats_collection:collection().
decompress(EncodedCollection) ->
    maps:fold(fun(StatName, Values, Acc) ->
        Acc#{decode_stat_name(StatName) => Values}
    end, #{}, EncodedCollection).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec init_existing_child(file_id:file_guid(), file_meta:doc()) -> dir_stats_collection:collection().
init_existing_child(Guid, #document{key = Uuid} = Doc) ->
    case file_meta:get_type(Doc) of
        ?DIRECTORY_TYPE ->
            try
                EmptyCurrentStats = gen_empty_current_stats(Guid), % TODO VFS-9204 - maybe refactor as gen_empty_current_stats
                % gets storage_id that is also used by prepare_file_size_summary
                EmptyCurrentStats#{?DIR_COUNT => 1}
            catch
                Error:Reason:Stacktrace ->
                    handle_init_error(Guid, Error, Reason, Stacktrace),
                    #{?DIR_COUNT => 1, ?DIR_ERROR_COUNT => 1}
            end;
        Type ->
            try
                case {Type, Uuid} of
                    {?REGULAR_FILE_TYPE,_} ->
                        init_reg_file(Guid);
                    {?LINK_TYPE, ?OPENED_DELETED_FILE_LINK_PATTERN} -> % Hardlink of deleted opened file
                        (init_reg_file(Guid))#{?LOGICAL_SIZE => 0};
                    {?LINK_TYPE, _} -> % Standard hardlink
                        case file_meta_hardlinks:list_references(fslogic_file_id:ensure_referenced_uuid(Uuid)) of
                            {ok, [Uuid | _]} ->
                                init_reg_file(Guid); % Referenced file_meta is deleted - first hardlink is counted as file
                            _ ->
                                init_hardlink(Guid)
                        end;
                    _ ->
                        % Syminks are counted with size 0
                        EmptyCurrentStats = gen_empty_current_stats(Guid),
                        EmptyCurrentStats#{?REG_FILE_AND_LINK_COUNT => 1}
                end
            catch
                Error:Reason:Stacktrace ->
                    handle_init_error(Guid, Error, Reason, Stacktrace),
                    #{?REG_FILE_AND_LINK_COUNT => 1, ?FILE_ERROR_COUNT => 1}
            end
    end.


%% @private
-spec init_reg_file(file_id:file_guid()) -> dir_stats_collection:collection().
init_reg_file(Guid) ->
    EmptyCurrentStats = gen_empty_current_stats(Guid),
    FileCtx = file_ctx:new_by_guid(fslogic_file_id:ensure_referenced_guid(Guid)),
    {FileSizes, _} = try
        file_ctx:prepare_file_size_summary(FileCtx, create_missing_location)
    catch
        throw:{error, {file_meta_missing, _}} ->
            % It is impossible to create file_location because of missing ancestor's file_meta.
            % Sizes will be counted on location creation.
            {[], FileCtx}
    end,
    size_summary_to_stats(FileSizes, EmptyCurrentStats#{?REG_FILE_AND_LINK_COUNT => 1}, true).


%% @private
-spec init_hardlink(file_id:file_guid()) -> dir_stats_collection:collection().
init_hardlink(Guid) ->
    EmptyCurrentStats = gen_empty_current_stats(Guid),
    FileCtx = file_ctx:new_by_guid(fslogic_file_id:ensure_referenced_guid(Guid)),
    case file_ctx:get_or_create_local_regular_file_location_doc(FileCtx, true, true) of
        {#document{value = #file_location{size = undefined}} = FLDoc, _} ->
            Size = fslogic_blocks:upper(fslogic_location_cache:get_blocks(FLDoc)),
            EmptyCurrentStats#{?REG_FILE_AND_LINK_COUNT => 1, ?LOGICAL_SIZE => Size};
        {#document{value = #file_location{size = Size}}, _} ->
            EmptyCurrentStats#{?REG_FILE_AND_LINK_COUNT => 1, ?LOGICAL_SIZE => Size}
    end.


%% @private
-spec update_stats(file_id:file_guid(), dir_stats_collection:collection()) -> ok.
update_stats(Guid, CollectionUpdate) ->
    ok = dir_stats_collector:update_stats_of_dir(Guid, ?MODULE, CollectionUpdate).


%% @private
-spec internal_stats_config(file_id:file_guid()) -> time_series_collection:config().
internal_stats_config(Guid) ->
    maps_utils:generate_from_list(fun
        (?INCARNATION_TIME_SERIES) ->
            {?INCARNATION_TIME_SERIES, current_metric_composition()};
        (StatName) ->
            {StatName, maps:merge(?DIR_SIZE_STATS_METRICS, current_metric_composition())}
    end, [?INCARNATION_TIME_SERIES | stat_names(Guid)]).


%% @private
-spec stat_names(file_id:file_guid()) -> [dir_stats_collection:stat_name()].
stat_names(Guid) ->
    SpaceId = file_id:guid_to_space_id(Guid),
    case space_logic:get_local_supporting_storage(SpaceId) of
        {ok, StorageId} ->
            [?REG_FILE_AND_LINK_COUNT, ?DIR_COUNT, ?FILE_ERROR_COUNT, ?DIR_ERROR_COUNT,
                ?VIRTUAL_SIZE, ?LOGICAL_SIZE, ?PHYSICAL_SIZE(StorageId)];
        {error, not_found} ->
            case space_logic:is_supported(?ROOT_SESS_ID, SpaceId, oneprovider:get_id_or_undefined()) of
                true -> throw({error, not_found});
                false -> throw({error, space_unsupported})
            end
    end.


%% @private
-spec current_metric_composition() -> time_series:metric_composition().
current_metric_composition() ->
    #{
        ?CURRENT_METRIC => #metric_config{
            resolution = 1,
            retention = 1,
            aggregator = last
        }
    }.


%% @private
-spec gen_default_historical_stats_layout(file_id:file_guid()) -> time_series_collection:layout().
gen_default_historical_stats_layout(Guid) ->
    MetricNames = maps:keys(?DIR_SIZE_STATS_METRICS),
    maps_utils:generate_from_list(fun(TimeSeriesName) -> {TimeSeriesName, MetricNames} end, stat_names(Guid)).


%% @private
-spec internal_stats_to_current_stats(internal_stats()) -> current_stats().
internal_stats_to_current_stats(InternalStats) ->
    maps:map(fun(_TimeSeriesName, #{?CURRENT_METRIC := Windows}) ->
        case Windows of
            [#window_info{value = Value}] -> Value;
            [] -> 0
        end
    end, maps:without([?INCARNATION_TIME_SERIES], InternalStats)).


%% @private
-spec internal_stats_to_historical_stats(internal_stats()) -> historical_stats().
internal_stats_to_historical_stats(InternalStats) ->
    maps:map(fun(_TimeSeriesName, WindowsPerMetric) ->
        maps:without([?CURRENT_METRIC], WindowsPerMetric)
    end, maps:without([?INCARNATION_TIME_SERIES], InternalStats)).


%% @private
-spec internal_layout_to_historical_stats_layout(time_series_collection:layout()) -> 
    time_series_collection:layout().
internal_layout_to_historical_stats_layout(InternalLayout) ->
    maps:map(fun(_TimeSeriesName, Metrics) ->
        lists:delete(?CURRENT_METRIC, Metrics)
    end, maps:without([?INCARNATION_TIME_SERIES], InternalLayout)).


%% @private
-spec internal_stats_to_incarnation(internal_stats()) -> non_neg_integer().
internal_stats_to_incarnation(#{?INCARNATION_TIME_SERIES := #{?CURRENT_METRIC := []}}) -> 0;
internal_stats_to_incarnation(#{?INCARNATION_TIME_SERIES := #{?CURRENT_METRIC := [#window_info{value = Value}]}}) -> Value.


%% @private
-spec internal_to_historical_stats_browse_result(ts_browse_result:record()) -> ts_browse_result:record().
internal_to_historical_stats_browse_result(#time_series_layout_get_result{layout = InternalLayout}) ->
    #time_series_layout_get_result{layout = internal_layout_to_historical_stats_layout(InternalLayout)};
internal_to_historical_stats_browse_result(#time_series_slice_get_result{slice = InternalStats}) ->
    #time_series_slice_get_result{slice = internal_stats_to_historical_stats(InternalStats)}.


%% @private
-spec gen_empty_historical_stats_browse_result(ts_browse_request:record(), file_id:file_guid()) ->
    ts_browse_result:record().
gen_empty_historical_stats_browse_result(#time_series_layout_get_request{}, Guid) ->
    #time_series_layout_get_result{layout = gen_default_historical_stats_layout(Guid)};
gen_empty_historical_stats_browse_result(#time_series_slice_get_request{}, Guid) ->
    #time_series_slice_get_result{slice = gen_empty_historical_stats(Guid)}.


%% @private
-spec gen_empty_current_stats(file_id:file_guid()) -> current_stats().
gen_empty_current_stats(Guid) ->
    maps_utils:generate_from_list(fun(StatName) -> {StatName, 0} end, stat_names(Guid)).


%% @private
-spec gen_empty_current_stats_and_handle_errors(file_id:file_guid()) -> current_stats().
gen_empty_current_stats_and_handle_errors(Guid) ->
    try
        gen_empty_current_stats(Guid)
    catch
        Error:Reason:Stacktrace ->
            handle_init_error(Guid, Error, Reason, Stacktrace),
            #{}
    end.


%% @private
-spec gen_empty_historical_stats(file_id:file_guid()) -> historical_stats().
gen_empty_historical_stats(Guid) ->
    MetricNames = maps:keys(?DIR_SIZE_STATS_METRICS),
    maps_utils:generate_from_list(fun(TimeSeriesName) ->
        {TimeSeriesName, maps_utils:generate_from_list(fun(MetricName) ->
            {MetricName, []}
        end, MetricNames)}
    end, stat_names(Guid)).


%% @private
-spec handle_init_error(file_id:file_guid(), term(), term(), list()) -> ok | no_return().
handle_init_error(Guid, Error, Reason, Stacktrace) ->
    case ?ERROR_HANDLING_MODE of
        ignore ->
            ?error_stacktrace("Error initializing size stats for ~p: ~p:~p",
                [Guid, Error, Reason], Stacktrace);
        silent_ignore ->
            ok;

        % throw to repeat init by collector
        repeat ->
            case datastore_runner:normalize_error(Reason) of
                no_connection_to_onezone ->
                    ok;
                _ ->
                    ?error_stacktrace("Error initializing size stats for ~p: ~p:~p",
                        [Guid, Error, Reason], Stacktrace)
            end,
            throw(dir_size_stats_init_error);
        silent_repeat ->
            throw(dir_size_stats_init_error);

        repeat_connection_errors ->
            case datastore_runner:normalize_error(Reason) of
                no_connection_to_onezone ->
                    % Collector handles problems with zone connection
                    throw(no_connection_to_onezone);
                _ ->
                    ?error_stacktrace("Error initializing size stats for ~p: ~p:~p",
                        [Guid, Error, Reason], Stacktrace)
            end
    end.


%% @private
-spec encode_stat_name(dir_stats_collection:stat_name()) -> non_neg_integer() | {non_neg_integer(), binary()}.
encode_stat_name(?REG_FILE_AND_LINK_COUNT) -> 0;
encode_stat_name(?DIR_COUNT) -> 1;
encode_stat_name(?FILE_ERROR_COUNT) -> 2;
encode_stat_name(?DIR_ERROR_COUNT) -> 3;
encode_stat_name(?VIRTUAL_SIZE) -> 4;
encode_stat_name(?PHYSICAL_SIZE(StorageId)) -> {5, StorageId};
encode_stat_name(?LOGICAL_SIZE) -> 6.


%% @private
-spec decode_stat_name(non_neg_integer() | {non_neg_integer(), binary()}) -> dir_stats_collection:stat_name().
decode_stat_name(0) -> ?REG_FILE_AND_LINK_COUNT;
decode_stat_name(1) -> ?DIR_COUNT;
decode_stat_name(2) -> ?FILE_ERROR_COUNT;
decode_stat_name(3) -> ?DIR_ERROR_COUNT;
decode_stat_name(4) -> ?VIRTUAL_SIZE;
decode_stat_name({5, StorageId}) -> ?PHYSICAL_SIZE(StorageId);
decode_stat_name(6) -> ?LOGICAL_SIZE.


%% @private
-spec get_conflict_protected_reference_list(file_meta:uuid()) -> file_meta_hardlinks:references_list().
get_conflict_protected_reference_list(Uuid) ->
    ReferencesList = case file_meta_hardlinks:list_references(Uuid) of
        {ok, List} ->
            List;
        {error, not_found} ->
            [Uuid] % Storage import creates location before file_meta
    end,

    #reference_list_changes{added = Added, removed = Removed, removed_main_refs = First} =
        node_cache:get({?MODULE, Uuid}, #reference_list_changes{}),
    case First ++ ReferencesList -- Added ++ Removed of
        [] ->
            case file_handles:is_file_opened(Uuid) of
                true -> [];
                false -> [Uuid] % Race on dbsync (file_meta deleted before size change is handled)
            end;
        FinalList ->
            FinalList
    end.


%% @private
-spec update_using_size_summary(file_id:file_guid(), file_ctx:file_size_summary(), boolean(), add | subtract) -> ok.
update_using_size_summary(Guid, SizeSummary, UpdateLogicalSize, add) ->
    ok = dir_stats_collector:update_stats_of_parent(Guid, ?MODULE,
        size_summary_to_stats(SizeSummary, #{}, UpdateLogicalSize));
update_using_size_summary(Guid, SizeSummary, UpdateLogicalSize, subtract) ->
    NegFileSizes = lists:map(fun({K, V}) -> {K, -V} end, SizeSummary),
    update_using_size_summary(Guid, NegFileSizes, UpdateLogicalSize, add).


%% @private
-spec size_summary_to_stats(file_ctx:file_size_summary(), dir_stats_collection:collection(), boolean()) ->
    dir_stats_collection:collection().
size_summary_to_stats(SizeSummary, InitialStats, UpdateLogicalSize) ->
    lists:foldl(fun
        ({virtual, Size}, Acc) when UpdateLogicalSize -> Acc#{?VIRTUAL_SIZE => Size, ?LOGICAL_SIZE => Size};
        ({virtual, Size}, Acc) -> Acc#{?VIRTUAL_SIZE => Size};
        ({StorageId, Size}, Acc) -> Acc#{?PHYSICAL_SIZE(StorageId) => Size}
    end, InitialStats, SizeSummary).


%% @private
-spec report_logical_size_changed_for_ref_list([file_meta:uuid()], od_space:id(), integer()) -> ok.
report_logical_size_changed_for_ref_list(Uuids, SpaceId, SizeDiff) ->
    lists:foreach(fun(RefUuid) ->
        report_logical_size_changed(file_id:pack_guid(RefUuid, SpaceId), SizeDiff)
    end, Uuids).