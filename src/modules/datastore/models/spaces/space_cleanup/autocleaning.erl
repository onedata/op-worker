%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model for holding information about autocleaning operations.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: binary().
-type status() :: scheduled | active | completed | cancelled  | failed.
-type autocleaning() :: #autocleaning{}.
-type doc() :: #document{value :: autocleaning()}.


-export_type([id/0, status/0]).

%% API
-export([list_reports_since/2, delete/2,
    mark_completed/1, mark_released_file/1, get_config/1, mark_active/2,
    mark_failed/1, start/3, mark_released_bytes/2, mark_released_file/2, get/1,
    list/1, remove_link/2, mark_processed_file/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0, get_posthooks/0,
    upgrade_record/2]).

-define(LINK_PREFIX, <<"autocleaning_">>).

-define(CTX, #{
    model => ?MODULE,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This function is responsible for starting autocleaning_controller.
%% If autocleaning operation is currently in progress, new operation
%% will be skipped ba autocleaning_controller.
%% @end
%%-------------------------------------------------------------------
-spec start(od_space:id(), autocleaning_config:config(), non_neg_integer()) -> ok.
start(SpaceId, CleanupConfig, CurrentSize) ->
    Target = autocleaning_config:get_target(CleanupConfig),
    BytesToRelease = CurrentSize - Target,
    case BytesToRelease > 0 of
        true ->
            NewDoc = #document{
                scope = SpaceId,
                value = Autocleaning = #autocleaning{
                    space_id = SpaceId,
                    started_at = time_utils:cluster_time_seconds(),
                    bytes_to_release = CurrentSize - Target,
                    status = scheduled,
                    config = CleanupConfig
                }
            },
            {ok, AutocleaningId} = ?extract_key(datastore_model:create(?CTX, NewDoc)),
            {ok, _} = space_storage:maybe_mark_cleanup_in_progress(SpaceId, AutocleaningId),
            ok = add_link(AutocleaningId, SpaceId),
            ok = autocleaning_controller:maybe_start(AutocleaningId, Autocleaning);
        _ ->
            ok
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns list of autocleaning reports, that has been scheduled later
%% than Since.
%% @end
%%-------------------------------------------------------------------
-spec list_reports_since(od_space:id(), non_neg_integer()) -> [maps:map()].
list_reports_since(SpaceId, Since) ->
    {ok, Reports} = for_each_autocleaning(SpaceId, fun(AutoCleaningId, AccIn) ->
        {ok, #document{value = Autocleaning}} = datastore_model:get(?CTX, AutoCleaningId),
        case {started_later_than(Autocleaning, Since),
              active_completed_or_failed(Autocleaning)} of
            {true, true} ->
                [get_info(Autocleaning) | AccIn];
            _ ->
                AccIn
        end
    end, []),
    Reports.

%%-------------------------------------------------------------------
%% @doc
%% Removes skipped autocleaning.
%% @end
%%-------------------------------------------------------------------
-spec delete(id(), od_space:id()) -> ok.
delete(AutocleaningId, SpaceId) ->
    remove_link(AutocleaningId, SpaceId),
    ok = datastore_model:delete(?CTX, AutocleaningId).

%%-------------------------------------------------------------------
%% @doc
%% Mark released file.
%% @end
%%-------------------------------------------------------------------
-spec mark_released_file(undefined | id()) -> {ok, id() | undefined}.
mark_released_file(undefined) ->
    {ok, undefined};
mark_released_file(AutocleaningId) ->
    datastore_model:update(?CTX, AutocleaningId, fun(AC = #autocleaning{
        released_files = ReleasedFiles
    }) ->
        {ok, AC#autocleaning{released_files = ReleasedFiles + 1}}
    end).

%%-------------------------------------------------------------------
%% @doc
%% Mark released file.
%% @end
%%-------------------------------------------------------------------
-spec mark_released_file(undefined | id(), non_neg_integer()) -> {ok, id() | undefined}.
mark_released_file(undefined, _) ->
    {ok, undefined};
mark_released_file(AutocleaningId, Size) ->
    datastore_model:update(?CTX, AutocleaningId, fun(AC = #autocleaning{
        released_files = ReleasedFiles,
        released_bytes = ReleasedBytes,
        files_processed = FilesProcessed
    }) ->
        {ok, AC#autocleaning{
            released_files = ReleasedFiles + 1,
            released_bytes = ReleasedBytes + Size,
            files_processed = FilesProcessed + 1
        }}
    end).

%%-------------------------------------------------------------------
%% @doc
%% Mark released bytes.
%% @end
%%-------------------------------------------------------------------
-spec mark_released_bytes(undefined | id(), non_neg_integer()) -> {ok, id() | undefined}.
mark_released_bytes(undefined, _) ->
    {ok, undefined};
mark_released_bytes(AutocleaningId, Size) ->
    datastore_model:update(?CTX, AutocleaningId, fun(AC = #autocleaning{
        released_bytes = ReleasedBytes
    }) ->
        {ok, AC#autocleaning{released_bytes = ReleasedBytes + Size}}
    end).

%%-------------------------------------------------------------------
%% @doc
%% Mark given autocleaning as active.
%% @end
%%-------------------------------------------------------------------
-spec mark_active(undefined | id(), non_neg_integer()) -> {ok, id() | undefined}.
mark_active(undefined, _) ->
    {ok, undefined};
mark_active(AutocleaningId, FilesToProcess) ->
    {ok, _} = datastore_model:update(?CTX, AutocleaningId, fun(AC) ->
        {ok, AC#autocleaning{
            status = active,
            files_to_process = FilesToProcess
        }}
    end).

%%-------------------------------------------------------------------
%% @doc
%% Mark given autocleaning as failed.
%% @end
%%-------------------------------------------------------------------
-spec mark_failed(undefined | id()) -> {ok, id() | undefined} | {error, term()}.
mark_failed(undefined) ->
    {ok, undefined};
mark_failed(AutocleaningId) ->
    case datastore_model:update(?CTX, AutocleaningId, fun(AC) ->
        {ok, AC#autocleaning{
            stopped_at = time_utils:cluster_time_seconds(),
            status = failed
        }}
    end) of
        {ok, #document{value = #autocleaning{space_id = SpaceId}}} ->
            {ok, _} = space_storage:mark_cleanup_finished(SpaceId);
        Error ->
            ?error_stacktrace("Fail to mark autocleaning ~p as failed due to ~p",
                [AutocleaningId, Error]),
            Error
    end.

%%-------------------------------------------------------------------
%% @doc
%% Mark given autocleaning as completed.
%% @end
%%-------------------------------------------------------------------
-spec mark_completed(undefined | id()) -> {ok, id() | undefined} | {error, term()}.
mark_completed(undefined) ->
    {ok, undefined};
mark_completed(AutocleaningId) ->
    case datastore_model:update(?CTX, AutocleaningId, fun(AC) ->
        {ok, AC#autocleaning{
            stopped_at = time_utils:cluster_time_seconds(),
            status = completed
        }}
    end) of
        {ok, #document{value = #autocleaning{space_id = SpaceId}}} ->
            {ok, _} = space_storage:mark_cleanup_finished(SpaceId);
        Error ->
            ?error_stacktrace("Fail to mark autocleaning ~p as completed due to ~p",
                [AutocleaningId, Error]),
            Error
    end.

%%-------------------------------------------------------------------
%% @doc
%% Increment files_processed counter.
%% @end
%%-------------------------------------------------------------------
-spec mark_processed_file(undefined | id()) -> {ok, id() | undefined} | {error, term()}.
mark_processed_file(undefined) ->
    {ok, undefined};
mark_processed_file(AutocleaningId) ->
    datastore_model:update(?CTX, AutocleaningId,
        fun(AC = #autocleaning{files_processed = FilesProcessed}) ->
            {ok, AC#autocleaning{files_processed = FilesProcessed + 1}}
        end).

%%-------------------------------------------------------------------
%% @doc
%% Returns autocleaning_config.
%% @end
%%-------------------------------------------------------------------
-spec get_config(autocleaning() | doc() | id()) -> autocleaning_config:config().
get_config(#autocleaning{config = Config}) ->
    Config;
get_config(#document{value = Autocleaning}) ->
    get_config(Autocleaning);
get_config(AutocleaningId) ->
    {ok, Doc} = datastore_model:get(?CTX, AutocleaningId),
    get_config(Doc).

%%-------------------------------------------------------------------
%% @doc
%% Returns autocleaning document.
%% @end
%%-------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(AutocleaningId) ->
    datastore_model:get(?CTX, AutocleaningId).

%%-------------------------------------------------------------------
%% @doc
%% Lists all autocleaning documents.
%% TODO autocleaning docs should be stored in sorted order VFS-4583
%% @end
%%-------------------------------------------------------------------
-spec list(od_space:id()) -> {ok, [id()]}.
list(SpaceId) ->
    for_each_autocleaning(SpaceId, fun(A, Acc) -> [A | Acc] end, []).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds link to autocleaning
%% @end
%%--------------------------------------------------------------------
-spec add_link(AutocleaningId :: id(), SpaceId :: od_space:id()) -> ok.
add_link(AutocleaningId, SpaceId) ->
    Ctx = ?CTX#{scope => SpaceId},
    TreeId = oneprovider:get_id(),
    {ok, _} = datastore_model:add_links(Ctx, space_link_root(SpaceId), TreeId,
        {AutocleaningId, <<>>}),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes link to autocleaning
%% @end
%%--------------------------------------------------------------------
-spec remove_link(AutocleaningId :: id(), SpaceId :: od_space:id()) -> ok.
remove_link(AutocleaningId, SpaceId) ->
    Ctx = ?CTX#{scope => SpaceId},
    TreeId = oneprovider:get_id(),
    ok = datastore_model:delete_links(Ctx, space_link_root(SpaceId), TreeId,
        AutocleaningId).

%%-------------------------------------------------------------------
%% @doc
%% Checks whether given autocleaning was started later than given timestamp.
%% @end
%%-------------------------------------------------------------------
-spec started_later_than(autocleaning(), non_neg_integer()) -> boolean().
started_later_than(#autocleaning{started_at = StartedAt}, Since) ->
    Since < StartedAt.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns true when given autocleaning status is active/completed/failed.
%% Returns false otherwise.
%% @end
%%-------------------------------------------------------------------
-spec active_completed_or_failed(autocleaning()) -> boolean().
active_completed_or_failed(#autocleaning{status = active}) -> true;
active_completed_or_failed(#autocleaning{status = completed}) -> true;
active_completed_or_failed(#autocleaning{status = failed}) -> true;
active_completed_or_failed(#autocleaning{}) -> false.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes callback for each successfully completed transfer
%% @end
%%--------------------------------------------------------------------
-spec for_each_autocleaning(SpaceId :: od_space:id(),
    Callback :: fun((id(), Acc0 :: term()) -> Acc :: term()),
    AccIn :: term()) -> {ok, Acc :: term()} | {error, term()}.
for_each_autocleaning(SpaceId, Callback, AccIn) ->
    Ctx = ?CTX#{scope => SpaceId},
    datastore_model:fold_links(Ctx, space_link_root(SpaceId), all,
        fun(#link{name = Name}, Acc) ->
            {ok, Callback(Name, Acc)}
        end, AccIn, #{}).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns info about given autocleaning.
%% @end
%%-------------------------------------------------------------------
-spec get_info(autocleaning()) -> proplists:proplist().
get_info(#autocleaning{
    started_at = StartedAt,
    stopped_at = StoppedAt,
    released_bytes = ReleasedBytes,
    bytes_to_release = BytesToRelease,
    released_files = ReleasedFiles
}) ->
    StoppedAt2 = case StoppedAt of
        undefined -> null;
        StoppedAt ->
            time_utils:epoch_to_iso8601(StoppedAt)
    end,
    [
        {startedAt, time_utils:epoch_to_iso8601(StartedAt)},
        {stoppedAt, StoppedAt2},
        {releasedBytes, ReleasedBytes},
        {bytesToRelease, BytesToRelease},
        {filesNumber, ReleasedFiles}
    ].

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns links tree root for given space.
%% @end
%%-------------------------------------------------------------------
-spec space_link_root(od_space:id()) -> binary().
space_link_root(SpaceId) ->
    <<?LINK_PREFIX/binary, SpaceId/binary>>.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Posthook responsible for calling autocleaning_changes:handle
%% function for locally updated document.
%% @end
%%-------------------------------------------------------------------
-spec run_on_doc_change(atom(), list(), term()) -> {ok, doc()}.
run_on_doc_change(update, [_, _, _], Result = {ok, Doc}) ->
    autocleaning_changes:handle(Doc),
    Result;
run_on_doc_change(_, _, Result) ->
    Result.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [
        fun run_on_doc_change/3
    ].

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {space_id, string},
        {started_at, integer},
        {stopped_at, integer},
        {released_bytes, integer},
        {bytes_to_release, integer},
        {released_files, integer},
        {status, atom},
        {config, {record, [
            {lower_file_size_limit, integer},
            {upper_file_size_limit, integer},
            {max_file_not_opened_hours, integer},
            {target, integer},
            {threshold, integer}
        ]}}
    ]};
get_record_struct(2) ->
    {record, [
        {space_id, string},
        {started_at, integer},
        {stopped_at, integer},
        {files_to_process, integer},
        {files_processed, integer},
        {released_bytes, integer},
        {bytes_to_release, integer},
        {released_files, integer},
        {status, atom},
        {config, {record, [
            {lower_file_size_limit, integer},
            {upper_file_size_limit, integer},
            {max_file_not_opened_hours, integer},
            {target, integer},
            {threshold, integer}
        ]}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, SpaceId, StartedAt, StoppedAt, ReleasedBytes,
    BytesToRelease, ReleasedFiles, Status, Config}
) -> {2, {?MODULE, SpaceId, StartedAt, StoppedAt, 0, 0, ReleasedBytes,
    BytesToRelease, ReleasedFiles, Status, Config}}.
