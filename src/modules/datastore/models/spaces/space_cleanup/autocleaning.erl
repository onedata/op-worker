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
-export([maybe_start/1, list_reports_since/2, remove_skipped/2,
    mark_completed/1, mark_released_file/2, get_config/1, status/1,
    mark_active/1, mark_failed/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-define(LINK, <<"AUTOCLEANING_LINK">>).

-define(CTX, #{
    model => ?MODULE,
    mutator => oneprovider:get_provider_id(),
    local_links_tree_id => oneprovider:get_provider_id()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This function is responsible for scheduling autocleaning operations.
%% It schedules autocleaning if cleanup is enabled and current storage
%% occupation has reached threshold defined in cleanup configuration in
%% space_storage record.
%% @end
%%-------------------------------------------------------------------
-spec maybe_start(od_space:id()) -> ok.
maybe_start(SpaceId) ->
    case space_storage:get(SpaceId) of
        {ok, SpaceStorageDoc} ->
            case space_storage:is_cleanup_enabled(SpaceStorageDoc) of
                true ->
                    CleanupConfig = space_storage:get_autocleaning_config(SpaceStorageDoc),
                    CurrentSize = space_quota:current_size(SpaceId),
                    case autocleaning_config:should_start_autoclean(CurrentSize, CleanupConfig) of
                        true -> start(SpaceId, CleanupConfig, CurrentSize);
                        _ -> ok
                    end;
                _ -> ok
            end;
        {error, _} ->
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
-spec remove_skipped(id(), od_space:id()) -> ok.
remove_skipped(AutocleaningId, SpaceId) ->
    remove_link(AutocleaningId, SpaceId),
    ok = datastore_model:delete(?CTX, AutocleaningId).


%%-------------------------------------------------------------------
%% @doc
%% Mark released file and it's size.
%% @end
%%-------------------------------------------------------------------
-spec mark_released_file(undefined | id(), non_neg_integer()) -> {ok, id()}.
mark_released_file(undefined, _Size) ->
    {ok, undefined};
mark_released_file(AutocleaningId, Size) ->
    datastore_model:update(?CTX, AutocleaningId, fun(AC = #autocleaning{
        released_bytes = ReleasedBytes,
        released_files = ReleasedFiles
    }) ->
        {ok, AC#autocleaning{
            released_bytes = ReleasedBytes + Size,
            released_files = ReleasedFiles + 1
        }}
    end).

%%-------------------------------------------------------------------
%% @doc
%% Mark given autocleaning as active.
%% @end
%%-------------------------------------------------------------------
-spec mark_active(undefined | id()) -> {ok, id()}.
mark_active(undefined) ->
    {ok, undefined};
mark_active(AutocleaningId) ->
    datastore_model:update(?CTX, AutocleaningId, fun(AC) ->
        {ok, AC#autocleaning{status = active}}
    end).

%%-------------------------------------------------------------------
%% @doc
%% Mark given autocleaning as failed.
%% @end
%%-------------------------------------------------------------------
-spec mark_failed(undefined | id()) -> {ok, id()}.
mark_failed(undefined) ->
    {ok, undefined};
mark_failed(AutocleaningId) ->
    datastore_model:update(?CTX, AutocleaningId, fun(AC = #autocleaning{space_id = SpaceId}) ->
        {ok, _} = space_storage:mark_cleanup_finished(SpaceId),
        {ok, AC#autocleaning{
            stopped_at = utils:system_time_seconds(),
            status = failed
        }}
    end).

%%-------------------------------------------------------------------
%% @doc
%% Mark given autocleaning as completed.
%% @end
%%-------------------------------------------------------------------
-spec mark_completed(undefined | id()) -> {ok, id()}.
mark_completed(undefined) ->
    {ok, undefined};
mark_completed(AutocleaningId) ->
    datastore_model:update(?CTX, AutocleaningId, fun(AC = #autocleaning{space_id = SpaceId}) ->
        {ok, _} = space_storage:mark_cleanup_finished(SpaceId),
        {ok, AC#autocleaning{
            stopped_at = utils:system_time_seconds(),
            status = completed
        }}
    end).

%%-------------------------------------------------------------------
%% @private
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
%% @private
%% @doc
%% Returns proplist describing status of autocleaning in given space.
%% @end
%%-------------------------------------------------------------------
-spec status(od_space:id()) -> proplists:proplist().
status(SpaceId) ->
    CurrentSize = space_quota:current_size(SpaceId),
    InProgress = case space_storage:get_cleanup_in_progress(SpaceId) of
        undefined ->
            false;
        _ ->
            true
    end,
    [
        {inProgress, InProgress},
        {spaceOccupancy, CurrentSize}
    ].


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This function is responsible for starting autocleaning_controller.
%% If autocleaning_operation is currently in progress
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
                    started_at = utils:system_time_seconds(),
                    bytes_to_release = CurrentSize - Target,
                    status = scheduled,
                    config = CleanupConfig
                }
            },
            {ok, AutocleaningId} = ?extract_key(datastore_model:create(?CTX, NewDoc)),
            {ok, _} = space_storage:maybe_mark_cleanup_in_progress(SpaceId, AutocleaningId),
            add_link(AutocleaningId, SpaceId),
            autocleaning_controller:maybe_start(AutocleaningId, Autocleaning),
            ok;
        _ ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds link to autocleaning
%% @end
%%--------------------------------------------------------------------
-spec add_link(AutocleaningId :: id(), SpaceId :: od_space:id()) -> ok.
add_link(AutocleaningId, SpaceId) ->
    Ctx = ?CTX#{scope => SpaceId},
    TreeId = oneprovider:get_provider_id(),
    {ok, _} = datastore_model:add_links(Ctx, ?LINK, TreeId, {AutocleaningId, <<>>}),
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
    TreeId = oneprovider:get_provider_id(),
    ok = datastore_model:delete_links(Ctx, ?LINK, TreeId, AutocleaningId).

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
    datastore_model:fold_links(Ctx, ?LINK, all, fun(#link{name = Name}, Acc) ->
        {ok, Callback(Name, Acc)}
    end, AccIn, #{}).

%%-------------------------------------------------------------------
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
            timestamp_utils:epoch_to_iso8601(StoppedAt)
    end,
    [
        {startedAt, timestamp_utils:epoch_to_iso8601(StartedAt)},
        {stoppedAt, StoppedAt2},
        {releasedBytes, ReleasedBytes},
        {bytesToRelease, BytesToRelease},
        {filesNumber, ReleasedFiles}
    ].

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
    1.

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
    ]}.
