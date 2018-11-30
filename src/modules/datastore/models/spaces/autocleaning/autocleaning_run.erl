%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Model for holding information about auto-cleaning runs.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_run).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_runner.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: binary().
-type status() :: active | completed | failed.
-type record() :: #autocleaning_run{}.
-type diff() :: datastore:diff(record()).
-type doc() :: #document{value :: record()}.
-type error() :: error().

-export_type([id/0, status/0]).

%% API
-export([get/1, update/2, delete/2,
    start/3, list_reports_since/2,
    mark_completed/1, mark_failed/1,
    mark_released_file/2,
    mark_started_batch/2,
    get_token/1, get_bytes_to_release/1, get_released_bytes/1,
    restart/1
]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-define(CTX, #{
    model => ?MODULE,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec get(id()) -> {ok, doc()} | error().
get(ARId) ->
    datastore_model:get(?CTX, ARId).

-spec update(id(), diff()) -> {ok, doc()} | error().
update(ARId, UpdateFun) ->
    datastore_model:update(?CTX, ARId, UpdateFun).

-spec delete(id(), od_space:id()) -> ok.
delete(ARId, SpaceId) ->
    autocleaning_run_links:delete_link(ARId, SpaceId, get_started_at(ARId)),
    ok = datastore_model:delete(?CTX, ARId).

%%-------------------------------------------------------------------
%% @doc
%% This function is responsible for starting autocleaning_controller.
%% If autocleaning process is currently in progress, new process
%% won't start.
%% @end
%%-------------------------------------------------------------------
-spec start(od_space:id(), autocleaning:config(), non_neg_integer()) -> ok.
start(SpaceId, Config, CurrentSize) ->
    Target = autocleaning_config:get_target(Config),
    BytesToRelease = CurrentSize - Target,
    case BytesToRelease > 0 of
        true ->
            NewDoc = #document{
                scope = SpaceId,
                value = AutocleaningRun = #autocleaning_run{
                    status = active,
                    space_id = SpaceId,
                    started_at = StartTime = time_utils:cluster_time_seconds(),
                    bytes_to_release = CurrentSize - Target
                }
            },
            {ok, ARId} = ?extract_key(datastore_model:create(?CTX, NewDoc)),
            {ok, ACDoc} = autocleaning:maybe_mark_current_run(SpaceId, ARId),
             case autocleaning:get_current_run(ACDoc) of
                 ARId ->
                     ok = autocleaning_run_links:add_link(ARId, SpaceId, StartTime),
                     ok = autocleaning_controller:start(ARId, AutocleaningRun, Config);
                 _ ->
                     % other auto-cleaning run is in progress
                     ok
            end;
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
    {ok, ARIds} = autocleaning_run_links:list_since(SpaceId, Since),
    lists:filtermap(fun(ARId) ->
        case datastore_model:get(?CTX, ARId) of
            {ok, #document{value = AutocleaningRun}} ->
                {true, get_info(AutocleaningRun)};
            {error, not_found} ->
                ?error("Auto-cleaning run document ~p not found", [ARId]),
                false
        end
    end, ARIds).

-spec mark_released_file(undefined | id(), non_neg_integer()) -> ok.
mark_released_file(undefined, _) -> ok;
mark_released_file(ARId, Size) ->
    ok = ?extract_ok(update(ARId, fun(AC = #autocleaning_run{
        released_files = ReleasedFiles,
        released_bytes = ReleasedBytes
    }) ->
        {ok, AC#autocleaning_run{
            released_files = ReleasedFiles + 1,
            released_bytes = ReleasedBytes + Size
        }}
    end)).

-spec mark_failed(undefined | id()) -> ok | error().
mark_failed(undefined) -> ok;
mark_failed(ARId) ->
    case update(ARId, fun(AC) ->
        {ok, AC#autocleaning_run{
            stopped_at = time_utils:cluster_time_seconds(),
            status = failed
        }}
    end) of
        {ok, #document{value = #autocleaning_run{space_id = SpaceId}}} ->
            autocleaning:mark_run_finished(SpaceId);
        Error ->
            ?error_stacktrace("Fail to mark auto-cleaning run ~p as failed due to ~p",
                [ARId, Error]),
            Error
    end.

-spec mark_completed(undefined | id()) -> ok | error().
mark_completed(undefined) -> ok;
mark_completed(ARId) ->
    case update(ARId, fun(AC) ->
        {ok, AC#autocleaning_run{
            stopped_at = time_utils:cluster_time_seconds(),
            status = completed
        }}
    end) of
        {ok, #document{value = #autocleaning_run{space_id = SpaceId}}} ->
            autocleaning:mark_run_finished(SpaceId);
        Error ->
            ?error_stacktrace("Fail to mark auto-cleaning run ~p as completed due to ~p",
                [ARId, Error]),
            Error
    end.

mark_started_batch(ARId, Token) ->
    ok = ?extract_ok(update(ARId, fun(AC) ->
        {ok, AC#autocleaning_run{token = Token}}
    end)).

-spec get_token(record()) -> file_popularity_view:token().
get_token(#autocleaning_run{token = Token}) -> Token.

-spec get_bytes_to_release(record()) -> non_neg_integer().
get_bytes_to_release(#autocleaning_run{bytes_to_release = BytesToRelease}) ->
    BytesToRelease.

-spec get_released_bytes(record()) -> non_neg_integer().
get_released_bytes(#autocleaning_run{released_bytes = ReleasedBytes}) ->
    ReleasedBytes.

-spec get_started_at(record() | id()) -> non_neg_integer().
get_started_at(#autocleaning_run{started_at = StartedAt}) ->
    StartedAt;
get_started_at(ARId) ->
    {ok, #document{value = AR}} = autocleaning_run:get(ARId),
    get_started_at(AR).

-spec restart(id()) -> ok | error().
restart(ARId) ->
    case autocleaning_run:get(ARId) of
        {ok, #document{value = AR = #autocleaning_run{
            space_id = SpaceId,
            started_at = StartTime
        }}} ->
            case finished(AR) of
                false ->
                    % ensure that there is a link for given autocleaning_run
                    ok = autocleaning_run_links:add_link(ARId, SpaceId, StartTime),
                    Config = autocleaning:get_config(SpaceId),
                    case autocleaning_config:is_enabled(Config) of
                        true ->
                            ok = autocleaning_controller:start(ARId, AR, Config);
                        _ ->
                            {error, autocleaning_disabled}
                    end;
                true ->
                    {error, autocleaning_finished} 
            end;
        Error ->
            Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns info about given auto-cleaning run in the form
%% understandable by onepanel.
%% @end
%%-------------------------------------------------------------------
-spec get_info(record()) -> maps:maps().
get_info(#autocleaning_run{
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
    #{
        started_at => time_utils:epoch_to_iso8601(StartedAt),
        stopped_at => StoppedAt2,
        released_bytes => ReleasedBytes,
        bytes_to_release => BytesToRelease,
        files_number => ReleasedFiles
    }.

-spec finished(record()) -> boolean().
finished(#autocleaning_run{status = active}) -> false;
finished(#autocleaning_run{}) -> true.

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
        {token, {record, [
            {last_doc_id, string},
            {last_key, string},
            {end_key, string}
        ]}}
    ]}.