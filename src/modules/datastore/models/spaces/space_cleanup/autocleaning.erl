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
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: binary().
-type status() :: scheduled | active | completed | cancelled  | failed.
-type autocleaning() :: #autocleaning{}.
-type doc() :: #document{value :: autocleaning()}.


-export_type([id/0, status/0]).

%% API
-export([list_reports_since/2, remove_skipped/2,
    mark_completed/1, mark_released_file/2, get_config/1, mark_active/1, mark_failed/1, start/3]).

%% model_behaviour callbacks
-export([get/1, save/1, exists/1, delete/1, update/2, create/1, create_or_update/2,
    model_init/0, 'after'/5, before/4, list/0]).
-export([record_struct/1]).

-define(LINK_PREFIX, <<"autocleaning_">>).


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
                    started_at = utils:system_time_seconds(),
                    bytes_to_release = CurrentSize - Target,
                    status = scheduled,
                    config = CleanupConfig
                }
            },
            {ok, AutocleaningId} = create(NewDoc),
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
        {ok, #document{value = Autocleaning}} = get(AutoCleaningId),
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
    ok = delete(AutocleaningId).

%%-------------------------------------------------------------------
%% @doc
%% Mark released file and it's size.
%% @end
%%-------------------------------------------------------------------
-spec mark_released_file(undefined | id(), non_neg_integer()) -> {ok, id()}.
mark_released_file(undefined, _Size) ->
    {ok, undefined};
mark_released_file(AutocleaningId, Size) ->
    update(AutocleaningId, fun(AC = #autocleaning{
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
    update(AutocleaningId, fun(AC) ->
        {ok, AC#autocleaning{
            status = active
        }}
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
    update(AutocleaningId, fun(AC = #autocleaning{space_id = SpaceId}) ->
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
    update(AutocleaningId, fun(AC = #autocleaning{space_id = SpaceId}) ->
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
    {ok, Doc} = get(AutocleaningId),
    get_config(Doc).


%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {space_id, string},
        {started_at, integer},
        {stopped_at, integer},
        {released_bytes, integer},
        {bytes_to_release, integer},
        {released_files, integer},
        {status, atom},
        {autocleaning_config, {record, [
            {lower_file_size_limit, integer},
            {upper_file_size_limit, integer},
            {max_file_not_opened_hours, integer},
            {target, integer},
            {threshold, integer}
        ]}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    model:execute_with_default_context(?MODULE, create, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    model:execute_with_default_context(?MODULE, get, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    model:execute_with_default_context(?MODULE, delete, [Key]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(model:execute_with_default_context(?MODULE, exists, [Key])).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(datastore:document(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:generic_error().
create_or_update(Doc, Diff) ->
    model:execute_with_default_context(?MODULE, create_or_update, [Doc, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(autocleaning_bucket, [{autocleaning, update}], ?LOCALLY_CACHED_LEVEL,
        ?LOCALLY_CACHED_LEVEL, true, false, oneprovider:get_provider_id())#model_config{
        list_enabled = {true, return_errors}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    model:execute_with_default_context(?MODULE, list, [?GET_ALL, []]).

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
    model:execute_with_default_context(?MODULE, add_links, [
        space_link_root(SpaceId), {AutocleaningId, {AutocleaningId, ?MODEL_NAME}}
    ], [{scope, SpaceId}]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes link to autocleaning
%% @end
%%--------------------------------------------------------------------
-spec remove_link(AutocleaningId :: id(), SpaceId :: od_space:id()) -> ok.
remove_link(AutocleaningId, SpaceId) ->
    model:execute_with_default_context(?MODULE, delete_links,
        [space_link_root(SpaceId), AutocleaningId], [{scope, SpaceId}]
    ).


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
    model:execute_with_default_context(?MODULE, foreach_link, [space_link_root(SpaceId),
        fun(LinkName, _LinkTarget, Acc) ->
            Callback(LinkName, Acc)
        end, AccIn], [{scope, SpaceId}]).

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
            timestamp_utils:epoch_to_iso8601(StoppedAt)
    end,
    [
        {startedAt, timestamp_utils:epoch_to_iso8601(StartedAt)},
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
