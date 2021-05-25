%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for storing information about archives.
%%% @end
%%%-------------------------------------------------------------------
-module(archive).
-author("Jakub Kudzia").

-include("modules/dataset/archive.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([create/7, get/1, modify_attrs/2, delete/1]).

% getters
-export([get_id/1, get_creation_time/1, get_dataset_id/1, get_space_id/1,
    get_state/1, get_config/1, get_preserved_callback/1, get_purged_callback/1,
    get_description/1, get_job_id/1, get_files_to_archive/1, get_files_archived/1,
    get_files_failed/1, get_bytes_archived/1, is_finished/1
]).

% setters
-export([mark_building/1, mark_preserved/5, mark_failed/5, mark_purging/2, set_job_id/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-compile([{no_auto_import, [get/1]}]).

-type id() :: binary().
-type record() :: #archive{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: map().
%% Below is the description of diff that can be applied to modify archive record.
%% #{
%%     <<"description">> => description(),
%%     <<"preservedCallback">> => callback(),
%%     <<"purgedCallback">> => callback(),
%% }

-type creator() :: od_user:id().

-type type() :: archive_config:incremental().
-type include_dip() :: archive_config:include_dip().
-type layout() :: archive_config:layout().

-type state() :: ?ARCHIVE_PENDING | ?ARCHIVE_BUILDING | ?ARCHIVE_PRESERVED | ?ARCHIVE_PURGING | ?ARCHIVE_FAILED.
-type timestamp() :: time:seconds().
-type description() :: binary().
-type callback() :: http_client:url() | undefined.

-type config() :: archive_config:config().

-type error() :: {error, term()}.

-export_type([
    id/0, doc/0,
    creator/0, type/0, state/0, include_dip/0,
    layout/0, timestamp/0, description/0,
    config/0, callback/0, diff/0
]).

% @formatter:on
-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).
% @formatter:off


%%%===================================================================
%%% API functions
%%%===================================================================

-spec create(dataset:id(), od_space:id(), creator(), config(), callback(), callback(), description()) ->
    {ok, doc()} | error().
create(DatasetId, SpaceId, Creator, Config, PreservedCallback, PurgedCallback, Description) ->
    datastore_model:create(?CTX, #document{
        value = #archive{
            dataset_id = DatasetId,
            creation_time = global_clock:timestamp_seconds(),
            creator = Creator,
            state = ?ARCHIVE_PENDING,
            config = Config,
            preserved_callback = PreservedCallback,
            purged_callback = PurgedCallback,
            description = Description
        },
        scope = SpaceId
    }).


-spec get(id()) -> {ok, doc()} | error().
get(ArchiveId) ->
    datastore_model:get(?CTX, ArchiveId).


-spec modify_attrs(id(), diff()) -> ok | error().
modify_attrs(ArchiveId, Diff) when is_map(Diff) ->
    ?extract_ok(update(ArchiveId, fun(Archive = #archive{
        description = PrevDescription,
        preserved_callback = PrevPreservedCallback,
        purged_callback = PrevPurgedCallback
    }) ->
        {ok, Archive#archive{
            description = utils:ensure_defined(maps:get(<<"description">>, Diff, undefined), PrevDescription),
            preserved_callback = utils:ensure_defined(maps:get(<<"preservedCallback">>, Diff, undefined), PrevPreservedCallback),
            purged_callback = utils:ensure_defined(maps:get(<<"purgedCallback">>, Diff, undefined), PrevPurgedCallback)
        }}
    end)).


-spec delete(archive:id()) -> ok | error().
delete(ArchiveId) ->
    datastore_model:delete(?CTX, ArchiveId).

%%%===================================================================
%%% Getters for #archive record
%%%===================================================================

-spec get_id(doc()) -> {ok, id()}.
get_id(#document{key = ArchiveId}) ->
    {ok, ArchiveId}.

-spec get_creation_time(record() | doc()) -> {ok, timestamp()}.
get_creation_time(#archive{creation_time = CreationTime}) ->
    {ok, CreationTime};
get_creation_time(#document{value = Archive}) ->
    get_creation_time(Archive).

-spec get_dataset_id(record() | doc()) -> {ok, dataset:id()}.
get_dataset_id(#archive{dataset_id = DatasetId}) ->
    {ok, DatasetId};
get_dataset_id(#document{value = Archive}) ->
    get_dataset_id(Archive).

-spec get_space_id(id() | doc()) -> {ok, od_space:id()}.
get_space_id(#document{scope = SpaceId}) ->
    {ok, SpaceId};
get_space_id(ArchiveId) ->
    ?get_field(ArchiveId, fun get_space_id/1).

-spec get_state(record() | doc()) -> {ok, state()}.
get_state(#archive{state = State}) ->
    {ok, State};
get_state(#document{value = Archive}) ->
    get_state(Archive).

-spec get_config(record() | doc()) -> {ok, config()}.
get_config(#archive{config = Config}) ->
    {ok, Config};
get_config(#document{value = Archive}) ->
    get_config(Archive).

-spec get_preserved_callback(record() | doc()) -> {ok, callback()}.
get_preserved_callback(#archive{preserved_callback = PreservedCallback}) ->
    {ok, PreservedCallback};
get_preserved_callback(#document{value = Archive}) ->
    get_preserved_callback(Archive).

-spec get_purged_callback(record() | doc()) -> {ok, callback()}.
get_purged_callback(#archive{purged_callback = PurgedCallback}) ->
    {ok, PurgedCallback};
get_purged_callback(#document{value = Archive}) ->
    get_purged_callback(Archive).

-spec get_description(record() | doc()) -> {ok, description()}.
get_description(#archive{description = Description}) ->
    {ok, Description};
get_description(#document{value = Archive}) ->
    get_description(Archive).

-spec get_job_id(record() | doc()) -> {ok, archivisation_traverse:id()}.
get_job_id(#archive{job_id = JobId}) ->
    {ok, JobId};
get_job_id(#document{value = Archive}) ->
    get_job_id(Archive).

-spec get_files_to_archive(record() | doc()) -> {ok, non_neg_integer()}.
get_files_to_archive(#archive{files_to_archive = FilesToArchive}) ->
    {ok, FilesToArchive};
get_files_to_archive(#document{value = Archive}) ->
    get_files_to_archive(Archive).

-spec get_files_archived(record() | doc()) -> {ok, non_neg_integer()}.
get_files_archived(#archive{files_archived = FilesArchived}) ->
    {ok, FilesArchived};
get_files_archived(#document{value = Archive}) ->
    get_files_archived(Archive).

-spec get_files_failed(record() | doc()) -> {ok, non_neg_integer()}.
get_files_failed(#archive{files_failed = FilesFailed}) ->
    {ok, FilesFailed};
get_files_failed(#document{value = Archive}) ->
    get_files_failed(Archive).

-spec get_bytes_archived(record() | doc()) -> {ok, non_neg_integer()}.
get_bytes_archived(#archive{bytes_archived = BytesArchived}) ->
    {ok, BytesArchived};
get_bytes_archived(#document{value = Archive}) ->
    get_bytes_archived(Archive).

-spec is_finished(record() | doc()) -> boolean().
is_finished(#archive{state = State}) ->
    lists:member(State, [?ARCHIVE_PRESERVED, ?ARCHIVE_FAILED, ?ARCHIVE_PURGING]);
is_finished(#document{value = Archive}) ->
    is_finished(Archive).

%%%===================================================================
%%% Setters for #archive record
%%%===================================================================

-spec mark_purging(id(), callback()) -> {ok, doc()} | error().
mark_purging(ArchiveId, Callback) ->
    update(ArchiveId, fun(Archive = #archive{
        state = PrevState,
        purged_callback = PrevPurgedCallback
    }) ->
        case PrevState =:= ?ARCHIVE_PENDING orelse PrevState =:= ?ARCHIVE_BUILDING of
            true ->
                {error, ?EBUSY};
            false ->
                {ok, Archive#archive{
                    state = ?ARCHIVE_PURGING,
                    purged_callback = utils:ensure_defined(Callback, PrevPurgedCallback)
                }}
        end
    end).


-spec mark_building(id()) -> ok | error().
mark_building(ArchiveId) ->
    ?extract_ok(update(ArchiveId, fun(Archive) ->
        {ok, Archive#archive{state = ?ARCHIVE_BUILDING}}
    end)).


-spec mark_preserved(id(), non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
    ok | error().
mark_preserved(ArchiveId, FilesToArchive, FilesArchived, FilesFailed, BytesArchived) ->
    mark_finished(ArchiveId, ?ARCHIVE_PRESERVED, FilesToArchive, FilesArchived, FilesFailed, BytesArchived).


-spec mark_failed(id(), non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
    ok | error().
mark_failed(ArchiveId, FilesToArchive, FilesArchived, FilesFailed, BytesArchived) ->
    mark_finished(ArchiveId, ?ARCHIVE_FAILED, FilesToArchive, FilesArchived, FilesFailed, BytesArchived).


-spec set_job_id(id(), archivisation_traverse:id()) -> {ok, doc()}.
set_job_id(ArchiveId, JobId) ->
    update(ArchiveId, fun(Archive) ->
        {ok, Archive#archive{job_id = JobId}}
    end).


%% @private
-spec mark_finished(id(), ?ARCHIVE_PRESERVED | ?ARCHIVE_FAILED,
    non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()) -> ok.
mark_finished(ArchiveId, NewState, FilesToArchive, FilesArchived, FilesFailed, BytesArchived) ->
    ?extract_ok(update(ArchiveId, fun(Archive) ->
        {ok, Archive#archive{
            state = NewState,
            files_to_archive = FilesToArchive,
            files_archived = FilesArchived,
            files_failed = FilesFailed,
            bytes_archived = BytesArchived
        }}
    end)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec update(id(), datastore_doc:diff(record())) -> {ok, doc()} | error().
update(ArchiveId, Diff) when is_function(Diff)->
    datastore_model:update(?CTX, ArchiveId, Diff).

%%%===================================================================
%%% Datastore callbacks
%%%===================================================================

%%--------------------------------------------------------------------
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
-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {dataset_id, string},
        {creation_time, integer},
        {creator, string},
        {state, atom},
        {config, {record, [
            {incremental, boolean},
            {include_dip, boolean},
            {layout, atom}
        ]}},
        {preserved_callback, string},
        {purged_callback, string},
        {description, binary},
        {files_to_archive, integer},
        {files_archived, integer},
        {files_failed, integer},
        {bytes_archived, integer},
        {job_id, string}
    ]}.
