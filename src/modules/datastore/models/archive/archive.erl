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
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/8, create_nested/2, create_dip_archive/1, get/1, modify_attrs/2, delete/1]).
-export([get_root_dir_ctx/1, get_all_ancestors/1, get_dataset_root_file_ctx/1, find_file/3]).

% getters
-export([get_id/1, get_creation_time/1, get_dataset_id/1, get_dataset_root_file_guid/1, get_space_id/1,
    get_state/1, get_config/1, get_preserved_callback/1, get_purged_callback/1,
    get_description/1, get_stats/1, get_root_dir_guid/1,
    get_data_dir_guid/1, get_parent/1, get_parent_doc/1, get_base_archive_id/1,
    get_related_dip/1, get_related_aip/1, is_finished/1
]).

% setters
-export([mark_building/1, mark_purging/2,
    mark_file_archived/2, mark_file_failed/1, mark_finished/2,
    set_root_dir_guid/2, set_data_dir_guid/2, set_base_archive_id/2,
    set_related_dip/2, set_related_aip/2
]).

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

-type state() :: ?ARCHIVE_PENDING | ?ARCHIVE_BUILDING | ?ARCHIVE_PRESERVED | ?ARCHIVE_PURGING | ?ARCHIVE_FAILED.
-type timestamp() :: time:seconds().
-type description() :: binary().
-type callback() :: http_client:url() | undefined.

-type config() :: archive_config:record().

-type error() :: {error, term()}.

-export_type([
    id/0, doc/0, creator/0,
    state/0, timestamp/0, description/0,
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

-spec create(dataset:id(), od_space:id(), creator(), config(), callback(), callback(), description(), archive:id() | undefined) ->
    {ok, doc()} | error().
create(DatasetId, SpaceId, Creator, Config, PreservedCallback, PurgedCallback, Description, BaseArchiveId) ->
    datastore_model:create(?CTX, #document{
        value = #archive{
            dataset_id = DatasetId,
            creation_time = global_clock:timestamp_seconds(),
            creator = Creator,
            state = ?ARCHIVE_PENDING,
            config = Config,
            preserved_callback = PreservedCallback,
            purged_callback = PurgedCallback,
            description = Description,
            stats = archive_stats:empty(),
            base_archive_id = BaseArchiveId
        },
        scope = SpaceId
    }).


-spec create_nested(dataset:id(), doc()) -> {ok, doc()} | error().
create_nested(DatasetId, #document{
    key = ParentArchiveId,
    value = #archive{
        config = Config,
        creator = Creator,
        description = Description
    },
    scope = SpaceId
}) ->
    datastore_model:create(?CTX, #document{
        value = #archive{
            dataset_id = DatasetId,
            creation_time = global_clock:timestamp_seconds(),
            creator = Creator,
            % nested archive is created when parent archive is already in building state
            state = ?ARCHIVE_BUILDING,
            config = Config,
            parent = ParentArchiveId,
            description = Description,
            stats = archive_stats:empty()
        },
        scope = SpaceId
    }).


-spec create_dip_archive(archive:doc()) -> {ok, archive:doc()} | {error, term()}.
create_dip_archive(#document{key = AipArchiveId, value = AipArchiveValue, scope = Scope} = AipArchiveDoc) ->
    case datastore_model:create(?CTX, #document{
        value = AipArchiveValue#archive{
            related_aip = AipArchiveId
        },
        scope = Scope
    }) of
        {ok, #document{key = DipArchiveId}} ->
            {ok, AipArchiveDoc#document{value = AipArchiveValue#archive{related_dip = DipArchiveId}}};
        {error, _} = Error ->
            Error
    end.


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


-spec get_root_dir_ctx(record() | doc()) -> {ok, file_ctx:ctx()}.
get_root_dir_ctx(Archive) ->
    {ok, RootDirGuid} = get_root_dir_guid(Archive),
    {ok, file_ctx:new_by_guid(RootDirGuid)}.


-spec get_dataset_root_file_ctx(record() | doc()) -> {ok, file_ctx:ctx()}.
get_dataset_root_file_ctx(Archive) ->
    {ok, DatasetRootFileGuid} = archive:get_dataset_root_file_guid(Archive),
    {ok, file_ctx:new_by_guid(DatasetRootFileGuid)}.


-spec get_all_ancestors(doc() | record()) -> {ok, [doc()]}.
get_all_ancestors(#archive{parent = ParentArchive}) ->
    get_all_ancestors(ParentArchive, []);
get_all_ancestors(#document{value = Archive}) ->
    get_all_ancestors(Archive).


%%--------------------------------------------------------------------
%% @doc
%% Returns file_ctx:ctx() of file that can be found under
%% RelativeFilePath in archive associated with ArchiveDoc.
%% @end
%%--------------------------------------------------------------------
-spec find_file(archive:doc(), file_meta:path(), user_ctx:ctx()) ->
    {ok, file_ctx:ctx()} | {error, term()}.
find_file(ArchiveDoc, RelativeFilePath, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    {ok, DataDirGuid} = get_data_dir_guid(ArchiveDoc),
    DataDirCtx = file_ctx:new_by_guid(DataDirGuid),
    RelativeFilePathTokens = filename:split(RelativeFilePath),
    SessionId = user_ctx:get_session_id(UserCtx),
    try
        lists:foldl(fun
            (ChildName, {ok, Ctx}) ->
                {ChildCtx, _} = files_tree:get_child(Ctx, ChildName, UserCtx),
                %% @TODO VFS-7923 - use unified symlink resolving functionality
                case file_ctx:is_symlink_const(ChildCtx) of
                    true ->
                        ChildGuid = file_ctx:get_logical_guid_const(ChildCtx),
                        case lfm:resolve_symlink(SessionId, ?FILE_REF(ChildGuid)) of
                            {ok, Guid} -> {ok, file_ctx:new_by_guid(Guid)};
                            {error, ?ENOENT} -> ?ERROR_NOT_FOUND
                        end;
                    false ->
                        {ok, ChildCtx}
                end;
            (_, {error, _} = Error) ->
                Error
        end, {ok, DataDirCtx}, RelativeFilePathTokens)
    catch
        throw:?ENOENT ->
            ?ERROR_NOT_FOUND
    end.

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

-spec get_dataset_root_file_guid(id() | doc()) -> {ok, file_id:file_guid()}.
get_dataset_root_file_guid(Doc = #document{}) ->
    {ok, DatasetId} = get_dataset_id(Doc),
    {ok, RootFileUuid} = dataset:get_root_file_uuid(DatasetId),
    {ok, SpaceId} = get_space_id(Doc),
    {ok, file_id:pack_guid(RootFileUuid, SpaceId)};
get_dataset_root_file_guid(ArchiveId) when is_binary(ArchiveId) ->
    {ok, ArchiveDoc} = get(ArchiveId),
    get_dataset_root_file_guid(ArchiveDoc).

-spec get_space_id(id() | doc()) -> {ok, od_space:id()}.
get_space_id(#document{scope = SpaceId}) ->
    {ok, SpaceId};
get_space_id(ArchiveId) ->
    ?get_field(ArchiveId, fun get_space_id/1).

-spec get_state(id() | record() | doc()) -> {ok, state()} | error().
get_state(#archive{state = State}) ->
    {ok, State};
get_state(#document{value = Archive}) ->
    get_state(Archive);
get_state(ArchiveId) ->
    ?get_field(ArchiveId, fun get_state/1).

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

-spec get_stats(record() | doc()) -> {ok, archive_stats:record()}.
get_stats(#archive{stats = Stats}) ->
    {ok, Stats};
get_stats(#document{value = Archive}) ->
    get_stats(Archive).

-spec get_root_dir_guid(record() | doc()) -> {ok, file_id:file_guid()}.
get_root_dir_guid(#archive{root_dir_guid = RootDirGuid}) ->
    {ok, RootDirGuid};
get_root_dir_guid(#document{value = Archive}) ->
    get_root_dir_guid(Archive).

-spec get_data_dir_guid
    (record() | doc() | id()) -> {ok, file_id:file_guid()};
    (undefined) -> {ok, undefined}.
get_data_dir_guid(undefined) -> 
    {ok, undefined};
get_data_dir_guid(#archive{data_dir_guid = DataDirGuid}) -> 
    {ok, DataDirGuid};
get_data_dir_guid(#document{value = Archive}) -> 
    get_data_dir_guid(Archive);
get_data_dir_guid(ArchiveId) ->
    ?get_field(ArchiveId, fun get_data_dir_guid/1).

-spec get_parent(record() | doc() | id()) -> {ok, archive:id() | undefined}.
get_parent(#archive{parent = Parent}) ->
    {ok, Parent};
get_parent(#document{value = Archive}) ->
    get_parent(Archive);
get_parent(ArchiveId) ->
    ?get_field(ArchiveId, fun get_parent/1).

-spec get_parent_doc(record() | doc()) -> {ok, doc() | undefined} | {error, term()}.
get_parent_doc(Archive) ->
    case get_parent(Archive) of
        {ok, undefined} -> {ok, undefined};
        {ok, ParentArchiveId} -> get(ParentArchiveId)
    end.

-spec get_base_archive_id(record() | doc() | id()) -> {ok, archive:id() | undefined}.
get_base_archive_id(#archive{base_archive_id = BaseArchiveId}) ->
    {ok, BaseArchiveId};
get_base_archive_id(#document{value = Archive}) ->
    get_base_archive_id(Archive);
get_base_archive_id(ArchiveId) ->
    ?get_field(ArchiveId, fun get_base_archive_id/1).

-spec get_related_dip(record() | doc()) -> {ok, archive:id() | undefined}.
get_related_dip(#archive{related_dip = RelatedDip}) ->
    {ok, RelatedDip};
get_related_dip(#document{value = Archive}) ->
    get_related_dip(Archive);
get_related_dip(ArchiveId) ->
    ?get_field(ArchiveId, fun get_related_dip/1).

-spec get_related_aip(record() | doc()) -> {ok, archive:id() | undefined}.
get_related_aip(#archive{related_aip = RelatedAIP}) ->
    {ok, RelatedAIP};
get_related_aip(#document{value = Archive}) ->
    get_related_aip(Archive);
get_related_aip(ArchiveId) ->
    ?get_field(ArchiveId, fun get_related_aip/1).

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
        purged_callback = PrevPurgedCallback,
        parent = Parent
    }) ->
        case PrevState =:= ?ARCHIVE_PENDING
            orelse PrevState =:= ?ARCHIVE_BUILDING
            orelse Parent =/= undefined % nested archive cannot be removed as it would destroy parent archive
        of
            true ->
                % TODO VFS-7718 return better error for nested dataset?
                {error, ?EBUSY};
            false ->
                {ok, Archive#archive{
                    state = ?ARCHIVE_PURGING,
                    purged_callback = utils:ensure_defined(Callback, PrevPurgedCallback)
                }}
        end
    end).


-spec mark_building(id() | doc()) -> ok | error().
mark_building(ArchiveDocOrId) ->
    ?extract_ok(update(ArchiveDocOrId, fun(Archive) ->
        {ok, Archive#archive{
            state = ?ARCHIVE_BUILDING
        }}
    end)).


-spec mark_finished(id() | doc(), archive_stats:record()) -> ok.
mark_finished(ArchiveDocOrId, NestedArchivesStats) ->
    ?extract_ok(update(ArchiveDocOrId, fun(Archive = #archive{stats = CurrentStats}) ->
        AggregatedStats = archive_stats:sum(CurrentStats, NestedArchivesStats),
        {ok, Archive#archive{
            state = case AggregatedStats#archive_stats.files_failed =:= 0 of
                true -> ?ARCHIVE_PRESERVED;
                false -> ?ARCHIVE_FAILED
            end,
            stats = AggregatedStats
        }}
    end)).


-spec mark_file_archived(id() | doc(), non_neg_integer()) -> ok | error().
mark_file_archived(ArchiveDocOrId, FileSize) ->
    ?extract_ok(update(ArchiveDocOrId, fun(Archive0 = #archive{stats = Stats}) ->
        {ok, Archive0#archive{stats = archive_stats:mark_file_archived(Stats, FileSize)}}
    end)).


-spec mark_file_failed(id() | doc()) -> ok | error().
mark_file_failed(ArchiveDocOrId) ->
    ?extract_ok(update(ArchiveDocOrId, fun(Archive = #archive{stats = Stats}) ->
        {ok, Archive#archive{stats = archive_stats:mark_file_failed(Stats)}}
    end)).


-spec set_root_dir_guid(id() | doc(), file_id:file_guid()) -> {ok, doc()} | error().
set_root_dir_guid(ArchiveDocOrId, RootDirGuid) ->
    update(ArchiveDocOrId, fun(Archive) ->
        {ok, Archive#archive{root_dir_guid = RootDirGuid}}
    end).

-spec set_data_dir_guid(id() | doc(), file_id:file_guid()) -> {ok, doc()} | error().
set_data_dir_guid(ArchiveDocOrId, DataDirGuid) ->
    update(ArchiveDocOrId, fun(Archive) ->
        {ok, Archive#archive{data_dir_guid = DataDirGuid}}
    end).


-spec set_base_archive_id(doc(), undefined | id()) -> {ok, doc()} | error().
set_base_archive_id(ArchiveDoc, undefined) ->
    {ok, ArchiveDoc};
set_base_archive_id(ArchiveDoc, #document{key = BaseArchiveId}) when is_binary(BaseArchiveId) ->
    set_base_archive_id(ArchiveDoc, BaseArchiveId);
set_base_archive_id(ArchiveDoc, BaseArchiveId) when is_binary(BaseArchiveId) ->
    update(ArchiveDoc, fun(Archive) ->
        {ok, Archive#archive{base_archive_id = BaseArchiveId}}
    end).



-spec set_related_dip(id() | doc(), archive:id() | undefined) -> {ok, doc()} | error().
set_related_dip(ArchiveDocOrId, DipArchiveId) ->
    update(ArchiveDocOrId, fun(Archive) ->
        {ok, Archive#archive{related_dip = DipArchiveId}}
    end).


-spec set_related_aip(id() | doc(), archive:id() | undefined) -> {ok, doc()} | error().
set_related_aip(ArchiveDocOrId, AipArchiveId) ->
    update(ArchiveDocOrId, fun(Archive) ->
        {ok, Archive#archive{related_aip = AipArchiveId}}
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec update(id() | doc(), datastore_doc:diff(record())) -> {ok, doc()} | error().
update(#document{key = ArchiveId}, Diff) ->
    update(ArchiveId, Diff);
update(ArchiveId, Diff) ->
    datastore_model:update(?CTX, ArchiveId, Diff).


-spec get_all_ancestors(undefined | id(), [doc()]) -> {ok, [doc()]}.
get_all_ancestors(undefined, AncestorArchives) ->
    {ok, lists:reverse(AncestorArchives)};
get_all_ancestors(ArchiveId, AncestorArchives) ->
    {ok, ArchiveDoc} = get(ArchiveId),
    {ok, ParentArchiveId} = get_parent(ArchiveDoc),
    get_all_ancestors(ParentArchiveId, [ArchiveDoc | AncestorArchives]).

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
        {config, {custom, string, {persistent_record, encode, decode, archive_config}}},
        {preserved_callback, string},
        {purged_callback, string},
        {description, string},
        {root_dir_guid, string},
        {data_dir_guid, string},
        {stats, {custom, string, {persistent_record, encode, decode, archive_stats}}},
        {parent, string},
        {base_archive_id, string},
        {related_dip, string},
        {related_aip, string}
    ]}.
