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
-export([get_root_dir_ctx/1, get_all_ancestors/1, get_dataset_root_file_ctx/1,
    get_dataset_root_parent_path/2, find_file/3]).

% getters
-export([get_id/1, get_creation_time/1, get_dataset_id/1, get_archiving_provider_id/1, 
    get_dataset_root_file_guid/1, get_space_id/1,
    get_state/1, get_config/1, get_preserved_callback/1, get_deleted_callback/1,
    get_description/1, get_stats/1, get_root_dir_guid/1,
    get_data_dir_guid/1, get_parent_id/1, get_parent_doc/1, get_base_archive_id/1,
    get_related_dip_id/1, get_related_aip_id/1, 
    is_finished/1, is_building/1
]).

% setters
-export([mark_building/1, mark_deleting/2,
    mark_file_archived/2, mark_file_failed/1, mark_creation_finished/2,
    mark_preserved/1, mark_verification_failed/1, mark_cancelling/2, mark_cancelled/1,
    set_root_dir_guid/2, set_data_dir_guid/2, set_base_archive_id/2,
    set_related_dip/2, set_related_aip/2
]).


%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, resolve_conflict/3]).
-export([encode_state/1, decode_state/1]).

-compile([{no_auto_import, [get/1]}]).

-type id() :: binary().
-type record() :: #archive{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: map().
%% Below is the description of diff that can be applied to modify archive record.
%% #{
%%     <<"description">> => description(),
%%     <<"preservedCallback">> => callback(),
%%     <<"deletedCallback">> => callback(),
%% }

-type creator() :: od_user:id().

-type state() :: ?ARCHIVE_PENDING | ?ARCHIVE_BUILDING | ?ARCHIVE_PRESERVED | ?ARCHIVE_DELETING 
    | ?ARCHIVE_FAILED | ?ARCHIVE_VERIFYING | ?ARCHIVE_VERIFICATION_FAILED 
    | ?ARCHIVE_CANCELLING(retain) | ?ARCHIVE_CANCELLING(delete) | ?ARCHIVE_CANCELLED.
-type timestamp() :: time:seconds().
-type description() :: binary().
-type callback() :: http_client:url() | undefined.

-type config() :: archive_config:record().

-type cancel_preservation_policy() :: retain | delete.

-type error() :: {error, term()}.

% archive fields that can be modified by any provider. Other fields can be changed only by archiving 
% provider (except setting state to ?ARCHIVE_CANCELLING, which also can be done by any provider).
-record(modifiable_fields, {
    incarnation = 0 :: non_neg_integer(),
    preserved_callback :: archive:callback(),
    deleted_callback :: archive:callback(),
    description :: archive:description()
}).

-type modifiable_fields() :: #modifiable_fields{}.

-export_type([
    id/0, doc/0, creator/0,
    state/0, timestamp/0, description/0,
    config/0, callback/0, diff/0,
    cancel_preservation_policy/0,
    modifiable_fields/0
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

-spec create(dataset:id(), od_space:id(), creator(), config(), callback(), callback(), 
    description(), id() | undefined) -> {ok, doc()} | error().
create(DatasetId, SpaceId, Creator, Config, PreservedCallback, DeletedCallback, Description, BaseArchiveId) ->
    create(#document{
        value = #archive{
            archiving_provider = oneprovider:get_id(),
            dataset_id = DatasetId,
            creation_time = global_clock:timestamp_seconds(),
            creator = Creator,
            state = ?ARCHIVE_PENDING,
            config = Config,
            modifiable_fields = #modifiable_fields{
                preserved_callback = PreservedCallback,
                deleted_callback = DeletedCallback,
                description = Description
            },
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
        modifiable_fields = #modifiable_fields{
            description = Description
        }
    },
    scope = SpaceId
}) ->
    create(#document{
        value = #archive{
            archiving_provider = oneprovider:get_id(),
            dataset_id = DatasetId,
            creation_time = global_clock:timestamp_seconds(),
            creator = Creator,
            % nested archive is created when parent archive is already in building state
            state = ?ARCHIVE_BUILDING,
            config = Config,
            parent = ParentArchiveId,
            modifiable_fields = #modifiable_fields{
                description = Description
            },
            stats = archive_stats:empty()
        },
        scope = SpaceId
    }).


-spec create_dip_archive(doc()) -> {ok, doc()} | {error, term()}.
create_dip_archive(#document{
    key = AipArchiveId, 
    value = #archive{config = AipConfig} = AipArchiveValue, 
    scope = Scope
}) -> 
    create(#document{
        value = AipArchiveValue#archive{
            config = archive_config:enforce_plain_layout(AipConfig),
            related_aip = AipArchiveId,
            related_dip = undefined
        },
        scope = Scope
    }).


-spec create(doc()) -> {ok, doc()} | {error, term()}.
create(DocToCreate) ->
    case datastore_model:create(?CTX, DocToCreate) of
        {ok, #document{key = ArchiveId} = Doc} ->
            archivisation_audit_log:create(ArchiveId),
            {ok, Doc};
        {error, _} = Error ->
            Error
    end.
    

-spec get(id()) -> {ok, doc()} | error().
get(ArchiveId) ->
    datastore_model:get(?CTX, ArchiveId).


-spec modify_attrs(id(), diff()) -> ok | error().
modify_attrs(ArchiveId, Diff) when is_map(Diff) ->
    Result = update(ArchiveId, fun(Archive = #archive{
        modifiable_fields = #modifiable_fields{
            incarnation = Incarnation,
            description = PrevDescription,
            preserved_callback = PrevPreservedCallback,
            deleted_callback = PrevDeletedCallback
        }
    }) ->
        {ok, Archive#archive{
            modifiable_fields = #modifiable_fields{
                incarnation = Incarnation + 1,
                description = maps:get(<<"description">>, Diff, PrevDescription),
                preserved_callback = maps:get(<<"preservedCallback">>, Diff, PrevPreservedCallback),
                deleted_callback = maps:get(<<"deletedCallback">>, Diff, PrevDeletedCallback)
            }
        }}
    end),
    ?extract_ok(Result).


-spec delete(archive:id()) -> ok | error().
delete(ArchiveId) ->
    archivisation_audit_log:destroy(ArchiveId),
    datastore_model:delete(?CTX, ArchiveId).


-spec get_root_dir_ctx(record() | doc()) -> {ok, file_ctx:ctx()}.
get_root_dir_ctx(Archive) ->
    {ok, RootDirGuid} = get_root_dir_guid(Archive),
    {ok, file_ctx:new_by_guid(RootDirGuid)}.


-spec get_dataset_root_file_ctx(record() | doc()) -> {ok, file_ctx:ctx()}.
get_dataset_root_file_ctx(Archive) ->
    {ok, DatasetRootFileGuid} = archive:get_dataset_root_file_guid(Archive),
    {ok, file_ctx:new_by_guid(DatasetRootFileGuid)}.


-spec get_dataset_root_parent_path(record() | doc(), user_ctx:ctx()) -> {ok, file_meta:path()}.
get_dataset_root_parent_path(Archive, UserCtx) ->
    {ok, DatasetRootFileCtx} = get_dataset_root_file_ctx(Archive),
    {DatasetRootPath, _} = file_ctx:get_logical_path(DatasetRootFileCtx, UserCtx),
    {ok, filename:dirname(DatasetRootPath)}.


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
                {ChildCtx, _} = file_tree:get_child(Ctx, ChildName, UserCtx),
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

-spec get_id(doc() | id()) -> {ok, id()}.
get_id(#document{key = ArchiveId}) ->
    {ok, ArchiveId};
get_id(ArchiveId) ->
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

-spec get_archiving_provider_id(record() | doc()) -> {ok, oneprovider:id()}.
get_archiving_provider_id(#archive{archiving_provider = ProviderId}) ->
    {ok, ProviderId};
get_archiving_provider_id(#document{value = Archive}) ->
    get_archiving_provider_id(Archive).

-spec get_dataset_root_file_guid(id() | doc()) -> {ok, file_id:file_guid()}.
get_dataset_root_file_guid(Doc = #document{}) ->
    {ok, DatasetId} = get_dataset_id(Doc),
    {ok, RootFileUuid} = dataset:get_root_file_uuid(DatasetId),
    {ok, SpaceId} = get_space_id(Doc),
    {ok, file_id:pack_guid(RootFileUuid, SpaceId)};
get_dataset_root_file_guid(ArchiveId) when is_binary(ArchiveId) ->
    {ok, ArchiveDoc} = get(ArchiveId),
    get_dataset_root_file_guid(ArchiveDoc).

-spec get_space_id(id() | doc()) -> {ok, od_space:id()} | {error, term()}.
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
get_preserved_callback(#archive{modifiable_fields = #modifiable_fields{preserved_callback = PreservedCallback}}) ->
    {ok, PreservedCallback};
get_preserved_callback(#document{value = Archive}) ->
    get_preserved_callback(Archive).

-spec get_deleted_callback(record() | doc()) -> {ok, callback()}.
get_deleted_callback(#archive{modifiable_fields = #modifiable_fields{deleted_callback = DeletedCallback}}) ->
    {ok, DeletedCallback};
get_deleted_callback(#document{value = Archive}) ->
    get_deleted_callback(Archive).

-spec get_description(id() | record() | doc()) -> {ok, description()}.
get_description(#archive{modifiable_fields = #modifiable_fields{description = Description}}) ->
    {ok, Description};
get_description(#document{value = Archive}) ->
    get_description(Archive);
get_description(ArchiveId) ->
    ?get_field(ArchiveId, fun get_description/1).

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

-spec get_data_dir_guid(record() | doc() | id()) -> {ok, file_id:file_guid()}.
get_data_dir_guid(#archive{data_dir_guid = DataDirGuid}) -> 
    {ok, DataDirGuid};
get_data_dir_guid(#document{value = Archive}) -> 
    get_data_dir_guid(Archive);
get_data_dir_guid(ArchiveId) ->
    ?get_field(ArchiveId, fun get_data_dir_guid/1).

-spec get_parent_id(record() | doc() | id()) -> {ok, archive:id() | undefined}.
get_parent_id(#archive{parent = Parent}) ->
    {ok, Parent};
get_parent_id(#document{value = Archive}) ->
    get_parent_id(Archive);
get_parent_id(ArchiveId) ->
    ?get_field(ArchiveId, fun get_parent_id/1).

-spec get_parent_doc(record() | doc()) -> {ok, doc() | undefined} | {error, term()}.
get_parent_doc(Archive) ->
    case get_parent_id(Archive) of
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

-spec get_related_dip_id(record() | doc()) -> {ok, archive:id() | undefined}.
get_related_dip_id(#archive{related_dip = RelatedDip}) ->
    {ok, RelatedDip};
get_related_dip_id(#document{value = Archive}) ->
    get_related_dip_id(Archive);
get_related_dip_id(ArchiveId) ->
    ?get_field(ArchiveId, fun get_related_dip_id/1).

-spec get_related_aip_id(record() | doc()) -> {ok, archive:id() | undefined}.
get_related_aip_id(#archive{related_aip = RelatedAIP}) ->
    {ok, RelatedAIP};
get_related_aip_id(#document{value = Archive}) ->
    get_related_aip_id(Archive);
get_related_aip_id(ArchiveId) ->
    ?get_field(ArchiveId, fun get_related_aip_id/1).

-spec is_finished(record() | doc()) -> boolean().
is_finished(#archive{state = State}) ->
    lists:member(State, [?ARCHIVE_PRESERVED, ?ARCHIVE_FAILED, ?ARCHIVE_DELETING, 
        ?ARCHIVE_VERIFICATION_FAILED, ?ARCHIVE_CANCELLED]);
is_finished(#document{value = Archive}) ->
    is_finished(Archive).

-spec is_building(record() | doc()) -> boolean().
is_building(#archive{state = State}) ->
    lists:member(State, [?ARCHIVE_PENDING, ?ARCHIVE_BUILDING]);
is_building(#document{value = Archive}) ->
    is_building(Archive).

%%%===================================================================
%%% Setters for #archive record
%%%===================================================================

-spec mark_deleting(id(), callback()) -> {ok, doc()} | error().
mark_deleting(ArchiveId, Callback) ->
    update(ArchiveId, fun(Archive = #archive{
        state = PrevState,
        modifiable_fields = ModifiableFields = #modifiable_fields{
            deleted_callback = PrevDeletedCallback
        },
        parent = Parent
    }) ->
        case PrevState =:= ?ARCHIVE_PENDING
            orelse PrevState =:= ?ARCHIVE_BUILDING
            orelse Parent =/= undefined % nested archive cannot be deleted as it would destroy parent archive
        of
            true ->
                %% @TODO VFS-8840 - create more descriptive error (also for nested archives)
                ?ERROR_POSIX(?EBUSY);
            false ->
                {ok, Archive#archive{
                    state = ?ARCHIVE_DELETING,
                    modifiable_fields = ModifiableFields#modifiable_fields{
                        deleted_callback = utils:ensure_defined(Callback, PrevDeletedCallback)
                    }
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


-spec mark_creation_finished(id() | doc(), archive_stats:record()) -> ok | delete.
mark_creation_finished(ArchiveDocOrId, NestedArchivesStats) ->
    UpdateResult = update(ArchiveDocOrId, fun
        (Archive = #archive{stats = CurrentStats, state = ?ARCHIVE_BUILDING}) ->
            AggregatedStats = archive_stats:sum(CurrentStats, NestedArchivesStats),
            {ok, Archive#archive{
                state = case AggregatedStats#archive_stats.files_failed =:= 0 of
                    true -> ?ARCHIVE_VERIFYING;
                    false -> ?ARCHIVE_FAILED
                end,
                stats = AggregatedStats
            }};
        (#archive{state = ?ARCHIVE_CANCELLING(delete), related_aip = undefined}) ->
            % do not delete DIP archive (RelatedAip =/= undefined) as it will be deleted alongside AIP
            {error, delete};
        (Archive = #archive{state = ?ARCHIVE_CANCELLING(_)}) ->
            {ok, Archive#archive{state = ?ARCHIVE_CANCELLED}}
    end),
    case UpdateResult of
        {ok, #document{value = #archive{state = ?ARCHIVE_VERIFYING}} = Doc} ->
            archive_verification_traverse:block_archive_modification(Doc),
            archive_verification_traverse:start(Doc);
        {ok, #document{value = #archive{}}} ->
            ok;
        {error, not_found} -> 
            ok;
        {error, delete} ->
            delete
    end.


-spec mark_preserved(id() | doc()) -> ok | error().
mark_preserved(ArchiveDocOrId) ->
    ?extract_ok(update(ArchiveDocOrId, fun
        (#archive{state = State} = Archive) when State =/= ?ARCHIVE_VERIFICATION_FAILED ->
            {ok, Archive#archive{state = ?ARCHIVE_PRESERVED}};
        (Archive) ->
            {ok, Archive}
    end)).


-spec mark_cancelling(id() | doc(), cancel_preservation_policy()) -> 
    ok | {error, already_finished} | error().
mark_cancelling(ArchiveDocOrId, PreservationPolicy) ->
    ?extract_ok(update(ArchiveDocOrId, fun
        (#archive{} = Archive) ->
            case is_finished(Archive) of
                true -> {error, already_finished};
                false -> {ok, Archive#archive{state = ?ARCHIVE_CANCELLING(PreservationPolicy)}}
            end
    end)).


-spec mark_cancelled(id() | doc()) -> ok | delete | error().
mark_cancelled(ArchiveDocOrId) ->
    UpdateResult = ?ok_if_no_change(?ok_if_not_found(?extract_ok(update(ArchiveDocOrId, fun
        (#archive{state = State, related_aip = RelatedAip} = Archive) ->
            case {is_finished(Archive), State, RelatedAip} of
                {true, _, _} -> 
                    {error, no_change}; 
                {false, ?ARCHIVE_CANCELLING(delete), undefined} -> 
                    % do not delete DIP archive (RelatedAip =/= undefined) as it will be deleted alongside AIP
                    {error, delete};
                _ ->
                    {ok, Archive#archive{state = ?ARCHIVE_CANCELLED}}
            end
    end)))),
    case UpdateResult of
        ok -> ok;
        {error, not_found} -> ok;
        {error, delete} -> delete
    end.


-spec mark_verification_failed(id() | doc()) -> ok | error().
mark_verification_failed(ArchiveDocOrId) ->
    ?extract_ok(update(ArchiveDocOrId, fun(Archive) ->
        {ok, Archive#archive{
            state = ?ARCHIVE_VERIFICATION_FAILED
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
    {ok, ParentArchiveId} = get_parent_id(ArchiveDoc),
    get_all_ancestors(ParentArchiveId, [ArchiveDoc | AncestorArchives]).

%%%===================================================================
%%% Datastore callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {archiving_provider, string},
        {dataset_id, string},
        {creation_time, integer},
        {creator, string},
        {state, {custom, atom, {archive, encode_state, decode_state}}},
        {config, {custom, string, {persistent_record, encode, decode, archive_config}}},
        {modifiable_fields, {record, [
            {incarnation, integer},
            {preserved_callback, string},
            {deleted_callback, string},
            {description, string}
        ]}},
        {root_dir_guid, string},
        {data_dir_guid, string},
        {stats, {custom, string, {persistent_record, encode, decode, archive_stats}}},
        {parent, string},
        {base_archive_id, string},
        {related_dip, string},
        {related_aip, string}
    ]}.


-spec resolve_conflict(datastore_model:ctx(), doc(), doc()) ->
    {boolean(), doc()} | ignore | default.
resolve_conflict(_Ctx, #document{value = RemoteValue} = RemoteDoc, #document{value = LocalValue} = LocalDoc) ->
    % Archive fields that can be modified by any provider are stored under modifiable fields field. 
    % Other fields can be changed only by archiving provider (except setting state to ?ARCHIVE_CANCELLING, 
    % which also can be done by any provider).
    
    #document{revs = [LocalRev | _], mutators = [RemoteDocMutator], deleted = LocalDeleted} = LocalDoc,
    #document{revs = [RemoteRev | _], deleted = RemoteDeleted} = RemoteDoc,
    
    case datastore_rev:is_greater(RemoteRev, LocalRev) of
        true ->
            case {LocalDeleted, resolve_conflict_remote_rev_greater(LocalValue, RemoteValue)} of
                {true, _} ->
                    case RemoteDeleted of
                        true -> {false, RemoteDoc};
                        false -> {true, RemoteDoc#document{deleted = true}}
                    end;
                {false, {true, NewRecord}} ->
                    {true, RemoteDoc#document{value = NewRecord}};
                {false, remote} ->
                    {false, RemoteDoc}
            end;
        false ->
            case {RemoteDeleted, resolve_conflict_local_rev_greater(LocalValue, RemoteValue, RemoteDocMutator)} of
                {true, _} ->
                    case LocalDeleted of
                        true -> ignore;
                        false -> {true, LocalDoc#document{deleted = true}}
                    end;
                {false, {true, NewRecord}} ->
                    {true, LocalDoc#document{value = NewRecord}};
                {false, ignore} ->
                    ignore
            end
    end.


-spec resolve_conflict_remote_rev_greater(record(), record()) -> 
    {true, record()} | remote.
resolve_conflict_remote_rev_greater(
    #archive{archiving_provider = ArchivingProviderId} = LocalValue,
    #archive{state = RemoteState} = RemoteValue
) ->
    LocalProviderId = oneprovider:get_id_or_undefined(),
    case {LocalProviderId, is_finished(LocalValue), RemoteState} of
        {ArchivingProviderId, false = _LocalFinished, ?ARCHIVE_CANCELLING(_) = CancellingState} ->
            {true, LocalValue#archive{
                state = CancellingState, 
                modifiable_fields = resolve_modifiable_fields_conflict(RemoteValue, LocalValue)
            }};
        {ArchivingProviderId, _, _} ->
            {true, LocalValue#archive{
                modifiable_fields = resolve_modifiable_fields_conflict(RemoteValue, LocalValue)
            }};
        { _, _, _} ->
            case is_modifiable_fields_conflict(RemoteValue, LocalValue) of
                true -> 
                    {true, RemoteValue#archive{
                        modifiable_fields = resolve_modifiable_fields_conflict(RemoteValue, LocalValue)
                    }};
                false ->
                    remote
            end
    end.


-spec resolve_conflict_local_rev_greater(record(), record(), oneprovider:id()) -> 
    {true, record()} | ignore.
resolve_conflict_local_rev_greater(
    #archive{archiving_provider = ArchivingProviderId, state = LocalState} = LocalValue,
    #archive{state = RemoteState} = RemoteValue,
    RemoteDocMutator
) ->
    Result = case {RemoteDocMutator, {LocalState, is_finished(LocalValue)}, {RemoteState, is_finished(RemoteValue)}} of
        {ArchivingProviderId, {?ARCHIVE_CANCELLING(_) = CancellingState, _}, {_, false = _RemoteFinished}} ->
            {true, RemoteValue#archive{state = CancellingState}};
        {ArchivingProviderId, _, _} ->
            {true, RemoteValue};
        {_, {?ARCHIVE_CANCELLING(_), _}, {?ARCHIVE_CANCELLING(_), _}} ->
            ignore;
        {_, {_, true = _LocalFinished}, _} ->
            ignore;
        {_, _, {?ARCHIVE_CANCELLING(_) = CancellingState, _}} ->
            {true, LocalValue#archive{state = CancellingState}};
        {_, _, _} ->
            ignore
    end,
    case {Result, is_modifiable_fields_conflict(LocalValue, RemoteValue)} of
        {ignore, true} ->
            {true, LocalValue#archive{modifiable_fields = resolve_modifiable_fields_conflict(LocalValue, RemoteValue)}};
        {{true, UpdatedValue}, true} ->
            {true, UpdatedValue#archive{modifiable_fields = resolve_modifiable_fields_conflict(LocalValue, RemoteValue)}};
        _ ->
            Result
    end.


-spec is_modifiable_fields_conflict(record(), record()) -> boolean().
is_modifiable_fields_conflict(#archive{modifiable_fields = MF1}, #archive{modifiable_fields = MF2}) ->
    MF1 =/= MF2.


-spec resolve_modifiable_fields_conflict(GreaterRevRecord :: record(), LowerRevRecord :: record()) -> 
    modifiable_fields().
resolve_modifiable_fields_conflict(
    #archive{modifiable_fields = #modifiable_fields{incarnation = Incarnation1} = MF1},
    #archive{modifiable_fields = #modifiable_fields{incarnation = Incarnation2} = _MF2}
) when Incarnation1 > Incarnation2 ->
    MF1;
resolve_modifiable_fields_conflict(
    #archive{modifiable_fields = #modifiable_fields{incarnation = Incarnation1} = _MF1},
    #archive{modifiable_fields = #modifiable_fields{incarnation = Incarnation2} = MF2}
) when Incarnation1 < Incarnation2 ->
    MF2;
resolve_modifiable_fields_conflict(
    #archive{modifiable_fields = GreaterRevMF}, 
    #archive{modifiable_fields = _LoverRevMF2}
) ->
    GreaterRevMF.


-spec encode_state(state()) -> atom().
encode_state(?ARCHIVE_CANCELLING(delete)) -> cancelling_delete;
encode_state(?ARCHIVE_CANCELLING(retain)) -> cancelling;
encode_state(State) -> State.


-spec decode_state(atom()) -> state().
decode_state(cancelling_delete) -> ?ARCHIVE_CANCELLING(delete);
decode_state(cancelling) -> ?ARCHIVE_CANCELLING(retain);
decode_state(State) -> State.
