%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for file's metadata. Implemets low-level metadata operations such as
%%%      walking through file graph.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta).
-author("Rafal Slota").
-behaviour(model_behaviour).

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("cluster_worker/include/elements/task_manager/task_manager.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").

%% How many processes shall be process single set_scope operation.
-define(SET_SCOPER_WORKERS, 25).

%% How many entries shall be processed in one batch for set_scope operation.
-define(SET_SCOPE_BATCH_SIZE, 100).

-define(SET_LINK_SCOPE(ScopeId), [{scope, ScopeId}]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, delete_without_link/1, update/2,
    create/1, model_init/0, 'after'/5, before/4]).

-export([resolve_path/1, resolve_path/2, create/2, create/3,
    get_scope_id/1, list_children/3, get_parent/1, get_parent_uuid/1,
    setup_onedata_user/2, get_including_deleted/1, make_space_exist/1,
    new_doc/7, type/1, get_ancestors/1, get_locations_by_uuid/1, rename/4]).
-export([delete_child_link/2, foreach_child/3]).
-export([hidden_file_name/1, is_hidden/1]).
-export([add_share/2, remove_share/2]).
-export([record_struct/1, record_upgrade/2]).

-type doc() :: datastore:document().
-type uuid() :: datastore:key().
-type path() :: binary().
-type name() :: binary().
-type uuid_or_path() :: {path, path()} | {uuid, uuid()}.
-type entry() :: uuid_or_path() | datastore:document().
-type type() :: ?REGULAR_FILE_TYPE | ?DIRECTORY_TYPE | ?SYMLINK_TYPE.
-type offset() :: non_neg_integer().
-type size() :: non_neg_integer().
-type mode() :: non_neg_integer().
-type time() :: non_neg_integer().
-type symlink_value() :: binary().
-type file_meta() :: model_record().
-type posix_permissions() :: non_neg_integer().
-type storage_sync_info() :: #storage_sync_info{}.

-export_type([doc/0, uuid/0, path/0, name/0, uuid_or_path/0, entry/0, type/0,
    offset/0, size/0, mode/0, time/0, symlink_value/0, posix_permissions/0,
    file_meta/0, storage_sync_info/0]).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {uid, string},
        {size, integer},
        {version, integer},
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {link_value, string},
        {shares, [string]}
    ]};
record_struct(2) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {owner, string},
        {size, integer},
        {version, integer},
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {link_value, string},
        {shares, [string]}
    ]};
record_struct(3) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {owner, string},
        {size, integer},
        {version, integer},
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {link_value, string},
        {shares, [string]},
        {deleted, boolean},
        {storage_sync_info, {record, [
            {children_attrs_hash, #{integer => binary}},
            {last_synchronized_mtime, integer}
        ]}}
    ]};
record_struct(4) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {owner, string},
        {size, integer},
        {version, integer},
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {link_value, string},
        {shares, [string]},
        {deleted, boolean},
        {storage_sync_info, {record, [
            {children_attrs_hash, #{integer => binary}},
            {last_synchronized_mtime, integer}
        ]}},
        {parent_uuid, string}
    ]};
record_struct(5) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {owner, string},
        {group_owner, string},
        {size, integer},
        {version, integer},
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {link_value, string},
        {shares, [string]},
        {deleted, boolean},
        {storage_sync_info, {record, [
            {children_attrs_hash, #{integer => binary}},
            {last_synchronized_mtime, integer}
        ]}},
        {parent_uuid, string}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades record from specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_upgrade(datastore_json:record_version(), tuple()) ->
    {datastore_json:record_version(), tuple()}.
record_upgrade(1, {?MODEL_NAME, Name, Type, Mode, Uid, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares}
) ->
    {2, #file_meta{name = Name, type = Type, mode = Mode, owner = Uid, size = Size,
        version = Version, is_scope = IsScope, scope = Scope,
        provider_id = ProviderId, link_value = LinkValue, shares = Shares}};
record_upgrade(2, {?MODEL_NAME, Name, Type, Mode, Owner, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares}
) ->
    {3, #file_meta{name = Name, type = Type, mode = Mode, owner = Owner, size = Size,
        version = Version, is_scope = IsScope, scope = Scope,
        provider_id = ProviderId, link_value = LinkValue, shares = Shares,
        deleted = false, storage_sync_info = #storage_sync_info{}
    }};
record_upgrade(3, {?MODEL_NAME, Name, Type, Mode, Owner, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares, StorageSyncInfo}
) ->
    {4, #file_meta{name = Name, type = Type, mode = Mode, owner = Owner, size = Size,
        version = Version, is_scope = IsScope, scope = Scope,
        provider_id = ProviderId, link_value = LinkValue, shares = Shares,
        deleted = false, storage_sync_info = StorageSyncInfo,
        parent_uuid = undefined
    }};
record_upgrade(4, {?MODEL_NAME, Name, Type, Mode, Owner, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares, StorageSyncInfo, ParentUuid}
) ->
    {5, #file_meta{name = Name, type = Type, mode = Mode, owner = Owner, size = Size,
        version = Version, is_scope = IsScope, scope = Scope,
        provider_id = ProviderId, link_value = LinkValue, shares = Shares,
        deleted = false, storage_sync_info = StorageSyncInfo,
        parent_uuid = ParentUuid, group_owner = undefined
    }}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, uuid()} | datastore:generic_error().
save(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(uuid() | entry(), Diff :: datastore:document_diff()) ->
    {ok, uuid()} | datastore:update_error().
update({uuid, Key}, Diff) ->
    update(Key, Diff);
update(#document{value = #file_meta{}, key = Key}, Diff) ->
    update(Key, Diff);
update({path, Path}, Diff) ->
    ?run(begin
        {ok, {#document{} = Document, _}} = resolve_path(Path),
        update(Document, Diff)
    end);
update(Key, Diff) ->
    model:execute_with_default_context(?MODULE, update, [Key, Diff]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, doc()} | datastore:create_error().
create(#document{value = #file_meta{is_scope = true}} = Document) ->
    model:execute_with_default_context(?MODULE, save, [Document]);
create(Document) ->
    model:execute_with_default_context(?MODULE, save, [Document],
        [{generated_uuid, true}]).

%%--------------------------------------------------------------------
%% @equiv create({uuid, ParentUuid}, FileDoc, false).
%% @end
%%--------------------------------------------------------------------
-spec create({uuid, uuid()}, doc()) ->
    {ok, uuid()} | datastore:create_error().
create({uuid, ParentUuid}, FileDoc) ->
    create({uuid, ParentUuid}, FileDoc, false).

%%--------------------------------------------------------------------
%% @doc
%% Creates new #file_meta and links it as a new child of given as first argument
%% existing #file_meta.
%% @end
%%--------------------------------------------------------------------
-spec create({uuid, ParentUuid :: uuid()}, doc(), AllowConflicts :: boolean()) ->
    {ok, uuid()} | datastore:create_error().
create({uuid, ParentUuid}, FileDoc = #document{
    value = FileMeta = #file_meta{
        name = FileName,
        is_scope = IsScope
    }
}, AllowConflicts) ->
    ?run(begin
        true = is_valid_filename(FileName),
        {ok, ParentDoc} = get(ParentUuid),
        FileDoc2 = fill_uuid(FileDoc),
        SpaceDirUuid = case IsScope of
            true ->
                FileDoc2#document.key;
            false ->
                {ok, ScopeId} = get_scope_id(ParentDoc),
                ScopeId
        end,
        SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid_no_error(SpaceDirUuid),
        FileDoc3 = FileDoc2#document{
            scope = SpaceId,
            value = FileMeta#file_meta{
                scope = SpaceDirUuid,
                provider_id = oneprovider:get_provider_id(),
                parent_uuid = ParentUuid
            }
        },
        AddFun = case AllowConflicts of
            true -> add_links;
            false -> create_link
        end,
        case model:execute_with_default_context(?MODULE, AddFun,
            [ParentDoc, {FileName, FileDoc3}])
        of
            ok ->
                case create(FileDoc3) of
                    {ok, #document{key = FileUuid}} ->
                        {ok, FileUuid};
                    Error -> Error
                end;
            Error ->
                Error
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Delete link from parent to child
%% @end
%%--------------------------------------------------------------------
-spec delete_child_link(ParentDoc :: doc(), ChildName :: name()) -> ok.
delete_child_link(ParentDoc, ChildName) ->
    ok = model:execute_with_default_context(?MODULE, delete_links, [ParentDoc, ChildName]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(uuid() | entry()) -> {ok, datastore:document()} | datastore:get_error().
get(undefined) ->
    {error, {not_found, ?MODULE}};
get({uuid, Key}) ->
    get(Key);
get(#document{value = #file_meta{}} = Document) ->
    {ok, Document};
get({path, Path}) ->
    ?run(begin
        {ok, {Doc, _}} = resolve_path(Path),
        {ok, Doc}
    end);
get(?ROOT_DIR_UUID) ->
    {ok, #document{
        key = ?ROOT_DIR_UUID,
        value = #file_meta{
            name = ?ROOT_DIR_NAME,
            is_scope = true,
            mode = 8#111,
            owner = ?ROOT_USER_ID,
            parent_uuid = ?ROOT_DIR_UUID
        }
    }};
get(Key) ->
    case get_including_deleted(Key) of
        {ok, #document{value = #file_meta{deleted = true}}} ->
            {error, {not_found, ?MODULE}};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file_meta doc even if its marked as deleted
%% @end
%%--------------------------------------------------------------------
-spec get_including_deleted(uuid()) -> {ok, datastore:document()} | datastore:get_error().
get_including_deleted(FileUuid) ->
    model:execute_with_default_context(?MODULE, get, [FileUuid]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(uuid() | entry()) -> ok | datastore:generic_error().
delete({uuid, Key}) ->
    delete(Key);
delete(#document{
    key = FileUuid,
    value = #file_meta{
        name = FileName,
        parent_uuid = ParentUuid
    }
}) ->
    ?run(begin
        ok = delete_child_link_in_parent(ParentUuid, FileName, FileUuid),
        LocalLocaitonId = file_location:local_id(FileUuid),
        file_location:delete(LocalLocaitonId),
        model:execute_with_default_context(?MODULE, delete, [FileUuid])
    end);
delete({path, Path}) ->
    ?run(begin
        {ok, {#document{} = Document, _}} = resolve_path(Path),
        delete(Document)
    end);
delete(FileUuid) ->
    ?run(begin
        case get(FileUuid) of
            {ok, #document{} = Document} ->
                delete(Document);
            {error, {not_found, _}} ->
                ok
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Similar to delete/1 but does not delete link in parent.
%% @end
%%--------------------------------------------------------------------
-spec delete_without_link(uuid() | datastore:document()) -> ok | datastore:generic_error().
delete_without_link({uuid, Key}) ->
    delete_without_link(Key);
delete_without_link(#document{
    key = FileUuid
}) ->
    delete_without_link(FileUuid);
delete_without_link(FileUuid) ->
    ?run(begin
        LocalLocaitonId = file_location:local_id(FileUuid),
        file_location:delete(LocalLocaitonId),
        model:execute_with_default_context(?MODULE, delete, [FileUuid])
    end).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(uuid() | entry()) -> datastore:exists_return().
exists({uuid, Key}) ->
    exists(Key);
exists(#document{value = #file_meta{}, key = Key}) ->
    exists(Key);
exists({path, Path}) ->
    case resolve_path(Path) of
        {ok, {#document{}, _}} ->
            true;
        {error, {not_found, _}} ->
            false;
        {error, ghost_file} ->
            false;
        {error, link_not_found} ->
            false
    end;
exists(Key) ->
    case get_including_deleted(Key) of
        {ok, #document{value = #file_meta{deleted = Deleted}}} ->
            not Deleted;
        {error, {not_found, _}} ->
            false;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    Config = ?MODEL_CONFIG(files, [], ?GLOBALLY_CACHED_LEVEL,
        ?GLOBALLY_CACHED_LEVEL, true, false, oneprovider:get_provider_id(), true),
    Config#model_config{sync_enabled = true, version = 5}.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) ->
    ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Lists children of given #file_meta.
%% @end
%%--------------------------------------------------------------------
-spec list_children(Entry :: entry(), Offset :: non_neg_integer(), Count :: non_neg_integer()) ->
    {ok, [#child_link_uuid{}]} | {error, Reason :: term()}.
list_children(Entry, Offset, Count) ->
    ?run(begin
        {ok, #document{} = File} = get(Entry),
        Res = model:execute_with_default_context(?MODULE, foreach_link, [File,
            fun
                (_LinkName, _LinkTarget, {_, 0, _} = Acc) ->
                    Acc;
                (LinkName, {_V, [{_, _, _Key, ?MODEL_NAME} | _] = Targets}, {Skip, Count1, Acc}) when is_binary(LinkName), Skip > 0 ->
                    TargetCount = length(Targets),
                    case is_hidden(LinkName) of
                        true ->
                            {Skip, Count1, Acc};
                        false when TargetCount > Skip ->
                            TargetsTagged = tag_children(LinkName, Targets),
                            SelectedTargetsTagged = lists:sublist(TargetsTagged, Skip + 1, TargetCount - Skip),
                            ChildLinks = lists:map(
                                fun({LName, LKey}) ->
                                    #child_link_uuid{name = LName, uuid = LKey}
                                end, SelectedTargetsTagged),
                            {0, Count1 + (TargetCount - Skip), ChildLinks ++ Acc};
                        false ->
                            {Skip - TargetCount, Count1, Acc}
                    end;
                (LinkName, {_V, [{_, _, _Key, ?MODEL_NAME} | _] = Targets}, {0, Count1, Acc}) when is_binary(LinkName), Count > 0 ->
                    TargetCount = length(Targets),
                    TargetsTagged = tag_children(LinkName, Targets),
                    SelectedTargetsTagged = lists:sublist(TargetsTagged, min(Count, TargetCount)),
                    case is_hidden(LinkName) of
                        true ->
                            {0, Count1, Acc};
                        false ->
                            ChildLinks = lists:map(
                                fun({LName, LKey}) ->
                                    #child_link_uuid{name = LName, uuid = LKey}
                                end, SelectedTargetsTagged),
                            {0, Count1 - length(ChildLinks), ChildLinks ++ Acc}
                    end;
                (_LinkName, _LinkTarget, AccIn) ->
                    AccIn
            end, {Offset, Count, []}]),
        case Res of
            {ok, {_, _, Uuids}} ->
                {ok, lists:reverse(Uuids)};
            {error, Reason} ->
                {error, Reason}
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Iterate over all children links and apply Fun.
%% @end
%%--------------------------------------------------------------------
-spec foreach_child(Entry :: entry(),
    fun((datastore:link_name(), datastore:link_target(), AccIn :: term()) ->
        Acc :: term()
    ),
    AccIn :: term()) -> term().
foreach_child(Entry, Fun, AccIn) ->
    ?run(begin
        {ok, #document{} = File} = get(Entry),
        model:execute_with_default_context(?MODULE, foreach_link, [File,
            fun
                (LinkName, LinkTarget, Acc) ->
                    Fun(LinkName, LinkTarget, Acc)
            end, AccIn])
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's locations for given file
%% @end
%%--------------------------------------------------------------------
-spec get_locations_by_uuid(uuid()) -> {ok, [file_location:id()]} | datastore:get_error().
get_locations_by_uuid(Uuid) ->
    ?run(begin
        case get(Uuid) of
            {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}}} ->
                {ok, []};
            {ok, #document{scope = SpaceId}} ->
                {ok, #document{value = #od_space{providers = Providers}}} =
                    od_space:get(SpaceId),
                Locations = lists:map(fun(ProviderId) ->
                    file_location:id(Uuid, ProviderId)
                end, Providers),
                {ok, Locations};
            {error, {not_found, file_meta}} ->
                {ok, []}
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Rename file_meta and change link targets
%% @end
%%--------------------------------------------------------------------
-spec rename(doc(), doc(), doc(), name()) -> ok.
rename(SourceDoc, SourceParentDoc, TargetParentDoc, TargetName) ->
    #document{
        key = FileUuid,
        value = SourceFileMeta = #file_meta{
            name = SourceName
        }
    } = SourceDoc,
    TargetDoc = SourceDoc#document{
        value = SourceFileMeta#file_meta{name = TargetName}
    },
    {ok, _} = file_meta:update(FileUuid,
        #{name => TargetName, parent_uuid => TargetParentDoc#document.key}),
    ok = file_meta:delete_child_link(SourceParentDoc, SourceName),

    ok = model:execute_with_default_context(?MODULE, add_links, [
        TargetParentDoc, {TargetName, TargetDoc}
    ]).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's parent document.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(Entry :: entry()) -> {ok, datastore:document()} | datastore:get_error().
get_parent(Entry) ->
    case get_parent_uuid(Entry) of
        {ok, ParentUuid} ->
            get({uuid, ParentUuid});
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's parent uuid.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_uuid(Entry :: entry()) -> {ok, datastore:key()} | datastore:get_error().
get_parent_uuid(Entry) ->
    ?run(begin
        {ok, #document{value = #file_meta{parent_uuid = ParentUuid}}} = get(Entry),
        {ok, ParentUuid}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns all file's ancestors' uuids.
%% @end
%%--------------------------------------------------------------------
-spec get_ancestors(uuid()) -> {ok, [uuid()]} | datastore:get_error().
get_ancestors(FileUuid) ->
    ?run(begin
        {ok, #document{key = Key}} = get(FileUuid),
        {ok, get_ancestors2(Key, [])}
    end).
get_ancestors2(?ROOT_DIR_UUID, Acc) ->
    Acc;
get_ancestors2(FileUuid, Acc) ->
    {ok, ParentUuid} = get_parent_uuid(FileUuid),
    get_ancestors2(ParentUuid, [ParentUuid | Acc]).

%%--------------------------------------------------------------------
%% @doc
%% Resolves given file_meta:path() and returns file_meta:entry() along with list of
%% all ancestors' UUIDs.
%% @end
%%--------------------------------------------------------------------
-spec resolve_path(path()) -> {ok, {datastore:document(), [uuid()]}} | datastore:generic_error().
resolve_path(Path) ->
    resolve_path({uuid, ?ROOT_DIR_UUID}, Path).

-spec resolve_path(Parent :: entry(), path()) -> {ok, {datastore:document(), [uuid()]}} | datastore:generic_error().
resolve_path(ParentEntry, <<?DIRECTORY_SEPARATOR, Path/binary>>) ->
    ?run(begin
        {ok, #document{key = RootUuid} = Root} = get(ParentEntry),
        case fslogic_path:split(Path) of
            [] ->
                {ok, {Root, [RootUuid]}};
            [First | Rest] when RootUuid =:= ?ROOT_DIR_UUID ->
                case model:execute_with_default_context(?MODULE,
                    fetch_link_target, [Root, First]) of
                    {ok, NewRoot} ->
                        NewPath = fslogic_path:join(Rest),
                        case resolve_path(NewRoot, <<?DIRECTORY_SEPARATOR, NewPath/binary>>) of
                            {ok, {Leaf, KeyPath}} ->
                                {ok, {Leaf, [RootUuid | KeyPath]}};
                            Err ->
                                Err
                        end;
                    {error, link_not_found} -> %% Map links errors to document errors
                        {error, {not_found, ?MODEL_NAME}};
                    {error, Reason} ->
                        {error, Reason}
                end;
            Tokens ->
                case model:execute_with_default_context(?MODULE, link_walk,
                    [Root, Tokens, get_leaf]) of
                    {ok, {Leaf, KeyPath}} ->
                        [_ | [RealParentUuid | _]] = lists:reverse([RootUuid | KeyPath]),
                        {ok, #document{value = #file_meta{parent_uuid = ParentUuid}}} = get(Leaf),
                        case ParentUuid of
                            RealParentUuid ->
                                {ok, {Leaf, [RootUuid | KeyPath]}};
                            _ ->
                                {error, ghost_file}
                        end;
                    {error, link_not_found} -> %% Map links errors to document errors
                        {error, {not_found, ?MODEL_NAME}};
                    {error, Reason} ->
                        {error, Reason}
                end
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Gets "scope" id of given document. "Scope" document is the nearest ancestor with #file_meta.is_scope == true.
%% @end
%%--------------------------------------------------------------------
-spec get_scope_id(Entry :: entry()) -> {ok, ScopeId :: datastore:ext_key()} | datastore:generic_error().
get_scope_id(#document{key = FileUuid, value = #file_meta{is_scope = true}}) ->
    {ok, FileUuid};
get_scope_id(#document{value = #file_meta{is_scope = false, scope = Scope}}) ->
    {ok, Scope};
get_scope_id(Entry) ->
    ?run(begin
        {ok, Doc} = get(Entry),
        get_scope_id(Doc)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Initializes files metadata for onedata user.
%% This function can and should be used to ensure that user's FS is fully synchronised. Normally
%% this function is called asynchronously automatically after user's document is updated.
%% @end
%%--------------------------------------------------------------------
-spec setup_onedata_user(oz_endpoint:auth(), UserId :: od_user:id()) -> ok.
setup_onedata_user(_Client, UserId) ->
    ?info("setup_onedata_user ~p as ~p", [_Client, UserId]),
    critical_section:run([od_user, UserId], fun() ->
        {ok, #document{value = #od_user{space_aliases = Spaces}}} =
            od_user:get(UserId),

        CTime = utils:system_time_seconds(),

        lists:foreach(fun({SpaceId, _}) ->
            make_space_exist(SpaceId)
        end, Spaces),

        FileUuid = fslogic_uuid:user_root_dir_uuid(UserId),
        ScopeId = <<>>, % TODO - do we need scope for user dir
        case create({uuid, ?ROOT_DIR_UUID},
            #document{key = FileUuid,
                value = #file_meta{
                    name = UserId, type = ?DIRECTORY_TYPE, mode = 8#1755,
                    owner = ?ROOT_USER_ID, is_scope = true, parent_uuid = ?ROOT_DIR_UUID
                }
            }) of
            {ok, _RootUuid} ->
                {ok, _} = times:save(#document{key = FileUuid, value =
                    #times{mtime = CTime, atime = CTime, ctime = CTime},
                    scope = ScopeId}),
                ok;
            {error, already_exists} -> ok
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Add shareId to file meta
%% @end
%%--------------------------------------------------------------------
-spec add_share(file_ctx:ctx(), od_share:id()) -> {ok, uuid()}  | datastore:generic_error().
add_share(FileCtx, ShareId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    update({uuid, FileUuid},
        fun(FileMeta = #file_meta{shares = Shares}) ->
            {ok, FileMeta#file_meta{shares = [ShareId | Shares]}}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Remove shareId from file meta
%% @end
%%--------------------------------------------------------------------
-spec remove_share(file_ctx:ctx(), od_share:id()) -> {ok, uuid()} | datastore:generic_error().
remove_share(FileCtx, ShareId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    update({uuid, FileUuid},
        fun(FileMeta = #file_meta{shares = Shares}) ->
            {ok, FileMeta#file_meta{shares = Shares -- [ShareId]}}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Creates file meta entry for space if not exists
%% @end
%%--------------------------------------------------------------------
-spec make_space_exist(SpaceId :: datastore:id()) -> ok | no_return().
make_space_exist(SpaceId) ->
    CTime = utils:system_time_seconds(),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    case file_meta:exists({uuid, SpaceDirUuid}) of
        true ->
            fix_parent_links({uuid, ?ROOT_DIR_UUID},
                {uuid, SpaceDirUuid});
        false ->
            case file_meta:create({uuid, ?ROOT_DIR_UUID},
                #document{key = SpaceDirUuid,
                    value = #file_meta{
                        name = SpaceId, type = ?DIRECTORY_TYPE,
                        mode = 8#1775, owner = ?ROOT_USER_ID, is_scope = true,
                        parent_uuid = ?ROOT_DIR_UUID
                    }}) of
                {ok, _} ->
                    case times:save_new(#document{key = SpaceDirUuid, value =
                    #times{mtime = CTime, atime = CTime, ctime = CTime},
                        scope = SpaceId}) of
                        {ok, _} -> ok;
                        {error, already_exists} -> ok
                    end;
                {error, already_exists} ->
                    ok
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Return file_meta doc.
%% @end
%%--------------------------------------------------------------------
-spec new_doc(undefined | file_meta:name(), undefined | file_meta:type(),
    file_meta:posix_permissions(), undefined | od_user:id(), undefined | od_group:id(),
    undefined | file_meta:size(), uuid()) -> datastore:document().
new_doc(FileName, FileType, Mode, Owner, GroupOwner, Size, ParentUuid) ->
    #document{value = #file_meta{
        name = FileName,
        type = FileType,
        mode = Mode,
        owner = Owner,
        group_owner = GroupOwner,
        size = Size,
        parent_uuid = ParentUuid
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Return type of file depending on its posix mode.
%% @end
%%--------------------------------------------------------------------
-spec type(Mode :: non_neg_integer()) -> type().
type(Mode) ->
    IsDir = (Mode band 8#100000) == 0,
    case IsDir of
        true -> ?DIRECTORY_TYPE;
        false -> ?REGULAR_FILE_TYPE
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file name with added hidden file prefix.
%% @end
%%--------------------------------------------------------------------
-spec hidden_file_name(NAme :: name()) -> name().
hidden_file_name(FileName) ->
    <<?HIDDEN_FILE_PREFIX, FileName/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Checks if given filename contains hidden file prefix.
%% @end
%%--------------------------------------------------------------------
-spec is_hidden(FileName :: name()) -> boolean().
is_hidden(FileName) ->
    case FileName of
        <<?HIDDEN_FILE_PREFIX, _/binary>> -> true;
        _ -> false
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates uuid if needed.
%% @end
%%--------------------------------------------------------------------
-spec fill_uuid(doc()) -> doc().
fill_uuid(Doc = #document{key = undefined}) ->
    NewUuid = datastore_utils:gen_uuid(),
    Doc#document{key = NewUuid};
fill_uuid(Doc) ->
    Doc.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fixes links to given document in its parent. Also fixes 'parent' field.
%% @end
%%--------------------------------------------------------------------
-spec fix_parent_links(Parent :: entry(), File :: entry()) ->
    ok | no_return().
fix_parent_links(Parent, Entry) ->
    {ok, ParentDoc = #document{key = ParentUuid}} = get(Parent),
    {ok, FileDoc = #document{
        key = FileUuid,
        value = #file_meta{
            name = FileName,
            parent_uuid = ParentUuidInDoc
        }
    }} = get(Entry),
    {ok, ScopeId} = get_scope_id(Parent),
    ok = model:execute_with_default_context(?MODULE, set_links,
        [ParentDoc, {FileName, FileDoc}]),
    ok = set_scope(FileDoc, ScopeId),
    case ParentUuidInDoc =/= ParentUuid of
        true ->
            update(FileUuid, #{parent_uuid => ParentUuid});
        false ->
            ok
    end,
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tag each given link with its scope.
%% @end
%%--------------------------------------------------------------------
-spec tag_children(LinkName :: datastore:link_name(), [datastore:link_final_target()]) ->
    [{datastore:link_name(), datastore:ext_key()}].
tag_children(LinkName, [{_Scope, _VH, Key, _Model}]) ->
    [{LinkName, Key}];
tag_children(LinkName, Targets) ->
    MPID = oneprovider:get_provider_id(),
    Scopes = lists:map(
        fun({Scope, _VH, _Key, _Model}) ->
            Scope
        end, Targets),
    MinScope = lists:min([4 | lists:map(fun size/1, Scopes)]),
    LongestPrefix = max(MinScope, binary:longest_common_prefix(Scopes)),
    lists:map(
        fun({Scope, VH, Key, _}) ->
            case MPID of
                Scope ->
                    {LinkName, Key};
                _ ->
                    case LongestPrefix >= size(Scope) of
                        true ->
                            {links_utils:make_scoped_link_name(LinkName, Scope, VH, size(Scope)), Key};
                        false ->
                            {links_utils:make_scoped_link_name(LinkName, Scope, undefined, LongestPrefix + 1), Key}
                    end
            end
        end, Targets).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Remove the child's links in given parent that corresponds to given child's Name and UUID.
%% @end
%%--------------------------------------------------------------------
-spec delete_child_link_in_parent(ParentUuid :: uuid(), ChildName :: name(), ChildUuid :: uuid()) ->
    ok | {error, Reason :: any()}.
delete_child_link_in_parent(ParentUuid, ChildName, ChildUuid) ->
    case model:execute_with_default_context(?MODULE, fetch_full_link,
        [ParentUuid, ChildName]) of
        {ok, {_, ParentTargets}} ->
            {ok, #document{scope = Scope}} = get({uuid, ParentUuid}),
            lists:foreach(
                fun({Scope0, VHash0, Key0, _}) ->
                    case Key0 of
                        ChildUuid ->
                            ok = model:execute_with_default_context(?MODULE, delete_links,
                                [ParentUuid, [links_utils:make_scoped_link_name(ChildName,
                                    Scope0, VHash0, size(Scope0))]], ?SET_LINK_SCOPE(Scope));
                        _ -> ok
                    end
                end, ParentTargets);
        {error, link_not_found} ->
            ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets scope for single entry
%% @end
%%--------------------------------------------------------------------
-spec set_scope(Entry :: entry(), Scope :: datastore:key()) -> ok | datastore:generic_error().
set_scope(Entry, Scope) ->
    Diff = #{scope => Scope},
    case update(Entry, Diff) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check if given term is valid path()
%% @end
%%--------------------------------------------------------------------
-spec is_valid_filename(term()) -> boolean().
is_valid_filename(<<"">>) ->
    false;
is_valid_filename(<<".">>) ->
    false;
is_valid_filename(<<"..">>) ->
    false;
is_valid_filename(FileName) when not is_binary(FileName) ->
    false;
is_valid_filename(FileName) when is_binary(FileName) ->
    case binary:matches(FileName, <<?DIRECTORY_SEPARATOR>>) of
        [] -> true;
        _ -> false
    end.