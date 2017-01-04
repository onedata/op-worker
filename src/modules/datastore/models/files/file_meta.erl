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

%% Separator used in filename for specifying snapshot version.
-define(SNAPSHOT_SEPARATOR, "::").

%% Prefix for link name for #file_location link
-define(LOCATION_PREFIX, "location_").

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).

-export([resolve_path/1, resolve_path/2, create/2, create/3, get_scope/1, get_scope_id/1, list_children/3, get_parent/1,
    get_parent_uuid/1, get_parent_uuid/2, get_parent_uuid_in_context/1, rename/2, setup_onedata_user/2,
    get_name/1]).
-export([get_ancestors/1, attach_location/3, get_locations/1, get_space_dir/1, location_ref/1]).
-export([snapshot_name/2, get_current_snapshot/1, to_uuid/1, is_root_dir/1]).
-export([fix_parent_links/2, fix_parent_links/1, exists_local_link_doc/1, get_child/2]).
-export([create_phantom_file/3, get_guid_from_phantom_file/1]).
-export([hidden_file_name/1]).
-export([add_share/2, remove_share/2]).
-export([get_uuid/1]).
-export([record_struct/1]).

-type doc() :: datastore:document().
-type uuid() :: datastore:key().
-type path() :: binary().
-type name() :: binary().
-type uuid_or_path() :: {path, path()} | {uuid, uuid()}.
-type entry() :: uuid_or_path() | datastore:document().
-type type() :: ?REGULAR_FILE_TYPE | ?DIRECTORY_TYPE | ?SYMLINK_TYPE | ?PHANTOM_TYPE.
-type offset() :: non_neg_integer().
-type size() :: non_neg_integer().
-type mode() :: non_neg_integer().
-type time() :: non_neg_integer().
-type symlink_value() :: binary().
-type file_meta() :: model_record().
-type posix_permissions() :: non_neg_integer().

-export_type([doc/0, uuid/0, path/0, name/0, uuid_or_path/0, entry/0, type/0, offset/0,
    size/0, mode/0, time/0, symlink_value/0, posix_permissions/0, file_meta/0]).


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
    ]}.


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
    datastore:save(?STORE_LEVEL, Document).

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
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, uuid()} | datastore:create_error().
create(#document{value = #file_meta{name = FileName}} = Document) ->
    case is_valid_filename(FileName) of
        true ->
            datastore:create(?STORE_LEVEL, Document);
        false ->
            {error, invalid_filename}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates new #file_meta and links it as a new child of given as first argument existing #file_meta.
%% @end
%%--------------------------------------------------------------------
-spec create(entry(), file_meta() | datastore:document()) -> {ok, uuid()} | datastore:create_error().
create(Parent, File) ->
    create(Parent, File, false).

%%--------------------------------------------------------------------
%% @doc
%% Creates new #file_meta and links it as a new child of given as first argument existing #file_meta.
%% @end
%%--------------------------------------------------------------------
-spec create(entry(), file_meta() | datastore:document(), AllowConflicts :: boolean()) -> {ok, uuid()} | datastore:create_error().
create({uuid, ParentUUID}, File, AllowConflicts) ->
    ?run(begin
        {ok, Parent} = get(ParentUUID),
        create(Parent, File, AllowConflicts)
    end);
create({path, Path}, File, AllowConflicts) ->
    ?run(begin
        {ok, {Parent, _}} = resolve_path(Path),
        create(Parent, File, AllowConflicts)
    end);
create(#document{} = Parent, #file_meta{} = File, AllowConflicts) ->
    create(Parent, #document{value = File}, AllowConflicts);
create(#document{key = ParentUUID} = Parent, #document{value = #file_meta{name = FileName, version = V} = FM} = FileDoc0, AllowConflicts) ->
    ?run(begin
        {ok, Scope} = get_scope(Parent),
        FM1 = FM#file_meta{scope = Scope#document.key, provider_id = oneprovider:get_provider_id()},
        FileDoc =
            case FileDoc0 of
                #document{key = undefined} = Doc ->
                    NewUUID = fslogic_uuid:gen_file_uuid(),
                    Doc#document{key = NewUUID, value = FM1, generated_uuid = true};
                _ ->
                    FileDoc0#document{value = FM1}
            end,
        false = is_snapshot(FileName),
        critical_section:run_on_mnesia([?MODEL_NAME, ParentUUID],
            fun() ->
                Exists = case AllowConflicts of
                    true -> false;
                    false ->
                        case resolve_path(ParentUUID, fslogic_path:join([<<?DIRECTORY_SEPARATOR>>, FileName])) of
                            {error, {not_found, _}} ->
                                false;
                            {ok, _} ->
                                true
                        end
                end,

                case Exists of
                    false ->
                        datastore:run_transaction(fun() ->
                            case create(FileDoc) of
                                {ok, UUID} ->
                                    SavedDoc = FileDoc#document{key = UUID},
                                    ok = datastore:add_links(?LINK_STORE_LEVEL, Parent, {FileName, SavedDoc}),
                                    ok = datastore:add_links(?LINK_STORE_LEVEL, Parent, {snapshot_name(FileName, V), SavedDoc}),
                                    ok = datastore:add_links(?LINK_STORE_LEVEL, SavedDoc, [{parent, Parent}]),
                                    {ok, UUID};
                                {error, Reason} ->
                                    {error, Reason}
                            end
                        end);
                    true ->
                        {error, already_exists}
                end
            end)

    end).


%%--------------------------------------------------------------------
%% @doc
%% Retrieves name of the file.
%% @end
%%--------------------------------------------------------------------
-spec get_name(entry()) -> {ok, file_meta:name()} | {datastore:get_error()}.
get_name(Entry) ->
    case file_meta:get(Entry) of
        {ok, #document{value = #file_meta{name = Name}}} -> {ok, Name};
        {error, Reason} -> {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Fixes links to given document in its parent. Assumes that link to parent is valid.
%% If the parent entry() is known its safer to use fix_parent_links/2.
%% @end
%%--------------------------------------------------------------------
-spec fix_parent_links(entry()) ->
    ok | no_return().
fix_parent_links(Entry) ->
    {ok, Parent} = get_parent(Entry),
    fix_parent_links(Parent, Entry).


%%--------------------------------------------------------------------
%% @doc
%% Fixes links to given document in its parent. Also fixes 'parent' link.
%% @end
%%--------------------------------------------------------------------
-spec fix_parent_links(Parent :: entry(), File :: entry()) ->
    ok | no_return().
fix_parent_links(Parent, Entry) ->
    {ok, #document{} = ParentDoc} = get(Parent),
    {ok, #document{value = #file_meta{name = FileName, version = V}} = FileDoc} = get(Entry),
    {ok, Scope} = get_scope(Parent),
    datastore:run_transaction(fun() ->
        ok = datastore:set_links(?LINK_STORE_LEVEL, ParentDoc, {FileName, FileDoc}),
        ok = datastore:set_links(?LINK_STORE_LEVEL, ParentDoc, {snapshot_name(FileName, V), FileDoc}),
        ok = datastore:set_links(?LINK_STORE_LEVEL, FileDoc, [{parent, ParentDoc}])
    end),
    ok = set_scope(FileDoc, Scope#document.key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(uuid() | entry()) -> {ok, datastore:document()} | datastore:get_error().
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
    {ok, #document{key = ?ROOT_DIR_UUID, value =
    #file_meta{name = ?ROOT_DIR_NAME, is_scope = true, mode = 8#111, uid = ?ROOT_USER_ID}}};
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(uuid() | entry()) -> ok | datastore:generic_error().
delete({uuid, Key}) ->
    delete(Key);
delete(#document{value = #file_meta{name = FileName, version = Version}, key = Key} = Doc) ->
    ?run(begin
        case datastore:fetch_link(?LINK_STORE_LEVEL, Key, ?MODEL_NAME, parent) of
            {ok, {ParentKey, ?MODEL_NAME}} ->
                ok = delete_child_link_in_parent(ParentKey, FileName, Key),
                ok = delete_child_link_in_parent(ParentKey, snapshot_name(FileName, Version), Key);
            _ ->
                ok
        end,
        case datastore:fetch_link(?LINK_STORE_LEVEL, Doc, location_ref(oneprovider:get_provider_id())) of
            {ok, {LocKey, LocationModel}} ->
                LocationModel:delete(LocKey);
            _ ->
                ok
        end,
        datastore:delete(?STORE_LEVEL, ?MODULE, Key)
    end);
delete({path, Path}) ->
    ?run(begin
        {ok, {#document{} = Document, _}} = resolve_path(Path),
        delete(Document)
    end);
delete(Key) ->
    ?run(begin
        case get(Key) of
            {ok, #document{} = Document} ->
                delete(Document);
            {error, {not_found, _}} ->
                ok
        end
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
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% Checks if local link doc exists for key/
%% @end
%%--------------------------------------------------------------------
-spec exists_local_link_doc(uuid()) -> datastore:exists_return().
exists_local_link_doc(Key) ->
    ?RESPONSE(datastore:exists_link_doc(?LINK_STORE_LEVEL, Key, ?MODULE, oneprovider:get_provider_id())).

%%--------------------------------------------------------------------
%% @doc
%% Returns child UUIDs
%% @end
%%--------------------------------------------------------------------
-spec get_child(datastore:document() | {uuid, uuid()}, name()) ->
    {ok, [uuid()]} | datastore:link_error() | datastore:generic_error().
get_child({uuid, Uuid}, Name) ->
    case get({uuid, Uuid}) of
        {ok, #document{} = Doc} ->
            get_child(Doc, Name);
        Error ->
            Error
    end;
get_child(Doc, Name) ->
    case datastore:fetch_full_link(?LINK_STORE_LEVEL, Doc, Name) of
        {ok, {_, Targets}} ->
            {ok, [UUID || {_, _, UUID, _} <- Targets]};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(files, [{od_user, create}, {od_user, create_or_update}, {od_user, save}, {od_user, update}],
        ?GLOBALLY_CACHED_LEVEL, ?GLOBALLY_CACHED_LEVEL, true, false,
        oneprovider:get_provider_id(), true)#model_config{sync_enabled = true}.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(od_user, create, ?GLOBAL_ONLY_LEVEL, _, {ok, UUID}) ->
    setup_onedata_user(provider, UUID);
'after'(od_user, save, ?GLOBAL_ONLY_LEVEL, _, {ok, UUID}) ->
    setup_onedata_user(provider, UUID);
'after'(od_user, update, ?GLOBAL_ONLY_LEVEL, _, {ok, UUID}) ->
    setup_onedata_user(provider, UUID);
'after'(od_user, create_or_update, ?GLOBAL_ONLY_LEVEL, _, {ok, UUID}) ->
    setup_onedata_user(provider, UUID);
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
    {ok, [#child_link{}]} | {error, Reason :: term()}.
list_children(Entry, Offset, Count) ->
    ?run(begin
        {ok, #document{} = File} = get(Entry),
        Res = datastore:foreach_link(?LINK_STORE_LEVEL, File,
            fun
                (_LinkName, _LinkTarget, {_, 0, _} = Acc) ->
                    Acc;
                (LinkName, {_V, [{_, _, _Key, ?MODEL_NAME} | _] = Targets}, {Skip, Count1, Acc}) when is_binary(LinkName), Skip > 0 ->
                    TargetCount = length(Targets),
                    case is_snapshot(LinkName) orelse is_hidden(LinkName) of
                        true ->
                            {Skip, Count1, Acc};
                        false when TargetCount > Skip ->
                            TargetsTagged = tag_children(LinkName, Targets),
                            SelectedTargetsTagged = lists:sublist(TargetsTagged, Skip + 1, TargetCount - Skip),
                            ChildLinks = lists:map(
                                fun({LName, LKey}) ->
                                    #child_link{name = LName, uuid = LKey}
                                end, SelectedTargetsTagged),
                            {0, Count1 + (TargetCount - Skip), ChildLinks ++ Acc};
                        false ->
                            {Skip - TargetCount, Count1, Acc}
                    end;
                (LinkName, {_V, [{_, _, _Key, ?MODEL_NAME} | _] = Targets}, {0, Count1, Acc}) when is_binary(LinkName), Count > 0 ->
                    TargetCount = length(Targets),
                    TargetsTagged = tag_children(LinkName, Targets),
                    SelectedTargetsTagged = lists:sublist(TargetsTagged, min(Count, TargetCount)),
                    case is_snapshot(LinkName) orelse is_hidden(LinkName) of
                        true ->
                            {0, Count1, Acc};
                        false ->
                            ChildLinks = lists:map(
                                fun({LName, LKey}) ->
                                    #child_link{name = LName, uuid = LKey}
                                end, SelectedTargetsTagged),
                            {0, Count1 - length(ChildLinks), ChildLinks ++ Acc}
                    end;
                (_LinkName, _LinkTarget, AccIn) ->
                    AccIn
            end, {Offset, Count, []}),
        case Res of
            {ok, {_, _, UUIDs}} ->
                {ok, lists:reverse(UUIDs)};
            {error, Reason} ->
                {error, Reason}
        end
    end).

%%--------------------------------------------------------------------
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
%% @doc
%% Returns file's locations attached with attach_location/3.
%% @end
%%--------------------------------------------------------------------
-spec get_locations(entry()) -> {ok, [file_location:id()]} | datastore:get_error().
get_locations(Entry) ->
    ?run(begin
        case get(Entry) of
            {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}}} ->
                {ok, []};
            {ok, File} ->
                datastore:foreach_link(?LINK_STORE_LEVEL, File,
                    fun
                        (<<?LOCATION_PREFIX, _/binary>>, {_V, [{_, _, Key, file_location}]}, AccIn) ->
                            [Key | AccIn];
                        (_LinkName, _LinkTarget, AccIn) ->
                            AccIn
                    end, [])
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's parent document.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(Entry :: entry()) -> {ok, datastore:document()} | datastore:get_error().
get_parent(Entry) ->
    ?run(begin
        case get(Entry) of
            {ok, #document{key = ?ROOT_DIR_UUID}} = RootResp ->
                RootResp;
            {ok, #document{key = Key} = Doc} ->
                {ok, {ParentKey, ?MODEL_NAME}} =
                    datastore:fetch_link(?LINK_STORE_LEVEL, Key, ?MODEL_NAME, parent),
                get({uuid, ParentKey})
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's parent uuid.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_uuid(Entry :: entry()) -> {ok, datastore:key()} | datastore:get_error().
get_parent_uuid(Entry) ->
    ?run(begin
        case get(Entry) of
            {ok, #document{key = ?ROOT_DIR_UUID}} = RootResp ->
                {ok, ?ROOT_DIR_UUID};
            {ok, #document{key = Key} = Doc} ->
                {ok, {ParentKey, ?MODEL_NAME}} =
                    datastore:fetch_link(?LINK_STORE_LEVEL, Key, ?MODEL_NAME, parent),
                {ok, ParentKey}
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's parent uuid.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_uuid(file_meta:uuid(), od_space:id()) -> {ok, datastore:key()} | datastore:get_error().
get_parent_uuid(?ROOT_DIR_UUID, _SpaceId) ->
    ?ROOT_DIR_UUID;
get_parent_uuid(FileUuid, SpaceId) ->
    ?run(begin
        {ok, {ParentKey, ?MODEL_NAME}} =
            datastore:fetch_link(?LINK_STORE_LEVEL, FileUuid, ?MODEL_NAME, parent),
        {ok, ParentKey}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's parent uuid in context.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_uuid_in_context(file_meta:uuid()) -> {ok, datastore:key()} | datastore:get_error().
get_parent_uuid_in_context(?ROOT_DIR_UUID) ->
    ?ROOT_DIR_UUID;
get_parent_uuid_in_context(FileUuid) ->
    {ok, {ParentKey, ?MODEL_NAME}} =
        datastore:fetch_link(?LINK_STORE_LEVEL, FileUuid, ?MODEL_NAME, parent),
    {ok, ParentKey}.

%%--------------------------------------------------------------------
%% @doc
%% Returns all file's ancestors' uuids.
%% @end
%%--------------------------------------------------------------------
-spec get_ancestors(Entry :: entry()) -> {ok, [uuid()]} | datastore:get_error().
get_ancestors(Entry) ->
    ?run(begin
        {ok, #document{key = Key} = Doc} = get(Entry),
        {ok, get_ancestors2(Key, [])}
    end).
get_ancestors2(?ROOT_DIR_UUID, Acc) ->
    Acc;
get_ancestors2(Key, Acc) ->
    {ok, {ParentKey, ?MODEL_NAME}} = datastore:fetch_link(?LINK_STORE_LEVEL, Key, ?MODEL_NAME, parent),
    get_ancestors2(ParentKey, [ParentKey | Acc]).

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
        {ok, #document{key = RootUUID} = Root} = get(ParentEntry),
        case fslogic_path:split(Path) of
            [] ->
                {ok, {Root, [RootUUID]}};
            [First | Rest] when RootUUID =:= ?ROOT_DIR_UUID ->
                case datastore:fetch_link_target(?LINK_STORE_LEVEL, Root, First) of
                    {ok, NewRoot} ->
                        NewPath = fslogic_path:join(Rest),
                        case resolve_path(NewRoot, <<?DIRECTORY_SEPARATOR, NewPath/binary>>) of
                            {ok, {Leaf, KeyPath}} ->
                                {ok, {Leaf, [RootUUID | KeyPath]}};
                            Err ->
                                Err
                        end;
                    {error, link_not_found} -> %% Map links errors to document errors
                        {error, {not_found, ?MODEL_NAME}};
                    {error, Reason} ->
                        {error, Reason}
                end;
            Tokens ->
                case datastore:link_walk(?LINK_STORE_LEVEL, Root, Tokens, get_leaf) of
                    {ok, {Leaf, KeyPath}} ->
                        [_ | [RealParentUUID | _]] = lists:reverse([RootUUID | KeyPath]),
                        {ok, {ParentUUID, _}} = datastore:fetch_link(?LINK_STORE_LEVEL, Leaf, parent),
                        case ParentUUID of
                            RealParentUUID ->
                                {ok, {Leaf, [RootUUID | KeyPath]}};
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
%% Moves given file to specific location. Move operation ({path, _}) is more generic, but
%% rename using simple file name ({name, _}) is faster because it does not change parent of the file.
%% @end
%%--------------------------------------------------------------------
-spec rename(entry(), {name, name()} | {path, path()}) -> ok | datastore:generic_error().
rename({path, Path}, Op) ->
    ?run(begin
        {ok, {Subj, KeyPath}} = resolve_path(Path),
        [_ | [ParentUUID | _]] = lists:reverse(KeyPath),
        rename3(Subj, ParentUUID, Op)
    end);
rename(Entry, Op) ->
    ?run(begin
        {ok, Subj} = get(Entry),
        {ok, {ParentUUID, _}} = datastore:fetch_link(?LINK_STORE_LEVEL, Subj, parent),
        rename3(Subj, ParentUUID, Op)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Gets "scope" document of given document. "Scope" document is the nearest ancestor with #file_meta.is_scope == true.
%% @end
%%--------------------------------------------------------------------
-spec get_scope(Entry :: entry()) -> {ok, ScopeDoc :: datastore:document()} | datastore:generic_error().
get_scope(#document{value = #file_meta{is_scope = true}} = Document) ->
    {ok, Document};
get_scope(#document{value = #file_meta{is_scope = false, scope = Scope}}) ->
    get(Scope);
get_scope(Entry) ->
    ?run(begin
        {ok, Doc} = get(Entry),
        get_scope(Doc)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Gets "scope" document of given document. "Scope" document is the nearest ancestor with #file_meta.is_scope == true.
%% @end
%%--------------------------------------------------------------------
-spec get_scope_id(Entry :: entry()) -> {ok, ScopeId :: datastore:ext_key()} | datastore:generic_error().
get_scope_id(#document{key = K, value = #file_meta{is_scope = true}}) ->
    {ok, K};
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

        CTime = erlang:system_time(seconds),

        lists:foreach(fun({SpaceId, _}) ->
            fslogic_spaces:make_space_exist(SpaceId)
        end, Spaces),

        FileUuid = fslogic_uuid:user_root_dir_uuid(UserId),
        case create({uuid, ?ROOT_DIR_UUID},
            #document{key = FileUuid,
                value = #file_meta{
                    name = UserId, type = ?DIRECTORY_TYPE, mode = 8#1755,
                   uid = ?ROOT_USER_ID, is_scope = true
                }
            }) of
            {ok, _RootUUID} ->
                {ok, _} = times:save(#document{key = FileUuid, value =
                    #times{mtime = CTime, atime = CTime, ctime = CTime}}),
                ok;
            {error, already_exists} -> ok
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% Adds links between given file_meta and given location document.
%% @end
%%--------------------------------------------------------------------
-spec attach_location(entry(), Location :: datastore:document() | datastore:key(), ProviderId :: oneprovider:id()) ->
    ok.
attach_location(Entry, #document{key = LocId}, ProviderId) ->
    attach_location(Entry, LocId, ProviderId);
attach_location(Entry, LocId, ProviderId) ->
    {ok, #document{key = FileId} = FDoc} = get(Entry),
    datastore:run_transaction(fun() ->
        ok = datastore:add_links(?LINK_STORE_LEVEL, FDoc, {location_ref(ProviderId), {LocId, file_location}}),
        ok = datastore:add_links(?LINK_STORE_LEVEL, LocId, file_location, {file_meta, {FileId, file_meta}})
    end).

%%--------------------------------------------------------------------
%% @doc Get space dir document for given SpaceId
%%--------------------------------------------------------------------
-spec get_space_dir(SpaceId :: binary()) ->
    {ok, datastore:document()} | datastore:get_error().
get_space_dir(SpaceId) ->
    get(fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)).

%%--------------------------------------------------------------------
%% @doc
%% Returns uuid() for given file_meta:entry(). Providers for example path() -> uuid() conversion.
%% @end
%%--------------------------------------------------------------------
-spec to_uuid(entry() | {guid, fslogic_worker:file_guid()}) -> {ok, uuid()} | datastore:generic_error().
to_uuid({uuid, UUID}) ->
    {ok, UUID};
to_uuid({guid, FileGUID}) ->
    {ok, fslogic_uuid:guid_to_uuid(FileGUID)};
to_uuid(#document{key = UUID}) ->
    {ok, UUID};
to_uuid({path, Path}) ->
    ?run(begin
        {ok, {Doc, _}} = resolve_path(Path),
        to_uuid(Doc)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Checks if given file doc represents root directory with empty path.
%% @end
%%--------------------------------------------------------------------
-spec is_root_dir(datastore:document()) -> boolean().
is_root_dir(#document{key = Key}) ->
    Key =:= ?ROOT_DIR_UUID.

%%--------------------------------------------------------------------
%% @doc
%% Creates phantom file serving as redirection to file that has
%% recently changed its GUID.
%% @end
%%--------------------------------------------------------------------
-spec create_phantom_file(uuid(), uuid(), fslogic_worker:file_guid()) ->
    {ok, uuid()} | datastore:generic_error().
create_phantom_file(OldUUID, OldScope, NewGUID) ->
    {ok, PhantomUuid} = save(#document{key = fslogic_uuid:uuid_to_phantom_uuid(OldUUID),
        value = #file_meta{type = ?PHANTOM_TYPE, scope = OldScope, link_value = NewGUID}}),
    CreationTime = erlang:system_time(seconds),
    task_manager:start_task(fun() ->
        TimeSinceCreation = erlang:system_time(seconds) - CreationTime,
        {ok, PhantomLifespan} = application:get_env(?APP_NAME, phantom_lifespan_seconds),
        case TimeSinceCreation > PhantomLifespan of
            false ->
                timer:sleep(timer:seconds(PhantomLifespan));
            true ->
                ok
        end,
        case file_meta:delete(PhantomUuid) of
            ok ->
                ?debug("Deleted phantom file redirecting to ~p", [NewGUID]),
                ok;
            Error ->
                ?debug("Error deleting phantom file redirecting to ~p: ~p", [NewGUID, Error]),
                Error
        end
    end, ?NODE_LEVEL),
    {ok, PhantomUuid}.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves new GUID from phantom file basing on missing file UUID.
%% @end
%%--------------------------------------------------------------------
-spec get_guid_from_phantom_file(uuid()) ->
    {ok, fslogic_worker:file_guid()} | datastore:get_error().
get_guid_from_phantom_file(OldUUID) ->
    case get(fslogic_uuid:uuid_to_phantom_uuid(OldUUID)) of
        {ok, #document{value = #file_meta{link_value = NewGuid, type = ?PHANTOM_TYPE}}} ->
            {ok, NewGuid};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Add shareId to file meta
%% @end
%%--------------------------------------------------------------------
-spec add_share(file_info:file_info(), od_share:id()) -> {ok, uuid()}  | datastore:generic_error().
add_share(File, ShareId) ->
    {FileEntry, _File2} = file_info:get_uuid_entry(File),
    update(FileEntry,
        fun(FileMeta = #file_meta{shares = Shares}) ->
            {ok, FileMeta#file_meta{shares = [ShareId | Shares]}}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Remove shareId from file meta
%% @end
%%--------------------------------------------------------------------
-spec remove_share(file_info:file_info(), od_share:id()) -> {ok, uuid()} | datastore:generic_error().
remove_share(File, ShareId) ->
    {FileEntry, _File2} = file_info:get_uuid_entry(File),
    update(FileEntry,
        fun(FileMeta = #file_meta{shares = Shares}) ->
            {ok, FileMeta#file_meta{shares = Shares -- [ShareId]}}
        end).

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Remove the child's links in given parent that corresponds to given child's Name and UUID.
%% @end
%%--------------------------------------------------------------------
-spec delete_child_link_in_parent(ParentUUID :: uuid(), ChildName :: name(), ChildUUID :: uuid()) ->
    ok | {error, Reason :: any()}.
delete_child_link_in_parent(ParentUUID, ChildName, ChildUUID) ->
    case datastore:fetch_full_link(?LINK_STORE_LEVEL, ParentUUID, ?MODEL_NAME, ChildName) of
        {ok, {_, ParentTargets}} ->
            lists:foreach(
                fun({Scope0, VHash0, Key0, _}) ->
                    case Key0 of
                        ChildUUID ->
                            ok = datastore:delete_links(?LINK_STORE_LEVEL, ParentUUID, ?MODEL_NAME,
                                [links_utils:make_scoped_link_name(ChildName, Scope0, VHash0, size(Scope0))]);
                        _ -> ok
                    end
                end, ParentTargets);
        {error,link_not_found} ->
            ok;
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Internal helper function for rename/2.
%% @end
%%--------------------------------------------------------------------
-spec rename3(Subject :: datastore:document(), ParentUUID :: uuid(),
    {name, NewName :: name()} | {path, NewPath :: path()}) ->
    ok | datastore:generic_error().
rename3(#document{key = FileUUID, value = #file_meta{name = OldName, version = V}} = Subject, ParentUUID, {name, NewName}) ->
    ?run(begin
        critical_section:run_on_mnesia([?MODEL_NAME, ParentUUID], fun() ->
            {ok, FileUUID} = update(Subject, #{name => NewName}),
            ok = update_links_in_parents(ParentUUID, ParentUUID, OldName, NewName, V, {uuid, FileUUID})
        end)
    end);

rename3(#document{key = FileUUID, value = #file_meta{name = OldName, version = V}} = Subject, OldParentUUID, {path, NewPath}) ->
    ?run(begin
        NewTokens = fslogic_path:split(NewPath),
        [NewName | NewParentTokens] = lists:reverse(NewTokens),
        NewParentPath = fslogic_path:join(lists:reverse(NewParentTokens)),
        {ok, #document{key = NewParentUUID} = NewParent} = get({path, NewParentPath}),
        case NewParentUUID =:= OldParentUUID of
            true ->
                rename3(Subject, OldParentUUID, {name, NewName});
            false ->
                %% Sort keys to avoid deadlock with rename from target to source
                [Key1, Key2] = lists:sort([OldParentUUID, NewParentUUID]),

                critical_section:run_on_mnesia([?MODEL_NAME, Key1], fun() ->
                    critical_section:run_on_mnesia([?MODEL_NAME, Key2], fun() ->
                        {ok, #document{key = NewScopeUUID} = NewScope} = get_scope(NewParent),
                        {ok, FileUUID} = update(Subject, #{name => NewName, scope => NewScopeUUID}),
                        ok = datastore:set_links(?LINK_STORE_LEVEL, FileUUID, ?MODEL_NAME, {parent, NewParent}),
                        ok = update_links_in_parents(OldParentUUID, NewParentUUID, OldName, NewName, V, {uuid, FileUUID}),

                        ok = update_scopes(Subject, NewScope)
                    end)
                end)
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Remove snapshot child links from old parent to entry and
%% create snapshot child links from new parent to entry using new name.
%% If entry is current snapshot of the file, do the same for non-snapshot
%% child links.
%% @end
%%--------------------------------------------------------------------
-spec update_links_in_parents(OldParentUUID :: uuid(), NewParentUUID :: uuid(), OldName :: name(),
    NewName :: name(), Version :: non_neg_integer(), Subject :: entry()) -> ok.
update_links_in_parents(OldParentUUID, NewParentUUID, OldName, NewName, Version, Subject) ->
    {ok, #document{key = SubjectUUID} = SubjectDoc} = get(Subject),
    case get_current_snapshot(SubjectDoc) =:= SubjectDoc of
        true ->
            ok = delete_child_link_in_parent(OldParentUUID, OldName, SubjectUUID),
            ok = delete_child_link_in_parent(OldParentUUID, snapshot_name(OldName, Version), SubjectUUID),
            ok = datastore:add_links(?LINK_STORE_LEVEL, NewParentUUID, ?MODEL_NAME,
                {snapshot_name(NewName, Version), {SubjectUUID, ?MODEL_NAME}}),
            ok = datastore:add_links(?LINK_STORE_LEVEL, NewParentUUID, ?MODEL_NAME,
                {NewName, {SubjectUUID, ?MODEL_NAME}});
        false ->
            ok = delete_child_link_in_parent(OldParentUUID, OldName, SubjectUUID),
            ok = datastore:add_links(?LINK_STORE_LEVEL, NewParentUUID, ?MODEL_NAME,
                {snapshot_name(NewName, Version), {SubjectUUID, ?MODEL_NAME}})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Force set "scope" document for given file_meta:entry() and all its children recursively but only if
%% given file_meta:entry() has different "scope" document.
%% @end
%%--------------------------------------------------------------------
-spec update_scopes(Entry :: entry(), NewScope :: datastore:document()) -> ok | datastore:generic_error().
update_scopes(Entry, #document{key = NewScopeUUID} = NewScope) ->
    ?run(begin
        {ok, #document{key = OldScopeUUID}} = get_scope(Entry),
        case OldScopeUUID of
            NewScopeUUID -> ok;
            _ ->
                set_scopes(Entry, NewScope)
        end
    end).

%%--------------------------------------------------------------------
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
%% @doc
%% Force set "scope" document for given file_meta:entry() and all its children recursively.
%% @end
%%--------------------------------------------------------------------
-spec set_scopes(entry(), datastore:document()) -> ok | datastore:generic_error().
set_scopes(Entry, #document{key = NewScopeUUID}) ->
    ?run(begin
        SetterFun =
            fun(CurrentEntry, ScopeUUID) ->
                case CurrentEntry of
                    Entry ->
                        ok;
                    _ ->
                        set_scope(CurrentEntry, ScopeUUID)
                end
            end,

        Master = self(),
        ReceiverFun =
            fun Receiver() ->
                receive
                    {Entry0, ScopeUUID0} ->
                        SetterFun(Entry0, ScopeUUID0),
                        Receiver();
                    exit ->
                        ok,
                        Master ! scope_setting_done
                end
            end,
        Setters = [spawn_link(ReceiverFun) || _ <- lists:seq(1, ?SET_SCOPER_WORKERS)],

        Res =
            try set_scopes6(Entry, NewScopeUUID, Setters, [], 0, ?SET_SCOPE_BATCH_SIZE) of
                Result -> Result
            catch
                _:Reason ->
                    {error, Reason}
            end,

        lists:foreach(fun(Setter) ->
            Setter ! exit,
            receive
                scope_setting_done -> ok
            after 2000 ->
                ?error("set_scopes error for entry: ~p", [Entry])
            end
        end, Setters),
        Res
    end).

%%--------------------------------------------------------------------
%% @doc
%% Internal helper fo set_scopes/2. Dispatch all set_scope jobs across all worker proceses.
%% @end
%%--------------------------------------------------------------------
-spec set_scopes6(Entry :: entry() | [entry()], NewScopeUUID :: uuid(), [pid()], [pid()],
    Offset :: non_neg_integer(), BatchSize :: non_neg_integer()) -> ok | no_return().
set_scopes6(Entry, NewScopeUUID, [], SettersBak, Offset, BatchSize) -> %% Empty workers list -> restore from busy workers list
    set_scopes6(Entry, NewScopeUUID, SettersBak, [], Offset, BatchSize);
set_scopes6([], _NewScopeUUID, _Setters, _SettersBak, _Offset, _BatchSize) ->
    ok; %% Nothing to do
set_scopes6([Entry | R], NewScopeUUID, [Setter | Setters], SettersBak, Offset, BatchSize) ->  %% set_scopes for all given entries
    ok = set_scopes6(Entry, NewScopeUUID, [Setter | Setters], SettersBak, Offset, BatchSize), %% set_scopes for current entry
    ok = set_scopes6(R, NewScopeUUID, Setters, [Setter | SettersBak], Offset, BatchSize);     %% set_scopes for other entries
set_scopes6(Entry, NewScopeUUID, [Setter | Setters], SettersBak, Offset, BatchSize) -> %% set_scopes for current entry
    {ok, ChildLinks} = list_children(Entry, Offset, BatchSize), %% Apply this fuction for all children
    case length(ChildLinks) < BatchSize of
        true ->
            Setter ! {Entry, NewScopeUUID}; %% Send job to first available process;
        false ->
            ok = set_scopes6(Entry, NewScopeUUID, Setters, [Setter | SettersBak], Offset + BatchSize, BatchSize)
    end,
    ok = set_scopes6([{uuid, UUID} || #child_link{uuid = UUID} <- ChildLinks], NewScopeUUID, Setters, [Setter | SettersBak], 0, BatchSize).


%%--------------------------------------------------------------------
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
    DirSep =
        case binary:matches(FileName, <<?DIRECTORY_SEPARATOR>>) of
            [] -> true;
            _ -> false
        end,
    SnapSep =
        case binary:matches(FileName, <<?SNAPSHOT_SEPARATOR>>) of
            [] -> true;
            _ -> false
        end,

    SnapSep andalso DirSep.


%%--------------------------------------------------------------------
%% @doc
%% Returns filename than explicity points at given version of snaphot.
%% @end
%%--------------------------------------------------------------------
-spec snapshot_name(FileName :: name(), Version :: non_neg_integer()) -> binary().
snapshot_name(FileName, Version) ->
    <<FileName/binary, ?SNAPSHOT_SEPARATOR, (integer_to_binary(Version))/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Returns current version of given file.
%% @end
%%--------------------------------------------------------------------
-spec get_current_snapshot(Entry :: entry()) -> entry().
get_current_snapshot(Entry) ->
    %% TODO: VFS-1965
    %% TODO: VFS-1966
    {ok, CurrentSnapshot} = get(Entry),
    CurrentSnapshot.

%%--------------------------------------------------------------------
%% @doc
%% Checks if given filename explicity points at specific version of snaphot.
%% @end
%%--------------------------------------------------------------------
-spec is_snapshot(FileName :: name()) -> boolean().
is_snapshot(FileName0) ->
    try
        FileName = case binary:split(FileName0, <<"##">>) of
            [FileName1, _] -> FileName1;
            [FileName1] -> FileName1
        end,
        case binary:split(FileName, <<?SNAPSHOT_SEPARATOR>>) of
            [FN, VR] ->
                _ = binary_to_integer(VR),
                is_valid_filename(FN);
            _ ->
                false
        end
    catch
        _:_ ->
            false
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates location reference (that is used to name link) using provider ID.
%% @end
%%--------------------------------------------------------------------
-spec location_ref(ProviderID :: oneprovider:id()) -> LocationReference :: binary().
location_ref(ProviderId) ->
    <<?LOCATION_PREFIX, ProviderId/binary>>.


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

%%--------------------------------------------------------------------
%% @doc
%% Get uuid of file.
%% @end
%%--------------------------------------------------------------------
-spec get_uuid(uuid() | entry()) -> {ok, uuid()} | datastore:get_file_error().
get_uuid({uuid, Key}) ->
    {ok, Key};
get_uuid(#document{key = Uuid, value = #file_meta{}}) ->
    {ok, Uuid};
get_uuid({path, Path}) ->
    case get({path, Path}) of
        {ok, #document{key = Uuid}} ->
            {ok, Uuid};
        Error ->
            Error
    end;
get_uuid(?ROOT_DIR_UUID) ->
    {ok, ?ROOT_DIR_UUID};
get_uuid(Uuid) ->
    {ok, Uuid}.
