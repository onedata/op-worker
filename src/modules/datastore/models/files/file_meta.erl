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

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_sufix.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").

%% How many processes shall be process single set_scope operation.
-define(SET_SCOPER_WORKERS, 25).

%% How many entries shall be processed in one batch for set_scope operation.
-define(SET_SCOPE_BATCH_SIZE, 100).

-export([save/1, create/2, save/2, get/1, exists/1, update/2, delete/1,
    delete_without_link/1]).
-export([delete_child_link/4, foreach_child/3, add_child_link/4, delete_deletion_link/3]).
-export([hidden_file_name/1, is_hidden/1, is_child_of_hidden_dir/1]).
-export([add_share/2, remove_share/2]).
-export([get_parent/1, get_parent_uuid/1]).
-export([
    get_child/2, get_child_uuid/2,
    list_children/2, list_children/3, list_children/4,
    list_children/5, list_children/6
]).
-export([get_scope_id/1, setup_onedata_user/2, get_including_deleted/1,
    make_space_exist/1, new_doc/8, type/1, get_ancestors/1,
    get_locations_by_uuid/1, rename/4]).


%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type doc() :: datastore:doc().
-type diff() :: datastore:diff(file_meta()).
-type uuid() :: datastore:key().
-type path() :: binary().
-type name() :: binary().
-type uuid_or_path() :: {path, path()} | {uuid, uuid()}.
-type entry() :: uuid_or_path() | doc().
-type type() :: ?REGULAR_FILE_TYPE | ?DIRECTORY_TYPE | ?SYMLINK_TYPE.
-type offset() :: non_neg_integer().
-type size() :: non_neg_integer().
-type mode() :: non_neg_integer().
-type time() :: non_neg_integer().
-type file_meta() :: #file_meta{}.
-type posix_permissions() :: non_neg_integer().
% Listing options (see datastore_links_iter.erl in cluster_worker for more information about link listing options)
-type list_opts() :: #{
    token => datastore_links_iter:token() | undefined,
    size => non_neg_integer(),
    offset => non_neg_integer(),
    prev_link_name => name(),
    prev_tree_id => od_provider:id()}.
% Map returned from listing functions, containing information needed for next batch listing
-type list_extended_info() :: #{
    token => datastore_links_iter:token(),
    last_name => name(),
    last_tree => od_provider:id()}.

-export_type([doc/0, uuid/0, path/0, name/0, uuid_or_path/0, entry/0, type/0,
    offset/0, size/0, mode/0, time/0, posix_permissions/0,
    file_meta/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves file meta doc.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, uuid()} | {error, term()}.
save(Doc) ->
    save(Doc, true).

%%--------------------------------------------------------------------
%% @doc
%% Saves file meta doc.
%% @end
%%--------------------------------------------------------------------
-spec save(doc(), boolean()) -> {ok, uuid()} | {error, term()}.

save(#document{value = #file_meta{is_scope = true}} = Doc, _GeneratedKey) ->
    ?extract_key(datastore_model:save(?CTX, Doc));
save(Doc, GeneratedKey) ->
    ?extract_key(datastore_model:save(?CTX#{generated_key => GeneratedKey}, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Creates new #file_meta and links it as a new child of given as first argument
%% existing #file_meta.
%% @end
%%--------------------------------------------------------------------
-spec create({uuid, ParentUuid :: uuid()}, doc()) ->
    {ok, uuid()} | {error, term()}.
create({uuid, ParentUuid}, FileDoc = #document{value = FileMeta = #file_meta{
    name = FileName,
    is_scope = IsScope
}}) ->
    ?run(begin
        true = is_valid_filename(FileName),
        {ok, ParentDoc} = file_meta:get(ParentUuid),
        FileDoc2 = #document{key = FileUuid} = fill_uuid(FileDoc),
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
                provider_id = oneprovider:get_id(),
                parent_uuid = ParentUuid
            }
        },
        TreeId = oneprovider:get_id(),
        Ctx = ?CTX#{scope => ParentDoc#document.scope},
        Link = {FileName, FileUuid},
        case datastore_model:add_links(Ctx, ParentUuid, TreeId, Link) of
            {ok, #link{}} ->
                case file_meta:save(FileDoc3) of
                    {ok, FileUuid} -> {ok, FileUuid};
                    Error -> Error
                end;
            {error, already_exists} = Eexists ->
                case datastore_model:get_links(Ctx, ParentUuid, TreeId, FileName) of
                    {ok, [#link{target = OldUuid}]} ->
                        Deleted = case datastore_model:get(
                            Ctx#{include_deleted => true}, OldUuid) of
                            {ok, #document{deleted = true}} ->
                                true;
                            {ok, #document{value = #file_meta{deleted = true}}} ->
                                true;
                            _ ->
                                false
                        end,
                        case Deleted of
                            true ->
                                datastore_model:delete_links(Ctx, ParentUuid,
                                    TreeId, FileName),
                                create({uuid, ParentUuid}, FileDoc);
                            _ ->
                                Eexists
                        end;
                    _ ->
                        Eexists
                end;
            {error, Reason} ->
                {error, Reason}
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% Returns file meta.
%% @end
%%--------------------------------------------------------------------
-spec get(uuid() | entry()) -> {ok, doc()} | {error, term()}.
get({uuid, FileUuid}) ->
    file_meta:get(FileUuid);
get(#document{value = #file_meta{}} = Doc) ->
    {ok, Doc};
get({path, Path}) ->
    ?run(fslogic_path:resolve(Path));
get(FileUuid) ->
    case get_including_deleted(FileUuid) of
        {ok, #document{value = #file_meta{deleted = true}}} ->
            {error, not_found};
        {ok, #document{deleted = true}} ->
            {error, not_found};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file_meta doc even if its marked as deleted
%% @end
%%--------------------------------------------------------------------
-spec get_including_deleted(uuid()) -> {ok, doc()} | {error, term()}.
get_including_deleted(?ROOT_DIR_UUID) ->
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
get_including_deleted(FileUuid) ->
    datastore_model:get(?CTX#{include_deleted => true}, FileUuid).

%%--------------------------------------------------------------------
%% @doc
%% Updates file meta.
%% @end
%%--------------------------------------------------------------------
-spec update(uuid() | entry(), diff()) -> {ok, uuid()} | {error, term()}.
update({uuid, FileUuid}, Diff) ->
    update(FileUuid, Diff);
update(#document{value = #file_meta{}, key = Key}, Diff) ->
    update(Key, Diff);
update({path, Path}, Diff) ->
    ?run(begin
        {ok, #document{} = Doc} = fslogic_path:resolve(Path),
        update(Doc, Diff)
    end);
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Deletes file meta.
%% @end
%%--------------------------------------------------------------------
-spec delete(uuid() | entry()) -> ok | {error, term()}.
delete({uuid, FileUuid}) ->
    delete(FileUuid);
delete(#document{
    key = FileUuid,
    scope = Scope,
    value = #file_meta{
        name = FileName,
        parent_uuid = ParentUuid
    }
}) ->
    ?run(begin
        ok = delete_child_link(ParentUuid, Scope, FileUuid, FileName),
        LocalLocationId = file_location:local_id(FileUuid),
        fslogic_location_cache:delete_location(FileUuid, LocalLocationId),
        datastore_model:delete(?CTX, FileUuid)
    end);
delete({path, Path}) ->
    ?run(begin
        {ok, #document{} = Doc} = fslogic_path:resolve(Path),
        delete(Doc)
    end);
delete(FileUuid) ->
    ?run(begin
        case file_meta:get(FileUuid) of
            {ok, #document{} = Doc} -> delete(Doc);
            {error, not_found} -> ok
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Similar to delete/1 but does not delete link in parent.
%% @end
%%--------------------------------------------------------------------
-spec delete_without_link(uuid() | doc()) -> ok | {error, term()}.
delete_without_link(#document{
    key = FileUuid
}) ->
    delete_without_link(FileUuid);
delete_without_link(FileUuid) ->
    ?run(begin
        LocalLocationId = file_location:local_id(FileUuid),
        fslogic_location_cache:delete_location(FileUuid, LocalLocationId),
        datastore_model:delete(?CTX, FileUuid)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Adds link from parent to child
%% @end
%%--------------------------------------------------------------------
-spec add_child_link(uuid(), datastore_doc:scope(), name(), uuid()) ->  ok | {error, term()}.
add_child_link(ParentUuid, Scope, Name, Uuid) ->
    Ctx = ?CTX#{scope => Scope},
    Link = {Name, Uuid},
    ?extract_ok(datastore_model:add_links(Ctx, ParentUuid, oneprovider:get_id(), Link)).

%%--------------------------------------------------------------------
%% @doc
%% Deletes link from parent to child
%% @end
%%--------------------------------------------------------------------
-spec delete_child_link(ParentUuid :: uuid(), Scope :: datastore_doc:scope(),
    FileUuid :: uuid(), FileName :: name()) -> ok.
delete_child_link(ParentUuid, Scope, FileUuid, FileName) ->
    {ok, Links} = datastore_model:get_links(?CTX, ParentUuid, all, FileName),
    [#link{tree_id = ProviderId, name = FileName, rev = Rev}] = lists:filter(fun
        (#link{target = Uuid}) -> Uuid == FileUuid
    end, Links),
    Ctx = ?CTX#{scope => Scope},
    Link = {FileName, Rev},
    case oneprovider:is_self(ProviderId) of
        true ->
            ok = datastore_model:delete_links(Ctx, ParentUuid, ProviderId, Link);
        false ->
            ok = datastore_model:mark_links_deleted(Ctx, ParentUuid, ProviderId, Link)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes deletion link for local provider.
%% @end
%%--------------------------------------------------------------------
-spec delete_deletion_link(ParentUuid :: uuid(), Scope :: datastore_doc:scope(),
    FileName :: name()) -> ok | {error, term()}.
delete_deletion_link(ParentUuid, Scope, Link) ->
    Ctx = ?CTX#{scope => Scope},
    datastore_model:delete_links(Ctx, ParentUuid, oneprovider:get_id(), Link).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether file meta exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(uuid() | entry()) -> boolean() | {error, term()}.
exists({uuid, FileUuid}) ->
    exists(FileUuid);
exists(#document{value = #file_meta{}, key = Key}) ->
    exists(Key);
exists({path, Path}) ->
    case fslogic_path:resolve(Path) of
        {ok, #document{}} -> true;
        {error, not_found} -> false
    end;
exists(Key) ->
    case file_meta:get(Key) of
        {ok, _} -> true;
        {error, not_found} -> false;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns parent child by name.
%% @end
%%--------------------------------------------------------------------
-spec get_child(uuid(), name()) -> {ok, doc()} | {error, term()}.
get_child(ParentUuid, Name) ->
    case get_child_uuid(ParentUuid, Name) of
        {ok, ChildUuid} ->
            file_meta:get({uuid, ChildUuid});
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns parent child's UUID by Name.
%% @end
%%--------------------------------------------------------------------
-spec get_child_uuid(uuid(), name()) -> {ok, uuid()} | {error, term()}.
get_child_uuid(ParentUuid, Name) ->
    Tokens = binary:split(Name, ?CONFLICTING_LOGICAL_FILE_SUFFIX_SEPARATOR, [global]),
    case lists:reverse(Tokens) of
        [Name] ->
            case get_child_uuid(ParentUuid, oneprovider:get_id(), Name) of
                {ok, Doc} -> {ok, Doc};
                {error, not_found} -> get_child_uuid(ParentUuid, all, Name);
                {error, Reason} -> {error, Reason}
            end;
        [TreeIdPrefix | Tokens2] ->
            Name2 = list_to_binary(lists:reverse(Tokens2)),
            PrefixSize = erlang:size(TreeIdPrefix),
            {ok, TreeIds} = datastore_model:get_links_trees(?CTX, ParentUuid),
            TreeIds2 = lists:filter(fun(TreeId) ->
                case TreeId of
                    <<TreeIdPrefix:PrefixSize/binary, _/binary>> -> true;
                    _ -> false
                end
            end, TreeIds),
            case TreeIds2 of
                [TreeId] ->
                    case get_child_uuid(ParentUuid, TreeId, Name2) of
                        {ok, Doc} ->
                            {ok, Doc};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                [] ->
                    get_child_uuid(ParentUuid, all, Name)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_children_internal(Entry, #{size => Size, token => #link_token{}}).
%% @end
%%--------------------------------------------------------------------
-spec list_children(entry(), non_neg_integer()) ->
    {ok, [#child_link_uuid{}], list_extended_info()} | {error, term()}.
list_children(Entry, Size) ->
    list_children_internal(Entry, #{size => Size, token => #link_token{}}).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_children(Entry, Offset, Size, undefined)
%% @end
%%--------------------------------------------------------------------
-spec list_children(entry(), non_neg_integer(), non_neg_integer()) ->
    {ok, [#child_link_uuid{}], list_extended_info()} | {error, term()}.
list_children(Entry, Offset, Size) ->
    list_children_internal(Entry, #{offset => Offset, size => Size}).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_children(Entry, Offset, Size, Token, undefined).
%% @end
%%--------------------------------------------------------------------
-spec list_children(entry(), non_neg_integer(), non_neg_integer(),
    datastore_links_iter:token() | undefined) ->
    {ok, [#child_link_uuid{}], list_extended_info()} | {error, term()}.
list_children(Entry, Offset, Size, Token) ->
    list_children(Entry, Offset, Size, Token, undefined).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_children(Entry, Offset, Size, Token, PrevLinkKey, undefined).
%% @end
%%--------------------------------------------------------------------
-spec list_children(
    Entry :: entry(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer(),
    Token :: undefined | datastore_links_iter:token(),
    PrevLinkKey :: undefined | name()
) ->
    {ok, [#child_link_uuid{}], list_extended_info()} | {error, term()}.
list_children(Entry, Offset, Size, Token, PrevLinkKey) ->
    list_children(Entry, Offset, Size, Token, PrevLinkKey, undefined).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_children_internal(Entry, #{offset => Offset, size => Size, token => Token}).
%% @end
%%--------------------------------------------------------------------
-spec list_children(
    Entry :: entry(),
    Offset :: non_neg_integer(),
    Size :: non_neg_integer(),
    Token :: undefined | datastore_links_iter:token(),
    PrevLinkKey :: undefined | name(),
    PrevTeeID :: undefined | oneprovider:id()
) ->
    {ok, [#child_link_uuid{}], list_extended_info()} | {error, term()}.
list_children(Entry, Offset, Size, Token, PrevLinkKey, PrevTeeID) ->
    Opts = case Offset of
        0 -> #{size => Size};
        _ -> #{offset => Offset, size => Size}
    end,

    Opts2 = case Token of
        undefined -> Opts;
        _ -> Opts#{token => Token}
    end,

    Opts3 = case PrevLinkKey of
        undefined -> Opts2;
        _ -> Opts2#{prev_link_name => PrevLinkKey}
    end,

    Opts4 = case PrevTeeID of
        undefined -> Opts3;
        _ -> Opts3#{prev_tree_id => PrevTeeID}
    end,

    list_children_internal(Entry, Opts4).

%%--------------------------------------------------------------------
%% @doc
%% Iterate over all children links and apply Fun.
%% @end
%%--------------------------------------------------------------------
-spec foreach_child(entry(), fun((datastore:link_name(), datastore:link_target(),
datastore:fold_acc()) -> datastore:fold_acc()), datastore:fold_acc()) ->
    datastore:fold_acc().
foreach_child(Entry, Fun, AccIn) ->
    ?run(begin
        {ok, #document{key = FileUuid}} = file_meta:get(Entry),
        datastore_model:fold_links(?CTX, FileUuid, all, fun
            (#link{name = Name, target = Target}, Acc) ->
                {ok, Fun(Name, Target, Acc)}
        end, AccIn, #{})
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's locations for given file
%% @end
%%--------------------------------------------------------------------
-spec get_locations_by_uuid(uuid()) -> {ok, [file_location:id()]} | {error, term()}.
get_locations_by_uuid(FileUuid) ->
    ?run(begin
        case file_meta:get(FileUuid) of
            {ok, #document{value = #file_meta{type = ?DIRECTORY_TYPE}}} ->
                {ok, []};
            {ok, #document{scope = SpaceId}} ->
                {ok, Providers} = space_logic:get_provider_ids(?ROOT_SESS_ID, SpaceId),
                Locations = lists:map(fun(ProviderId) ->
                    file_location:id(FileUuid, ProviderId)
                end, Providers),
                {ok, Locations};
            {error, not_found} ->
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
    #document{key = FileUuid, value = #file_meta{name = FileName}} = SourceDoc,
    #document{key = ParentUuid, scope = Scope} = SourceParentDoc,
    {ok, _} = file_meta:update(FileUuid, fun(FileMeta = #file_meta{}) ->
        {ok, FileMeta#file_meta{
            name = TargetName,
            parent_uuid = TargetParentDoc#document.key
        }}
    end),
    Ctx = ?CTX#{scope => TargetParentDoc#document.scope},
    TreeId = oneprovider:get_id(),
    {ok, _} = datastore_model:add_links(Ctx, TargetParentDoc#document.key,
        TreeId, {TargetName, FileUuid}
    ),
    ok = delete_child_link(ParentUuid, Scope, FileUuid, FileName).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's parent document.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(entry()) -> {ok, doc()} | {error, term()}.
get_parent(Entry) ->
    case get_parent_uuid(Entry) of
        {ok, ParentUuid} -> file_meta:get({uuid, ParentUuid});
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file's parent uuid.
%% @end
%%--------------------------------------------------------------------
-spec get_parent_uuid(entry()) -> {ok, datastore:key()} | {error, term()}.
get_parent_uuid(Entry) ->
    ?run(begin
        {ok, #document{value = #file_meta{parent_uuid = ParentUuid}}} =
            file_meta:get(Entry),
        {ok, ParentUuid}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns all file's ancestors' uuids.
%% @end
%%--------------------------------------------------------------------
-spec get_ancestors(uuid()) -> {ok, [uuid()]} | {error, term()}.
get_ancestors(FileUuid) ->
    ?run(begin
        {ok, #document{key = Key}} = file_meta:get(FileUuid),
        {ok, get_ancestors2(Key, [])}
    end).
get_ancestors2(?ROOT_DIR_UUID, Acc) ->
    Acc;
get_ancestors2(FileUuid, Acc) ->
    {ok, ParentUuid} = get_parent_uuid({uuid, FileUuid}),
    get_ancestors2(ParentUuid, [ParentUuid | Acc]).

%%--------------------------------------------------------------------
%% @doc
%% Gets "scope" id of given document. "Scope" document is the nearest ancestor
%% with #file_meta.is_scope == true.
%% @end
%%--------------------------------------------------------------------
-spec get_scope_id(entry()) -> {ok, ScopeId :: datastore:key()} | {error, term()}.
get_scope_id(#document{key = FileUuid, value = #file_meta{is_scope = true}}) ->
    {ok, FileUuid};
get_scope_id(#document{value = #file_meta{is_scope = false, scope = Scope}}) ->
    {ok, Scope};
get_scope_id(Entry) ->
    ?run(begin
        {ok, Doc} = file_meta:get(Entry),
        get_scope_id(Doc)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Initializes files metadata for onedata user.
%% This function can and should be used to ensure that user's FS is fully synchronised. Normally
%% this function is called asynchronously automatically after user's document is updated.
%% @end
%%--------------------------------------------------------------------
-spec setup_onedata_user(UserId :: od_user:id(), EffSpaces :: [od_space:id()]) -> ok.
setup_onedata_user(UserId, EffSpaces) ->
    ?debug("Setting up user: ~p", [UserId]),
    critical_section:run([od_user, UserId], fun() ->
        try
            CTime = time_utils:cluster_time_seconds(),

            lists:foreach(fun(SpaceId) ->
                make_space_exist(SpaceId)
            end, EffSpaces),

            FileUuid = fslogic_uuid:user_root_dir_uuid(UserId),
            ScopeId = <<>>, % TODO - do we need scope for user dir
            case create({uuid, ?ROOT_DIR_UUID},
                #document{key = FileUuid,
                    value = #file_meta{
                        name = UserId, type = ?DIRECTORY_TYPE, mode = 8#1755,
                        owner = ?ROOT_USER_ID, is_scope = true,
                        parent_uuid = ?ROOT_DIR_UUID
                    }
                }) of
                {ok, _RootUuid} ->
                    {ok, _} = times:save(#document{key = FileUuid, value =
                    #times{mtime = CTime, atime = CTime, ctime = CTime},
                        scope = ScopeId}),
                    ok;
                {error, already_exists} ->
                    ok
            end
        catch Type:Message ->
            ?error_stacktrace("Failed to setup user ~s - ~p:~p", [
                UserId, Type, Message
            ])
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Add shareId to file meta. Only one share per file is allowed.
%% @end
%%--------------------------------------------------------------------
-spec add_share(file_ctx:ctx(), od_share:id()) -> {ok, uuid()}  | {error, term()}.
add_share(FileCtx, ShareId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    update({uuid, FileUuid}, fun
        (FileMeta = #file_meta{shares = []}) ->
            {ok, FileMeta#file_meta{shares = [ShareId]}};
        (#file_meta{shares = _}) ->
            {error, already_exists}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Remove shareId from file meta. Only one share per file is allowed.
%% @end
%%--------------------------------------------------------------------
-spec remove_share(file_ctx:ctx(), od_share:id()) -> {ok, uuid()} | {error, term()}.
remove_share(FileCtx, ShareId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    update({uuid, FileUuid}, fun(FileMeta = #file_meta{shares = Shares}) ->
        case Shares of
            [ShareId] -> {ok, FileMeta#file_meta{shares = []}};
            _ -> {error, not_found}
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Creates file meta entry for space if not exists
%% @end
%%--------------------------------------------------------------------
-spec make_space_exist(SpaceId :: datastore:key()) -> ok | no_return().
make_space_exist(SpaceId) ->
    CTime = time_utils:cluster_time_seconds(),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    FileDoc = #document{
        key = SpaceDirUuid,
        value = #file_meta{
            name = SpaceId, type = ?DIRECTORY_TYPE,
            mode = ?DEFAULT_SPACE_DIR_MODE, owner = ?ROOT_USER_ID, is_scope = true,
            parent_uuid = ?ROOT_DIR_UUID
        }
    },
    case file_meta:create({uuid, ?ROOT_DIR_UUID}, FileDoc) of
        {ok, _} ->
            TimesDoc = #document{
                key = SpaceDirUuid,
                value = #times{mtime = CTime, atime = CTime, ctime = CTime},
                scope = SpaceId
            },
            case times:save(TimesDoc) of
                {ok, _} -> ok;
                {error, already_exists} -> ok
            end;
        {error, already_exists} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Return file_meta doc.
%% @end
%%--------------------------------------------------------------------
-spec new_doc(undefined | uuid(), undefined | name(), undefined | type(),
    posix_permissions(), undefined | od_user:id(), undefined | od_group:id(),
    uuid(), od_space:id()) -> doc().
new_doc(FileUuid, FileName, FileType, Mode, Owner, GroupOwner, ParentUuid,
    SpaceId
) ->
    #document{
        key = FileUuid,
        value = #file_meta{
            name = FileName,
            type = FileType,
            mode = Mode,
            owner = Owner,
            group_owner = GroupOwner,
            parent_uuid = ParentUuid,
            provider_id = oneprovider:get_id(),
            scope = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId)
        },
        scope = SpaceId
    }.

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

%%--------------------------------------------------------------------
%% @doc
%% Checks if given link is a deletion link.
%% @end
%%--------------------------------------------------------------------
-spec is_deletion_link(binary()) -> boolean().
is_deletion_link(LinkName) ->
    case binary:match(LinkName, ?FILE_DELETION_LINK_SUFFIX) of
        nomatch -> false;
        _ -> true
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if given filename is child of hidden directory.
%% @end
%%--------------------------------------------------------------------
-spec is_child_of_hidden_dir(FileName :: name()) -> boolean().
is_child_of_hidden_dir(Path) ->
    {_, ParentPath} = fslogic_path:basename_and_parent(Path),
    {Parent, _} = fslogic_path:basename_and_parent(ParentPath),
    is_hidden(Parent).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Lists children of given #file_meta using different listing options (see datastore_links_iter.erl in cluster_worker)
%% @end
%%--------------------------------------------------------------------
-spec list_children_internal(entry(), list_opts()) ->
    {ok, [#child_link_uuid{}], list_extended_info()} | {error, term()}.
list_children_internal(Entry, Opts) ->
    ?run(begin
        {ok, FileUuid} = get_uuid(Entry),
        Result = datastore_model:fold_links(?CTX, FileUuid, all, fun
            (Link = #link{name = Name}, Acc) ->
                case {is_hidden(Name), is_deletion_link(Name)} of
                    {false, false} -> {ok, [Link | Acc]};
                    _ -> {ok, Acc}
                end
        end, [], Opts),

        case Result of
            {{ok, Links}, Token2} ->
                prepare_list_ans(Links, #{token => Token2});
            {ok, Links} ->
                prepare_list_ans(Links, #{});
            {error, Reason} ->
                {error, Reason}
        end
    end).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepares list answer tagging children and setting information needed to list next batch.
%% @end
%%--------------------------------------------------------------------
-spec prepare_list_ans([datastore_links:link()], list_extended_info()) ->
    {ok, [#child_link_uuid{}], list_extended_info()}.
prepare_list_ans(Links, ExtendedInfo) ->
    ExtendedInfo2 = case Links of
        [#link{name = Name, tree_id = Tree} | _] ->
            ExtendedInfo#{last_name => Name, last_tree => Tree};
        _ ->
            ExtendedInfo#{last_name => undefined, last_tree => undefined}
    end,
    {ok, tag_children(lists:reverse(Links)), ExtendedInfo2}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds links tree ID suffix to file children with ambiguous names.
%% @end
%%--------------------------------------------------------------------
-spec tag_children([datastore_links:link()]) -> [#child_link_uuid{}].
tag_children([]) ->
    [];
tag_children(Links) ->
    {Group2, Groups2} = lists:foldl(fun
        (Link = #link{}, {[], Groups}) ->
            {[Link], Groups};
        (Link = #link{name = N}, {Group = [#link{name = N} | _], Groups}) ->
            {[Link | Group], Groups};
        (Link = #link{}, {Group, Groups}) ->
            {[Link], [Group | Groups]}
    end, {[], []}, Links),
    lists:foldl(fun
        ([#link{name = Name, target = FileUuid}], Children) ->
            [#child_link_uuid{
                uuid = FileUuid,
                name = Name
            } | Children];
        (Group, Children) ->
            LocalTreeId = oneprovider:get_id(),
            {LocalLinks, RemoteLinks} = lists:partition(fun
                (#link{tree_id = TreeId}) -> TreeId == LocalTreeId
            end, Group),
            RemoteTreeIds = [Link#link.tree_id || Link <- RemoteLinks],
            RemoteTreeIdsLen = [size(TreeId) || TreeId <- RemoteTreeIds],
            Len = binary:longest_common_prefix(RemoteTreeIds),
            Len2 = min(max(4, Len + 1), lists:min(RemoteTreeIdsLen)),
            lists:foldl(fun
                (#link{
                    tree_id = TreeId, name = Name, target = FileUuid
                }, Children2) when TreeId == LocalTreeId ->
                    [#child_link_uuid{
                        uuid = FileUuid,
                        name = Name
                    } | Children2];
                (#link{
                    tree_id = TreeId, name = Name, target = FileUuid
                }, Children2) ->
                    [#child_link_uuid{
                        uuid = FileUuid,
                        name = <<Name/binary, "@", TreeId:Len2/binary>>
                    } | Children2]
            end, Children, LocalLinks ++ RemoteLinks)
    end, [], [Group2 | Groups2]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns uuid form entry.
%% @end
%%--------------------------------------------------------------------
-spec get_uuid(uuid() | entry()) -> {ok, uuid()} | {error, term()}.
get_uuid({uuid, FileUuid}) ->
    {ok, FileUuid};
get_uuid(#document{key = FileUuid, value = #file_meta{}}) ->
    {ok, FileUuid};
get_uuid({path, Path}) ->
    case fslogic_path:resolve(Path) of
        {ok, Doc} -> get_uuid(Doc);
        Error -> Error
    end;
get_uuid(FileUuid) ->
    {ok, FileUuid}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates uuid if needed.
%% @end
%%--------------------------------------------------------------------
-spec fill_uuid(doc()) -> doc().
fill_uuid(Doc = #document{key = undefined}) ->
    NewUuid = datastore_utils:gen_key(),
    Doc#document{key = NewUuid};
fill_uuid(Doc) ->
    Doc.

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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns parent child's UUID by name within given links tree set.
%% @end
%%--------------------------------------------------------------------
-spec get_child_uuid(uuid(), datastore_links:tree_ids(), name()) ->
    {ok, uuid()} | {error, term()}.
get_child_uuid(ParentUuid, TreeIds, Name) ->
    case datastore_model:get_links(?CTX, ParentUuid, TreeIds, Name) of
        {ok, [#link{target = FileUuid}]} ->
            {ok, FileUuid};
        {ok, [#link{} | _]} ->
            {error, ?EINVAL};
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% datastore_model callbacks
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
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    7.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
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
get_record_struct(2) ->
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
get_record_struct(3) ->
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
get_record_struct(4) ->
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
get_record_struct(5) ->
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
    ]};
get_record_struct(6) ->
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
        {parent_uuid, string}
    ]};
get_record_struct(7) ->
    {record, [
        {name, string},
        {type, atom},
        {mode, integer},
        {owner, string},
        {group_owner, string},
        % size field  has been removed in this version
        {is_scope, boolean},
        {scope, string},
        {provider_id, string},
        {shares, [string]},
        {deleted, boolean},
        {parent_uuid, string}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Name, Type, Mode, Uid, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares}
) ->
    {2, {?MODULE, Name, Type, Mode, Uid, Size, Version, IsScope, Scope,
        ProviderId, LinkValue, Shares}};
upgrade_record(2, {?MODULE, Name, Type, Mode, Owner, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares}
) ->
    {3, {?MODULE, Name, Type, Mode, Owner, Size, Version, IsScope,
        Scope, ProviderId, LinkValue, Shares, false, {storage_sync_info, #{}, undefined}}};
upgrade_record(3, {?MODULE, Name, Type, Mode, Owner, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares, Deleted, StorageSyncInfo}
) ->
    {4, {?MODULE, Name, Type, Mode, Owner, Size, Version, IsScope,
        Scope, ProviderId, LinkValue, Shares, Deleted, StorageSyncInfo, undefined}
    };
upgrade_record(4, {?MODULE, Name, Type, Mode, Owner, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares, Deleted, StorageSyncInfo, ParentUuid}
) ->
    {5, {?MODULE, Name, Type, Mode, Owner, undefined, Size, Version, IsScope,
        Scope, ProviderId, LinkValue, Shares, Deleted, StorageSyncInfo, ParentUuid}
    };
upgrade_record(5, {?MODULE, Name, Type, Mode, Owner, GroupOwner, Size, Version, IsScope,
    Scope, ProviderId, LinkValue, Shares, Deleted, _StorageSyncInfo, ParentUuid}
) ->
    {6, {?MODULE, Name, Type, Mode, Owner, GroupOwner, Size, Version, IsScope,
        Scope, ProviderId, LinkValue, Shares, Deleted, ParentUuid}
    };
upgrade_record(6, {?MODULE, Name, Type, Mode, Owner, GroupOwner, _Size, _Version, IsScope,
    Scope, ProviderId, _LinkValue, Shares, Deleted, ParentUuid}
) ->
    {7, {?MODULE, Name, Type, Mode, Owner, GroupOwner, IsScope,
        Scope, ProviderId, Shares, Deleted, ParentUuid}
    }.