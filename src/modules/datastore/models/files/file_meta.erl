%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for file's metadata. Implements low-level metadata
%%% operations such as walking through file graph.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/onedata.hrl").

%% How many processes shall be process single set_scope operation.
-define(SET_SCOPER_WORKERS, 25).

%% How many entries shall be processed in one batch for set_scope operation.
-define(SET_SCOPE_BATCH_SIZE, 100).

-export([save/1, create/2, save/2, get/1, exists/1, update/2, delete/1,
    delete_without_link/1]).
-export([delete_child_link/4, foreach_child/3, add_child_link/4, delete_deletion_link/3]).
-export([hidden_file_name/1, is_hidden/1, is_child_of_hidden_dir/1]).
-export([add_share/2, remove_share/2, get_shares/1]).
-export([get_parent/1, get_parent_uuid/1, get_provider_id/1]).
-export([
    get_child/2, get_child_uuid_and_tree_id/2,
    list_children/2, list_children/3, list_children/4,
    list_children/5, list_children/6,
    list_children_whitelisted/4
]).
-export([get_name/1, set_name/2]).
-export([get_active_perms_type/1, update_mode/2, update_acl/2]).
-export([get_scope_id/1, setup_onedata_user/2, get_including_deleted/1,
    make_space_exist/1, new_doc/6, new_doc/7, type/1, get_ancestors/1,
    get_locations_by_uuid/1, rename/4, get_owner/1, get_type/1,
    get_mode/1]).
-export([check_name/3, has_suffix/1, is_deleted/1]).
% For tests
-export([get_all_links/2]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2, resolve_conflict/3]).

-type doc() :: datastore:doc().
-type diff() :: datastore_doc:diff(file_meta()).
-type uuid() :: datastore:key().
-type path() :: binary().
-type uuid_based_path() :: binary(). % similar to canonical, but path elements are uuids instead of filenames/dirnames
-type name() :: binary().
-type uuid_or_path() :: {path, path()} | {uuid, uuid()}.
-type entry() :: uuid_or_path() | doc().
-type type() :: ?REGULAR_FILE_TYPE | ?DIRECTORY_TYPE | ?SYMLINK_TYPE.
-type size() :: non_neg_integer().
-type mode() :: non_neg_integer().
-type time() :: non_neg_integer().
-type file_meta() :: #file_meta{}.
-type posix_permissions() :: non_neg_integer().
-type permissions_type() :: posix | acl.

%% @formatter:off
% Listing options (see datastore_links_iter.erl in cluster_worker for more information about link listing options)
-type offset() :: integer().
-type non_neg_offset() :: non_neg_integer().
-type limit() :: non_neg_integer().
-type list_opts() :: #{
    token => datastore_links_iter:token() | undefined,
    size => limit(),
    offset => offset(),
    prev_link_name => name(),
    prev_tree_id => od_provider:id()
}.

% Map returned from listing functions, containing information needed for next batch listing
-type list_extended_info() :: #{
    token => datastore_links_iter:token(),
    last_name => name(),
    last_tree => od_provider:id()
}.
%% @formatter:on

-export_type([
    doc/0, uuid/0, path/0, uuid_based_path/0, name/0, uuid_or_path/0, entry/0,
    type/0, size/0, mode/0, time/0, posix_permissions/0, permissions_type/0,
    offset/0, non_neg_offset/0, limit/0, file_meta/0
]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

% For each "normal" file (including spaces) scope is id of a space to
% which the file belongs.
% For root directory and users' root directories we use "special" scope
% as they don't belong to any space
-define(ROOT_DIR_SCOPE, <<>>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves file meta doc.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, doc()} | {error, term()}.
save(Doc) ->
    save(Doc, true).

%%--------------------------------------------------------------------
%% @doc
%% Saves file meta doc.
%% @end
%%--------------------------------------------------------------------
-spec save(doc(), boolean()) -> {ok, doc()} | {error, term()}.
save(#document{value = #file_meta{is_scope = true}} = Doc, _GeneratedKey) ->
    datastore_model:save(?CTX#{memory_copies => all}, Doc);
save(Doc, GeneratedKey) ->
    datastore_model:save(?CTX#{generated_key => GeneratedKey}, Doc).

%%--------------------------------------------------------------------
%% @doc
%% @equiv create(Parent, FileDoc, all)
%% @end
%%--------------------------------------------------------------------
-spec create({uuid, ParentUuid :: uuid()}, doc()) ->
    {ok, uuid()} | {error, term()}.
create(Parent, FileDoc) ->
    create(Parent, FileDoc, all).

%%--------------------------------------------------------------------
%% @doc
%% Creates new #file_meta and links it as a new child of given as first argument
%% existing #file_meta.
%% @end
%%--------------------------------------------------------------------
-spec create({uuid, ParentUuid :: uuid()}, doc(), datastore:tree_ids()) ->
    {ok, doc()} | {error, term()}.
create({uuid, ParentUuid}, FileDoc = #document{value = FileMeta = #file_meta{name = FileName}}, CheckTrees) ->
    ?run(begin
        true = is_valid_filename(FileName),
        FileDoc2 = #document{key = FileUuid} = fill_uuid(FileDoc, ParentUuid),
        {ok, ParentDoc} = file_meta:get({uuid, ParentUuid}),
        {ok, ParentScopeId} = get_scope_id(ParentDoc),
        {ok, ScopeId} = get_scope_id(FileDoc2),
        ScopeId2 = utils:ensure_defined(ScopeId, ParentScopeId),
        FileDoc3 = FileDoc2#document{
            scope = ScopeId2,
            value = FileMeta#file_meta{
                provider_id = oneprovider:get_id(),
                parent_uuid = ParentUuid
            }
        },
        LocalTreeId = oneprovider:get_id(),
        Ctx = ?CTX#{scope => ParentScopeId},
        Link = {FileName, FileUuid},
        case create_internal(FileDoc3) of
            {ok, FileDocFinal = #document{key = FileUuid}} ->
                case datastore_model:check_and_add_links(Ctx, ParentUuid, LocalTreeId, CheckTrees, Link) of
                    {ok, #link{}} ->
                        {ok, FileDocFinal};
                    {error, already_exists} = Eexists ->
                        case datastore_model:get_links(Ctx, ParentUuid, CheckTrees, FileName) of
                            {ok, Links} ->
                                FileExists = lists:any(fun(#link{target = Uuid, tree_id = TreeId}) ->
                                    Deleted = case get_including_deleted(Uuid) of
                                        {ok, #document{deleted = true}} ->
                                            true;
                                        {ok, #document{value = #file_meta{deleted = true}}} ->
                                            true;
                                        _ ->
                                            false
                                    end,
                                    case {Deleted, TreeId} of
                                        {true, LocalTreeId} ->
                                            datastore_model:delete_links(
                                                Ctx, ParentUuid,
                                                LocalTreeId, FileName
                                            ),
                                            false;
                                        _ ->
                                            not Deleted
                                    end
                                end, Links),

                                case FileExists of
                                    false ->
                                        create({uuid, ParentUuid}, FileDoc, [LocalTreeId]);
                                    _ ->
                                        delete_doc_if_not_special(FileUuid),
                                        Eexists
                                end;
                            {error, not_found} ->
                                create({uuid, ParentUuid}, FileDoc, CheckTrees);
                            _ ->
                                delete_doc_if_not_special(FileUuid),
                                Eexists
                        end;
                    {error, Reason} ->
                        {error, Reason}
                end;
            Error ->
                Error
        end
    end).

%% @private
-spec create_internal(doc()) -> {ok, doc()} | {error, term()}.
create_internal(#document{key = FileUuid, value = #file_meta{is_scope = IsScope}} = Doc) ->
    Ctx = case IsScope of
        true -> ?CTX#{memory_copies => all};
        false -> ?CTX#{generated_key => true}
    end,

    case datastore_model:create(Ctx, Doc) of
        ?ERROR_ALREADY_EXISTS ->
            % This should happen only for space (or other special files with
            % deterministic uuids). Others should have unique ones.
            file_meta:get(FileUuid);
        Result ->
            Result
    end.

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
    ?run(canonical_path:resolve(Path));
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
get_including_deleted(?GLOBAL_ROOT_DIR_UUID) ->
    {ok, #document{
        key = ?GLOBAL_ROOT_DIR_UUID,
        value = #file_meta{
            name = ?GLOBAL_ROOT_DIR_NAME,
            is_scope = true,
            mode = ?DEFAULT_DIR_PERMS,
            owner = ?ROOT_USER_ID,
            parent_uuid = ?GLOBAL_ROOT_DIR_UUID
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
        {ok, #document{} = Doc} = canonical_path:resolve(Path),
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
        delete_without_link(FileUuid)
    end);
delete({path, Path}) ->
    ?run(begin
        {ok, #document{} = Doc} = canonical_path:resolve(Path),
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
    ?run(begin datastore_model:delete(?CTX, FileUuid) end).


-spec delete_doc_if_not_special(uuid()) -> ok.
delete_doc_if_not_special(FileUuid) ->
    case fslogic_uuid:is_special_uuid(FileUuid) of
        true -> ok;
        false -> delete_without_link(FileUuid)
    end.


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
    case lists:filter(fun(#link{target = Uuid}) -> Uuid == FileUuid end, Links) of
        [#link{tree_id = ProviderId, name = FileName, rev = Rev}] ->
            Ctx = ?CTX#{scope => Scope},
            Link = {FileName, Rev},
            case oneprovider:is_self(ProviderId) of
                true ->
                    ok = datastore_model:delete_links(Ctx, ParentUuid, ProviderId, Link);
                false ->
                    ok = datastore_model:mark_links_deleted(Ctx, ParentUuid, ProviderId, Link)
            end;
        [] ->
            ok
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
    case canonical_path:resolve(Path) of
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
    case get_child_uuid_and_tree_id(ParentUuid, Name) of
        {ok, ChildUuid, _} ->
            file_meta:get({uuid, ChildUuid});
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns parent child's UUID by Name and TreeId of a tree in which
%% the link was found.
%% @end
%%--------------------------------------------------------------------
-spec get_child_uuid_and_tree_id(uuid(), name()) -> {ok, uuid(), datastore_links:tree_id()} | {error, term()}.
get_child_uuid_and_tree_id(ParentUuid, Name) ->
    Tokens = binary:split(Name, ?CONFLICTING_LOGICAL_FILE_SUFFIX_SEPARATOR, [global]),
    case lists:reverse(Tokens) of
        [Name] ->
            case get_child_uuid_and_tree_id(ParentUuid, oneprovider:get_id(), Name) of
                {ok, Uuid, TreeId} -> {ok, Uuid, TreeId};
                {error, not_found} -> get_child_uuid_and_tree_id(ParentUuid, all, Name);
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
                    case get_child_uuid_and_tree_id(ParentUuid, TreeId, Name2) of
                        {ok, Doc, TreeId} ->
                            {ok, Doc, TreeId};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                [] ->
                    get_child_uuid_and_tree_id(ParentUuid, all, Name)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_children_internal(Entry, #{size => Size, token => #link_token{}}).
%% @end
%%--------------------------------------------------------------------
-spec list_children(entry(), limit()) ->
    {ok, [#child_link_uuid{}], list_extended_info()} | {error, term()}.
list_children(Entry, Limit) ->
    list_children_internal(Entry, #{size => Limit, token => #link_token{}}).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_children(Entry, Offset, Size, undefined)
%% @end
%%--------------------------------------------------------------------
-spec list_children(entry(), offset(), limit()) ->
    {ok, [#child_link_uuid{}], list_extended_info()} | {error, term()}.
list_children(Entry, Offset, Limit) ->
    list_children_internal(Entry, #{offset => Offset, size => Limit}).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_children(Entry, Offset, Size, Token, undefined).
%% @end
%%--------------------------------------------------------------------
-spec list_children(entry(), offset(), limit(),
    datastore_links_iter:token() | undefined) ->
    {ok, [#child_link_uuid{}], list_extended_info()} | {error, term()}.
list_children(Entry, Offset, Limit, Token) ->
    list_children(Entry, Offset, Limit, Token, undefined).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_children(Entry, Offset, Size, Token, PrevLinkKey, undefined).
%% @end
%%--------------------------------------------------------------------
-spec list_children(
    Entry :: entry(),
    Offset :: offset(),
    Limit :: limit(),
    Token :: undefined | datastore_links_iter:token(),
    PrevLinkKey :: undefined | name()
) ->
    {ok, [#child_link_uuid{}], list_extended_info()} | {error, term()}.
list_children(Entry, Offset, Limit, Token, PrevLinkKey) ->
    list_children(Entry, Offset, Limit, Token, PrevLinkKey, undefined).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_children_internal(Entry, #{offset => Offset, size => Size, token => Token}).
%% @end
%%--------------------------------------------------------------------
-spec list_children(
    Entry :: entry(),
    Offset :: offset(),
    Limit :: limit(),
    Token :: undefined | datastore_links_iter:token(),
    PrevLinkKey :: undefined | name(),
    PrevTreeID :: undefined | oneprovider:id()
) ->
    {ok, [#child_link_uuid{}], list_extended_info()} | {error, term()}.
list_children(Entry, Offset, Limit, Token, PrevLinkKey, PrevTreeID) ->
    Opts = case Offset of
        0 -> #{size => Limit};
        _ -> #{offset => Offset, size => Limit}
    end,

    Opts2 = case Token of
        undefined -> Opts;
        _ -> Opts#{token => Token}
    end,

    Opts3 = case PrevLinkKey of
        undefined -> Opts2;
        _ -> Opts2#{prev_link_name => PrevLinkKey}
    end,

    Opts4 = case PrevTreeID of
        undefined -> Opts3;
        _ -> Opts3#{prev_tree_id => PrevTreeID}
    end,

    list_children_internal(Entry, Opts4).

%%--------------------------------------------------------------------
%% @doc
%% Lists children of given #file_meta bounded by specified AllowedChildren
%% and given options (PositiveOffset and Limit).
%% @end
%%--------------------------------------------------------------------
-spec list_children_whitelisted(
    Entry :: entry(),
    NonNegOffset :: non_neg_offset(),
    Limit :: limit(),
    ChildrenWhiteList :: [file_meta:name()]
) ->
    {ok, [#child_link_uuid{}]} | {error, term()}.
list_children_whitelisted(Entry, NonNegOffset, Limit, ChildrenWhiteList) when NonNegOffset >= 0 ->
    Names = lists:filter(fun(ChildName) ->
        not (is_hidden(ChildName) orelse is_deletion_link(ChildName))
    end, ChildrenWhiteList),

    ?run(begin
        {ok, FileUuid} = get_uuid(Entry),

        ValidLinks = lists:flatmap(fun
            ({ok, L}) ->
                L;
            ({error, not_found}) ->
                [];
            ({error, _} = Error) ->
                error(Error)
        end, datastore_model:get_links(?CTX, FileUuid, all, Names)),

        case NonNegOffset < length(ValidLinks) of
            true ->
                RequestedLinks = lists:sublist(ValidLinks, NonNegOffset+1, Limit),
                {ok, tag_children(RequestedLinks)};
            false ->
                {ok, []}
        end
    end).

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
get_ancestors2(?GLOBAL_ROOT_DIR_UUID, Acc) ->
    Acc;
get_ancestors2(FileUuid, Acc) ->
    {ok, ParentUuid} = get_parent_uuid({uuid, FileUuid}),
    get_ancestors2(ParentUuid, [ParentUuid | Acc]).

%%--------------------------------------------------------------------
%% @doc
%% Gets "scope" id of given document.
%% @end
%%--------------------------------------------------------------------
-spec get_scope_id(entry()) -> {ok, ScopeId :: od_space:id() | undefined} | {error, term()}.
get_scope_id(#document{key = FileUuid, value = #file_meta{is_scope = true}, scope = <<>>}) ->
    % scope has not been set yet
    case fslogic_uuid:is_space_dir_uuid(FileUuid) of
        true -> {ok, fslogic_uuid:space_dir_uuid_to_spaceid(FileUuid)};
        false -> {ok, ?ROOT_DIR_SCOPE}
    end;
get_scope_id(#document{value = #file_meta{is_scope = false}, scope = <<>>}) ->
    % scope has not been set yet
    {ok, undefined};
get_scope_id(#document{value = #file_meta{}, scope = Scope}) ->
    {ok, Scope};
get_scope_id(Entry) ->
    ?run(begin
        {ok, Doc} = file_meta:get(Entry),
        get_scope_id(Doc)
    end).

-spec get_type(file_meta() | doc()) -> type().
get_type(#file_meta{type = Type}) ->
    Type;
get_type(#document{value = FileMeta}) ->
    get_type(FileMeta).

-spec get_owner(file_meta() | doc()) -> od_user:id().
get_owner(#document{value = FileMeta}) ->
    get_owner(FileMeta) ;
get_owner(#file_meta{owner = Owner}) ->
    Owner.

-spec get_mode(file_meta() | doc()) -> mode().
get_mode(#document{value = FileMeta}) ->
    get_mode(FileMeta) ;
get_mode(#file_meta{mode = Mode}) ->
    Mode.

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
            CTime = global_clock:timestamp_seconds(),

            lists:foreach(fun(SpaceId) ->
                make_space_exist(SpaceId)
            end, EffSpaces),

            FileUuid = fslogic_uuid:user_root_dir_uuid(UserId),
            case create({uuid, ?GLOBAL_ROOT_DIR_UUID},
                #document{
                    key = FileUuid,
                    value = #file_meta{
                        name = UserId,
                        type = ?DIRECTORY_TYPE,
                        mode = 8#1755,
                        owner = ?ROOT_USER_ID,
                        is_scope = true,
                        parent_uuid = ?GLOBAL_ROOT_DIR_UUID
                    }
                })
            of
                {ok, _} ->
                    {ok, _} = times:save(#document{
                        key = FileUuid,
                        value = #times{mtime = CTime, atime = CTime, ctime = CTime},
                        scope = ?ROOT_DIR_SCOPE
                    }),
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
%% Add shareId to file meta.
%% @end
%%--------------------------------------------------------------------
-spec add_share(file_ctx:ctx(), od_share:id()) -> {ok, uuid()}  | {error, term()}.
add_share(FileCtx, ShareId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    update({uuid, FileUuid}, fun(FileMeta = #file_meta{shares = Shares}) ->
        {ok, FileMeta#file_meta{shares = [ShareId | Shares]}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Remove shareId from file meta.
%% @end
%%--------------------------------------------------------------------
-spec remove_share(file_ctx:ctx(), od_share:id()) -> {ok, uuid()} | {error, term()}.
remove_share(FileCtx, ShareId) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    update({uuid, FileUuid}, fun(FileMeta = #file_meta{shares = Shares}) ->
        Result = lists:foldl(fun(ShId, {IsMember, Acc}) ->
            case ShareId == ShId of
                true -> {found, Acc};
                false -> {IsMember, [ShId | Acc]}
            end
        end, {not_found, []}, Shares),

        case Result of
            {found, FilteredShares} ->
                {ok, FileMeta#file_meta{shares = FilteredShares}};
            {not_found, _} ->
                {error, not_found}
        end
    end).

-spec get_shares(doc() | file_meta()) -> [od_share:id()].
get_shares(#document{value = FileMeta}) ->
    get_shares(FileMeta);
get_shares(#file_meta{shares = Shares}) ->
    Shares.


%%--------------------------------------------------------------------
%% @doc
%% Creates file meta entry for space if not exists
%% @end
%%--------------------------------------------------------------------
-spec make_space_exist(SpaceId :: datastore:key()) -> ok | no_return().
make_space_exist(SpaceId) ->
    CTime = global_clock:timestamp_seconds(),
    SpaceDirUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    FileDoc = #document{
        key = SpaceDirUuid,
        value = #file_meta{
            name = SpaceId,
            type = ?DIRECTORY_TYPE,
            mode = ?DEFAULT_DIR_PERMS,
            owner = ?SPACE_OWNER_ID(SpaceId),
            is_scope = true,
            parent_uuid = ?GLOBAL_ROOT_DIR_UUID
        }
    },
    case file_meta:create({uuid, ?GLOBAL_ROOT_DIR_UUID}, FileDoc) of
        {ok, _} ->
            TimesDoc = #document{
                key = SpaceDirUuid,
                value = #times{mtime = CTime, atime = CTime, ctime = CTime},
                scope = SpaceId
            },
            case times:save(TimesDoc) of
                {ok, _} -> ok;
                {error, already_exists} -> ok
            end,
            emit_space_dir_created(SpaceDirUuid, SpaceId);
        {error, already_exists} ->
            ok
    end.

-spec new_doc(name(), type(), posix_permissions(), od_user:id(), uuid(), od_space:id()) -> doc().
new_doc(FileName, FileType, Mode, Owner, ParentUuid, SpaceId) ->
    new_doc(undefined, FileName, FileType, Mode, Owner, ParentUuid, SpaceId).

-spec new_doc(undefined | uuid(), name(), type(), posix_permissions(), od_user:id(),
    uuid(), od_space:id()) -> doc().
new_doc(FileUuid, FileName, FileType, Mode, Owner, ParentUuid, SpaceId) ->
    #document{
        key = FileUuid,
        value = #file_meta{
            name = FileName,
            type = FileType,
            mode = Mode,
            owner = Owner,
            parent_uuid = ParentUuid,
            provider_id = oneprovider:get_id()
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
    {_, ParentPath} = filepath_utils:basename_and_parent_dir(Path),
    {Parent, _} = filepath_utils:basename_and_parent_dir(ParentPath),
    is_hidden(Parent).

-spec get_name(doc()) -> binary().
get_name(#document{value = #file_meta{name = Name}}) ->
    Name.


-spec set_name(doc(), name()) -> doc().
set_name(Doc = #document{value = FileMeta}, NewName) ->
    Doc#document{value = FileMeta#file_meta{name = NewName}}.


%%--------------------------------------------------------------------
%% @doc
%% Returns file active permissions type, that is info which permissions
%% are taken into account when checking authorization (acl if it is defined
%% or posix otherwise).
%% @end
%%--------------------------------------------------------------------
-spec get_active_perms_type(file_meta:uuid() | doc()) ->
    {ok, file_meta:permissions_type()} | {error, term()}.
get_active_perms_type(#document{value = #file_meta{acl = []}}) ->
    {ok, posix};
get_active_perms_type(#document{value = #file_meta{}}) ->
    {ok, acl};
get_active_perms_type(FileUuid) ->
    case file_meta:get({uuid, FileUuid}) of
        {ok, FileDoc} ->
            get_active_perms_type(FileDoc);
        {error, _} = Error ->
            Error
    end.

-spec update_mode(uuid(), posix_permissions()) -> ok | {error, term()}.
update_mode(FileUuid, NewMode) ->
    ?extract_ok(update({uuid, FileUuid}, fun(#file_meta{} = FileMeta) ->
        {ok, FileMeta#file_meta{mode = NewMode}}
    end)).

-spec update_acl(uuid(), acl:acl()) -> ok | {error, term()}.
update_acl(FileUuid, NewAcl) ->
    ?extract_ok(update({uuid, FileUuid}, fun(#file_meta{} = FileMeta) ->
        {ok, FileMeta#file_meta{acl = NewAcl}}
    end)).

%%--------------------------------------------------------------------
%% @doc
%% Checks if given file has conflicts with other files on name field.
%% ParentUuid and Name cannot be got from document as this function may be used
%% as a result of conflict resolving that involve renamed file document.
%% @end
%%--------------------------------------------------------------------
-spec check_name(ParentUuid :: uuid(), name(), CheckDoc :: doc()) ->
    ok | {conflicting, ExtendedName :: name(), Conflicts :: [{uuid(), name()}]}.
check_name(undefined, _Name, _ChildDoc) ->
    ok; % Root directory
check_name(ParentUuid, Name, #document{
    key = ChildUuid,
    value = #file_meta{
        provider_id = ChildProvider
    }
}) ->
    case file_meta:get_all_links(ParentUuid, Name) of
        {ok, [#link{target = ChildUuid}]} ->
            ok;
        {ok, []} ->
            ok;
        {ok, Links} ->
            Links2 = case lists:any(fun(#link{tree_id = TreeID}) -> TreeID =:= ChildProvider end, Links) of
                false ->
                    % Link is missing, possible race on dbsync
                    [#link{tree_id = ChildProvider, name = Name, target = ChildUuid} | Links];
                _ ->
                    Links
            end,
            WithTag = tag_children(Links2),
            {NameAns, OtherFiles} = lists:foldl(fun
                (#child_link_uuid{uuid = Uuid, name = ExtendedName}, {_NameAcc, OtherAcc}) when Uuid =:= ChildUuid->
                    {ExtendedName, OtherAcc};
                (#child_link_uuid{uuid = Uuid, name = ExtendedName}, {NameAcc, OtherAcc}) ->
                    {NameAcc, [{Uuid, ExtendedName} | OtherAcc]}
            end, {Name, []}, WithTag),
            {conflicting, NameAns, OtherFiles};
        {error, _Reason} ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Function created to be mocked during tests.
%% @end
%%--------------------------------------------------------------------
-spec get_all_links(uuid(), name()) -> {ok, [datastore:link()]} | {error, term()}.
get_all_links(Uuid, Name) ->
    datastore_model:get_links(?CTX, Uuid, all, Name).

%%--------------------------------------------------------------------
%% @doc
%% Checks if file has suffix. Returns name without suffix if true.
%% @end
%%--------------------------------------------------------------------
-spec has_suffix(name()) -> {true, NameWithoutSuffix :: name()} | false.
has_suffix(Name) ->
    case binary:split(Name, ?CONFLICTING_LOGICAL_FILE_SUFFIX_SEPARATOR) of
        [BaseName | _] -> {true, BaseName};
        _ -> false
    end.

-spec is_deleted(doc()) -> boolean().
is_deleted(#document{value = #file_meta{deleted = Deleted1}, deleted = Deleted2}) ->
    Deleted1 orelse Deleted2.

-spec get_provider_id(doc() | file_meta()) -> oneprovider:id().
get_provider_id(#file_meta{provider_id = ProviderId}) ->
    ProviderId;
get_provider_id(#document{value = FileMeta}) ->
    get_provider_id(FileMeta).

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
            {RemoteTreeIds, RemoteTreeIdsLen} = lists:foldl(
                fun(#link{tree_id = TreeId}, {IdsAcc, IdsLenAcc} = Acc) ->
                    case TreeId of
                        LocalTreeId ->
                            Acc;
                        _ ->
                            {[TreeId | IdsAcc], [size(TreeId), IdsLenAcc]}
                    end
                end,
                {[], []},
                Group
            ),
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
                        name = <<
                            Name/binary,
                            ?CONFLICTING_LOGICAL_FILE_SUFFIX_SEPARATOR_CHAR,
                            TreeId:Len2/binary
                        >>
                    } | Children2]
            end, Children, Group)
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
    case canonical_path:resolve(Path) of
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
-spec fill_uuid(doc(), uuid()) -> doc().
fill_uuid(Doc = #document{key = undefined, value = #file_meta{type = ?DIRECTORY_TYPE}}, _ParentUuid) ->
    Doc#document{key = datastore_key:new()};
fill_uuid(Doc = #document{key = undefined}, ParentUuid) ->
    Doc#document{key = datastore_key:new_adjacent_to(ParentUuid)};
fill_uuid(Doc, _ParentUuid) ->
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
%% Returns parent child's UUID by name within given links tree set
%% alongside with TreeId in which it was found.
%% @end
%%--------------------------------------------------------------------
-spec get_child_uuid_and_tree_id(uuid(), datastore_links:tree_ids(), name()) ->
    {ok, uuid(), datastore_links:tree_id()} | {error, term()}.
get_child_uuid_and_tree_id(ParentUuid, TreeIds, Name) ->
    case datastore_model:get_links(?CTX, ParentUuid, TreeIds, Name) of
        {ok, [#link{target = FileUuid, tree_id = TreeId}]} ->
            {ok, FileUuid, TreeId};
        {ok, [#link{} | _]} ->
            {error, ?EINVAL};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends event about space dir creation
%% @end
%%--------------------------------------------------------------------
-spec emit_space_dir_created(DirUuid :: uuid(), SpaceId :: datastore:key()) -> ok | no_return().
emit_space_dir_created(DirUuid, SpaceId) ->
    FileCtx = file_ctx:new_by_guid(file_id:pack_guid(DirUuid, SpaceId)),
    #fuse_response{fuse_response = FileAttr} =
        attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID), FileCtx, #{
            allow_deleted_files => false,
            include_size => false,
            name_conflicts_resolution_policy => allow_name_conflicts
        }),
    FileAttr2 = FileAttr#file_attr{size = 0},
    ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, FileAttr2, []).

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
    10.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(Version) ->
    file_meta_model:get_record_struct(Version).

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(Version, Record) ->
    file_meta_model:upgrade_record(Version, Record).

%%--------------------------------------------------------------------
%% @doc
%% Function called when saving changes from other providers (checks conflicts: local doc vs. remote changes).
%% It is used to check if file has been renamed remotely to send appropriate event.
%% TODO - VFS-5962 - delete when event emission is possible in dbsync_events.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(datastore_model:ctx(), doc(), doc()) -> default.
resolve_conflict(Ctx, Doc1, Doc2) ->
    file_meta_model:resolve_conflict(Ctx, Doc1, Doc2).