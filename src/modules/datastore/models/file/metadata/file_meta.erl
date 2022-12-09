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
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_suffix.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/onedata.hrl").

-export([save/1, create/2, save/2, get/1, exists/1, update/2]).
-export([delete/1, delete_without_link/1]).
-export([hidden_file_name/1, is_hidden/1, is_child_of_hidden_dir/1, is_deletion_link/1]).
-export([add_share/2, remove_share/2, get_shares/1]).
-export([get_protection_flags/1]).
-export([get_parent/1, get_parent_uuid/1, get_provider_id/1]).
-export([
    get_uuid/1, get_child/2, trim_filename_tree_id/2,
    get_child_uuid_and_tree_id/2, get_matching_child_uuids_with_tree_ids/3
]).
-export([get_name/1, set_name/2]).
-export([
    get_active_perms_type/1, update_mode/2,  update_acl/2,
    update_protection_flags/3, protection_flags_to_json/1, protection_flags_from_json/1
]).
-export([get_scope_id/1, setup_onedata_user/2, get_including_deleted/1,
    make_space_exist/1, new_doc/6, new_doc/7, new_share_root_dir_doc/2, get_ancestors/1,
    get_locations_by_uuid/1, rename/4, get_owner/1, get_type/1, get_effective_type/1,
    get_mode/1]).
-export([check_name_and_get_conflicting_files/1, check_name_and_get_conflicting_files/4, has_suffix/1, is_deleted/1]).


%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2, resolve_conflict/3, on_remote_doc_created/2]).

-type doc() :: datastore_doc:doc(file_meta()).
-type diff() :: datastore_doc:diff(file_meta()).
-type uuid() :: datastore:key().
-type path() :: binary().
-type uuid_based_path() :: binary(). % similar to canonical, but path elements are uuids instead of filenames/dirnames
-type name() :: binary().
-type uuid_or_path() :: {path, path()} | {uuid, uuid()}.
-type entry() :: uuid_or_path() | doc().
-type type() :: ?REGULAR_FILE_TYPE | ?DIRECTORY_TYPE | ?SYMLINK_TYPE | ?LINK_TYPE.
-type size() :: non_neg_integer().
-type mode() :: non_neg_integer().
-type time() :: non_neg_integer().
-type file_meta() :: #file_meta{}.
-type posix_permissions() :: non_neg_integer().
-type permissions_type() :: posix | acl.
-type conflicts() :: [link()].
-type path_type() :: ?CANONICAL_PATH | ?UUID_BASED_PATH.
-type link() :: file_meta_forest:link().

-export_type([
    doc/0, file_meta/0, uuid/0, path/0, uuid_based_path/0, name/0, uuid_or_path/0, entry/0,
    type/0, size/0, mode/0, time/0, posix_permissions/0, permissions_type/0,
    conflicts/0, path_type/0, link/0
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
save(#document{key = FileUuid, value = #file_meta{is_scope = true}} = Doc, _GeneratedKey) ->
    % Spaces are handled specially so as to not overwrite file_meta if it already
    % exists ('make_space_exist' may be called several times for each space)
    case datastore_model:create(?CTX#{memory_copies => all}, Doc) of
        ?ERROR_ALREADY_EXISTS -> file_meta:get(FileUuid);
        Result -> Result
    end;
save(Doc, GeneratedKey) ->
    datastore_model:save(?CTX#{generated_key => GeneratedKey}, Doc).


%%--------------------------------------------------------------------
%% @doc
%% @equiv create(Parent, FileDoc, all)
%% @end
%%--------------------------------------------------------------------
-spec create({uuid, ParentUuid :: uuid()}, doc()) ->
    {ok, doc()} | {error, term()}.
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
create({uuid, ParentUuid}, FileDoc = #document{value = FileMeta = #file_meta{name = FileName}}, TreesToCheck) ->
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
        % Warning: if uuid exists, two files with same #file_meta{} will be created
        % (names are checked for conflicts, not uuids)
        case file_meta:save(FileDoc3) of
            {ok, FileDocFinal = #document{key = FileUuid}} ->
                case file_meta_forest:check_and_add(ParentUuid, ParentScopeId, TreesToCheck, FileName, FileUuid) of
                    ok ->
                        {ok, FileDocFinal};
                    {error, already_exists} = Eexists ->
                        case file_meta_forest:get(ParentUuid, TreesToCheck, FileName) of
                            {ok, Links} ->
                                FileExists = lists:any(fun(#link{target = Uuid, tree_id = TreeId, rev = Rev}) ->
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
                                            file_meta_forest:delete_local(ParentUuid, ParentScopeId, FileName, Rev),
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
                                create({uuid, ParentUuid}, FileDoc, TreesToCheck);
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
get_including_deleted(Uuid) ->
    case fslogic_file_id:is_link_uuid(Uuid) of
        true ->
            % When hardlink document is requested it is merged using document
            % representing hardlink and document representing target file
            case datastore_model:get(?CTX#{include_deleted => true}, Uuid) of
                {ok, LinkDoc} ->
                    FileUuid = fslogic_file_id:ensure_referenced_uuid(Uuid),
                    case datastore_model:get(?CTX#{include_deleted => true}, FileUuid) of
                        {ok, FileDoc} -> file_meta_hardlinks:merge_link_and_file_doc(LinkDoc, FileDoc);
                        Error2 -> Error2
                    end;
                Error -> Error
            end;
        false ->
            datastore_model:get(?CTX#{include_deleted => true}, Uuid)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Updates file meta.
%% @end
%%--------------------------------------------------------------------
-spec update(uuid() | entry(), diff()) -> {ok, doc()} | {error, term()}.
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
    datastore_model:update(?CTX, Key, Diff).


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
        ok = file_meta_forest:delete(ParentUuid, Scope, FileName, FileUuid),
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
delete_without_link(#document{key = FileUuid}) ->
    delete_without_link(FileUuid);
delete_without_link(FileUuid) ->
    ?run(begin datastore_model:delete(?CTX, FileUuid) end).


-spec delete_doc_if_not_special(uuid()) -> ok.
delete_doc_if_not_special(FileUuid) ->
    case fslogic_file_id:is_special_uuid(FileUuid) of
        true -> ok;
        false -> delete_without_link(FileUuid)
    end.


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
        Error ->
            Error
    end.


-spec trim_filename_tree_id(name(), {all, uuid()} | file_meta_forest:tree_id()) -> name().
trim_filename_tree_id(Name, {all, ParentUuid}) ->
    TreeIds = case file_meta_forest:get_trees(ParentUuid) of
        {ok, T} -> T;
        ?ERROR_NOT_FOUND -> []
    end,
    lists_utils:foldl_while(fun(TreeId, NameAcc) ->
        case trim_filename_tree_id(NameAcc, TreeId) of
            NameAcc -> {cont, NameAcc};
            TrimmedName -> {halt, TrimmedName}
        end
    end, Name, TreeIds);
trim_filename_tree_id(Name, TreeId) ->
    Tokens = binary:split(Name, ?CONFLICTING_LOGICAL_FILE_SUFFIX_SEPARATOR, [global]),
    case lists:reverse(Tokens) of
        [TreeId | NameTokens] -> str_utils:join_binary(NameTokens, ?CONFLICTING_LOGICAL_FILE_SUFFIX_SEPARATOR);
        _ -> Name
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns parent child's UUID by Name and TreeId of a tree in which
%% the link was found.
%% @end
%%--------------------------------------------------------------------
-spec get_child_uuid_and_tree_id(uuid(), name()) -> {ok, uuid(), file_meta_forest:tree_ids()} | {error, term()}.
get_child_uuid_and_tree_id(ParentUuid, Name) ->
    Tokens = binary:split(Name, ?CONFLICTING_LOGICAL_FILE_SUFFIX_SEPARATOR, [global]),
    case lists:reverse(Tokens) of
        % TODO VFS-9001 - consider change of behaviour when filename is equal to name with suffix (?EINVAL)
        [TreeIdSuffix | Tokens2]  when TreeIdSuffix =/= <<>>, Tokens2 =/= [], Tokens2 =/= [<<>>] ->
            SuffixSize = size(TreeIdSuffix),
            NameWithoutTreeSuffix = binary:part(
                Name, 0, size(Name) - SuffixSize - size(?CONFLICTING_LOGICAL_FILE_SUFFIX_SEPARATOR)),
            MatchingTreeIds = case file_meta_forest:get_trees(ParentUuid) of
                {ok, TreeIds} ->
                    lists:filter(fun(TreeId) ->
                        case TreeId of
                            <<TreeIdSuffix:SuffixSize/binary, _/binary>> -> true;
                            _ -> false
                        end
                    end, TreeIds);
                ?ERROR_NOT_FOUND ->
                    []
            end,
            case MatchingTreeIds of
                [TreeId] ->
                    case get_matching_child_uuids_with_tree_ids(ParentUuid, TreeId, NameWithoutTreeSuffix) of
                        {ok, [{Uuid, TreeId}]} -> {ok, Uuid, TreeId};
                        {ok, _} -> {error, ?EINVAL};
                        {error, not_found} -> get_child_uuid_and_tree_id_for_name_without_suffix(ParentUuid, Name);
                        {error, Reason} -> {error, Reason}
                    end;
                [] ->
                    get_child_uuid_and_tree_id_for_name_without_suffix(ParentUuid, Name);
                _ ->
                    {error, ?EINVAL}
            end;
        _ ->
            get_child_uuid_and_tree_id_for_name_without_suffix(ParentUuid, Name)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns child's UUIDs matching to name within given links tree set
%% alongside with TreeIds in which they were found.
%% @end
%%--------------------------------------------------------------------
-spec get_matching_child_uuids_with_tree_ids(uuid(), datastore_links:tree_ids(), name()) ->
    {ok, [{uuid(), file_meta_forest:tree_ids()}]} | {error, term()}.
get_matching_child_uuids_with_tree_ids(ParentUuid, TreeIds, Name) ->
    case file_meta_forest:get(ParentUuid, TreeIds, Name) of
        {ok, [#link{} | _] = Links} ->
            UuidsWithTreeIds = lists:map(fun(#link{target = FileUuid, tree_id = TreeId}) -> {FileUuid, TreeId} end, Links),
            {ok, UuidsWithTreeIds};
        {error, Reason} ->
            {error, Reason}
    end.


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
-spec rename(doc(), doc() | uuid(), doc() | uuid(), name()) -> ok.
rename(SourceDoc, #document{key = SourceParentUuid}, #document{key = TargetParentUuid}, TargetName) ->
    rename(SourceDoc, SourceParentUuid, TargetParentUuid, TargetName);
rename(SourceDoc, SourceParentUuid, TargetParentUuid, TargetName) ->
    #document{
        key = FileUuid,
        value = #file_meta{name = FileName, type = Type},
        scope = Scope
    } = SourceDoc,
    ok = file_meta_forest:add(TargetParentUuid, Scope, TargetName, FileUuid),
    ok = file_meta_forest:delete(SourceParentUuid, Scope, FileName, FileUuid),
    {ok, TargetDoc} = file_meta:update(FileUuid, fun(FileMeta = #file_meta{}) ->
        {ok, FileMeta#file_meta{
            name = TargetName,
            parent_uuid = TargetParentUuid
        }}
    end),

    % TODO VFS-8835 - test if other mechanisms handle size change
    case SourceParentUuid =/= TargetParentUuid of
        true ->
            dir_stats_collector:report_file_moved(Type, file_id:pack_guid(FileUuid, Scope),
                file_id:pack_guid(SourceParentUuid, Scope), file_id:pack_guid(TargetParentUuid, Scope));
        false ->
            ok
    end,

    dataset_api:move_if_applicable(SourceDoc, TargetDoc).

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
-spec get_parent_uuid(uuid() | entry()) -> {ok, uuid()} | {error, term()}.
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
    ?run(begin get_ancestors(FileUuid, []) end).

-spec get_ancestors(uuid(), [uuid()]) -> {ok, [uuid()]} | {error, term()}.
get_ancestors(?GLOBAL_ROOT_DIR_UUID, Acc) ->
    {ok, Acc};
get_ancestors(FileUuid, Acc) ->
    {ok, ParentUuid} = get_parent_uuid({uuid, FileUuid}),
    get_ancestors(ParentUuid, [ParentUuid | Acc]).


%%--------------------------------------------------------------------
%% @doc
%% Gets "scope" id of given document.
%% @end
%%--------------------------------------------------------------------
-spec get_scope_id(entry()) -> {ok, ScopeId :: od_space:id() | undefined} | {error, term()}.
get_scope_id(#document{key = FileUuid, value = #file_meta{is_scope = true}, scope = <<>>}) ->
    % scope has not been set yet
    case fslogic_file_id:is_space_dir_uuid(FileUuid) of
        true -> {ok, fslogic_file_id:space_dir_uuid_to_spaceid(FileUuid)};
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


-spec get_effective_type(file_meta() | doc()) -> type().
get_effective_type(#file_meta{type = ?LINK_TYPE}) ->
    ?REGULAR_FILE_TYPE;
get_effective_type(#file_meta{type = Type}) ->
    Type;
get_effective_type(#document{value = FileMeta}) ->
    get_effective_type(FileMeta).


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

            FileUuid = fslogic_file_id:user_root_dir_uuid(UserId),
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
        catch Type:Message:Stacktrace ->
            ?error_stacktrace("Failed to setup user ~s - ~p:~p", [
                UserId, Type, Message
            ], Stacktrace)
        end
    end).


%%--------------------------------------------------------------------
%% @doc
%% Add shareId to file meta.
%% @end
%%--------------------------------------------------------------------
-spec add_share(file_ctx:ctx(), od_share:id()) -> ok | {error, term()}.
add_share(FileCtx, ShareId) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    ?extract_ok(update({uuid, FileUuid}, fun(FileMeta = #file_meta{shares = Shares}) ->
        {ok, FileMeta#file_meta{shares = [ShareId | Shares]}}
    end)).


%%--------------------------------------------------------------------
%% @doc
%% Remove shareId from file meta.
%% @end
%%--------------------------------------------------------------------
-spec remove_share(file_ctx:ctx(), od_share:id()) -> ok | {error, term()}.
remove_share(FileCtx, ShareId) ->
    FileUuid = file_ctx:get_logical_uuid_const(FileCtx),
    ?extract_ok(update({uuid, FileUuid}, fun(FileMeta = #file_meta{shares = Shares}) ->
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
    end)).


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
-spec make_space_exist(SpaceId :: od_space:id()) -> ok | no_return().
make_space_exist(SpaceId) ->
    SpaceDirUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
    FileDoc = #document{
        key = SpaceDirUuid,
        value = #file_meta{
            name = SpaceId,
            type = ?DIRECTORY_TYPE,
            mode = ?DEFAULT_DIR_MODE,
            owner = ?SPACE_OWNER_ID(SpaceId),
            is_scope = true,
            parent_uuid = ?GLOBAL_ROOT_DIR_UUID
        }
    },
    case file_meta:create({uuid, ?GLOBAL_ROOT_DIR_UUID}, FileDoc) of
        {ok, _} ->
            case times:save_with_current_times(SpaceDirUuid, SpaceId) of
                {ok, _} -> ok;
                {error, already_exists} -> ok
            end,
            trash:create(SpaceId),
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


-spec new_share_root_dir_doc(uuid(), od_space:id()) -> doc().
new_share_root_dir_doc(ShareRootDirUuid, SpaceId) ->
    ShareId = fslogic_file_id:share_root_dir_uuid_to_shareid(ShareRootDirUuid),

    #document{
        key = ShareRootDirUuid,
        value = #file_meta{
            name = ShareId,
            type = ?DIRECTORY_TYPE,
            is_scope = false,
            mode = ?DEFAULT_SHARE_ROOT_DIR_PERMS,
            owner = ?ROOT_USER_ID,
            parent_uuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
            provider_id = oneprovider:get_id(),
            deleted = case share_logic:get(?ROOT_SESS_ID, ShareId) of
                {ok, _} -> false;
                ?ERROR_NOT_FOUND -> true
            end
        },
        scope = SpaceId
    }.


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
%% NOTE:
%% Deletion links are deprecated. This function is left
%% only to filter out deletion links from the result of list operation.
%% @end
%%--------------------------------------------------------------------
-spec is_deletion_link(binary()) -> boolean().
is_deletion_link(LinkName) ->
    DeletionLinkSuffix = <<"####TO_DELETE">>,
    case binary:match(LinkName, DeletionLinkSuffix) of
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


-spec get_protection_flags(file_meta() | doc()) -> data_access_control:bitmask().
get_protection_flags(#document{value = FM}) ->
    get_protection_flags(FM);
get_protection_flags(#file_meta{protection_flags = ProtectionFlags}) ->
    ProtectionFlags.


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


-spec update_mode(uuid(), posix_permissions()) -> {ok, doc()} | {error, term()}.
update_mode(FileUuid, NewMode) ->
    update({uuid, FileUuid}, fun(#file_meta{} = FileMeta) ->
        {ok, FileMeta#file_meta{mode = NewMode}}
    end).


-spec update_acl(uuid(), acl:acl()) -> ok | {error, term()}.
update_acl(FileUuid, NewAcl) ->
    ?extract_ok(update({uuid, FileUuid}, fun(#file_meta{} = FileMeta) ->
        {ok, FileMeta#file_meta{acl = NewAcl}}
    end)).


-spec update_protection_flags(uuid(), data_access_control:bitmask(), data_access_control:bitmask()) ->
    ok | {error, nothing_changed} | {error, term()}.
update_protection_flags(FileUuid, FlagsToSet, FlagsToUnset) ->
    ?extract_ok(update({uuid, FileUuid}, fun(#file_meta{protection_flags = CurrFlags} = FileMeta) ->
        NewFlags = ?set_flags(?reset_flags(CurrFlags, FlagsToUnset), FlagsToSet),
        case NewFlags =:= CurrFlags of
            true -> {error, nothing_changed};
            false -> {ok, FileMeta#file_meta{protection_flags = NewFlags}}
        end
    end)).


-spec protection_flags_to_json(data_access_control:bitmask()) -> [binary()].
protection_flags_to_json(ProtectionFlags) ->
    lists:filtermap(fun({FlagName, FlagMask}) ->
        case ?has_all_flags(ProtectionFlags, FlagMask) of
            true -> {true, FlagName};
            false -> false
        end
    end, [
        {?DATA_PROTECTION_BIN, ?DATA_PROTECTION},
        {?METADATA_PROTECTION_BIN, ?METADATA_PROTECTION}
    ]).


-spec protection_flags_from_json([binary()]) -> data_access_control:bitmask().
protection_flags_from_json(ProtectionFlagsJson) ->
    lists:foldl(fun(ProtectionFlag, Bitmask) ->
        ?set_flags(Bitmask, protection_flag_from_json(ProtectionFlag))
    end, ?no_flags_mask, ProtectionFlagsJson).


%% @private
-spec protection_flag_from_json(binary()) -> data_access_control:bitmask().
protection_flag_from_json(?DATA_PROTECTION_BIN) -> ?DATA_PROTECTION;
protection_flag_from_json(?METADATA_PROTECTION_BIN) -> ?METADATA_PROTECTION.


-spec check_name_and_get_conflicting_files(doc()) ->
    ok | {conflicting, ExtendedName :: name(), Conflicts :: conflicts()}.
check_name_and_get_conflicting_files(#document{
    key = FileUuid,
    value = #file_meta{
        name = FileName,
        provider_id = FileProviderId,
        parent_uuid = ParentUuid
    }}) ->
    check_name_and_get_conflicting_files(ParentUuid, FileName, FileUuid, FileProviderId).


%%--------------------------------------------------------------------
%% @doc
%% Checks if given file has conflicts with other files on name field.
%%
%% NOTE !!!
%% Sometimes ParentUuid and Name cannot be got from document as this
%% function may be used as a result of conflict resolving that involve
%% renamed file document.
%% @end
%%--------------------------------------------------------------------
-spec check_name_and_get_conflicting_files(uuid(), name(), uuid(), od_provider:id()) ->
    ok | {conflicting, ExtendedName :: name(), Conflicts :: conflicts()}.
check_name_and_get_conflicting_files(ParentUuid, FileName, FileUuid, FileProviderId) ->
    file_meta_forest:check_name_and_get_conflicting_files(ParentUuid, FileName, FileUuid, FileProviderId).


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


%%--------------------------------------------------------------------
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


%%%===================================================================
%%% Internal functions
%%%===================================================================

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


%% @private
-spec get_child_uuid_and_tree_id_for_name_without_suffix(uuid(), name()) ->
    {ok, uuid(), file_meta_forest:tree_ids()} | {error, term()}.
get_child_uuid_and_tree_id_for_name_without_suffix(ParentUuid, Name) ->
    case get_matching_child_uuids_with_tree_ids(ParentUuid, oneprovider:get_id(), Name) of
        {ok, [{Uuid, TreeId}]} ->
            {ok, Uuid, TreeId};
        {ok, _} ->
            {error, ?EINVAL};
        {error, not_found} ->
            case get_matching_child_uuids_with_tree_ids(ParentUuid, all, Name) of
                {ok, [{Uuid, TreeId}]} -> {ok, Uuid, TreeId};
                {ok, _} -> {error, ?EINVAL};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends event about space dir creation
%% @end
%%--------------------------------------------------------------------
-spec emit_space_dir_created(DirUuid :: uuid(), SpaceId :: od_space:id()) -> ok | no_return().
emit_space_dir_created(DirUuid, SpaceId) ->
    FileCtx = file_ctx:new_by_uuid(DirUuid, SpaceId),
    #fuse_response{fuse_response = FileAttr} =
        attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID), FileCtx, #{
            allow_deleted_files => false,
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
    file_meta_model:get_record_version().

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


%%--------------------------------------------------------------------
%% @doc
%% Function called when new record appears from remote provider.
%% @end
%%--------------------------------------------------------------------
-spec on_remote_doc_created(datastore_model:ctx(), doc()) -> ok.
on_remote_doc_created(Ctx, Doc) ->
    file_meta_model:on_remote_doc_created(Ctx, Doc).