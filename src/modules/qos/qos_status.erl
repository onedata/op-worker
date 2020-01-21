%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for calculating QoS entry fulfillment status.
%%% QoS entry is fulfilled when:
%%%     - there is no information that qos_entry cannot be satisfied (see qos_entry.erl)
%%%     - there are no traverse requests in qos_entry document (see qos_entry.erl)
%%%     - there are no links indicating that file has been changed and it should
%%%       be reconciled. When provider notices file_location change that impacts
%%%       local replica it calculates effective_file_qos for given file. For
%%%       each QoS entry in effective_file_qos provider creates appropriate link
%%%       and schedules file reconciliation. Links are created as follow:
%%%         * value is set to traverse task ID
%%%         * key is expressed as combined: relative path to QoS entry origin
%%%           file (file that QoS entry was added to) and traverse task id
%%%
%%% QoS fulfillment status of directory is checked by getting next status link to
%%% given directory relative path. Because status links keys start with relative
%%% path, if there is unfinished transfer in subtree of this directory next
%%% status link will start with given parent directory relative path.
% fixme explain status during traverse
%%% @end
%%%-------------------------------------------------------------------
-module(qos_status).
-author("Michal Stanisz").

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([check_fulfillment/2]).
-export([report_traverse_started/2, report_traverse_batch_calculated/6]).
-export([report_last_batch_for_dir/3, report_file_transferred_in_traverse/2]).
-export([report_file_changed/4, report_file_reconciled/4]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type path() :: binary().
-type id() :: datastore_doc:key().
-type record() :: #qos_status{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type one_or_many(Type) :: Type | [Type].

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

-define(RECONCILE_LINK_NAME(RelativePath, TraverseId),
    <<RelativePath/binary, "###", TraverseId/binary>>).

-define(RECONCILE_LINKS_KEY(QosEntryId), <<"qos_status_reconcile", QosEntryId/binary>>).
-define(TRAVERSE_LINKS_KEY(TraverseId), <<"qos_status_traverse", TraverseId/binary>>).

%%%===================================================================
%%% API
%%%===================================================================

% fixme doc and spec
check_fulfillment(FileCtx, QosEntryId) ->
    {ok, QosDoc} = qos_entry:get(QosEntryId),
    qos_entry:is_possible(QosDoc) andalso
        check_reconciling(FileCtx, QosDoc) andalso
        check_traverses(FileCtx, QosDoc).

report_traverse_started(TraverseId, FileCtx) ->
    create(TraverseId, file_ctx:get_uuid_const(FileCtx), file_ctx:get_space_id_const(FileCtx), true).

% fixme name
report_traverse_batch_calculated(SpaceId, TraverseId, Uuid, ChildrenDirs, ChildrenFiles, PreviousBatchLastName) ->
    {ok, _} = update(TraverseId, Uuid,
        fun(#qos_status{files_list = FilesList, child_dirs = ChildDirs} = Value) ->
            {ok, Value#qos_status{
                files_list = FilesList ++ ChildrenFiles,
                last_file = PreviousBatchLastName,
                child_dirs = ChildDirs + length(ChildrenDirs)}
            }
        end),
    lists:foreach(fun(ChildDirUuid) ->
        create(TraverseId, ChildDirUuid, SpaceId, false)
    end, ChildrenDirs).

report_last_batch_for_dir(TraverseId, DirUuid, SpaceId) ->
    update_and_handle_finished(TraverseId, DirUuid, SpaceId,
        fun(#qos_status{} = Value) ->
            {ok, Value#qos_status{is_last_batch = true}}
        end
    ).

report_file_transferred_in_traverse(TraverseId, #document{key = FileId, scope = SpaceId} = FileDoc) ->
    {ok, ParentUuid} = file_meta:get_parent_uuid(FileDoc),
    update_and_handle_finished(TraverseId, ParentUuid, SpaceId,
        fun(#qos_status{files_list = FilesList} = Value) ->
            {ok, Value#qos_status{files_list = FilesList -- [FileId]}}
        end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Creates new link indicating that requirements for given qos_entry are not
%% fulfilled as file has been changed.
%% @end
%%--------------------------------------------------------------------
-spec report_file_changed(qos_entry:id(), datastore_doc:scope(), file_meta:uuid(), traverse:id()) ->
    ok | {error, term()}.
report_file_changed(QosEntryId, SpaceId, FileUuid, TraverseId) ->
    {ok, OriginFileGuid} = qos_entry:get_file_guid(QosEntryId),
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    RelativePath = get_relative_path(OriginFileGuid, FileGuid),
    Link = {?RECONCILE_LINK_NAME(RelativePath, TraverseId), TraverseId},
    {ok, _} = add_synced_links(SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), oneprovider:get_id(), Link),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Removes link indicating that requirements for given qos_entry are not
%% fulfilled.
%% @end
%%--------------------------------------------------------------------
-spec report_file_reconciled(qos_entry:id(), datastore_doc:scope(), file_meta:uuid(), traverse:id()) ->
    ok | {error, term()}.
report_file_reconciled(QosEntryId, SpaceId, FileUuid, TraverseId) ->
    {ok, OriginFileGuid} = qos_entry:get_file_guid(QosEntryId),
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    RelativePath = get_relative_path(OriginFileGuid, FileGuid),
    ok = delete_synced_links(SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), oneprovider:get_id(),
        ?RECONCILE_LINK_NAME(RelativePath, TraverseId)).

%%%===================================================================
%%% Functions concerning traverse status check %fixme
%%%===================================================================

check_traverses(FileCtx, #document{key = QosEntryId} = QosDoc) ->
    {ok, AllTraverseReqs} = qos_entry:get_traverse_reqs(QosDoc),
    AllTraverseIds = qos_traverse_req:get_traverse_ids(AllTraverseReqs),
    {ok, QosOriginUuid} = qos_entry:get_file_uuid(QosDoc),
    
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx),
    NotFinishedTraverseIds = lists:filter(fun(TraverseId) ->
        not check_during_traverse(TraverseId, FileCtx1, QosOriginUuid, IsDir)
    end, AllTraverseIds),
    
    % fetch traverse list again to secure against possible race
    % between status check and traverse finish
    {ok, QosDoc1} = qos_entry:get(QosEntryId),
    {ok, TraverseReqsAfter} = qos_entry:get_traverse_reqs(QosDoc1),
    TraverseIdsAfter = qos_traverse_req:get_traverse_ids(TraverseReqsAfter),
    [] == lists_utils:intersect(TraverseIdsAfter, NotFinishedTraverseIds).

check_during_traverse(TraverseId, FileCtx, QosOriginUuid, true = _IsDir) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    case get(TraverseId, Uuid) of
        {ok, _} -> false;
        ?ERROR_NOT_FOUND -> has_dir_traversed_link(TraverseId, FileCtx)
            orelse check_parents(TraverseId, FileCtx, QosOriginUuid);
        {error, _} = Error -> Error
    end;
check_during_traverse(TraverseId, FileCtx, QosOriginUuid, false = _IsDir) ->
    {FileName, _} = file_ctx:get_aliased_name(FileCtx, undefined),
    {ParentFileCtx, FileCtx1} = file_ctx:get_parent(FileCtx, undefined),
    Uuid = file_ctx:get_uuid_const(FileCtx1),
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    case get(TraverseId, ParentUuid) of
        {ok, #document{value = #qos_status{last_file = LastFile, files_list = FilesList}}} when is_binary(LastFile) ->
            FileName =< LastFile orelse not lists:member(Uuid, FilesList);
        {ok, _} -> false; % doc exists but traverse have not started yet
        ?ERROR_NOT_FOUND -> check_parents(TraverseId, FileCtx1, QosOriginUuid);
        {error, _} = Error -> Error
    end.

% fixme check dla space'a
% fixme better name
% fixme refactor
check_parents(TraverseId, FileCtx, QosOriginUuid) ->
    {ParentFileCtx, _FileCtx1} = file_ctx:get_parent(FileCtx, undefined),
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    has_dir_traversed_link(TraverseId, ParentFileCtx)
        orelse (not has_status_doc(TraverseId, ParentUuid)
        andalso (ParentUuid =/= QosOriginUuid
            andalso check_parents(TraverseId, ParentFileCtx, QosOriginUuid))).

has_dir_traversed_link(TraverseId, FileCtx) ->
    {Path, _} = file_ctx:get_canonical_path(FileCtx),
    case get_next_status_link(?TRAVERSE_LINKS_KEY(TraverseId), Path) of
        {ok, Path} -> true;
        _ -> false
    end.

has_status_doc(TraverseId, Uuid) ->
    case get(TraverseId, Uuid) of
        {ok, _} -> true;
        ?ERROR_NOT_FOUND -> false;
        {error, _} = Error -> Error
    end.

%%%===================================================================
%%% Functions concerning reconcile
%%%===================================================================

% fixme name
check_reconciling(FileCtx, #document{key = QosEntryId} = QosDoc) ->
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ok, OriginGuid} = qos_entry:get_file_guid(QosDoc),
    RelativePath = get_relative_path(OriginGuid, FileGuid),
    case get_next_status_link(?RECONCILE_LINKS_KEY(QosEntryId), RelativePath) of
        {ok, empty} -> true;
        {ok, Path} ->
            %% TODO VFS-5633 use uuid instead of filename
            % check that is not ancestor directory or self
            not str_utils:binary_starts_with(Path, <<RelativePath/binary, "/">>) andalso
                not str_utils:binary_starts_with(Path, <<RelativePath/binary, "###">>)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% TODO VFS-5633 use uuid instead of filename
%% Returns child file path relative to ancestor's, e.g:
%%    AncestorAbsolutePath: space1/dir1/dir2
%%    ChildAbsolutePath: space1/dir1/dir2/dir3/file
%%    Result: dir2/dir3/file
%% @end
%%--------------------------------------------------------------------
-spec get_relative_path([binary()] | file_id:file_guid(), file_id:file_guid()) -> path().
get_relative_path(AncestorPathTokens, ChildGuid) when is_list(AncestorPathTokens) ->
    {FilePathTokens, _FileCtx} = file_ctx:get_canonical_path_tokens(file_ctx:new_by_guid(ChildGuid)),
    filename:join(lists:sublist(FilePathTokens, length(AncestorPathTokens), length(FilePathTokens)));
get_relative_path(AncestorGuid, ChildGuid) ->
    {AncestorPathTokens, _FileCtx} = file_ctx:get_canonical_path_tokens(file_ctx:new_by_guid(AncestorGuid)),
    get_relative_path(AncestorPathTokens, ChildGuid).

%%%===================================================================
% fixme
%%%===================================================================

update_and_handle_finished(TraverseId, Uuid, SpaceId, UpdateFun) ->
    case update(TraverseId, Uuid, UpdateFun) of
        {ok, #document{value = #qos_status{child_dirs = 0, files_list = [], is_last_batch = true, is_start_dir = true}}} ->
            handle_finished(TraverseId, Uuid, SpaceId);
        {ok, #document{value = #qos_status{child_dirs = 0, files_list = [], is_last_batch = true}}} ->
            handle_finished(TraverseId, Uuid, SpaceId),
            {ok, ParentUuid} = file_meta:get_parent_uuid({uuid, Uuid}),
            ok = report_child_dir_traversed(TraverseId, ParentUuid, SpaceId);
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.

report_child_dir_traversed(TraverseId, Uuid, SpaceId) ->
    update_and_handle_finished(TraverseId, Uuid, SpaceId,
        fun(#qos_status{child_dirs = ChildDirs} = Value) ->
            {ok, Value#qos_status{child_dirs = ChildDirs - 1}}
        end
    ).

handle_finished(TraverseId, Uuid, SpaceId) ->
    {Path, _} = file_ctx:get_canonical_path(file_ctx:new_by_guid(file_id:pack_guid(Uuid, SpaceId))),
    ?notice("Adding link: {~p ~p}", [Path, Uuid]),
    add_synced_links(SpaceId, ?TRAVERSE_LINKS_KEY(TraverseId), oneprovider:get_id(), {Path, Uuid}),
    delete_children_links(TraverseId, <<Path/binary, "/">>, SpaceId),
    delete(TraverseId, Uuid).

% fixme larger batch
delete_children_links(TraverseId, Path, SpaceId) ->
    case get_next_status_link(?TRAVERSE_LINKS_KEY(TraverseId), Path) of
        {ok, empty} -> ok;
        {ok, Link} ->
            case str_utils:binary_starts_with(Link, Path) of
                true ->
                    ?notice("Deleting link: ~p", [Link]),
                    delete_synced_links(
                        SpaceId, ?TRAVERSE_LINKS_KEY(TraverseId), oneprovider:get_id(), Link
                    ),
                    delete_children_links(TraverseId, Path, SpaceId);
                false -> ok
            end
    end.

%%%===================================================================
%%% Internal functions using datastore_model API
%%%===================================================================

create(TaskId, DirUuid, SpaceId, IsStartDir) ->
    Id = gen_status_doc_id(TaskId, DirUuid),
    ?notice("New doc created ~p", [Id]),
    datastore_model:create(?CTX, #document{key = Id, scope = SpaceId,
        value = #qos_status{is_start_dir = IsStartDir}
    }).

update(TaskId, Uuid, Diff) ->
    Id = gen_status_doc_id(TaskId, Uuid),
    datastore_model:update(?CTX, Id, Diff).

get(TaskId, Uuid) ->
    Id = gen_status_doc_id(TaskId, Uuid),
    datastore_model:get(?CTX, Id).

delete(TaskId, Uuid)->
    Id = gen_status_doc_id(TaskId, Uuid),
    ?notice("Doc deleted ~p", [Id]),
    datastore_model:delete(?CTX, Id).

-spec add_synced_links(datastore_doc:scope(), datastore:key(), datastore:tree_id(),
    one_or_many({datastore:link_name(), datastore:link_target()})) ->
    one_or_many({ok, datastore:link()} | {error, term()}).
add_synced_links(Scope, Key, TreeId, Links) ->
    datastore_model:add_links(?CTX#{scope => Scope}, Key, TreeId, Links).

-spec delete_synced_links(datastore_doc:scope(), datastore:key(), datastore:tree_id(),
    one_or_many(datastore:link_name() | {datastore:link_name(), datastore:link_rev()})) ->
    one_or_many(ok | {error, term()}).
delete_synced_links(Scope, Key, TreeId, Links) ->
    datastore_model:delete_links(?CTX#{scope => Scope}, Key, TreeId, Links).

-spec fold_links(id(), datastore_model:tree_ids(), datastore:fold_fun(datastore:link()),
    datastore:fold_acc(), datastore:fold_opts()) -> {ok, datastore:fold_acc()} |
{{ok, datastore:fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_links(Key, TreeIds, Fun, Acc, Opts) ->
    datastore_model:fold_links(?CTX, Key, TreeIds, Fun, Acc, Opts).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.

-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {last_file, binary},
        {files_list, [string]},
        {child_dirs, integer},
        {is_last_batch, boolean},
        {is_start_dir, boolean}
    ]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_next_status_link(Key, Path) ->
    fold_links(Key, all,
        fun(#link{name = N}, _Acc) -> {ok, N} end,
        empty,
        #{prev_link_name => Path, size => 1}
    ).

gen_status_doc_id(TaskId, DirUuid) ->
    datastore_key:adjacent_from_digest([DirUuid, TaskId], DirUuid).
