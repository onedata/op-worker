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

% QoS status for given file is 


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
-export([report_traverse_started/2, report_traverse_finished/3,
    report_next_traverse_batch/6, report_traverse_finished_for_dir/3,
    report_traverse_finished_for_file/2]).
-export([report_file_changed/3, report_file_reconciled/4]).
-export([report_file_deleted/2]).

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

-spec check_fulfillment(file_ctx:ctx(), qos_entry:id()) -> boolean().
check_fulfillment(FileCtx, QosEntryId) ->
    {ok, QosDoc} = qos_entry:get(QosEntryId),
    qos_entry:is_possible(QosDoc) andalso
        check_reconciling(FileCtx, QosDoc) andalso
        check_traverses(FileCtx, QosDoc).


-spec report_traverse_started(traverse:id(), file_ctx:ctx()) -> ok.
report_traverse_started(TraverseId, FileCtx) ->
    {ok, _} = create(TraverseId, file_ctx:get_uuid_const(FileCtx), file_ctx:get_space_id_const(FileCtx), true),
    ok.


-spec report_traverse_finished(od_space:id(), traverse:id(), file_meta:uuid()) -> ok | {error, term()}.
report_traverse_finished(SpaceId, TraverseId, Uuid) ->
    {Path, _} = file_ctx:get_uuid_path(file_ctx:new_by_guid(file_id:pack_guid(Uuid, SpaceId))),
    delete_synced_links(SpaceId, ?TRAVERSE_LINKS_KEY(TraverseId), oneprovider:get_id(), Path).


-spec report_next_traverse_batch(od_space:id(), traverse:id(), file_meta:uuid(),
    ChildrenDirs :: [file_meta:uuid()], ChildrenFiles :: [file_meta:uuid()], BatchLastName :: binary()) -> ok.
report_next_traverse_batch(SpaceId, TraverseId, Uuid, ChildrenDirs, ChildrenFiles, BatchLastName) ->
    {ok, _} = update(TraverseId, Uuid,
        fun(#qos_status{files_list = FilesList, child_dirs = ChildDirs, this_batch_last_file = LN} = Value) ->
            {ok, Value#qos_status{
                files_list = FilesList ++ ChildrenFiles,
                previous_batch_last_file = LN,
                this_batch_last_file = BatchLastName,
                child_dirs = ChildDirs + length(ChildrenDirs)}
            }
        end),
    lists:foreach(fun(ChildDirUuid) ->
        create(TraverseId, ChildDirUuid, SpaceId, false)
    end, ChildrenDirs).


-spec report_traverse_finished_for_dir(traverse:id(), file_meta:uuid(), od_space:id()) -> ok | {error, term()}.
report_traverse_finished_for_dir(TraverseId, DirUuid, SpaceId) ->
    FileCtx = file_ctx:new_by_guid(file_id:pack_guid(DirUuid, SpaceId)),
    update_and_handle_finished(TraverseId, FileCtx,
        fun(#qos_status{} = Value) ->
            {ok, Value#qos_status{is_last_batch = true}}
        end
    ).


-spec report_traverse_finished_for_file(traverse:id(), file_ctx:ctx()) -> ok | {error, term()}.
report_traverse_finished_for_file(TraverseId, FileCtx) ->
    {ParentFileCtx, FileCtx1} = file_ctx:get_parent(FileCtx, undefined),
    FileUuid = file_ctx:get_uuid_const(FileCtx1),
    update_and_handle_finished(TraverseId, ParentFileCtx,
        fun(#qos_status{files_list = FilesList} = Value) ->
            {ok, Value#qos_status{files_list = FilesList -- [FileUuid]}}
        end
    ).


-spec report_file_changed(qos_entry:id(), file_ctx:ctx(), traverse:id()) -> ok | {error, term()}.
report_file_changed(QosEntryId, FileCtx, TraverseId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {UuidPath, _} = file_ctx:get_uuid_path(FileCtx),
    Link = {?RECONCILE_LINK_NAME(UuidPath, TraverseId), TraverseId},
    {ok, _} = add_synced_links(SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), oneprovider:get_id(), Link),
    ok.


-spec report_file_reconciled(qos_entry:id(), datastore_doc:scope(), file_meta:uuid(), traverse:id()) ->
    ok | {error, term()}.
report_file_reconciled(QosEntryId, SpaceId, FileUuid, TraverseId) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    {UuidPath, _} = file_ctx:get_uuid_path(file_ctx:new_by_guid(FileGuid)),
    ok = delete_synced_links(SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), oneprovider:get_id(),
        ?RECONCILE_LINK_NAME(UuidPath, TraverseId)).


-spec report_file_deleted(file_ctx:ctx(), qos_entry:id()) -> ok.
report_file_deleted(FileCtx, QosEntryId) ->
    {ok, QosDoc} = qos_entry:get(QosEntryId),
    {ok, TraverseReqs} = qos_entry:get_traverse_reqs(QosDoc),
    Uuid = file_ctx:get_uuid_const(FileCtx),
    TraverseIds = qos_traverse_req:get_traverse_ids(TraverseReqs),
    
    lists:foreach(fun(TraverseId) ->
        ok = delete(TraverseId, Uuid),
        report_traverse_finished_for_file(TraverseId, FileCtx)
    end, TraverseIds),
    
    {ok, SpaceId} = qos_entry:get_space_id(QosDoc),
    {UuidPath, _} = file_ctx:get_uuid_path(FileCtx),
    % delete links from all traverses
    delete_children_links(?RECONCILE_LINKS_KEY(QosEntryId), ?RECONCILE_LINK_NAME(UuidPath, <<"">>), SpaceId).
    

%%%===================================================================
%%% Functions concerning traverse status check %fixme
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% For each traverse in given QoS entry %fixme
%% @end
%%--------------------------------------------------------------------
-spec check_traverses(file_ctx:ctx(), qos_entry:doc()) -> boolean().
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


%% @private
-spec check_during_traverse(traverse:id(), file_ctx:ctx(), QosOriginGuid :: file_id:file_guid(),
    IsDir :: boolean()) -> boolean().
check_during_traverse(TraverseId, FileCtx, QosOriginUuid, _IsDir = true) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    case get(TraverseId, Uuid) of
        {ok, _} -> false;
        ?ERROR_NOT_FOUND -> has_dir_traversed_link(TraverseId, FileCtx)
            orelse check_parents(TraverseId, FileCtx, QosOriginUuid);
        {error, _} = Error -> Error
    end;
check_during_traverse(TraverseId, FileCtx, QosOriginUuid, _IsDir = false) ->
    {FileName, _} = file_ctx:get_aliased_name(FileCtx, undefined),
    {ParentFileCtx, FileCtx1} = file_ctx:get_parent(FileCtx, undefined),
    Uuid = file_ctx:get_uuid_const(FileCtx1),
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    case get(TraverseId, ParentUuid) of
        {ok, #document{
            value = #qos_status{
                previous_batch_last_file = PreviousBatchLastFile, 
                this_batch_last_file = LastFile,
                files_list = FilesList
            }
        }} when is_binary(PreviousBatchLastFile) ->
            FileName =< PreviousBatchLastFile orelse 
                (not FileName > LastFile and not lists:member(Uuid, FilesList));
        {ok, _} -> 
            false; 
        ?ERROR_NOT_FOUND -> 
            check_parents(TraverseId, FileCtx1, QosOriginUuid);
        {error, _} = Error -> 
            Error
    end.


% fixme check dla space'a (file_ctx:is_space_dir_const)
% fixme better name
% fixme refactor
%% @private
-spec check_parents(traverse:id(), file_ctx:ctx(), file_id:file_guid()) -> boolean().
check_parents(TraverseId, FileCtx, QosOriginUuid) ->
    {ParentFileCtx, _FileCtx1} = file_ctx:get_parent(FileCtx, undefined),
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    has_dir_traversed_link(TraverseId, ParentFileCtx)
        orelse (not has_status_doc(TraverseId, ParentUuid)
        andalso (ParentUuid =/= QosOriginUuid
            andalso check_parents(TraverseId, ParentFileCtx, QosOriginUuid))).


%% @private
-spec has_dir_traversed_link(traverse:id(), file_ctx:ctx()) -> boolean().
has_dir_traversed_link(TraverseId, FileCtx) ->
    {Path, _} = file_ctx:get_uuid_path(FileCtx),
    case get_next_status_link(?TRAVERSE_LINKS_KEY(TraverseId), Path) of
        {ok, Path} -> true;
        _ -> false
    end.


%% @private
-spec has_status_doc(traverse:id(), file_meta:uuid()) -> boolean().
has_status_doc(TraverseId, Uuid) ->
    case get(TraverseId, Uuid) of
        {ok, _} -> true;
        ?ERROR_NOT_FOUND -> false
    end.

%%%===================================================================
%%% Functions concerning reconcile %fixme
%%%===================================================================

% fixme name
%% @private
-spec check_reconciling(file_ctx:ctx(), qos_entry:doc()) -> boolean().
check_reconciling(FileCtx, #document{key = QosEntryId}) ->
    {UuidPath, _} = file_ctx:get_uuid_path(FileCtx),
    case get_next_status_link(?RECONCILE_LINKS_KEY(QosEntryId), UuidPath) of
        {ok, empty} -> true;
        {ok, Path} -> not str_utils:binary_starts_with(Path, UuidPath)
    end.


%%%===================================================================
% fixme
%%%===================================================================

%% @private
-spec update_and_handle_finished(traverse:id(), file_ctx:ctx(), diff()) ->
    ok | {error, term()}.
update_and_handle_finished(TraverseId, FileCtx, UpdateFun) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    case update(TraverseId, Uuid, UpdateFun) of
        {ok, #document{value = #qos_status{child_dirs = 0, files_list = [], is_last_batch = true, is_start_dir = true}}} ->
            handle_finished(TraverseId, FileCtx);
        {ok, #document{value = #qos_status{child_dirs = 0, files_list = [], is_last_batch = true}}} ->
            handle_finished(TraverseId, FileCtx),
            {ParentFileCtx, _} = file_ctx:get_parent(FileCtx, undefined),
            ok = report_child_dir_traversed(TraverseId, ParentFileCtx);
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.


%% @private
-spec report_child_dir_traversed(traverse:id(), file_ctx:ctx()) ->
    ok | {error, term()}.
report_child_dir_traversed(TraverseId, FileCtx) ->
    update_and_handle_finished(TraverseId, FileCtx,
        fun(#qos_status{child_dirs = ChildDirs} = Value) ->
            {ok, Value#qos_status{child_dirs = ChildDirs - 1}}
        end
    ).


%% @private
-spec handle_finished(traverse:id(), file_ctx:ctx()) -> ok.
handle_finished(TraverseId, FileCtx) ->
    {Path, FileCtx1} = file_ctx:get_uuid_path(FileCtx),
    Uuid = file_ctx:get_uuid_const(FileCtx1),
    SpaceId = file_ctx:get_space_id_const(FileCtx1),
    % fixme
    ?notice("Adding link: {~p ~p}", [Path, Uuid]),
    {ok, _} = add_synced_links(SpaceId, ?TRAVERSE_LINKS_KEY(TraverseId), oneprovider:get_id(), {Path, Uuid}),
    ok = delete_children_links(?TRAVERSE_LINKS_KEY(TraverseId), <<Path/binary, "/">>, SpaceId),
    ok = delete(TraverseId, Uuid).


% fixme larger batch
%% @private
-spec delete_children_links(datastore:key(), path(), od_space:id()) -> ok.
delete_children_links(Key, Path, SpaceId) ->
    case get_next_status_link(Key, Path) of
        {ok, empty} -> ok;
        {ok, Link} ->
            case str_utils:binary_starts_with(Link, Path) of
                true ->
                    % fixme
                    ?notice("Deleting link: ~p", [Link]),
                    delete_synced_links(
                        SpaceId, Key, oneprovider:get_id(), Link
                    ),
                    delete_children_links(Key, Path, SpaceId);
                false -> ok
            end
    end.

%%%===================================================================
%%% Internal functions using datastore_model API
%%%===================================================================

%% @private
-spec create(traverse:id(), file_meta:uuid(), od_space:id(), boolean()) -> {ok, id()}.
create(TraverseId, DirUuid, SpaceId, IsStartDir) ->
    Id = generate_status_doc_id(TraverseId, DirUuid),
    % fixme
    ?notice("New doc created ~p", [Id]),
    datastore_model:create(?CTX, #document{key = Id, scope = SpaceId,
        value = #qos_status{is_start_dir = IsStartDir}
    }).


%% @private
-spec update(traverse:id(), file_meta:uuid(), diff()) -> ok | {error, term()}.
update(TraverseId, Uuid, Diff) ->
    Id = generate_status_doc_id(TraverseId, Uuid),
    datastore_model:update(?CTX, Id, Diff).


%% @private
-spec get(traverse:id(), file_meta:uuid()) -> {ok, doc()} | {error, term()}.
get(TraverseId, Uuid) ->
    Id = generate_status_doc_id(TraverseId, Uuid),
    datastore_model:get(?CTX, Id).


%% @private
-spec delete(traverse:id(), file_meta:uuid()) -> ok | {error, term()}.
delete(TraverseId, Uuid)->
    Id = generate_status_doc_id(TraverseId, Uuid),
    % fixme
    ?notice("Doc deleted ~p", [Id]),
    case datastore_model:delete(?CTX, Id) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, _} = Error -> Error
    end.


%% @private
-spec add_synced_links(datastore_doc:scope(), datastore:key(), datastore:tree_id(),
    one_or_many({datastore:link_name(), datastore:link_target()})) ->
    one_or_many({ok, datastore:link()} | {error, term()}).
add_synced_links(Scope, Key, TreeId, Links) ->
    datastore_model:add_links(?CTX#{scope => Scope}, Key, TreeId, Links).


%% @private
-spec delete_synced_links(datastore_doc:scope(), datastore:key(), datastore:tree_id(),
    one_or_many(datastore:link_name() | {datastore:link_name(), datastore:link_rev()})) ->
    one_or_many(ok | {error, term()}).
delete_synced_links(Scope, Key, TreeId, Links) ->
    datastore_model:delete_links(?CTX#{scope => Scope}, Key, TreeId, Links).


%% @private
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
        {previous_batch_last_file, binary},
        {this_batch_last_file, binary},
        {files_list, [string]},
        {child_dirs, integer},
        {is_last_batch, boolean},
        {is_start_dir, boolean}
    ]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_next_status_link(datastore:key(), path()) -> path().
get_next_status_link(Key, Path) ->
    fold_links(Key, all,
        fun(#link{name = N}, _Acc) -> {ok, N} end,
        empty,
        #{prev_link_name => Path, size => 1}
    ).

%% @private
-spec generate_status_doc_id(traverse:id(), file_meta:uuid()) -> id().
generate_status_doc_id(TraverseId, DirUuid) ->
    datastore_key:adjacent_from_digest([DirUuid, TraverseId], DirUuid).
