%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for calculating QoS entry fulfillment status.
%%%
%%% QoS entry for given file/directory is fulfilled when:
%%%     - there is no information that qos_entry cannot be satisfied (see qos_entry.erl)
%%%     - each traverse job created as result of this QoS entry has already 
%%%       synchronized this file/all its files in subtree
%%%     - file is not being reconciled/no file is currently being reconciled in 
%%%       subtree of this directory
%%%
%%% In order to be able to check that given file/directory has been traversed additional 
%%% document(qos_status) is created for a directory when it was encountered during traverse. 
%%% This document contains information of traverse state in a directory subtree.
%%% 
%%% QoS traverse lists files in batches and after each batch have been evaluated it is 
%%% reported to this module so qos_status document could be appropriately updated. 
%%% Next batch is started when previous one is finished.
%%% 
%%% qos_status document contains previous and current batch last filenames and list of not 
%%% finished files in current batch. It also has number of children directories for which 
%%% traverse have started. 
%%%
%%% When traverse of subtree of a directory is finished (i.e. all children files have been 
%%% synchronized and all children directories have traverse_links), traverse_link is created 
%%% for this directory and qos_status document is deleted. A traverse_link for a directory indicates 
%%% that traverse of subtree of this directory is finished. When traverse_link is created, links 
%%% for all children are deleted, so when traverse job is finished only one traverse_link is left 
%%% for QoS entry origin directory. This traverse_link is then deleted.
%%% 
%%% To check whether given file have been traversed(synchronized in a traverse): 
%%%     - qos_status document for parent exists: 
%%%         * given file's name is lower than last filename of previous batch -> traversed 
%%%         * given file's name is higher than last filename of current batch -> not traversed 
%%%         * file is on list of files not finished in current batch -> not traversed
%%%     - there is no qos_status document for parent directory: 
%%%         * any ancestor has traverse_link -> traversed
%%%         * otherwise -> not traversed
%%%
%%% To check whether given directory have been traversed: 
%%%     - qos_status document for directory exists -> not traversed
%%%     - there is no qos_status document for directory: 
%%%         * any ancestor has traverse_link -> traversed
%%%         * otherwise -> not traversed 
%%% 
%%% 
%%% In order to be able to check whether file is being reconciled, when file_change was 
%%% reported reconcile_link is created. This link is file uuid_path (similar to canonical bath, 
%%% but instead of filenames, uuids are used). This link is deleted after reconcile job is done. 
%%% 
%%% To check if there is any reconcile job in subtree of a directory simply check if there is any 
%%% reconcile_link with its prefix being this directory uuid path.
%%% 
%%% @end
%%%-------------------------------------------------------------------
-module(qos_status).
-author("Michal Stanisz").

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([check_fulfillment/2]).
-export([report_traverse_started/2, report_traverse_finished/3,
    report_next_traverse_batch/6, report_traverse_finished_for_dir/3,
    report_traverse_finished_for_file/2]).
-export([report_file_changed/3, report_file_reconciled/3]).
-export([report_file_deleted/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).

-type path() :: file_meta:path().
-type id() :: datastore_doc:key().
-type record() :: #qos_status{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

-define(RECONCILE_LINK_NAME(Path, TraverseId),
    <<Path/binary, "###", TraverseId/binary>>).

-define(RECONCILE_LINKS_KEY(QosEntryId), <<"qos_status_reconcile", QosEntryId/binary>>).
-define(TRAVERSE_LINKS_KEY(TraverseId), <<"qos_status_traverse", TraverseId/binary>>).

-define(LIST_LINKS_BATCH_SIZE, 20).

%%%===================================================================
%%% API
%%%===================================================================

-spec check_fulfillment(file_ctx:ctx(), qos_entry:id()) -> boolean().
check_fulfillment(FileCtx, QosEntryId) ->
    {ok, QosDoc} = qos_entry:get(QosEntryId),
    qos_entry:is_possible(QosDoc) andalso
        is_file_reconciled(FileCtx, QosDoc) andalso
        are_traverses_finished_for_file(FileCtx, QosDoc).


-spec report_traverse_started(traverse:id(), file_ctx:ctx()) -> {ok, file_ctx:ctx()}.
report_traverse_started(TraverseId, FileCtx) ->
    {ok, case file_ctx:is_dir(FileCtx) of
        {true, FileCtx1} ->
            {ok, _} = create(TraverseId, file_ctx:get_uuid_const(FileCtx), 
                file_ctx:get_space_id_const(FileCtx), true),
            FileCtx1;
        {false, FileCtx1} -> FileCtx1
    end}.


-spec report_traverse_finished(od_space:id(), traverse:id(), file_meta:uuid()) -> 
    ok | {error, term()}.
report_traverse_finished(SpaceId, TraverseId, Uuid) ->
    {Path, _} = file_ctx:get_uuid_path(file_ctx:new_by_guid(file_id:pack_guid(Uuid, SpaceId))),
    delete_synced_link(SpaceId, ?TRAVERSE_LINKS_KEY(TraverseId), oneprovider:get_id(), Path).


-spec report_next_traverse_batch(od_space:id(), traverse:id(), file_meta:uuid(),
    ChildrenDirs :: [file_meta:uuid()], ChildrenFiles :: [file_meta:uuid()], 
    BatchLastName :: binary()) -> ok.
report_next_traverse_batch(SpaceId, TraverseId, Uuid, ChildrenDirs, ChildrenFiles, BatchLastName) ->
    {ok, _} = update(TraverseId, Uuid,
        fun(#qos_status{
            files_list = FilesList, child_dirs = ChildDirs, current_batch_last_file = LN} = Value) ->
            {ok, Value#qos_status{
                files_list = FilesList ++ ChildrenFiles,
                previous_batch_last_file = LN,
                current_batch_last_file = BatchLastName,
                child_dirs = ChildDirs + length(ChildrenDirs)}
            }
        end),
    lists:foreach(fun(ChildDirUuid) ->
        create(TraverseId, ChildDirUuid, SpaceId, false)
    end, ChildrenDirs).


-spec report_traverse_finished_for_dir(traverse:id(), file_meta:uuid(), od_space:id()) -> 
    ok | {error, term()}.
report_traverse_finished_for_dir(TraverseId, DirUuid, SpaceId) ->
    FileCtx = file_ctx:new_by_guid(file_id:pack_guid(DirUuid, SpaceId)),
    update_status_doc_and_handle_finished(TraverseId, FileCtx,
        fun(#qos_status{} = Value) ->
            {ok, Value#qos_status{is_last_batch = true}}
        end
    ).


-spec report_traverse_finished_for_file(traverse:id(), file_ctx:ctx()) -> 
    ok | {error, term()}.
report_traverse_finished_for_file(TraverseId, FileCtx) ->
    {ParentFileCtx, FileCtx1} = file_ctx:get_parent(FileCtx, undefined),
    FileUuid = file_ctx:get_uuid_const(FileCtx1),
    ?not_found_ok(update_status_doc_and_handle_finished(TraverseId, ParentFileCtx,
        fun(#qos_status{files_list = FilesList} = Value) ->
            {ok, Value#qos_status{files_list = FilesList -- [FileUuid]}}
        end
    )).


-spec report_file_changed([qos_entry:id()], file_ctx:ctx(), traverse:id()) -> 
    ok | {error, term()}.
report_file_changed(QosEntries, FileCtx, TraverseId) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {UuidPath, _} = file_ctx:get_uuid_path(FileCtx),
    Link = {?RECONCILE_LINK_NAME(UuidPath, TraverseId), TraverseId},
    lists:foreach(fun(QosEntryId) ->
        {ok, _} = add_synced_links(
            SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), oneprovider:get_id(), Link)
    end, QosEntries).


-spec report_file_reconciled(datastore_doc:scope(), file_meta:uuid(), traverse:id()) ->
    ok | {error, term()}.
report_file_reconciled(SpaceId, FileUuid, TraverseId) ->
    FileGuid = file_id:pack_guid(FileUuid, SpaceId),
    {UuidPath, _} = file_ctx:get_uuid_path(file_ctx:new_by_guid(FileGuid)),
    QosEntries = case file_qos:get_effective(FileUuid) of
        undefined -> [];
        {error, {file_meta_missing, FileUuid}} -> [];
        {error, _} = Error ->
            ?warning("Error after file ~p have been reconciled: ~p", [FileUuid, Error]),
            [];
        {ok, EffectiveFileQos} ->
            file_qos:get_qos_entries(EffectiveFileQos)
    end,
    lists:foreach(fun(QosEntryId) ->
        ok = delete_synced_link(SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), oneprovider:get_id(),
            ?RECONCILE_LINK_NAME(UuidPath, TraverseId))
    end, QosEntries).


-spec report_file_deleted(file_ctx:ctx(), qos_entry:id()) -> ok.
report_file_deleted(FileCtx, QosEntryId) ->
    {ok, QosDoc} = qos_entry:get(QosEntryId),
    {ok, TraverseReqs} = qos_entry:get_traverse_reqs(QosDoc),
    Uuid = file_ctx:get_uuid_const(FileCtx),
    TraverseIds = qos_traverse_req:get_traverse_ids(TraverseReqs),
    
    lists:foreach(fun(TraverseId) ->
        ok = delete(TraverseId, Uuid),
        ok = report_traverse_finished_for_file(TraverseId, FileCtx)
    end, TraverseIds),
    
    {ok, SpaceId} = qos_entry:get_space_id(QosDoc),
    {UuidPath, _} = file_ctx:get_uuid_path(FileCtx),
    % delete links from all traverses
    delete_all_links_with_prefix(
        SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), ?RECONCILE_LINK_NAME(UuidPath, <<"">>)).
    

%%%===================================================================
%%% Internal functions concerning QoS status check during traverse
%%%===================================================================

%% @private
-spec are_traverses_finished_for_file(file_ctx:ctx(), qos_entry:doc()) -> boolean().
are_traverses_finished_for_file(FileCtx, #document{key = QosEntryId} = QosDoc) ->
    {ok, AllTraverseReqs} = qos_entry:get_traverse_reqs(QosDoc),
    AllTraverseIds = qos_traverse_req:get_traverse_ids(AllTraverseReqs),
    {ok, QosOriginUuid} = qos_entry:get_file_uuid(QosDoc),
    
    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx),
    NotFinishedTraverseIds = lists:filter(fun(TraverseId) ->
        not is_traverse_finished_for_file(TraverseId, FileCtx1, QosOriginUuid, IsDir)
    end, AllTraverseIds),
    
    % fetch traverse list again to secure against possible race
    % between start of status check and traverse finish
    {ok, QosDoc1} = qos_entry:get(QosEntryId),
    {ok, TraverseReqsAfter} = qos_entry:get_traverse_reqs(QosDoc1),
    TraverseIdsAfter = qos_traverse_req:get_traverse_ids(TraverseReqsAfter),
    [] == lists_utils:intersect(TraverseIdsAfter, NotFinishedTraverseIds).


%% @private
-spec is_traverse_finished_for_file(traverse:id(), file_ctx:ctx(), 
    QosOriginGuid :: file_id:file_guid(), IsDir :: boolean()) -> boolean().
is_traverse_finished_for_file(TraverseId, FileCtx, QosOriginUuid, _IsDir = true) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    case get(TraverseId, Uuid) of
        {ok, _} -> false;
        ?ERROR_NOT_FOUND -> has_traverse_link(TraverseId, FileCtx)
            orelse is_parent_fulfilled(TraverseId, FileCtx, Uuid, QosOriginUuid);
        {error, _} = Error -> Error
    end;
is_traverse_finished_for_file(TraverseId, FileCtx, QosOriginUuid, _IsDir = false) ->
    {FileName, _} = file_ctx:get_aliased_name(FileCtx, undefined),
    {ParentFileCtx, FileCtx1} = file_ctx:get_parent(FileCtx, undefined),
    Uuid = file_ctx:get_uuid_const(FileCtx1),
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    case get(TraverseId, ParentUuid) of
        {ok, #document{
            value = #qos_status{
                previous_batch_last_file = PreviousBatchLastFile, 
                current_batch_last_file = LastFile, files_list = FilesList
            }
        }} ->
            FileName =< PreviousBatchLastFile orelse 
                (not (FileName > LastFile) and not lists:member(Uuid, FilesList));
        ?ERROR_NOT_FOUND -> 
            is_parent_fulfilled(TraverseId, FileCtx1, Uuid, QosOriginUuid);
        {error, _} = Error -> 
            Error
    end.


%% @private
-spec is_parent_fulfilled(traverse:id(), file_ctx:ctx(), file_meta:uuid(), file_meta:uuid()) -> 
    boolean().
is_parent_fulfilled(_TraverseId, _FileCtx, Uuid, QosOriginUuid) when Uuid == QosOriginUuid ->
    false;
is_parent_fulfilled(TraverseId, FileCtx, _Uuid, QosOriginUuid) ->
    {ParentFileCtx, _FileCtx1} = file_ctx:get_parent(FileCtx, undefined),
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    has_traverse_link(TraverseId, ParentFileCtx)
        orelse (not has_qos_status_doc(TraverseId, ParentUuid)
            andalso  is_parent_fulfilled(TraverseId, ParentFileCtx, ParentUuid, QosOriginUuid)).


%%%===================================================================
%%% Internal functions regarding check of reconcile jobs
%%%===================================================================

%% @private
-spec is_file_reconciled(file_ctx:ctx(), qos_entry:doc()) -> boolean().
is_file_reconciled(FileCtx, #document{key = QosEntryId}) ->
    {UuidPath, _} = file_ctx:get_uuid_path(FileCtx),
    case get_next_status_links(?RECONCILE_LINKS_KEY(QosEntryId), UuidPath, 1) of
        {ok, []} -> true;
        {ok, [{Path, _}]} -> not str_utils:binary_starts_with(Path, UuidPath)
    end.


%%%===================================================================
%%% Internal higher level functions operating on qos_status doc
%%%===================================================================

%% @private
-spec update_status_doc_and_handle_finished(traverse:id(), file_ctx:ctx(), diff()) ->
    ok | {error, term()}.
update_status_doc_and_handle_finished(TraverseId, FileCtx, UpdateFun) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    case update(TraverseId, Uuid, UpdateFun) of
        {ok, #document{value = #qos_status{
            child_dirs = 0, files_list = [], is_last_batch = true, is_start_dir = true}}
        } -> 
            handle_traverse_finished_for_dir(TraverseId, FileCtx);
        {ok, #document{value = #qos_status{
            child_dirs = 0, files_list = [], is_last_batch = true}}
        } ->
            handle_traverse_finished_for_dir(TraverseId, FileCtx),
            {ParentFileCtx, _} = file_ctx:get_parent(FileCtx, undefined),
            ok = report_child_dir_traversed(TraverseId, ParentFileCtx);
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.


%% @private
-spec report_child_dir_traversed(traverse:id(), file_ctx:ctx()) ->
    ok | {error, term()}.
report_child_dir_traversed(TraverseId, FileCtx) ->
    update_status_doc_and_handle_finished(TraverseId, FileCtx,
        fun(#qos_status{child_dirs = ChildDirs} = Value) ->
            {ok, Value#qos_status{child_dirs = ChildDirs - 1}}
        end
    ).


%% @private
-spec handle_traverse_finished_for_dir(traverse:id(), file_ctx:ctx()) -> ok.
handle_traverse_finished_for_dir(TraverseId, FileCtx) ->
    {Path, FileCtx1} = file_ctx:get_uuid_path(FileCtx),
    Uuid = file_ctx:get_uuid_const(FileCtx1),
    SpaceId = file_ctx:get_space_id_const(FileCtx1),
    {ok, _} = add_synced_links(
        SpaceId, ?TRAVERSE_LINKS_KEY(TraverseId), oneprovider:get_id(), {Path, Uuid}),
    ok = delete_all_links_with_prefix(
        SpaceId, ?TRAVERSE_LINKS_KEY(TraverseId), <<Path/binary, "/">>),
    ok = delete(TraverseId, Uuid).


%%%===================================================================
%%% Internal functions using datastore_model API
%%%===================================================================

%% @private
-spec create(traverse:id(), file_meta:uuid(), od_space:id(), boolean()) -> {ok, doc()}.
create(TraverseId, DirUuid, SpaceId, IsStartDir) ->
    Id = generate_status_doc_id(TraverseId, DirUuid),
    datastore_model:create(?CTX, #document{key = Id, scope = SpaceId,
        value = #qos_status{is_start_dir = IsStartDir}
    }).


%% @private
-spec update(traverse:id(), file_meta:uuid(), diff()) -> {ok, doc()} | {error, term()}.
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
    ?not_found_ok(datastore_model:delete(?CTX, Id)).


%% @private
-spec add_synced_links(datastore_doc:scope(), datastore:key(), datastore:tree_id(),
    {datastore:link_name(), datastore:link_target()}) -> {ok, datastore:link()} | {error, term()}.
add_synced_links(Scope, Key, TreeId, Link) ->
    datastore_model:add_links(?CTX#{scope => Scope}, Key, TreeId, Link).


%% @private
-spec delete_synced_link(datastore_doc:scope(), datastore:key(), datastore:tree_id(),
    datastore:link_name() | {datastore:link_name(), datastore:link_rev()}) ->
    ok | {error, term()}.
delete_synced_link(Scope, Key, TreeId, Link) ->
    ok = datastore_model:delete_links(?CTX#{scope => Scope}, Key, TreeId, Link).


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
        {current_batch_last_file, binary},
        {files_list, [string]},
        {child_dirs, integer},
        {is_last_batch, boolean},
        {is_start_dir, boolean}
    ]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec generate_status_doc_id(traverse:id(), file_meta:uuid()) -> id().
generate_status_doc_id(TraverseId, DirUuid) ->
    datastore_key:adjacent_from_digest([DirUuid, TraverseId], DirUuid).


%% @private
-spec has_traverse_link(traverse:id(), file_ctx:ctx()) -> boolean().
has_traverse_link(TraverseId, FileCtx) ->
    {Path, _} = file_ctx:get_uuid_path(FileCtx),
    case get_next_status_links(?TRAVERSE_LINKS_KEY(TraverseId), Path, 1) of
        {ok, [{Path, _}]} -> true;
        _ -> false
    end.


%% @private
-spec has_qos_status_doc(traverse:id(), file_meta:uuid()) -> boolean().
has_qos_status_doc(TraverseId, Uuid) ->
    case get(TraverseId, Uuid) of
        {ok, _} -> true;
        ?ERROR_NOT_FOUND -> false
    end.


%% @private
-spec delete_all_links_with_prefix(od_space:id(), datastore:key(), path()) -> ok.
delete_all_links_with_prefix(SpaceId, Key, Prefix) ->
    case get_next_status_links(Key, Prefix, ?LIST_LINKS_BATCH_SIZE) of
        {ok, []} -> ok;
        {ok, Links} ->
            case delete_links_with_prefix_in_batch(SpaceId, Key, Prefix, Links) of
                finished -> ok;
                false -> delete_all_links_with_prefix(SpaceId, Key, Prefix)
            end
    end.


%% @private
-spec delete_links_with_prefix_in_batch(od_space:id(), datastore:key(), path(), 
    [{datastore:link_name(), oneprovider:id()}]) -> finished | false.
delete_links_with_prefix_in_batch(SpaceId, Key, Prefix, Links) ->
    lists:foldl(fun 
        (_, finished) -> finished;
        ({LinkName, TreeId}, _) ->
            case str_utils:binary_starts_with(LinkName, Prefix) of
                true ->
                    ok = delete_synced_link(SpaceId, Key, TreeId, LinkName),
                    false;
                false -> 
                    finished
            end
        end, false, Links).


%% @private
-spec get_next_status_links(datastore:key(), path(), non_neg_integer()) -> 
    {ok, [{path(), datastore_links:tree_id()}]}.
get_next_status_links(Key, Path, BatchSize) ->
    fold_links(Key, all,
        fun(#link{name = Name, tree_id = TreeId}, Acc) -> {ok, [{Name, TreeId} | Acc]} end,
        [],
        #{prev_link_name => Path, size => BatchSize}
    ).
