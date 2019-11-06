%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc The qos_entry document is synchronized within space. It contains
%%% information about single QoS requirement including QoS expression, number
%%% of required replicas, UUID of file or directory for which requirement has
%%% been added, information whether QoS requirement can be satisfied and
%%% information about traverse requests.
%%% Such document is created when user adds QoS requirement for file or directory.
%%% qos_entry added for directory is inherited by whole directory structure.
%%% Each qos_entry is evaluated separately. It means that it is not
%%% possible to define inconsistent requirements. For example if one qos_entry
%%% says that file should be present on storage in Poland and other qos_entry
%%% says that file should be present on storage in any country but Poland,
%%% two different replicas will be created. On the other hand the same file
%%% replica can fulfill multiple different qos_entries. For example if
%%% there is storage of type disk in Poland, then replica on such storage can
%%% fulfill QoS entry that demands replica on storage in Poland and qos_entry
%%% that demands replica on storage of type disk.
%%% Multiple qos_entry documents can be created for the same file or directory.
%%% Adding two identical QoS requirements for the same file results in two
%%% different qos_entry documents.
%%% qos_entry is considered as fulfilled when:
%%%     - there is no information that qos_entry cannot be
%%%       satisfied (this information is stored in is_possible field
%%%       in qos_entry document. It is set to true if during evaluation of
%%%       QoS expression it was not possible to calculate list of storages
%%%       that would fulfill QoS requirements)
%%%     - there are no traverse requests in qos_entry document. Traverse requests
%%%       are added to qos_entry document on its creation and removed from document
%%%       when traverse task for this request is completed
%%%     - there are no links indicating that file has been changed and it should
%%%       be reconciled (see qos_status.erl)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(qos_entry).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% functions operating on document using datastore model API
-export([
    get/1, delete/1, create/5, create/7,
    add_links/4, delete_links/4, fold_links/4
]).

%% higher-level functions operating on qos_entry document
-export([
    add_impossible_qos/2, list_impossible_qos/0,
    is_possible/1, get_space_id/1
]).

%% functions operating on qos_entry record
-export([
    get_file_guid/1, get_expression/1, get_replicas_num/1,
    get_file_uuid/1, get_traverse_map/1, are_all_traverses_finished/1
]).

%% functions responsible for traverses under given qos_entry.
-export([remove_traverse_req/2]).

%% datastore_model callbacks
-export([
    get_ctx/0, get_record_struct/1, get_record_version/0,
    resolve_conflict/3
]).


-type id() :: datastore_doc:key().
-type record() :: #qos_entry{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type replicas_num() :: pos_integer().
-type traverse_map() :: #{traverse:id() => #qos_traverse_req{}}.
-type one_or_many(Type) :: Type | [Type].

-export_type([id/0, doc/0, record/0, replicas_num/0, traverse_map/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).


%%%===================================================================
%%% Functions operating on document using datastore_model API
%%%===================================================================

-spec create(od_space:id(), id(), file_meta:uuid(), qos_expression:rpn(),
    replicas_num()) -> {ok, doc()} | {error, term()}.
create(SpaceId, QosEntryId, FileUuid, Expression, ReplicasNum) ->
    create(SpaceId, QosEntryId, FileUuid, Expression, ReplicasNum, false, #{}).


-spec create(od_space:id(), id(), file_meta:uuid(), qos_expression:rpn(),
    replicas_num(), boolean(), traverse_map()) -> {ok, doc()} | {error, term()}.
create(SpaceId, QosEntryId, FileUuid, Expression, ReplicasNum, Possible, TraverseReqs) ->
    datastore_model:create(?CTX, #document{key = QosEntryId, scope = SpaceId,
        value = #qos_entry{
            file_uuid = FileUuid,
            expression = Expression,
            replicas_num = ReplicasNum,
            is_possible = Possible,
            traverse_reqs = TraverseReqs
        }
    }).


-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(Key, Diff) ->
    datastore_model:update(?CTX, Key, Diff).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(QosEntryId) ->
    datastore_model:get(?CTX, QosEntryId).


-spec delete(id()) -> ok | {error, term()}.
delete(QosEntryId) ->
    datastore_model:delete(?CTX, QosEntryId).


-spec add_links(datastore_doc:scope(), datastore:key(), datastore:tree_id(),
    one_or_many({datastore:link_name(), datastore:link_target()})) ->
    one_or_many({ok, datastore:link()} | {error, term()}).
add_links(Scope, Key, TreeId, Links) ->
    datastore_model:add_links(?CTX#{scope => Scope}, Key, TreeId, Links).


-spec delete_links(datastore_doc:scope(), datastore:key(), datastore:tree_id(),
    one_or_many(datastore:link_name() | {datastore:link_name(), datastore:link_rev()})) ->
    one_or_many(ok | {error, term()}).
delete_links(Scope, Key, TreeId, Links) ->
    datastore_model:delete_links(?CTX#{scope => Scope}, Key, TreeId, Links).


-spec fold_links(id(), datastore:fold_fun(datastore:link()), datastore:fold_acc(), datastore:fold_opts()) ->
    {ok, datastore:fold_acc()} | {{ok, datastore:fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_links(Key, Fun, Acc, Opts) ->
    datastore_model:fold_links(?CTX, Key, all, Fun, Acc, Opts).


%%%===================================================================
%%% Higher-level functions operating on qos_entry document.
%%%===================================================================

-spec get_file_guid(id() | doc()) -> {ok, file_id:file_guid()} | {error, term()}.
get_file_guid(#document{scope = SpaceId, value = #qos_entry{file_uuid = FileUuid}})  ->
    {ok, file_id:pack_guid(FileUuid, SpaceId)};

get_file_guid(QosEntryId) ->
    case qos_entry:get(QosEntryId) of
        {ok, Doc} ->
            get_file_guid(Doc);
        {error, _} = Error ->
            Error
    end.


-spec get_space_id(id()) -> {ok, od_space:id()} | {error, term()}.
get_space_id(QosEntryId) ->
    case qos_entry:get(QosEntryId) of
        {ok, #document{scope = SpaceId}} ->
            {ok, SpaceId};
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Adds QoS that cannot be fulfilled to links tree storing ID of all
%% qos_entry documents that cannot be fulfilled at the moment.
%% @end
%%--------------------------------------------------------------------
-spec add_impossible_qos(id(), datastore_doc:scope()) ->  ok.
add_impossible_qos(QosEntryId, Scope) ->
    ?extract_ok(
        add_links(Scope, ?IMPOSSIBLE_QOS_KEY, oneprovider:get_id(), {QosEntryId, QosEntryId})
    ).


-spec list_impossible_qos() ->  {ok, [id()]} | {error, term()}.
list_impossible_qos() ->
    fold_links(?IMPOSSIBLE_QOS_KEY,
        fun(#link{target = T}, Acc) -> {ok, [T | Acc]} end,
        [], #{}
    ).


%%--------------------------------------------------------------------
%% @doc
%% Removes given traverse from traverse reqs map.
%% @end
%%--------------------------------------------------------------------
-spec remove_traverse_req(id(), traverse:id()) -> ok.
remove_traverse_req(QosEntryId, TraverseId) ->
    Diff = fun(#qos_entry{traverse_reqs = TR} = QosEntry) ->
        {ok, QosEntry#qos_entry{
            traverse_reqs = maps:remove(TraverseId, TR)
        }}
    end,

    ?extract_ok(update(QosEntryId, Diff)).


%%%===================================================================
%%% Functions operating on qos_entry record.
%%%===================================================================

-spec get_expression(record()) -> qos_expression:rpn().
get_expression(QosEntry) ->
    QosEntry#qos_entry.expression.


-spec get_replicas_num(record()) -> replicas_num().
get_replicas_num(QosEntry) ->
    QosEntry#qos_entry.replicas_num.


-spec get_file_uuid(record()) -> file_meta:uuid().
get_file_uuid(QosEntry) ->
    QosEntry#qos_entry.file_uuid.


-spec get_traverse_map(record()) -> traverse_map().
get_traverse_map(QosEntry) ->
    QosEntry#qos_entry.traverse_reqs.


-spec is_possible(record() | doc()) -> boolean().
is_possible(#document{value = QosEntry}) ->
    is_possible(QosEntry);

is_possible(QosEntry) ->
    QosEntry#qos_entry.is_possible.


-spec are_all_traverses_finished(record() | doc()) -> boolean().
are_all_traverses_finished(#document{value = QosEntry}) ->
    are_all_traverses_finished(QosEntry);

are_all_traverses_finished(QosEntry) ->
    QosEntry#qos_entry.traverse_reqs == #{}.


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
        {file_uuid, string},
        {expression, [string]},
        {replicas_num, integer},
        {is_possible, boolean},
        {traverse_reqs, #{binary => {record, [
            {start_file_uuid, string},
            {storage_id, string}
        ]}}}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% This conflict resolution promotes changes done by provider
%% responsible for given traverse (assigned_entry includes one of its storages).
%% This means that only remote changes of remote providers traverses and
%% local changes of current provider traverses are taken into account.
%% (The only changes done to this document are removals of traverse_reqs or
%% removal of document).
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(datastore_model:ctx(), doc(), doc()) ->
    {boolean(), doc()} | ignore | default.
resolve_conflict(_Ctx, #document{value = RemoteValue} = RemoteDoc, PrevDoc) ->
    LocalReqs = PrevDoc#document.value#qos_entry.traverse_reqs,
    RemoteReqs = RemoteValue#qos_entry.traverse_reqs,
    case (LocalReqs == RemoteReqs) of
        true ->
            default;
        false ->
            {LocalTraverses, _} = split_traverses(LocalReqs),
            {_, RemoteTraverses} = split_traverses(RemoteReqs),

            % for now always take the remote document. This has to be changed
            % when it will be necessary to use revision history.
            {true, RemoteDoc#document{
                value = RemoteValue#qos_entry{
                    traverse_reqs = maps:with(LocalTraverses ++ RemoteTraverses, LocalReqs)
                }
            }}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Splits given traverses to those of current provider and those of remote providers.
%% @end
%%--------------------------------------------------------------------
-spec split_traverses(traverse_map()) -> {[traverse:id()], [traverse:id()]}.
split_traverses(Traverses) ->
    ProviderId = oneprovider:get_id(),
    maps:fold(fun(TaskId, QosTraverse, {LocalTraverses, RemoteTraverses}) ->
        % TODO VFS-5573 use storage id instead of provider
        case QosTraverse#qos_traverse_req.storage_id == ProviderId of
            true -> {[TaskId | LocalTraverses], RemoteTraverses};
            false -> {LocalTraverses, [TaskId | RemoteTraverses]}
        end
    end, {[], []}, Traverses).