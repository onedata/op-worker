%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc The qos_entry document contains information about single QoS requirement
%%% including QoS expression, number of required replicas, fulfillment status
%%% and UUID of file or directory for which requirement has been added.
%%% Such document is created when user adds QoS requirement for file or directory.
%%% Requirement added for directory is inherited by whole directory structure.
%%% Each QoS requirement is evaluated separately. It means that it is not
%%% possible to define inconsistent requirements. For example if one requirement
%%% says that file should be present on storage in Poland and other requirement
%%% says that file should be present on storage in any country but Poland,
%%% two different replicas will be created. On the other hand the same file
%%% replica can fulfill multiple different QoS requirements. For example if
%%% there is storage of type disk in Poland, then replica on such storage can
%%% fulfill requirements that demands replica on storage in Poland and requirements
%%% that demands replica on storage of type disk. System will create new file
%%% replica only if currently existing replicas don't fulfill QoS requirements.
%%% Multiple qos_entry documents can be created for the same file or directory.
%%% Adding two identical QoS requirements for the same file results in two
%%% different qos_entry documents. Each transfer scheduled to fulfill QoS
%%% is added to links tree.
%%% QoS requirement is considered as fulfilled when:
%%%     - all traverse tasks, triggered by creating this QoS requirement, are finished
%%%     - there are no remaining transfers, that were created to fulfill this QoS requirement
%%%
%%% NOTE!!!
%%% If you introduce any changes in this module, please ensure that
%%% docs in qos.hrl are up to date.
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
    get/1, delete/1, create/6, create/7, update/2,
    add_links/4, delete_links/4, fold_links/4
]).

%% higher-level functions operating on qos_entry document
-export([
    add_impossible_qos/2, list_impossible_qos/0,
    is_possible/1, get_space_id/1
]).

%% functions operating on qos_entry record
-export([get_file_guid/1, get_expression/1, get_replicas_num/1]).

%% functions responsible for traverse requests.
-export([mark_traverse_started/2, mark_traverse_finished/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0, resolve_conflict/3]).


-type id() :: datastore_doc:key().
-type record() :: #qos_entry{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type replicas_num() :: pos_integer().
-type traverse_req() :: #qos_traverse_req{}.
-type one_or_many(Type) :: Type | [Type].

-export_type([id/0, doc/0, record/0, replicas_num/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

-define(QOS_TRAVERSE_LIST(QosId), <<"qos_traverses", QosId/binary>>).


%%%===================================================================
%%% Functions operating on document using datastore_model API
%%%===================================================================

-spec create(od_space:id(), id(), file_meta:uuid(), qos_expression:expression(),
    replicas_num(), boolean()) -> {ok, doc()} | {error, term()}.
create(SpaceId, QosId, FileUuid, Expression, ReplicasNum, Possible) ->
    create(SpaceId, QosId, FileUuid, Expression, ReplicasNum, Possible, []).


-spec create(od_space:id(), id(), file_meta:uuid(), qos_expression:expression(),
    replicas_num(), boolean(), [traverse_req()]) -> {ok, doc()} | {error, term()}.
create(SpaceId, QosId, FileUuid, Expression, ReplicasNum, Possible, TraverseReqs) ->
    datastore_model:create(?CTX, #document{key = QosId, scope = SpaceId,
        value = #qos_entry{
            file_uuid = FileUuid,
            expression = Expression,
            replicas_num = ReplicasNum,
            is_possible = Possible,
            traverse_reqs = TraverseReqs
        }
    }).


-spec update(id(), diff()) -> {ok, id()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(QosId) ->
    datastore_model:get(?CTX, QosId).


-spec delete(id()) -> ok | {error, term()}.
delete(QosId) ->
    datastore_model:delete(?CTX, QosId).


-spec add_links(datastore_doc:scope(), datastore:id(), datastore:tree_id(),
    one_or_many({datastore:link_name(), datastore:link_target()})) ->
    one_or_many({ok, datastore:link()} | {error, term()}).
add_links(Scope, Key, TreeId, Links) ->
    datastore_model:add_links(?CTX#{scope => Scope}, Key, TreeId, Links).


-spec delete_links(datastore_doc:scope(), datastore:id(), datastore:tree_id(),
    one_or_many(datastore:link_name() | {datastore:link_name(), datastore:link_rev()})) ->
    one_or_many(ok | {error, term()}).
delete_links(Scope, Key, TreeId, Links) ->
    datastore_model:delete_links(?CTX#{scope => Scope}, Key, TreeId, Links).


%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Link, Acc) for each link.
%% @end
%%--------------------------------------------------------------------
-spec fold_links(id(), datastore:fold_fun(datastore:link()), datastore:fold_acc(), datastore:fold_opts()) ->
    {ok, datastore:fold_acc()} | {{ok, datastore:fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_links(Key, Fun, Acc, Opts) ->
    datastore_model:fold_links(?CTX, Key, all, Fun, Acc, Opts).


%%%===================================================================
%%% Higher-level functions operating on qos_entry document.
%%%===================================================================

-spec get_file_guid(id()) -> {ok, file_id:file_guid()} | {error, term()}.
get_file_guid(QosId) ->
    case qos_entry:get(QosId) of
        {ok, #document{value = QosEntry, scope = SpaceId}} ->
            {ok, file_id:pack_guid(QosEntry#qos_entry.file_uuid, SpaceId)};
        {error, _} = Error ->
            Error
    end.


-spec get_space_id(id()) -> {ok, od_space:id()} | {error, term()}.
get_space_id(QosId) ->
    case qos_entry:get(QosId) of
        {ok, #document{scope = SpaceId}} ->
            {ok, SpaceId};
        {error, _} = Error ->
            Error
    end.


-spec add_impossible_qos(id(), datastore_doc:scope()) ->  ok.
add_impossible_qos(QosId, Scope) ->
    case add_links(Scope, ?IMPOSSIBLE_QOS_KEY, oneprovider:get_id(), {QosId, QosId}) of
        {ok, _} ->
            ok;
        {error, Error} = Error ->
            Error
    end.


-spec list_impossible_qos() ->  {ok, [id()]} | {error, term()}.
list_impossible_qos() ->
    fold_links(?IMPOSSIBLE_QOS_KEY,
        fun(#link{target = T}, Acc) -> {ok, [T | Acc]} end,
        [], #{}
    ).


% fixme name
-spec mark_traverse_started(id(), traverse:id()) ->  {ok, id()}.
mark_traverse_started(QosId, TaskId) ->
    update(QosId, fun(#qos_entry{traverse_reqs = TR, traverses = Traverses} = QosEntry) ->
        {TraverseReq, NewTR} = maps:take(TaskId, TR),
        {ok, QosEntry#qos_entry{
            traverse_reqs = NewTR,
            traverses = Traverses#{TaskId => TraverseReq}
        }}
    end).

% fixme spec
mark_traverse_finished(QosId, TaskId) ->
    update(QosId, fun(#qos_entry{traverses = Traverses} = QosEntry) ->
        {ok, QosEntry#qos_entry{
            traverses = maps:remove(TaskId, Traverses)
        }}
    end).


%%%===================================================================
%%% Functions operating on qos_entry record.
%%%===================================================================

-spec get_expression(record()) -> qos_expression:expression().
get_expression(QosEntry) ->
    QosEntry#qos_entry.expression.


-spec get_replicas_num(record()) -> replicas_num().
get_replicas_num(QosEntry) ->
    QosEntry#qos_entry.replicas_num.


-spec is_possible(record()) -> boolean().
is_possible(QosEntry) ->
    QosEntry#qos_entry.is_possible.


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
            {target_storage, string}
        ]}}},
        {traverses, #{binary => {record, [
            {start_file_uuid, string},
            {target_storage, string}
        ]}}}
    ]}.

% fixme doc and spec
% fixme modified qos_traverse_req
resolve_conflict(_Ctx, #document{value = Value}, PrevDoc) ->
    Reqs1 = PrevDoc#document.value#qos_entry.traverse_reqs,
    Reqs2 = Value#qos_entry.traverse_reqs,
    Traverses1 = PrevDoc#document.value#qos_entry.traverses,
    Traverses2 = Value#qos_entry.traverses,
    case (Reqs1 == Reqs2) and (Traverses1 == Traverses2) of
        true ->
            default;
        false ->
            NewTraverseReqsKeys = [X || X <- maps:keys(Reqs1), Y <- maps:keys(Reqs2), X==Y],
            NewTraverseListKeys = [X || X <- maps:keys(Traverses1), Y <- maps:keys(Traverses2), X==Y],
            {true, PrevDoc#document{
                value = Value#qos_entry{
                    traverse_reqs = maps:with(NewTraverseReqsKeys, Reqs1),
                    traverses = maps:with(NewTraverseListKeys, Traverses1)
            }
        }}
    end.
