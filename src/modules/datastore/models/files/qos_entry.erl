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

%% functions operating on record using datastore model API
-export([get/1, delete/1, create/4, update/2, add_links/4, delete_links/4, fold_links/4]).

%% higher-level functions operating on qos_entry record.
-export([add_impossible_qos/2, list_impossible_qos/0, get_file_guid/1,
    set_status/2, get_expression/1, get_replicas_num/1, get_status/1, get_space_id/1]).

%% Functions responsible for traverse requests.
-export([add_traverse_reqs/2, remove_traverse_req/2]).

%% Functions operating on traverses list.
-export([add_traverse/3, remove_traverse/3, list_traverses/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0]).


-type id() :: binary().
-type key() :: datastore:key().
-type record() :: #qos_entry{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type status() :: ?QOS_NOT_FULFILLED | ?QOS_TRAVERSES_STARTED_STATUS | ?QOS_IMPOSSIBLE_STATUS.
-type replicas_num() :: pos_integer().
-type traverse_req() :: #qos_traverse_req{}.
-type one_or_many(Type) :: Type | [Type].

-export_type([id/0, record/0, status/0, replicas_num/0]).

-define(CTX, #{
    model => ?MODULE,
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).


%%%===================================================================
%%% Functions operating on record using datastore_model API
%%%===================================================================

-spec create(qos_expression:expression(), replicas_num(), file_meta:uuid(), od_space:id()) ->
    {ok, doc()} | {error, term()}.
create(QosExpression, ReplicasNum, FileUuid, SpaceId) ->
    QosEntry = #qos_entry{
        expression = QosExpression,
        replicas_num = ReplicasNum,
        file_uuid = FileUuid,
        status = ?QOS_NOT_FULFILLED
    },
    datastore_model:create(?CTX, #document{scope = SpaceId, value = QosEntry}).


-spec update(key(), diff()) -> {ok, key()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).


-spec get(key()) -> {ok, doc()} | {error, term()}.
get(QosId) ->
    datastore_model:get(?CTX, QosId).


-spec delete(key()) -> ok | {error, term()}.
delete(QosId) ->
    datastore_model:delete(?CTX, QosId).


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

%%--------------------------------------------------------------------
%% @doc
%% Calls Fun(Link, Acc) for each link.
%% @end
%%--------------------------------------------------------------------
-spec fold_links(key(), datastore:fold_fun(datastore:link()), datastore:fold_acc(), datastore:fold_opts()) ->
    {ok, datastore:fold_acc()} | {{ok, datastore:fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_links(Key, Fun, Acc, Opts) ->
    datastore_model:fold_links(?CTX, Key, all, Fun, Acc, Opts).


%%%===================================================================
%%% Higher-level functions operating on qos_entry record.
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



-spec set_status(id(), status()) -> {ok, key()} | {error, term}.
set_status(QosId, Status) ->
    Diff = fun(QosEntry) ->
        {ok, QosEntry#qos_entry{status = Status}}
    end,
    update(QosId, Diff).


%%--------------------------------------------------------------------
%% @doc
%% Adds given QosId to impossible QoS tree.
%% @end
%%--------------------------------------------------------------------
-spec add_impossible_qos(id(), datastore_doc:scope()) ->  ok.
add_impossible_qos(QosId, Scope) ->
    case update(QosId, fun(QosEntry) -> {ok, QosEntry#qos_entry{status = ?QOS_IMPOSSIBLE_STATUS}} end) of
        {ok, _} ->
            case add_links(Scope, ?IMPOSSIBLE_QOS_KEY, oneprovider:get_id(), {QosId, QosId}) of
                {ok, _} ->
                    ok;
                {error, Error} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.


-spec list_impossible_qos() ->  {ok, [id()]} | {error, term()}.
list_impossible_qos() ->
    fold_links(?IMPOSSIBLE_QOS_KEY,
        fun(#link{target = T}, Acc) -> {ok, [T | Acc]} end,
        [], #{}
    ).


get_expression(QosEntry) ->
    QosEntry#qos_entry.expression.


get_replicas_num(QosEntry) ->
    QosEntry#qos_entry.replicas_num.


get_status(QosEntry) ->
    QosEntry#qos_entry.status.


%%--------------------------------------------------------------------
%% @doc
%% Adds traverse requests for given QoS. Sets status to traverses_started.
%% @end
%%--------------------------------------------------------------------
-spec add_traverse_reqs(id(), [traverse_req()]) ->  ok.
add_traverse_reqs(QosId, TraverseReqs) ->
    {ok, _} = update(QosId, fun(QosEntry) ->
        {ok, QosEntry#qos_entry{traverse_reqs = TraverseReqs, status = ?QOS_TRAVERSES_STARTED_STATUS}}
    end),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Removes given traverse from requests list.
%% @end
%%--------------------------------------------------------------------
-spec remove_traverse_req(id(), traverse:id()) ->  ok.
remove_traverse_req(QosId, TaskId) ->
    {ok, _} = update(QosId, fun(#qos_entry{traverse_reqs = TR} = QosEntry) ->
        {ok, QosEntry#qos_entry{
            traverse_reqs = [X || X <- TR, X#qos_traverse_req.task_id =/= TaskId]
        }}
    end),
    ok.

%%%===================================================================
%%% Functions operating on traverses list.
%%%===================================================================

-define(QOS_TRAVERSE_LIST(QosId), <<"qos_traverses", QosId/binary>>).

%%--------------------------------------------------------------------
%% @doc
%% Add given TraverseId to traverses list of given QosId.
%% @end
%%--------------------------------------------------------------------
-spec add_traverse(od_space:id(), id(), traverse:id()) ->  ok.
add_traverse(SpaceId, QosId, TraverseId) ->
    Link = {TraverseId, TraverseId},
    {ok, _} = add_links(SpaceId, ?QOS_TRAVERSE_LIST(QosId), oneprovider:get_id(), Link),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Remove given TraverseId from traverses list of given QosId.
%% @end
%%--------------------------------------------------------------------
-spec remove_traverse(od_space:id(), id(), traverse:id()) ->  ok.
remove_traverse(SpaceId, QosId, TraverseId) ->
    ok = delete_links(SpaceId, ?QOS_TRAVERSE_LIST(QosId), oneprovider:get_id(), TraverseId).

%%--------------------------------------------------------------------
%% @doc
%% List traverses of given QosId.
%% @end
%%--------------------------------------------------------------------
-spec list_traverses(id()) ->  [traverse:id()].
list_traverses(QosId) ->
    {ok, Traverses} = fold_links(?QOS_TRAVERSE_LIST(QosId),
        fun(#link{target = T}, Acc) -> {ok, [T | Acc]} end,
        [], #{}
    ),
    Traverses.


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
        {status, atom},
        {traverse_req, [{record, [
            {task_id, string},
            {file_uuid, string},
            {target_storage, string}
        ]}]}
    ]}.
