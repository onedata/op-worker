%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This model holds information about single QoS, that is QoS requirement
%%% defined by the user for file or directory through QoS expression and
%%% number of required replicas. Information about requirement is stored in
%%% qos_entry document. Document is created for each requirement even if
%%% the same requirement was already defined. For each file / directory
%%% multiple qos_entry can be defined. New file replica is created only
%%% when new QoS requirement is defined and current replicas do not satisfy all
%%% QoS requirements. Otherwise there is no need to create new replica.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_entry).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% functions operating on record using datastore model API
-export([get/1, delete/1, create/2, update/2, add_links/4, delete_links/4]).

%% higher-level functions operating on qos_entry record.
-export([add_impossible_qos/2, list_impossible_qos/0, get_file_guid/1,
    set_status/2, get_qos_details/1, add_traverse_reqs/2, remove_traverse_req/2]).

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
%%--------------------------------------------------------------------
%% @doc
%% Creates qos_entry document.
%% @end
%%--------------------------------------------------------------------
-spec create(doc(), od_space:id()) -> {ok, doc()} | {error, term()}.
create(#document{value = QosEntry}, SpaceId) ->
    datastore_model:create(?CTX, #document{scope = SpaceId, value = QosEntry}).

%%--------------------------------------------------------------------
%% @doc
%% Updates qos_entry.
%% @end
%%--------------------------------------------------------------------
-spec update(key(), diff()) -> {ok, key()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Returns qos.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(QosId) ->
    datastore_model:get(?CTX, QosId).

%%--------------------------------------------------------------------
%% @doc
%% Deletes qos_entry document.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(QosId) ->
    datastore_model:delete(?CTX, QosId).

%%--------------------------------------------------------------------
%% @doc
%% Creates links.
%% @end
%%--------------------------------------------------------------------
-spec add_links(datastore_doc:scope(), datastore:key(), datastore:tree_id(),
    one_or_many({datastore:link_name(), datastore:link_target()})) ->
    one_or_many({ok, datastore:link()} | {error, term()}).
add_links(Scope, Key, TreeId, Links) ->
    datastore_model:add_links(?CTX#{scope => Scope}, Key, TreeId, Links).

%%--------------------------------------------------------------------
%% @doc
%% Deletes links.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(datastore_doc:scope(), datastore:key(), datastore:tree_id(),
    one_or_many(datastore:link_name() | {datastore:link_name(), datastore:link_rev()})) ->
    one_or_many(ok | {error, term()}).
delete_links(Scope, Key, TreeId, Links) ->
    datastore_model:delete_links(?CTX#{scope => Scope}, Key, TreeId, Links).

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

-spec set_status(id(), status()) -> {ok, key()} | {error, term}.
set_status(QosId, Status) ->
    Diff = fun(QosEntry) ->
        {ok, QosEntry#qos_entry{status = Status}}
    end,
    update(QosId, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Adds given QosId to impossible qos tree.
%% @end
%%--------------------------------------------------------------------
-spec add_impossible_qos(id(), datastore_doc:scope()) ->  ok | {error, term()}.
add_impossible_qos(QosId, Scope) ->
    {ok, _} = update(QosId, fun(QosEntry) ->
        {ok, QosEntry#qos_entry{status = ?QOS_IMPOSSIBLE_STATUS}}
    end),
    {ok, _} = add_links(Scope, ?IMPOSSIBLE_QOS_KEY, oneprovider:get_id(), {QosId, QosId}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Lists all impossible qos.
%% @end
%%--------------------------------------------------------------------
-spec list_impossible_qos() ->  {ok, [id()]} | {error, term()}.
list_impossible_qos() ->
    datastore_model:fold_links(?CTX, ?IMPOSSIBLE_QOS_KEY, all,
        fun(#link{target = T}, Acc) -> {ok, [T | Acc]} end,
        [], #{}
    ).

%%--------------------------------------------------------------------
%% @doc
%% Get information about QoS requirement from qos_entry document.
%% @end
%%--------------------------------------------------------------------
-spec get_qos_details(id()) -> {ok, {qos_expression:expression(), replicas_num(), status()}}.
get_qos_details(QosId) ->
    {ok, #document{key = QosId, value = QosEntry}} = qos_entry:get(QosId),
    {QosEntry#qos_entry.expression, QosEntry#qos_entry.replicas_num, QosEntry#qos_entry.status}.

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
    {ok, Traverses} = datastore_model:fold_links(?CTX, ?QOS_TRAVERSE_LIST(QosId), all,
        fun(#link{target = T}, Acc) -> {ok, [T | Acc]} end,
        [], #{}
    ),
    Traverses.


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
    1.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
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

