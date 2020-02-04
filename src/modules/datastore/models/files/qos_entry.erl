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
%%%     - there is no information that qos_entry cannot be satisfied (this
%%%       information is stored in `possibility_check` field in qos_entry document.
%%%       It is set to `{possible, ProviderId}` if during evaluation of QoS expression
%%%       provider ProviderId was able to calculate list of storages that would fulfill
%%%       QoS requirements, otherwise it is set to `{impossible, ProviderId}`)
% fixme
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
    get/1, delete/1, create/5, create/7
]).

%% higher-level functions operating on qos_entry document
-export([
    add_to_impossible_list/2, get_impossible_list/1, delete_from_impossible_list/2,
    mark_entry_possible/3, is_possible/1, get_space_id/1, remove_traverse_req/2
]).

%% functions operating on qos_entry record
-export([
    get_file_guid/1, get_expression/1, get_replicas_num/1,
    get_file_uuid/1, get_traverse_reqs/1
]).

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
-type one_or_many(Type) :: Type | [Type].

-export_type([id/0, doc/0, record/0, replicas_num/0]).

-define(LOCAL_CTX, #{
    model => ?MODULE
}).
-define(CTX, ?LOCAL_CTX#{
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined(),
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).

-define(IMPOSSIBLE_KEY(SpaceId), <<"impossible_qos_key_", SpaceId/binary>>).

%%%===================================================================
%%% Functions operating on document using datastore_model API
%%%===================================================================

-spec create(od_space:id(), id(), file_meta:uuid(), qos_expression:rpn(),
    replicas_num()) -> {ok, doc()} | {error, term()}.
create(SpaceId, QosEntryId, FileUuid, Expression, ReplicasNum) ->
    create(SpaceId, QosEntryId, FileUuid, Expression, ReplicasNum, false, #{}).


-spec create(od_space:id(), id(), file_meta:uuid(), qos_expression:rpn(),
    replicas_num(), boolean(), qos_traverse_req:traverse_reqs()) ->
    {ok, doc()} | {error, term()}.
create(SpaceId, QosEntryId, FileUuid, Expression, ReplicasNum, Possible, TraverseReqs) ->
    PossibilityCheck = case Possible of
        true ->
            {possible, oneprovider:get_id()};
        false ->
            ok = add_to_impossible_list(QosEntryId, SpaceId),
            {impossible, oneprovider:get_id()}
    end,
    datastore_model:create(?CTX, #document{key = QosEntryId, scope = SpaceId,
        value = #qos_entry{
            file_uuid = FileUuid,
            expression = Expression,
            replicas_num = ReplicasNum,
            possibility_check = PossibilityCheck,
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


-spec add_local_links(datastore:key(), datastore:tree_id(),
    one_or_many({datastore:link_name(), datastore:link_target()})) ->
    one_or_many({ok, datastore:link()} | {error, term()}).
add_local_links(Key, TreeId, Links) ->
    datastore_model:add_links(?LOCAL_CTX, Key, TreeId, Links).


-spec delete_local_links(datastore:key(), datastore:tree_id(),
    one_or_many(datastore:link_name() | {datastore:link_name(), datastore:link_rev()})) ->
    one_or_many(ok | {error, term()}).
delete_local_links(Key, TreeId, Links) ->
    datastore_model:delete_links(?LOCAL_CTX, Key, TreeId, Links).


-spec fold_local_links(id(), datastore_model:tree_ids(), datastore:fold_fun(datastore:link()),
    datastore:fold_acc(), datastore:fold_opts()) -> {ok, datastore:fold_acc()} |
    {{ok, datastore:fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_local_links(Key, TreeIds, Fun, Acc, Opts) ->
    datastore_model:fold_links(?LOCAL_CTX, Key, TreeIds, Fun, Acc, Opts).


%%%===================================================================
%%% Higher-level functions operating on qos_entry document.
%%%===================================================================

-spec get_file_guid(doc() | id()) -> {ok, file_id:file_guid()} | {error, term()}.
get_file_guid(#document{scope = SpaceId, value = #qos_entry{file_uuid = FileUuid}})  ->
    {ok, file_id:pack_guid(FileUuid, SpaceId)};

get_file_guid(QosEntryId) ->
    case qos_entry:get(QosEntryId) of
        {ok, Doc} ->
            get_file_guid(Doc);
        {error, _} = Error ->
            Error
    end.


-spec get_space_id(doc() | id()) -> {ok, od_space:id()} | {error, term()}.
get_space_id(#document{scope = SpaceId, value = #qos_entry{}}) ->
    {ok, SpaceId};
get_space_id(QosEntryId) ->
    case qos_entry:get(QosEntryId) of
        {ok, Doc} -> get_space_id(Doc);
        {error, _} = Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Adds QoS that cannot be fulfilled to links tree storing ID of all
%% qos_entry documents that cannot be fulfilled at the moment.
%% @end
%%--------------------------------------------------------------------
-spec add_to_impossible_list(id(), od_space:id()) ->  ok | {error, term()}.
add_to_impossible_list(QosEntryId, SpaceId) ->
    ?extract_ok(
        add_local_links(?IMPOSSIBLE_KEY(SpaceId), oneprovider:get_id(), {QosEntryId, QosEntryId})
    ).


-spec delete_from_impossible_list(id(), od_space:id()) ->  ok | {error, term()}.
delete_from_impossible_list(QosEntryId, SpaceId) ->
    ?extract_ok(
        delete_local_links(?IMPOSSIBLE_KEY(SpaceId), oneprovider:get_id(), QosEntryId)
    ).


-spec get_impossible_list(od_space:id()) ->  {ok, [id()]} | {error, term()}.
get_impossible_list(SpaceId) ->
    fold_local_links(?IMPOSSIBLE_KEY(SpaceId), oneprovider:get_id(),
        fun(#link{target = T}, Acc) -> {ok, [T | Acc]} end,
        [], #{}
    ).


%%--------------------------------------------------------------------
%% @doc
%% Marks given entry as possible and saves for it given traverse requests.
%% @end
%%--------------------------------------------------------------------
-spec mark_entry_possible(id(), od_space:id(), qos_traverse_req:traverse_reqs()) -> ok.
mark_entry_possible(QosEntryId, SpaceId, AllTraverseReqs) ->
    {ok, _} = update(QosEntryId, fun(QosEntry) ->
        {ok, QosEntry#qos_entry{
            possibility_check = {possible, oneprovider:get_id()},
            traverse_reqs = AllTraverseReqs
        }}
    end),
    ok = delete_from_impossible_list(QosEntryId, SpaceId).


%%--------------------------------------------------------------------
%% @doc
%% Removes given traverse from traverse reqs of given QoS entry.
%% @end
%%--------------------------------------------------------------------
-spec remove_traverse_req(id(), qos_traverse_req:id()) -> ok.
remove_traverse_req(QosEntryId, TraverseId) ->
    Diff = fun(#qos_entry{traverse_reqs = TR} = QosEntry) ->
        {ok, QosEntry#qos_entry{
            traverse_reqs = qos_traverse_req:remove_req(TraverseId, TR)
        }}
    end,

    ?extract_ok(update(QosEntryId, Diff)).

%%%===================================================================
%%% Functions operating on qos_entry record.
%%%===================================================================

-spec get_expression(doc() | record()) -> {ok, qos_expression:rpn()}.
get_expression(#document{value = QosEntry}) ->
    get_expression(QosEntry);
get_expression(QosEntry) ->
    {ok, QosEntry#qos_entry.expression}.


-spec get_replicas_num(doc() | record()) -> {ok, replicas_num()}.
get_replicas_num(#document{value = QosEntry}) ->
    get_replicas_num(QosEntry);
get_replicas_num(QosEntry) ->
    {ok, QosEntry#qos_entry.replicas_num}.


-spec get_file_uuid(doc() | record()) -> {ok, file_meta:uuid()}.
get_file_uuid(#document{value = QosEntry}) ->
    get_file_uuid(QosEntry);
get_file_uuid(QosEntry) ->
    {ok, QosEntry#qos_entry.file_uuid}.


-spec get_traverse_reqs(doc() | record()) -> {ok, qos_traverse_req:traverse_reqs()}.
get_traverse_reqs(#document{value = QosEntry}) ->
    get_traverse_reqs(QosEntry);
get_traverse_reqs(QosEntry) ->
    {ok, QosEntry#qos_entry.traverse_reqs}.


-spec is_possible(doc() | record()) -> boolean().
is_possible(#document{value = QosEntry}) ->
    is_possible(QosEntry);
is_possible(#qos_entry{possibility_check = {possible, _}}) ->
    true;
is_possible(#qos_entry{possibility_check = {impossible, _}}) ->
    false.


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
        {traverse_reqs, #{string => {record, [
            {start_file_uuid, string},
            {storage_id, string}
        ]}}},
        {possibility_check, {atom, string}}
    ]}.


%%--------------------------------------------------------------------
%% @doc
%% This conflict resolution promotes changes done by provider
%% responsible for given traverse (assigned_entry includes one of its storages).
%% This means that only remote changes of remote providers traverses and
%% local changes of current provider traverses are taken into account.
%% (When entry is possible to fulfill only changes done to this document are
%% removals of traverse_reqs or removal of document).
%%
%% When entry was marked possible and more than one provider calculated target
%% storages (this can happen when storages making entry possible were added on
%% different providers at the same time) only changes made by provider with
%% lowest id (in lexicographical order) will be saved, other providers will
%% revoke their changes.
%%
%% When entry was impossible and remote provider marked it as possible
%% (this can happen when entry was reevaluated) value set by remote provider
%% will be saved.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(datastore_model:ctx(), doc(), doc()) ->
    {boolean(), doc()} | ignore | default.
resolve_conflict(_Ctx, #document{key = QosId, value = RemoteValue, scope = SpaceId} = RemoteDoc,
    #document{value = LocalValue}) ->

    case resolve_conflict_internal(SpaceId, QosId, RemoteValue, LocalValue) of
        default ->
            default;
        #qos_entry{} = Value ->
            % for now always take the remote document. This has to be changed
            % when it will be necessary to use revision history.
            {true, RemoteDoc#document{
                value = Value
            }}
    end.

%% @private
-spec resolve_conflict_internal(od_space:id(), id(), record(), record()) -> default | record().
resolve_conflict_internal(SpaceId, QosId,
    #qos_entry{possibility_check = {possible, RemoteEntryProviderId}, file_uuid = FileUuid} = RemoteEntry,
    #qos_entry{possibility_check = {possible, LocalEntryProviderId}} = LocalEntry)
    when RemoteEntryProviderId =/= LocalEntryProviderId ->
    % target storages were calculated by different providers

    case RemoteEntryProviderId < LocalEntryProviderId of
        true ->
            % remote changes were made by provider with lower id ->
            %   trigger async removal of entry from file_qos and save remote changes
            spawn(fun() ->
                case file_qos:remove_qos_entry_id(FileUuid, QosId) of
                    ok ->
                        ok = qos_bounded_cache:invalidate_on_all_nodes(SpaceId);
                    {error, _} = Error ->
                        ?error("Could not remvove qos_entry ~p from file_qos of file ~p: ~p",
                            [QosId, FileUuid, Error])
                end
            end),
            RemoteEntry;
        false ->
            % remote changes were made by provider with higher id -> ignore them
            LocalEntry
    end;

resolve_conflict_internal(_SpaceId, _QosId,
    #qos_entry{possibility_check = {possible, _}} = RemoteEntry,
    #qos_entry{possibility_check = {impossible, _}}) ->
    % remote provider marked impossible entry as possible

    RemoteEntry;

resolve_conflict_internal(_SpaceId, _QosId, #qos_entry{traverse_reqs = TR},
    #qos_entry{traverse_reqs = TR}) ->
    % traverse requests are equal in both documents

    default;

resolve_conflict_internal(_SpaceId, _QosId, #qos_entry{traverse_reqs = RemoteReqs},
    #qos_entry{traverse_reqs = LocalReqs} = Value) ->
    % traverse requests are different

    {LocalTraverseIds, _} = split_traverse_reqs(LocalReqs),
    {_, RemoteTraverseIds} = split_traverse_reqs(RemoteReqs),
    Value#qos_entry{
        traverse_reqs = qos_traverse_req:select_traverse_reqs(
            LocalTraverseIds ++ RemoteTraverseIds, LocalReqs)
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Splits given traverse reqs to those of current provider and those of remote providers.
%% @end
%%--------------------------------------------------------------------
-spec split_traverse_reqs(qos_traverse_req:traverse_reqs()) ->
    {[qos_traverse_req:id()], [qos_traverse_req:id()]}.
split_traverse_reqs(AllTraverseReqs) ->
    maps:fold(fun(TaskId, TraverseReq, {LocalTraverseReqs, RemoteTraverseReqs}) ->
        StorageId = qos_traverse_req:get_storage(TraverseReq),
        case storage:is_local(StorageId) of
            true -> {[TaskId | LocalTraverseReqs], RemoteTraverseReqs};
            false -> {LocalTraverseReqs, [TaskId | RemoteTraverseReqs]}
        end
    end, {[], []}, AllTraverseReqs).