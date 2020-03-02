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
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% functions operating on document using datastore model API
-export([create/5, create/7, get/1, delete/1]).

%% higher-level functions operating on qos_entry document
-export([get_space_id/1, get_file_guid/1]).
-export([mark_possible/3, remove_traverse_req/2]).

%% functions operating on qos_entry record
-export([get_expression/1, get_replicas_num/1, get_file_uuid/1, 
    get_traverse_reqs/1, is_possible/1, is_internal/1]).

%%% functions operating on links tree lists
-export([add_to_impossible_list/2, remove_from_impossible_list/2, 
    apply_to_all_impossible_in_space/2]).
-export([add_transfer_to_list/2, remove_transfer_from_list/2, 
    apply_to_all_transfers/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0, resolve_conflict/3]).


% fixme remove callback module

-type id() :: datastore_doc:key().
-type record() :: #qos_entry{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type replicas_num() :: pos_integer().

-type qos_transfer_id() :: transfer:id().
-type one_or_many(Type) :: Type | [Type].
-type list_opts() :: #{
    token => datastore_links_iter:token(), 
    prev_link_name => datastore_links:link_name()
}.
-type list_apply_fun() :: fun((datastore_links:link_name()) -> any()).

-export_type([id/0, doc/0, record/0, replicas_num/0]).

-compile({no_auto_import, [get/1]}).

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
-define(TRANSFERS_KEY(QosEntryId), <<"transfer_qos_key_", QosEntryId/binary>>).

-define(FOLD_LINKS_BATCH_SIZE, 100).

%%%===================================================================
%%% Functions operating on document using datastore_model API
%%%===================================================================

-spec create(od_space:id(), file_meta:uuid(), qos_expression:rpn(),
    replicas_num(), module()) -> {ok, doc()} | {error, term()}.
create(SpaceId, FileUuid, Expression, ReplicasNum, CallbackModule) ->
    create(SpaceId, FileUuid, Expression, ReplicasNum, CallbackModule, false, 
        qos_traverse_req:build_traverse_reqs(FileUuid, [])).


-spec create(od_space:id(), file_meta:uuid(), qos_expression:rpn(),
    replicas_num(), module(), boolean(), qos_traverse_req:traverse_reqs()) ->
    {ok, doc()} | {error, term()}.
create(SpaceId, FileUuid, Expression, ReplicasNum, CallbackModule, Possible, TraverseReqs) ->
    QosEntryId = datastore_key:new(),
    PossibilityCheck = case Possible of
        true ->
            {possible, oneprovider:get_id()};
        false ->
            ok = add_to_impossible_list(SpaceId, QosEntryId),
            {impossible, oneprovider:get_id()}
    end,
    ?extract_key(datastore_model:create(?CTX, #document{key = QosEntryId, scope = SpaceId,
        value = #qos_entry{
            file_uuid = FileUuid,
            expression = Expression,
            replicas_num = ReplicasNum,
            possibility_check = PossibilityCheck,
            traverse_reqs = TraverseReqs,
            internal = CallbackModule =/= undefined, % fixme is it necessary?
            callback_module = CallbackModule
        }
    })).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(QosEntryId) ->
    datastore_model:get(?CTX, QosEntryId).


%% @private
-spec update(id(), diff()) -> {ok, doc()} | {error, term()}.
update(Key, Diff) ->
    datastore_model:update(?CTX, Key, Diff).


-spec delete(id()) -> ok | {error, term()}.
delete(QosEntryId) ->
    datastore_model:delete(?CTX, QosEntryId).


%% @private
-spec add_local_links(datastore:key(), datastore:tree_id(),
    one_or_many({datastore:link_name(), datastore:link_target()})) ->
    one_or_many({ok, datastore:link()} | {error, term()}).
add_local_links(Key, TreeId, Links) ->
    datastore_model:add_links(?LOCAL_CTX, Key, TreeId, Links).


%% @private
-spec delete_local_links(datastore:key(), datastore:tree_id(),
    one_or_many(datastore:link_name() | {datastore:link_name(), datastore:link_rev()})) ->
    one_or_many(ok | {error, term()}).
delete_local_links(Key, TreeId, Links) ->
    datastore_model:delete_links(?LOCAL_CTX, Key, TreeId, Links).


%% @private
-spec fold_local_links(id(), datastore:fold_fun(datastore:link()),
    datastore:fold_acc(), datastore:fold_opts()) -> {ok, datastore:fold_acc()} |
    {{ok, datastore:fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_local_links(Key, Fun, Acc, Opts) ->
    datastore_model:fold_links(?LOCAL_CTX, Key, oneprovider:get_id(), Fun, Acc, Opts).


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
%% Marks given entry as possible and saves for it given traverse requests.
%% @end
%%--------------------------------------------------------------------
-spec mark_possible(id(), od_space:id(), qos_traverse_req:traverse_reqs()) -> ok.
mark_possible(QosEntryId, SpaceId, AllTraverseReqs) ->
    {ok, _} = update(QosEntryId, fun(QosEntry) ->
        {ok, QosEntry#qos_entry{
            possibility_check = {possible, oneprovider:get_id()},
            traverse_reqs = AllTraverseReqs
        }}
    end),
    ok = remove_from_impossible_list(SpaceId, QosEntryId).


%%--------------------------------------------------------------------
%% @doc
%% Removes given traverse from traverse reqs of given QoS entry.
%% @end
%%--------------------------------------------------------------------
-spec remove_traverse_req(id(), qos_traverse_req:id()) -> ok | {error, term()}.
remove_traverse_req(QosEntryId, TraverseId) ->
    Diff = fun(#qos_entry{traverse_reqs = TR} = QosEntry) ->
        {ok, QosEntry#qos_entry{
            traverse_reqs = qos_traverse_req:remove_req(TraverseId, TR)
        }}
    end,
    ?ok_if_not_found(?extract_ok(update(QosEntryId, Diff))).

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


-spec is_internal(doc() | record()) -> boolean().
is_internal(#document{value = QosEntry}) ->
    is_internal(QosEntry);
is_internal(#qos_entry{internal = Internal}) ->
    Internal.


%%%===================================================================
%%% Functions operating on links tree lists
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds QoS that cannot be fulfilled to links tree storing ID of all
%% qos_entry documents that cannot be fulfilled at the moment.
%% @end
%%--------------------------------------------------------------------
-spec add_to_impossible_list(od_space:id(), id()) ->  ok | {error, term()}.
add_to_impossible_list(SpaceId, QosEntryId) ->
    ?ok_if_exists(?extract_ok(
        add_local_links(?IMPOSSIBLE_KEY(SpaceId), oneprovider:get_id(), {QosEntryId, QosEntryId}))).


-spec remove_from_impossible_list(od_space:id(), id()) ->  ok | {error, term()}.
remove_from_impossible_list(SpaceId, QosEntryId) ->
    ?extract_ok(
        delete_local_links(?IMPOSSIBLE_KEY(SpaceId), oneprovider:get_id(), QosEntryId)
    ).


-spec apply_to_all_impossible_in_space(od_space:id(), list_apply_fun()) -> ok.
apply_to_all_impossible_in_space(SpaceId, Fun) ->
    apply_to_all_in_list(?IMPOSSIBLE_KEY(SpaceId), Fun).


-spec add_transfer_to_list(id(), qos_transfer_id()) -> ok | {error, term()}.
add_transfer_to_list(QosEntryId, TransferId) ->
    add_local_links(?TRANSFERS_KEY(QosEntryId), oneprovider:get_id(), {TransferId, TransferId}).


-spec remove_transfer_from_list(id(), qos_transfer_id()) -> ok | {error, term()}.
remove_transfer_from_list(QosEntryId, TransferId)  ->
    delete_local_links(?TRANSFERS_KEY(QosEntryId), oneprovider:get_id(), TransferId).


-spec apply_to_all_transfers(od_space:id(), list_apply_fun()) -> ok.
apply_to_all_transfers(QosEntryId, Fun) ->
    apply_to_all_in_list(?TRANSFERS_KEY(QosEntryId), Fun).


%% @private
-spec apply_to_all_in_list(datastore:key(), list_apply_fun()) -> ok.
apply_to_all_in_list(Key, Fun) ->
    {List, NextBatchOpts} = list_next_batch(Key, #{}),
    apply_and_list_next_batch(Key, Fun, List, NextBatchOpts).


%% @private
-spec apply_and_list_next_batch(datastore:key(), list_apply_fun(), 
    [datastore_links:link_name()], list_opts()) -> ok.
apply_and_list_next_batch(_Key, _Fun, [], _Opts) -> ok;
apply_and_list_next_batch(Key, Fun, List, Opts) ->
    lists:foreach(Fun, List),
    {NextBatch, NextBatchOpts} = list_next_batch(Key, Opts),
    apply_and_list_next_batch(Key, Fun, NextBatch, NextBatchOpts).


%% @private
-spec list_next_batch(datastore:key(), list_opts()) ->
    {[datastore_links:link_name()], list_opts()}.
list_next_batch(Key, Opts) ->
    Opts1 = case maps:is_key(token, Opts) of
        true -> Opts;
        false -> Opts#{token => #link_token{}}
    end,
    {{ok, Res}, Token} = fold_local_links(Key,
        fun(#link{name = Name}, Acc) -> {ok, [Name | Acc]} end, [],
        Opts1#{size => ?FOLD_LINKS_BATCH_SIZE}),
    NextBatchOpts = case Res of
        [] -> #{token => Token};
        _ -> #{token => Token, prev_link_name => lists:last(Res)}
    end,
    {Res, NextBatchOpts}.


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
        {possibility_check, {atom, string}},
        {internal, boolean},
        {callback_module, atom}
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
                case file_qos:remove_qos_entry_id(SpaceId, FileUuid, QosId) of
                    ok -> ok;
                    {error, _} = Error ->
                        ?error("Could not remove qos_entry ~p from file_qos of file ~p: ~p",
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

    {LocalTraverseIds, _} = qos_traverse_req:split_local_and_remote(LocalReqs),
    {_, RemoteTraverseIds} = qos_traverse_req:split_local_and_remote(RemoteReqs),
    Value#qos_entry{
        traverse_reqs = qos_traverse_req:select_traverse_reqs(
            LocalTraverseIds ++ RemoteTraverseIds, LocalReqs)
    }.
