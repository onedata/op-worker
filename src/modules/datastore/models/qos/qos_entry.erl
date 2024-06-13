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
%%% For qos_entry fulfillment status details consult `qos_status` module.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_entry).
-author("Michal Cwiertnia").

-include("global_definitions.hrl").
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
-export([add_to_failed_files_list/2, remove_from_failed_files_list/2, 
    apply_to_all_in_failed_files_list/2]).
-export([add_to_traverses_list/3, remove_from_traverses_list/2, fold_traverses/3]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, get_record_version/0, upgrade_record/2, resolve_conflict/3]).
-export([encode_expression/1, decode_expression/1]).

%% for tests
-export([accumulate_links/2]).

-type id() :: datastore_doc:key().
-type record() :: #qos_entry{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type replicas_num() :: pos_integer().
% Entry type denotes whether entry was created as part of internal 
% provider logic (internal) or was created by a user (user_defined).
% Internal entries can only be deleted by root.
-type type() :: internal | user_defined.

-type qos_transfer_id() :: transfer:id().
-type list_opts() :: #{
    token => datastore_links_iter:token(), 
    prev_link_name => datastore_links:link_name()
}.
-type list_apply_fun() :: fun((datastore_links:link_name()) -> any()).
-type list_fold_fun() :: fun(({datastore_links:link_name(), datastore_links:link_target()}, any()) -> any()).

-export_type([id/0, doc/0, record/0, type/0, replicas_num/0]).

-compile({no_auto_import, [get/1]}).

-define(LOCAL_CTX, #{
    model => ?MODULE,
    local_links_tree_id => oneprovider:get_id_or_undefined()
}).
-define(CTX, ?LOCAL_CTX#{
    sync_enabled => true,
    remote_driver => datastore_remote_driver,
    mutator => oneprovider:get_id_or_undefined()
}).

-define(IMPOSSIBLE_KEY(SpaceId), <<"impossible_qos_key_", SpaceId/binary>>).
-define(TRANSFERS_KEY(QosEntryId), <<"transfer_qos_key_", QosEntryId/binary>>).
-define(FAILED_FILES_KEY(SpaceId), <<"failed_files_qos_key_", SpaceId/binary>>).
% List of ongoing traverses rooted in directories. Additionally it holds traverses
% rooted in regular files to which it was impossible to calculate path (as some
% documents where not synced) for sole purpose of QoS status calculation
% (for more details consult qos_downtree_status and qos_status module doc).
-define(TRAVERSES_KEY(QosEntryId), <<"qos_traverse_key_", QosEntryId/binary>>).

-define(FOLD_LINKS_BATCH_SIZE, op_worker:get_env(qos_fold_links_batch_size, 100)).

%%%===================================================================
%%% Functions operating on document using datastore_model API
%%%===================================================================

-spec create(od_space:id(), file_meta:uuid(), qos_expression:expression(),
    replicas_num(), type()) -> {ok, id()} | {error, term()}.
create(SpaceId, FileUuid, Expression, ReplicasNum, EntryType) ->
    create(SpaceId, FileUuid, Expression, ReplicasNum, EntryType, false, 
        qos_traverse_req:build_traverse_reqs(FileUuid, [])).


-spec create(od_space:id(), file_meta:uuid(), qos_expression:expression(),
    replicas_num(), type(), boolean(), qos_traverse_req:traverse_reqs()) ->
    {ok, id()} | {error, term()}.
create(SpaceId, FileUuid, Expression, ReplicasNum, EntryType, Possible, TraverseReqs) ->
    QosEntryId = datastore_key:new_adjacent_to(SpaceId),
    PossibilityCheck = case Possible of
        true ->
            {possible, oneprovider:get_id()};
        false ->
            ok = add_to_impossible_list(SpaceId, QosEntryId),
            {impossible, oneprovider:get_id()}
    end,
    case ?extract_key(datastore_model:create(?CTX, #document{key = QosEntryId, scope = SpaceId,
        value = #qos_entry{
            file_uuid = FileUuid,
            expression = Expression,
            replicas_num = ReplicasNum,
            possibility_check = PossibilityCheck,
            traverse_reqs = TraverseReqs,
            type = EntryType
        }
    })) of
        {ok, QosEntryId} -> 
            ok = qos_transfer_stats:ensure_exists(QosEntryId),
            {ok, QosEntryId};
        {error, _} = Error -> 
            Error
    end.


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
-spec add_local_link(datastore:key(), {datastore:link_name(), datastore:link_target()}) ->
    {ok, datastore:link()} | {error, term()}.
add_local_link(Key, Links) ->
    datastore_model:add_links(?LOCAL_CTX, Key, oneprovider:get_id(), Links).


%% @private
-spec delete_local_link(datastore:key(), datastore:link_name()) ->
    ok | {error, term()}.
delete_local_link(Key, Links) ->
    datastore_model:delete_links(?LOCAL_CTX, Key, oneprovider:get_id(), Links).


%% @private
-spec fold_local_links(id(), datastore:fold_fun(datastore:link()),
    datastore:fold_acc(), datastore:fold_opts()) -> {ok, datastore:fold_acc()} |
    {{ok, datastore:fold_acc()}, datastore_links_iter:token()} | {error, term()}.
fold_local_links(Key, Fun, Acc, Opts) ->
    datastore_model:fold_links(?CTX, Key, oneprovider:get_id(), Fun, Acc, Opts).


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

-spec get_expression(doc() | record()) -> {ok, qos_expression:expression()}.
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
is_internal(#qos_entry{type = Type}) ->
    Type =:= internal.


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
        add_local_link(?IMPOSSIBLE_KEY(SpaceId), {QosEntryId, QosEntryId}))).

-spec remove_from_impossible_list(od_space:id(), id()) ->  ok | {error, term()}.
remove_from_impossible_list(SpaceId, QosEntryId) ->
    ?extract_ok(
        delete_local_link(?IMPOSSIBLE_KEY(SpaceId), QosEntryId)).

-spec apply_to_all_impossible_in_space(od_space:id(), list_apply_fun()) -> ok.
apply_to_all_impossible_in_space(SpaceId, Fun) ->
    apply_to_all_link_names_in_list(?IMPOSSIBLE_KEY(SpaceId), Fun).


-spec add_transfer_to_list(id(), qos_transfer_id()) -> ok | {error, term()}.
add_transfer_to_list(QosEntryId, TransferId) ->
    ?extract_ok(add_local_link(?TRANSFERS_KEY(QosEntryId), {TransferId, TransferId})).

-spec remove_transfer_from_list(id(), qos_transfer_id()) -> ok | {error, term()}.
remove_transfer_from_list(QosEntryId, TransferId)  ->
    delete_local_link(?TRANSFERS_KEY(QosEntryId), TransferId).

-spec apply_to_all_transfers(od_space:id(), list_apply_fun()) -> ok.
apply_to_all_transfers(QosEntryId, Fun) ->
    apply_to_all_link_names_in_list(?TRANSFERS_KEY(QosEntryId), Fun).


-spec add_to_failed_files_list(od_space:id(), file_meta:uuid()) -> ok | {error, term()}.
add_to_failed_files_list(SpaceId, FileUuid) ->
    ?ok_if_exists(?extract_ok(add_local_link(?FAILED_FILES_KEY(SpaceId), {FileUuid, FileUuid}))).

-spec remove_from_failed_files_list(od_space:id(), file_meta:uuid()) -> ok | {error, term()}.
remove_from_failed_files_list(SpaceId, FileUuid)  ->
    delete_local_link(?FAILED_FILES_KEY(SpaceId), FileUuid).

-spec apply_to_all_in_failed_files_list(od_space:id(), list_apply_fun()) -> ok.
apply_to_all_in_failed_files_list(SpaceId, Fun) ->
    apply_to_all_link_names_in_list(?FAILED_FILES_KEY(SpaceId), Fun).


-spec add_to_traverses_list(id(), qos_traverse:id(), file_meta:uuid()) -> ok | {error, term()}.
add_to_traverses_list(QosEntryId, TraverseId, TraverseRootUuid) ->
    ?ok_if_exists(?extract_ok(add_local_link(?TRAVERSES_KEY(QosEntryId), {TraverseId, TraverseRootUuid}))).

-spec remove_from_traverses_list(id(), qos_traverse:id()) -> ok | {error, term()}.
remove_from_traverses_list(QosEntryId, TraverseId)  ->
    delete_local_link(?TRAVERSES_KEY(QosEntryId), TraverseId).

-spec fold_traverses(id(), list_fold_fun(), any()) -> any().
fold_traverses(QosEntryId, Fun, InitialAcc) ->
    fold_all_in_list(?TRAVERSES_KEY(QosEntryId), Fun, InitialAcc).


%% @private
-spec fold_all_in_list(datastore:key(), list_fold_fun(), any()) -> any().
fold_all_in_list(Key, Fun, InitialAcc) ->
    {List, NextBatchOpts} = list_next_batch(Key, #{}),
    fold_in_batches(Key, Fun, List, NextBatchOpts, InitialAcc).


%% @private
-spec fold_in_batches(datastore:key(), list_fold_fun(),
    [datastore_links:link_name()], list_opts(), any()) -> any().
fold_in_batches(_Key, _Fun, [], _Opts, Acc) -> Acc;
fold_in_batches(Key, Fun, List, Opts, Acc) ->
    NextAcc = lists:foldl(Fun, Acc, List),
    {NextBatch, NextBatchOpts} = list_next_batch(Key, Opts),
    fold_in_batches(Key, Fun, NextBatch, NextBatchOpts, NextAcc).


%% @private
-spec apply_to_all_link_names_in_list(datastore:key(), list_apply_fun()) -> ok.
apply_to_all_link_names_in_list(Key, Fun) ->
    FinalFun = fun({LinkName, _LinkTarget}, _Acc) ->
        Fun(LinkName),
        ok
    end,
    fold_all_in_list(Key, FinalFun, ok).


%% @private
-spec list_next_batch(datastore:key(), list_opts()) ->
    {[datastore_links:link_name()], list_opts()}.
list_next_batch(Key, Opts) ->
    Opts1 = case maps:is_key(token, Opts) of
        true -> Opts;
        false -> Opts#{token => #link_token{}}
    end,
    {{ok, Res}, Token} = fold_local_links(Key,
        fun accumulate_links/2, [],
        Opts1#{size => ?FOLD_LINKS_BATCH_SIZE}),
    NextBatchOpts = case Res of
        [] -> #{token => Token};
        _ -> #{token => Token, prev_link_name => lists:last(Res)}
    end,
    {Res, NextBatchOpts}.


%% @private
-spec accumulate_links(datastore_links:link(), [datastore_links:link_name()]) ->
    {ok, [datastore_links:link_name()]}.
accumulate_links(#link{name = Name, target = Target}, Acc) -> {ok, [{Name, Target} | Acc]}.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {type, atom},
        {file_uuid, string},
        {expression, [binary]},
        {replicas_num, integer},
        {traverse_reqs, #{string => {record, [
            {start_file_uuid, string},
            {storage_id, string}
        ]}}},
        {possibility_check, {atom, string}}
    ]};
get_record_struct(2) ->
    % * modified field - expression
    {record, [
        {type, atom},
        {file_uuid, string},
        {expression, {custom, json, {?MODULE, encode_expression, decode_expression}}}, 
        {replicas_num, integer},
        {traverse_reqs, #{string => {record, [
            {start_file_uuid, string},
            {storage_id, string}
        ]}}},
        {possibility_check, {atom, string}}
    ]}.


-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, QosEntry) -> 
    {qos_entry,
        Type,
        FileUuid,
        Expression,
        ReplicasNum,
        TraverseReqs,
        PossibilityCheck
    } = QosEntry,
    
    {2, #qos_entry{
        type = Type,
        file_uuid = FileUuid,
        expression = qos_expression:convert_from_old_version_rpn(Expression),
        replicas_num = ReplicasNum,
        traverse_reqs = TraverseReqs,
        possibility_check = PossibilityCheck
    }}.


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
resolve_conflict(_Ctx,
    #document{key = QosId, value = RemoteValue, scope = SpaceId, revs = [RemoteRev | _], deleted = RemoteDeleted} = RemoteDoc,
    #document{value = LocalValue, revs = [LocalRev | _], deleted = LocalDeleted} = LocalDoc
) ->
    case {datastore_rev:is_greater(RemoteRev, LocalRev), LocalDeleted, RemoteDeleted} of
        {true, true, false} ->
            {true, RemoteDoc#document{deleted = true}};
        {true, _, true} ->
            {false, RemoteDoc};
        {false, true, _} ->
            ignore;
        {false, false, true} ->
            {true, LocalDoc#document{deleted = true}};
        {_, false, false} ->
            case resolve_conflict_internal(SpaceId, QosId, RemoteValue, LocalValue) of
                default ->
                    default;
                #qos_entry{} = Value ->
                    % for now always take the remote document. This has to be changed
                    % when it will be necessary to use revision history.
                    {true, RemoteDoc#document{
                        value = Value
                    }}
            end
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
                        ?error("Could not remove qos_entry ~tp from file_qos of file ~tp: ~tp",
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


-spec encode_expression(qos_expression:expression()) -> binary().
encode_expression(Expression) ->
    json_utils:encode(qos_expression:to_json(Expression)).

-spec decode_expression(binary()) -> qos_expression:expression().
decode_expression(EncodedExpression) ->
    qos_expression:from_json(json_utils:decode(EncodedExpression)).
