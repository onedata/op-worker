%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for managing QoS (requests are delegated to middleware_worker).
%%% @end
%%%-------------------------------------------------------------------
-module(mi_qos).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("modules/logical_file_manager/lfm.hrl").

%% API
-export([
    add_qos_entry/4, add_qos_entry/5,
    get_effective_file_qos/2,
    get_qos_entry/2,
    remove_qos_entry/2,
    check_qos_status/2, check_qos_status/3
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec add_qos_entry(
    session:id(),
    lfm:file_key(),
    qos_expression:infix() | qos_expression:expression(),
    qos_entry:replicas_num()
) ->
    qos_entry:id() | no_return().
add_qos_entry(SessId, FileKey, Expression, ReplicasNum) ->
    add_qos_entry(SessId, FileKey, Expression, ReplicasNum, user_defined).


-spec add_qos_entry(
    session:id(),
    lfm:file_key(),
    qos_expression:infix() | qos_expression:expression(),
    qos_entry:replicas_num(),
    qos_entry:type()
) ->
    qos_entry:id() | no_return().
add_qos_entry(SessionId, FileKey, RawExpression, ReplicasNum, EntryType) ->
    Expression = case is_binary(RawExpression) of
        true -> qos_expression:parse(RawExpression);
        false -> RawExpression
    end,
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),

    middleware_worker:check_exec(SessionId, FileGuid, #add_qos_entry{
        expression = Expression,
        replicas_num = ReplicasNum,
        entry_type = EntryType
    }).


-spec get_effective_file_qos(session:id(), lfm:file_key()) -> qos_req:eff_file_qos() | no_return().
get_effective_file_qos(SessionId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),

    middleware_worker:check_exec(SessionId, FileGuid, #get_effective_file_qos{}).


-spec get_qos_entry(session:id(), qos_entry:id()) ->
    qos_entry:record() | no_return().
get_qos_entry(SessionId, QosEntryId) ->
    FileGuid = qos_entry_id_id_to_file_guid(QosEntryId),

    middleware_worker:check_exec(SessionId, FileGuid, #get_qos_entry{id = QosEntryId}).


-spec remove_qos_entry(session:id(), qos_entry:id()) -> ok | no_return().
remove_qos_entry(SessionId, QosEntryId) ->
    FileGuid = qos_entry_id_id_to_file_guid(QosEntryId),

    middleware_worker:check_exec(SessionId, FileGuid, #remove_qos_entry{id = QosEntryId}).


-spec check_qos_status(session:id(), qos_entry:id() | [qos_entry:id()]) ->
    qos_status:summary() | no_return().
check_qos_status(SessionId, QosEntries) ->
    check_qos_status(SessionId, QosEntries, undefined).


-spec check_qos_status(session:id(), qos_entry:id() | [qos_entry:id()], undefined | lfm:file_key()) ->
    qos_status:summary() | no_return().
check_qos_status(SessionId, QosEntries, FileKey) when is_list(QosEntries) ->
    Statuses = lists:map(fun(QosEntryId) ->
        check_qos_status(SessionId, QosEntryId, FileKey)
    end, QosEntries),
    qos_status:aggregate(Statuses);

check_qos_status(SessionId, QosEntryId, undefined) ->
    QosRootFileGuid = qos_entry_id_id_to_file_guid(QosEntryId),
    check_qos_status(SessionId, QosEntryId, ?FILE_REF(QosRootFileGuid));

check_qos_status(SessionId, QosEntryId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),

    middleware_worker:check_exec(SessionId, FileGuid, #check_qos_status{qos_id = QosEntryId}).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec qos_entry_id_id_to_file_guid(qos_entry:id()) -> file_id:file_guid() | no_return().
qos_entry_id_id_to_file_guid(QosEntryId) ->
    ?check(qos_entry:get_file_guid(QosEntryId)).
