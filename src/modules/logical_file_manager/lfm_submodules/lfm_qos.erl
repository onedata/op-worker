%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module performs qos-related operations of lfm_submodules.
%%% @end
%%%--------------------------------------------------------------------
-module(lfm_qos).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("common_test/include/ct.hrl").

%% API
-export([
    add_qos_entry/5,
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
    qos_entry:replicas_num(),
    qos_entry:type()
) ->
    {ok, qos_entry:id()} | lfm:error_reply().
add_qos_entry(SessId, FileKey, RawExpression, ReplicasNum, EntryType) ->
    Expression = case is_binary(RawExpression) of
        true -> qos_expression:parse(RawExpression);
        false -> RawExpression
    end,
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(SessId, provider_request, Guid,
        #add_qos_entry{
            expression = Expression, 
            replicas_num = ReplicasNum, 
            entry_type = EntryType
        },
        fun(#qos_entry_id{id = QosEntryId}) ->
            {ok, QosEntryId}
        end).


-spec get_effective_file_qos(session:id(), lfm:file_key()) ->
    {ok, {#{qos_entry:id() => qos_status:summary()}, file_qos:assigned_entries()}} | lfm:error_reply().
get_effective_file_qos(SessId, FileKey) ->
    remote_utils:call_fslogic(
        SessId,
        provider_request,
        lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
        #get_effective_file_qos{},
        fun(#eff_qos_response{entries_with_status = EntriesWithStatus, assigned_entries = AssignedEntries}) ->
            {ok, {EntriesWithStatus, AssignedEntries}}
        end
    ).


-spec get_qos_entry(session:id(), qos_entry:id()) ->
    {ok, qos_entry:record()} | lfm:error_reply().
get_qos_entry(SessId, QosEntryId) ->
    case qos_entry:get_file_guid(QosEntryId) of
        {ok, FileGuid} ->
            remote_utils:call_fslogic(
                SessId, provider_request, FileGuid,
                #get_qos_entry{id = QosEntryId},
                fun(QosEntry) -> {ok, QosEntry} end
            );
        {error, _} = Error ->
            Error
    end.


-spec remove_qos_entry(session:id(), qos_entry:id()) -> ok | lfm:error_reply().
remove_qos_entry(SessId, QosEntryId) ->
    case qos_entry:get_file_guid(QosEntryId) of
        {ok, FileGuid} ->
            remote_utils:call_fslogic(
                SessId, provider_request, FileGuid,
                #remove_qos_entry{id = QosEntryId},
                fun(_) -> ok end
            );
        {error, _} = Error ->
            Error
    end.


-spec check_qos_status(session:id(), qos_entry:id() | [qos_entry:id()]) ->
    {ok, qos_status:summary()} | lfm:error_reply().
check_qos_status(SessId, QosEntries) ->
    check_qos_status(SessId, QosEntries, undefined).


-spec check_qos_status(session:id(), qos_entry:id() | [qos_entry:id()], undefined | lfm:file_key()) ->
    {ok, qos_status:summary()} | lfm:error_reply().
check_qos_status(SessId, QosEntries, FileKey) when is_list(QosEntries) ->
    Statuses = lists:map(fun(QosEntryId) ->
        {ok, Status} = check_qos_status(SessId, QosEntryId, FileKey),
        Status
    end, QosEntries),
    {ok, qos_status:aggregate(Statuses)};

check_qos_status(SessId, QosEntryId, undefined) ->
    case qos_entry:get_file_guid(QosEntryId) of
        {error, _} = Error ->
            Error;
        {ok, QosRootFileGuid} ->
            check_qos_status(SessId, QosEntryId, ?FILE_REF(QosRootFileGuid))
    end;

check_qos_status(SessId, QosEntryId, FileKey) ->
    remote_utils:call_fslogic(
        SessId,
        provider_request,
        lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),
        #check_qos_status{qos_id = QosEntryId},
        fun(#qos_status_response{status = Status}) -> {ok, Status} end
    ).
