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
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("common_test/include/ct.hrl").

%% API
-export([add_qos_entry/5, get_qos_entry/2, remove_qos_entry/3, get_effective_file_qos/2,
    check_qos_fulfilled/2, check_qos_fulfilled/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds new qos_entry for file or directory, returns qos_entry ID.
%% @end
%%--------------------------------------------------------------------
-spec add_qos_entry(session:id(), lfm:file_key(), qos_expression:raw(),
    qos_entry:replicas_num(), module()) -> {ok, qos_entry:id()} | lfm:error_reply().
add_qos_entry(SessId, FileKey, Expression, ReplicasNum, CallbackModule) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, Guid,
        #add_qos_entry{
            expression = Expression, 
            replicas_num = ReplicasNum, 
            callback_module = CallbackModule
        },
        fun(#qos_entry_id{id = QosEntryId}) ->
            {ok, QosEntryId}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Gets effective QoS for file or directory.
%% @end
%%--------------------------------------------------------------------
-spec get_effective_file_qos(session:id(), lfm:file_key()) ->
    {ok, {[qos_entry:id()], file_qos:assigned_entries()}} | lfm:error_reply().
get_effective_file_qos(SessId, FileKey) ->
    {guid, Guid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, Guid, #get_effective_file_qos{},
        fun(#effective_file_qos{qos_entries = QosEntries, assigned_entries = AssignedEntries}) ->
            {ok, {QosEntries, AssignedEntries}}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Get details of specified qos_entry.
%% @end
%%--------------------------------------------------------------------
-spec get_qos_entry(session:id(), qos_entry:id()) ->
    {ok, qos_entry:record()} | lfm:error_reply().
get_qos_entry(SessId, QosEntryId) ->
    case qos_entry:get_file_guid(QosEntryId) of
        {ok, FileGuid} ->
            remote_utils:call_fslogic(SessId, provider_request, FileGuid, #get_qos_entry{id = QosEntryId},
                fun(QosEntry) -> {ok, QosEntry} end);
        {error, _} = Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% Remove qos_entry.
%% @end
%%--------------------------------------------------------------------
-spec remove_qos_entry(session:id(), qos_entry:id(), boolean()) -> ok | lfm:error_reply().
remove_qos_entry(SessId, QosEntryId, Force) ->
    case qos_entry:get_file_guid(QosEntryId) of
        {ok, FileGuid} ->
            remote_utils:call_fslogic(SessId, provider_request, FileGuid, 
                #remove_qos_entry{id = QosEntryId, force = Force},
                fun(_) -> ok end);
        {error, _} = Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv
%% check_qos_fulfilled(SessId, QosEntries, undefined)
%% @end
%%--------------------------------------------------------------------
-spec check_qos_fulfilled(session:id(), qos_entry:id() | [qos_entry:id()]) -> {ok, boolean()}.
check_qos_fulfilled(SessId, QosEntries) ->
    check_qos_fulfilled(SessId, QosEntries, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Check if QoS requirements defined in qos_entry document/documents are fulfilled.
%% @end
%%--------------------------------------------------------------------
-spec check_qos_fulfilled(session:id(), qos_entry:id() | [qos_entry:id()],
    lfm:file_key() | undefined) -> {ok, boolean()} | lfm:error_reply().
check_qos_fulfilled(SessId, QosEntries, FileKey) when is_list(QosEntries) ->
    FulfillmentStatus = lists:all(fun(QosEntryId) ->
        {ok, true} == check_qos_fulfilled(SessId, QosEntryId, FileKey)
    end, QosEntries),
    {ok, FulfillmentStatus};
check_qos_fulfilled(SessId, QosEntryId, undefined) ->
    case qos_entry:get_file_guid(QosEntryId) of
        {error, _} = Error ->
            Error;
        {ok, QosRootFileGuid} ->
            check_qos_fulfilled(SessId, QosEntryId, {guid, QosRootFileGuid})
    end;
check_qos_fulfilled(SessId, QosEntryId, FileKey) ->
    {guid, FileGuid} = guid_utils:ensure_guid(SessId, FileKey),
    remote_utils:call_fslogic(SessId, provider_request, FileGuid, #check_qos_fulfillment{qos_id = QosEntryId},
        fun(#qos_fulfillment{fulfilled = FulfillmentStatus}) -> {ok, FulfillmentStatus} end).

