%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests concerning QoS management.
%%% @end
%%%--------------------------------------------------------------------
-module(qos_req).
-author("Michal Cwiertnia").

-include("modules/datastore/qos.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    add_qos_entry/5,
    get_qos_entry/3, 
    remove_qos_entry/3, 
    get_effective_file_qos/2,
    check_status/3
]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @equiv add_qos_entry_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec add_qos_entry(user_ctx:ctx(), file_ctx:ctx(), qos_expression:expression(),
    qos_entry:replicas_num(), qos_entry:type()) -> fslogic_worker:provider_response().
add_qos_entry(UserCtx, FileCtx, Expression, ReplicasNum, EntryType) ->
    file_ctx:assert_not_trash_dir_const(FileCtx),
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx,
        [traverse_ancestors, ?write_metadata]
    ),
    add_qos_entry_insecure(FileCtx1, Expression, ReplicasNum, EntryType).


%%--------------------------------------------------------------------
%% @equiv get_effective_file_qos_insecure/1 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_effective_file_qos(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_effective_file_qos(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_metadata]
    ),
    get_effective_file_qos_insecure(FileCtx1).


%%--------------------------------------------------------------------
%% @equiv get_qos_entry_insecure/1 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_qos_entry(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
get_qos_entry(UserCtx, FileCtx0, QosEntryId) ->
    fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_metadata]
    ),
    get_qos_entry_insecure(QosEntryId).


%%--------------------------------------------------------------------
%% @equiv remove_qos_entry_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_qos_entry(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
remove_qos_entry(UserCtx, FileCtx0, QosEntryId) ->
    fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?write_metadata]
    ),
    remove_qos_entry_insecure(UserCtx, QosEntryId).


%%--------------------------------------------------------------------
%% @equiv check_fulfillment_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec check_status(user_ctx:ctx(), file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
check_status(UserCtx, FileCtx0, QosEntryId) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_metadata]
    ),
    check_status_insecure(FileCtx1, QosEntryId).


%%%===================================================================
%%% Insecure functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new qos_entry document.
%% Delegates traverse jobs responsible for data management for given QoS entry
%% to appropriate providers. This is done by creating qos_traverse_req for
%% given QoS entry that holds information for which file and on which storage
%% traverse should be run.
%% @end
%%--------------------------------------------------------------------
-spec add_qos_entry_insecure(file_ctx:ctx(), qos_expression:expression(), qos_entry:replicas_num(), 
    qos_entry:type()) -> fslogic_worker:provider_response().
add_qos_entry_insecure(FileCtx, Expression, ReplicasNum, EntryType) ->
    % TODO: VFS-5567 for now target storage for dir is selected here and
    % does not change in qos traverse task. Have to figure out how to
    % choose different storages for subdirs and/or file if multiple storage
    % fulfilling qos are available.
    
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    
    case qos_expression:try_assigning_storages(SpaceId, Expression, ReplicasNum) of
        {true, AssignedStorages} ->
            add_possible_qos(FileCtx, Expression, ReplicasNum, EntryType, AssignedStorages);
        false ->
            add_impossible_qos(FileCtx, Expression, ReplicasNum, EntryType)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets effective QoS for given file or directory. Returns empty effective QoS
%% if there is no QoS entry that influences file/directory.
%% @end
%%--------------------------------------------------------------------
-spec get_effective_file_qos_insecure(file_ctx:ctx()) -> fslogic_worker:provider_response().
get_effective_file_qos_insecure(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    case file_qos:get_effective(FileUuid) of
        {ok, EffQos} ->
            case file_qos:is_in_trash(EffQos) of
                true ->
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #eff_qos_response{}
                    };
                false ->
                    QosEntriesList = file_qos:get_qos_entries(EffQos),
                    EntriesWithStatus = lists:foldl(fun(QosEntryId, Acc) ->
                        Status = qos_status:check(FileCtx, QosEntryId),
                        Acc#{QosEntryId => Status}
                    end, #{}, QosEntriesList),
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #eff_qos_response{
                            entries_with_status = EntriesWithStatus,
                            assigned_entries = file_qos:get_assigned_entries(EffQos)
                        }
                    }
            end;
        undefined ->
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #eff_qos_response{}
            };
        {error, _} ->
            #provider_response{status = #status{code = ?EAGAIN}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets details about QoS entry.
%% @end
%%--------------------------------------------------------------------
-spec get_qos_entry_insecure(qos_entry:id()) ->
    fslogic_worker:provider_response().
get_qos_entry_insecure(QosEntryId) ->
    case qos_entry:get(QosEntryId) of
        {ok, #document{value = QosEntry}} ->
            #provider_response{status = #status{code = ?OK}, provider_response = QosEntry};
        {error, _} ->
            #provider_response{status = #status{code = ?EAGAIN}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes qos_entry ID from file_qos documents then removes qos_entry document.
%% @end
%%--------------------------------------------------------------------
-spec remove_qos_entry_insecure(user_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
remove_qos_entry_insecure(UserCtx, QosEntryId) ->
    {ok, QosDoc} = qos_entry:get(QosEntryId),
    
    % Only root can remove internal QoS entry
    case (not qos_entry:is_internal(QosDoc)) orelse user_ctx:is_root(UserCtx) of
        true ->
            % TODO: VFS-5567 For now QoS entry is added only for file or dir
            % for which it has been added, so starting traverse is not needed.
            ok = qos_hooks:handle_entry_delete(QosDoc),
            ok = qos_entry:delete(QosEntryId),
            #provider_response{status = #status{code = ?OK}};
        false ->
            #provider_response{status = #status{code = ?EACCES}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks status of requirements defined in given qos_entry.
%% @end
%%--------------------------------------------------------------------
-spec check_status_insecure(file_ctx:ctx(), qos_entry:id()) ->
    fslogic_worker:provider_response().
check_status_insecure(FileCtx, QosEntryId) ->
    QosStatus = qos_status:check(FileCtx, QosEntryId),

    #provider_response{status = #status{code = ?OK}, provider_response = #qos_status_response{
        status = QosStatus
    }}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new qos_entry document with appropriate traverse requests.
%% @end
%%--------------------------------------------------------------------
-spec add_possible_qos(file_ctx:ctx(), qos_expression:expression(), qos_entry:replicas_num(), 
    qos_entry:type(), [storage:id()]) -> fslogic_worker:provider_response().
add_possible_qos(FileCtx, QosExpression, ReplicasNum, EntryType, Storages) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    AllTraverseReqs = qos_traverse_req:build_traverse_reqs(FileUuid, Storages),

    case qos_entry:create(
        SpaceId, FileUuid, QosExpression, ReplicasNum, EntryType, true, AllTraverseReqs
    ) of
        {ok, QosEntryId} ->
            file_qos:add_qos_entry_id(SpaceId, FileUuid, QosEntryId),
            qos_traverse_req:start_applicable_traverses(QosEntryId, SpaceId, AllTraverseReqs),
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #qos_entry_id{id = QosEntryId}
            };
        _ ->
            #provider_response{status = #status{code = ?EAGAIN}}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new qos_entry document and adds it to impossible list.
%% @end
%%--------------------------------------------------------------------
-spec add_impossible_qos(file_ctx:ctx(), qos_expression:expression(), qos_entry:replicas_num(), 
    qos_entry:type()) -> fslogic_worker:provider_response().
add_impossible_qos(FileCtx, QosExpression, ReplicasNum, EntryType) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    case qos_entry:create(SpaceId, FileUuid, QosExpression, ReplicasNum, EntryType) of
        {ok, QosEntryId} ->
            ok = file_qos:add_qos_entry_id(SpaceId, FileUuid, QosEntryId),
            #provider_response{
                status = #status{code = ?OK},
                provider_response = #qos_entry_id{id = QosEntryId}
            };
        _ ->
            #provider_response{status = #status{code = ?EAGAIN}}
    end.
