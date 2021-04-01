%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for managing QoS status during files reconciliation. 
%%% For more details consult `qos_status` module doc.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_reconciliation_status).
-author("Michal Stanisz").

-include("modules/datastore/qos.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([
    check/2
]).
-export([
    report_started/3, report_finished/2,
    report_file_transfer_failure/2,
    report_file_deleted/3, report_entry_deleted/2
]).

-define(RECONCILE_LINK_NAME(Path, TraverseId), <<Path/binary, "###", TraverseId/binary>>).
-define(FAILED_TRANSFER_LINK_NAME(Path), <<Path/binary, "###failed_transfer">>).
-define(RECONCILE_LINKS_KEY(QosEntryId), <<"qos_status_reconcile", QosEntryId/binary>>).

%%%===================================================================
%%% API
%%%===================================================================

-spec check(file_ctx:ctx(), qos_entry:doc()) -> boolean().
check(FileCtx, #document{key = QosEntryId}) ->
    {UuidBasedPath, _} = file_ctx:get_uuid_based_path(FileCtx),
    case qos_status_links:get_next_links(?RECONCILE_LINKS_KEY(QosEntryId), UuidBasedPath, 1, all) of
        {ok, []} -> true;
        {ok, [Path]} -> not str_utils:binary_starts_with(Path, UuidBasedPath)
    end.


-spec report_started(traverse:id(), file_ctx:ctx(), [qos_entry:id()]) -> 
    ok | {error, term()}.
report_started(TraverseId, FileCtx, QosEntries) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, References} = file_ctx:list_references_const(FileCtx),
    lists:foreach(fun(FileUuid) ->
        InternalFileCtx = file_ctx:new_by_guid(file_id:pack_guid(FileUuid, SpaceId)),
        {UuidBasedPath, _} = file_ctx:get_uuid_based_path(InternalFileCtx),
        Link = {?RECONCILE_LINK_NAME(UuidBasedPath, TraverseId), TraverseId},
        lists:foreach(fun(QosEntryId) ->
            {ok, _} = qos_status_links:add_link(SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), Link),
            ok = qos_status_links:delete_link(SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId),
                ?FAILED_TRANSFER_LINK_NAME(UuidBasedPath))
        end, QosEntries)
    end, References).


-spec report_finished(traverse:id(), file_ctx:ctx()) -> ok | {error, term()}.
report_finished(TraverseId, FileCtx) ->
    lists:foreach(fun(InternalFileCtx) ->
        FileUuid = file_ctx:get_logical_uuid_const(InternalFileCtx),
        QosEntries = case file_qos:get_effective(FileUuid) of
            undefined -> [];
            {error, {file_meta_missing, FileUuid}} -> [];
            {error, _} = Error ->
                ?warning("Error after file ~p have been reconciled: ~p", [FileUuid, Error]),
                [];
            {ok, EffectiveFileQos} ->
                file_qos:get_qos_entries(EffectiveFileQos)
        end,
        {UuidBasedPath, _} = file_ctx:get_uuid_based_path(InternalFileCtx),
        lists:foreach(fun(QosEntryId) ->
            ok = qos_status_links:delete_link(
                file_ctx:get_space_id_const(InternalFileCtx), 
                ?RECONCILE_LINKS_KEY(QosEntryId),
                ?RECONCILE_LINK_NAME(UuidBasedPath, TraverseId))
        end, QosEntries)
    end, get_references(FileCtx)).


-spec report_file_transfer_failure(file_ctx:ctx(), [qos_entry:id()]) ->
    ok | {error, term()}.
report_file_transfer_failure(FileCtx, QosEntries) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    lists:foreach(fun(InternalFileCtx) ->
        {UuidBasedPath, _} = file_ctx:get_uuid_based_path(InternalFileCtx),
        Link = {?FAILED_TRANSFER_LINK_NAME(UuidBasedPath), <<"failed_transfer">>},
        lists:foreach(fun(QosEntryId) ->
            ok = ?extract_ok(?ok_if_exists(
                qos_status_links:add_link(SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), Link)))
        end, QosEntries),
        ok = ?extract_ok(?ok_if_exists(
            qos_entry:add_to_failed_files_list(SpaceId, file_ctx:get_logical_uuid_const(InternalFileCtx))))
    end, get_references(FileCtx)).


-spec report_file_deleted(file_ctx:ctx(), qos_entry:doc(), file_ctx:ctx() | undefined) -> ok.
report_file_deleted(FileCtx, #document{key = QosEntryId} = QosEntryDoc, OriginalRootParentCtx) ->
    {ok, SpaceId} = qos_entry:get_space_id(QosEntryDoc),
    {UuidBasedPath, _} = file_ctx:get_uuid_based_path(FileCtx),

    %% TODO VFS-7133 take original parent uuid from file_meta doc
    UuidBasedPath2 = case filepath_utils:split(UuidBasedPath) of
        Tokens = [<<"/">>, SpaceId, Token | Rest] ->
            case fslogic_uuid:is_trash_dir_uuid(Token) andalso OriginalRootParentCtx =/= undefined of
                true ->
                    {OriginalParentUuidBasedPath, _} = file_ctx:get_uuid_based_path(OriginalRootParentCtx),
                    filename:join([OriginalParentUuidBasedPath | Rest]);
                false ->
                    filename:join(Tokens)
            end;
        TokensOutsideTrash ->
            filename:join(TokensOutsideTrash)
    end,
    
    % delete all reconcile links for given file
    qos_status_links:delete_all_local_links_with_prefix(
        SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), UuidBasedPath2).


-spec report_entry_deleted(od_space:id(), qos_entry:id()) -> ok.
report_entry_deleted(SpaceId, QosEntryId) ->
    % delete all reconcile links for given entry
    qos_status_links:delete_all_local_links_with_prefix(
        SpaceId, ?RECONCILE_LINKS_KEY(QosEntryId), <<"">>).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_references(file_ctx:ctx()) -> [file_ctx:ctx()].
get_references(FileCtx) ->
    {ok, References} = file_ctx:list_references_const(FileCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    LogicalUuid = file_ctx:get_logical_uuid_const(FileCtx),
    lists:map(
        fun (FileUuid) when FileUuid == LogicalUuid -> FileCtx;
            (FileUuid) -> file_ctx:new_by_guid(file_id:pack_guid(FileUuid, SpaceId))
        end,
    References).
