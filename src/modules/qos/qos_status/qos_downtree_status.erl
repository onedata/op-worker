%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for managing QoS status downtree (top-down).
%%% For more details consult `qos_status` module doc.
%%% @end
%%%-------------------------------------------------------------------
-module(qos_downtree_status).
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
    report_file_deleted/3, report_entry_deleted/1
]).

-define(DOWNTREE_STATUS_LINK_NAME(Path, TraverseId), <<Path/binary, "###", TraverseId/binary>>).
-define(FAILED_TRANSFER_LINK_NAME(Path), <<Path/binary, "###failed_transfer">>).
-define(DOWNTREE_LINKS_KEY(QosEntryId), <<"qos_status_reconcile", QosEntryId/binary>>).

%%%===================================================================
%%% API
%%%===================================================================

-spec check(file_ctx:ctx(), qos_entry:doc()) -> boolean().
check(FileCtx, QosDoc) ->
    check_links(FileCtx, QosDoc) andalso check_traverses(FileCtx, QosDoc).


-spec report_started(traverse:id(), file_ctx:ctx(), [qos_entry:id()]) -> 
    ok | {error, term()}.
report_started(TraverseId, FileCtx, QosEntries) ->
    lists:foreach(fun(InternalFileCtx) ->
        case get_uuid_based_path(InternalFileCtx) of
            not_synced ->
                % add to traverses list so status can be properly checked after all files on path are synced
                % (for more details see check_traverses/2 and qos_status module doc).
                lists:foreach(fun(QosEntryId) ->
                    ok = qos_entry:add_to_traverses_list(QosEntryId, TraverseId,
                        file_ctx:get_logical_uuid_const(InternalFileCtx))
                end, QosEntries);
            UuidBasedPath ->
                Link = {?DOWNTREE_STATUS_LINK_NAME(UuidBasedPath, TraverseId), TraverseId},
                lists:foreach(fun(QosEntryId) ->
                    ok = qos_status_links:add_link(?DOWNTREE_LINKS_KEY(QosEntryId), Link),
                    ok = qos_status_links:delete_link(?DOWNTREE_LINKS_KEY(QosEntryId),
                        ?FAILED_TRANSFER_LINK_NAME(UuidBasedPath))
                end, QosEntries)
        end
    end, list_references(FileCtx)).


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
        case get_uuid_based_path(InternalFileCtx) of
            not_synced ->
                ok; % link was never added, so no need to delete it
            UuidBasedPath ->
                lists:foreach(fun(QosEntryId) ->
                    ok = qos_status_links:delete_link(
                        ?DOWNTREE_LINKS_KEY(QosEntryId),
                        ?DOWNTREE_STATUS_LINK_NAME(UuidBasedPath, TraverseId))
                end, QosEntries)
        end
    end, list_references(FileCtx)).


-spec report_file_transfer_failure(file_ctx:ctx(), [qos_entry:id()]) ->
    ok | {error, term()}.
report_file_transfer_failure(FileCtx, QosEntries) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    lists:foreach(fun(InternalFileCtx) ->
        case get_uuid_based_path(InternalFileCtx) of
            not_synced ->
                ok;
            UuidBasedPath ->
                Link = {?FAILED_TRANSFER_LINK_NAME(UuidBasedPath), <<"failed_transfer">>},
                lists:foreach(fun(QosEntryId) ->
                    ok = qos_status_links:add_link(?DOWNTREE_LINKS_KEY(QosEntryId), Link)
                end, QosEntries),
                ok = qos_entry:add_to_failed_files_list(SpaceId, file_ctx:get_logical_uuid_const(InternalFileCtx))
        end
    end, list_references(FileCtx)).


-spec report_file_deleted(file_ctx:ctx(), qos_entry:doc(), file_ctx:ctx() | undefined) -> ok.
report_file_deleted(FileCtx, #document{key = QosEntryId} = QosEntryDoc, OriginalRootParentCtx) ->
    {ok, SpaceId} = qos_entry:get_space_id(QosEntryDoc),
    case get_uuid_based_path(FileCtx) of
        not_synced ->
            ok;
        UuidBasedPath ->
            %% TODO VFS-7133 take original parent uuid from file_meta doc
            UuidBasedPath2 = case filepath_utils:split(UuidBasedPath) of
                Tokens = [<<"/">>, SpaceId, Token | Rest] ->
                    case fslogic_file_id:is_trash_dir_uuid(Token) andalso OriginalRootParentCtx =/= undefined of
                        true ->
                            {OriginalParentUuidBasedPath, _} = file_ctx:get_uuid_based_path(OriginalRootParentCtx),
                            filename:join([OriginalParentUuidBasedPath | Rest]);
                        false ->
                            filename:join(Tokens)
                    end;
                TokensOutsideTrash ->
                    filename:join(TokensOutsideTrash)
            end,
    
            % delete all downtree status links for given file
            qos_status_links:delete_all_local_links_with_prefix(?DOWNTREE_LINKS_KEY(QosEntryId), UuidBasedPath2)
    end.


-spec report_entry_deleted(qos_entry:id()) -> ok.
report_entry_deleted(QosEntryId) ->
    % delete all downtree status links for given entry
    qos_status_links:delete_all_local_links_with_prefix(?DOWNTREE_LINKS_KEY(QosEntryId), <<"">>).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec check_links(file_ctx:ctx(), qos_entry:doc()) -> boolean().
check_links(FileCtx, #document{key = QosEntryId}) ->
    {UuidBasedPath, _} = file_ctx:get_uuid_based_path(FileCtx),
    case qos_status_links:get_next_local_links(?DOWNTREE_LINKS_KEY(QosEntryId), UuidBasedPath, 1) of
        {ok, []} -> true;
        {ok, [Path]} -> not str_utils:binary_starts_with(Path, UuidBasedPath)
    end.


%% @private
-spec check_traverses(file_ctx:ctx(), qos_entry:doc()) -> boolean().
check_traverses(FileCtx, #document{key = QosEntryId}) ->
    qos_entry:fold_traverses(QosEntryId, fun({_TraverseId, TraverseRootUuid}, Acc) ->
        Acc andalso not is_traverse_in_subtree(FileCtx, TraverseRootUuid)
    end, true).


%% @private
-spec is_traverse_in_subtree(file_ctx:ctx(), file_meta:uuid()) -> boolean().
is_traverse_in_subtree(SubtreeRootCtx, CheckedFileUuid) ->
    SpaceId = file_ctx:get_space_id_const(SubtreeRootCtx),
    lists:any(fun(InternalFileCtx) ->
        case get_uuid_based_path(InternalFileCtx) of
            not_synced ->
                % no need to check as this subtree is disconnected from space root
                false;
            UuidBasedPath ->
                lists:member(file_ctx:get_logical_uuid_const(SubtreeRootCtx), lists:droplast(filename:split(UuidBasedPath)))
        end
    end, list_references(file_ctx:new_by_uuid(CheckedFileUuid, SpaceId))).


%% @private
-spec get_uuid_based_path(file_ctx:ctx()) -> file_meta:uuid_based_path() | not_synced.
get_uuid_based_path(FileCtx) ->
    try
        {UuidBasedPath, _} = file_ctx:get_uuid_based_path(FileCtx),
        UuidBasedPath
    catch throw:{error, {file_meta_missing, _}} ->
        not_synced
    end.


%% @private
-spec list_references(file_ctx:ctx()) -> [file_ctx:ctx()].
list_references(FileCtx) ->
    case file_ctx:list_references_ctx_const(FileCtx) of
        {ok, References} -> References;
        {error, not_found} -> []
    end.