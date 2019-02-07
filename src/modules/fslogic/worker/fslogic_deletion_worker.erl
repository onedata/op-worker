%%%--------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module implements worker_plugin_behaviour callbacks.
%%% This module handles file deletion.
%%% @end
%%%--------------------------------------------------------------------
-module(fslogic_deletion_worker).
-behaviour(worker_plugin_behaviour).

-author("Michal Wrona").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/1, handle/1, cleanup/0]).
-export([request_deletion/3, request_open_file_deletion/1,
    request_remote_deletion/1, add_deletion_link_and_remove_normal_link/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Request deletion of given file
%% @end
%%--------------------------------------------------------------------
-spec request_deletion(user_ctx:ctx(), file_ctx:ctx(), Silent :: boolean()) ->
    ok.
request_deletion(UserCtx, FileCtx, Silent) ->
    ok = worker_proxy:call(fslogic_deletion_worker,
        {fslogic_deletion_request, UserCtx, FileCtx, Silent}).

%%--------------------------------------------------------------------
%% @doc
%% Request deletion of given open file
%% @end
%%--------------------------------------------------------------------
-spec request_open_file_deletion(file_ctx:ctx()) -> ok.
request_open_file_deletion(FileCtx) ->
    ok = worker_proxy:cast(fslogic_deletion_worker,
        {open_file_deletion_request, FileCtx}).

%%--------------------------------------------------------------------
%% @doc
%% Request deletion of given file (delete triggered by dbsync).
%% @end
%%--------------------------------------------------------------------
-spec request_remote_deletion(file_ctx:ctx()) -> ok.
request_remote_deletion(FileCtx) ->
    ok = worker_proxy:call(fslogic_deletion_worker,
        {dbsync_deletion_request, FileCtx}).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    % TODO - refactor - do not use list
    case file_handles:list() of
        {ok, Docs} ->
            RemovedFiles = lists:filter(fun(#document{value = Handle}) ->
                Handle#file_handles.is_removed
            end, Docs),

            lists:foreach(fun(#document{key = FileUuid} = Doc) ->
                try
                    FileGuid = fslogic_uuid:uuid_to_guid(FileUuid),
                    FileCtx = file_ctx:new_by_guid(FileGuid),
                    UserCtx = user_ctx:new(?ROOT_SESS_ID),
                    ok = fslogic_delete:remove_file_and_file_meta(FileCtx, UserCtx, false)
                catch
                    E1:E2 ->
                        ?warning_stacktrace("Cannot remove old opened file ~p: ~p:~p",
                            [Doc, E1, E2])
                end
            end, RemovedFiles),

            lists:foreach(fun(#document{key = FileUuid}) ->
                ok = file_handles:delete(FileUuid)
            end, Docs);
        Error ->
            ?error_stacktrace("Cannot clean open files descriptors - ~p", [Error])
    end,
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck | term(),
    Result :: nagios_handler:healthcheck_response() | ok | {ok, Response} |
    {error, Reason} | pong,
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle({fslogic_deletion_request, UserCtx, FileCtx, Silent}) ->
    try
        FileUuid = file_ctx:get_uuid_const(FileCtx),
        case file_handles:exists(FileUuid) of
            true ->
                add_deletion_link_and_remove_normal_link(FileCtx, UserCtx),
                ok = file_handles:mark_to_remove(FileCtx),
                % Czemu nie bierzemy pod uwage silent?
                fslogic_event_emitter:emit_file_removed(FileCtx, [user_ctx:get_session_id(UserCtx)]);
            false ->
                fslogic_delete:remove_file_and_file_meta(FileCtx, UserCtx, Silent)
        end
    catch
        _:{badmatch, {error, not_found}} ->
            ok
    end,
    ok;
handle({open_file_deletion_request, FileCtx}) ->
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    case file_ctx:get_local_file_location_doc(FileCtx) of
        {#document{value = #file_location{storage_file_created = true}}, FileCtx} ->
            delete_file(FileCtx, UserCtx, fun delete_deletion_link_and_file/2);
        _ ->
            {FileDoc, _} = file_ctx:get_file_doc(FileCtx),
            file_meta:delete_without_link(FileDoc)
    end;
handle({dbsync_deletion_request, FileCtx}) ->
    try
        FileUuid = file_ctx:get_uuid_const(FileCtx),
        fslogic_event_emitter:emit_file_removed(FileCtx, []),
        UserCtx = user_ctx:new(?ROOT_SESS_ID),
        case file_handles:exists(FileUuid) of
            true ->
                add_deletion_link(FileCtx, UserCtx),
                ok = file_handles:mark_to_remove(FileCtx);
            false ->
                % TODO - ja zabezpieczyc synca przed reimportem (nie ma linka i file_meta)?
                delete_file(FileCtx, UserCtx, fun check_and_maybe_delete_storage_file/2)
        end
    catch
        _:{badmatch, {error, not_found}} ->
            ok
    end,
    ok;
handle(_Request) ->
    ?log_bad_request(_Request),
    {error, wrong_request}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% If file to remove and existing file are not the same file then does nothing. 
%% Otherwise removes storage file.
%% @end
%%-------------------------------------------------------------------
-spec check_and_maybe_delete_storage_file(file_ctx:ctx(), user_ctx:ctx()) ->
    ok | {error, term()}.
check_and_maybe_delete_storage_file(FileCtx, UserCtx) ->
    {#document{key = Uuid, value = #file_meta{name = Name}},
        FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
    try
        {ParentCtx, FileCtx3} = file_ctx:get_parent(FileCtx2, UserCtx),
        {ParentDoc, _} = file_ctx:get_file_doc_including_deleted(ParentCtx),
        % TODO - zamiast tego chyba powinnismy pobrac file_location i sprawdzic,
            % bo plik moglbyc na storage'u pod inna nazwa
            % czemu tego nie sprawdzamy ja nie jest kasowanie z dbsync tylko otwartego pliku?
            % czemy sfm_utils moze skasowac nie swoj plik?
        case fslogic_path:resolve(ParentDoc, <<"/", Name/binary>>) of
            {ok, #document{key = Uuid2}} when Uuid2 =/= Uuid ->
                ok;
            _ ->
                sfm_utils:recursive_delete(FileCtx3, UserCtx)
        end
    catch
        E1:E2 ->
            % Debug - parent could be deleted before
            ?debug_stacktrace("Cannot check parent during delete ~p: ~p:~p",
                [FileCtx, E1, E2]),
            sfm_utils:delete_storage_file_without_location(FileCtx, UserCtx)
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Tries to delete storage file using given function, logs expected errors 
%% and removes file document from metadata
%% @end
%%-------------------------------------------------------------------
-spec delete_file(file_ctx:ctx(), user_ctx:ctx(),
    fun((file_ctx:ctx(), user_ctx:ctx()) -> ok)) -> ok | {error, term()}.
delete_file(FileCtx, UserCtx, DeleteStorageFile) ->
    try
        ok = DeleteStorageFile(FileCtx, UserCtx)
    catch
        _:{badmatch, {error, not_found}} ->
            ?error_stacktrace("Cannot delete file at storage ~p", [FileCtx]),
            ok;
        _:{badmatch, {error, enoent}} ->
            ?debug_stacktrace("Cannot delete file at storage ~p", [FileCtx]),
            ok;
        _:{badmatch, {error, erofs}} ->
            ?warning_stacktrace("Cannot delete file at storage ~p", [FileCtx]),
            ok
    end,
    {FileDoc, _FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
    file_meta:delete_without_link(FileDoc).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function is used to delete the file which deletion was delayed
%% due to existing handles. It removes the file and its deletion_link,
%% @end
%%-------------------------------------------------------------------
-spec delete_deletion_link_and_file(file_ctx:ctx(), user_ctx:ctx()) -> ok | {error, term()}.
delete_deletion_link_and_file(FileCtx, UserCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {ParentGuid, FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
    ParentUuid = fslogic_uuid:guid_to_uuid(ParentGuid),
    {DeletionLinkName, FileCtx3} = file_deletion_link_name(FileCtx2),
    Scope = file_ctx:get_space_id_const(FileCtx3),
    ok =  sfm_utils:recursive_delete(FileCtx3, UserCtx),
    ok = file_meta:delete_child_link(ParentUuid, Scope, FileUuid, DeletionLinkName).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% This function adds a deletion_link for the file that is to be deleted.
%% It also deletes normal link from parent to the file.
%% @end
%%-------------------------------------------------------------------
-spec add_deletion_link_and_remove_normal_link(file_ctx:ctx(), user_ctx:ctx()) -> ok.
add_deletion_link_and_remove_normal_link(FileCtx, UserCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {ParentGuid, FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
    {FileName, FileCtx3} = file_ctx:get_aliased_name(FileCtx2, UserCtx),
    ParentUuid = fslogic_uuid:guid_to_uuid(ParentGuid),
    {DeletionLinkName, FileCtx4} = file_deletion_link_name(FileCtx3),
    Scope = file_ctx:get_space_id_const(FileCtx4),
    ok = file_meta:add_child_link(ParentUuid, Scope, DeletionLinkName, FileUuid),
    ok = file_meta:delete_child_link(ParentUuid, Scope, FileUuid, FileName).

add_deletion_link(FileCtx, UserCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    {ParentGuid, FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
    ParentUuid = fslogic_uuid:guid_to_uuid(ParentGuid),
    {DeletionLinkName, FileCtx3} = file_deletion_link_name(FileCtx2),
    Scope = file_ctx:get_space_id_const(FileCtx3),
    ok = file_meta:add_child_link(ParentUuid, Scope, DeletionLinkName, FileUuid).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Utility function that returns deletion_link name for given file.
%% @end
%%-------------------------------------------------------------------
-spec file_deletion_link_name(file_ctx:ctx()) -> {file_meta:name(), file_ctx:ctx()}.
file_deletion_link_name(FileCtx) ->
    {StorageFileId, FileCtx2} = file_ctx:get_storage_file_id(FileCtx),
    BaseName = filename:basename(StorageFileId),
    {?FILE_DELETION_LINK_NAME(BaseName), FileCtx2}.
