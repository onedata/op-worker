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
-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/1, handle/1, cleanup/0]).
-export([request_deletion/3, request_open_file_deletion/1]).

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
    {ok, Docs} = file_handles:list(),
    RemovedFiles = lists:filter(fun(#document{value = Handle}) ->
        Handle#file_handles.is_removed
    end, Docs),

    lists:foreach(fun(#document{key = FileUuid}) ->
        FileGuid = fslogic_uuid:uuid_to_guid(FileUuid),
        FileCtx = file_ctx:new_by_guid(FileGuid),
        ok = remove_file_and_file_meta(FileCtx, ?ROOT_SESS_ID, false)
    end, RemovedFiles),

    lists:foreach(fun(#document{key = FileUuid}) ->
        ok = file_handles:delete(FileUuid)
    end, Docs),
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
    SessId = user_ctx:get_session_id(UserCtx),
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),

    case file_handles:exists(FileUuid) of
        true ->
            UserId = user_ctx:get_user_id(UserCtx),
            {ParentFile, FileCtx2} = file_ctx:get_parent(FileCtx, UserId),
            NewName = <<?HIDDEN_FILE_PREFIX, FileUuid/binary>>,

            #fuse_response{status = #status{code = ?OK}} = rename_req:rename(
                UserCtx, FileCtx2, ParentFile, NewName),
            ok = file_handles:mark_to_remove(FileUuid),
            fslogic_event:emit_file_renamed_to_client(FileCtx2, NewName, SessId);
        false ->
            remove_file_and_file_meta(FileCtx, SessId, Silent)
    end,
    ok;
handle({open_file_deletion_request, FileCtx}) ->
    remove_file_and_file_meta(FileCtx, ?ROOT_SESS_ID, false);
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes file and file meta. If parameter Silent is true, file_removed_event
%% will not be emitted.
%% @end
%%--------------------------------------------------------------------
-spec remove_file_and_file_meta(file_ctx:ctx(), session:id(), boolean()) -> ok.
remove_file_and_file_meta(FileCtx, SessId, Silent) ->
    {FileDoc = #document{
        value = #file_meta{
            type = Type,
            shares = Shares
        }
    }, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {ok, UserId} = session:get_user_id(SessId),
    {ParentCtx, FileCtx3} = file_ctx:get_parent(FileCtx2, UserId),
    ok = delete_shares(SessId, Shares),
    fslogic_times:update_mtime_ctime(ParentCtx, UserId),
    case Type of
        ?REGULAR_FILE_TYPE ->
            delete_file_on_storage(FileCtx3, SessId);
        _ -> ok
    end,
    ok = file_meta:delete(FileDoc),
    case Silent of
        true ->
            ok;
        false ->
            fslogic_event:emit_file_removed(FileCtx3, [SessId]),
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes file from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_file_on_storage(file_ctx:ctx(), session:id()) -> ok.
delete_file_on_storage(FileCtx, SessId) ->
    {[#document{
        value = #file_location{
            storage_id = StorageId,
            file_id = FileId
        }
    }], FileCtx2} = file_ctx:get_local_file_location_docs(FileCtx),
    {ok, Storage} = storage:get(StorageId),
    SpaceDirUuid = file_ctx:get_space_dir_uuid_const(FileCtx2),
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx2),
    SFMHandle = storage_file_manager:new_handle(SessId, SpaceDirUuid, FileUuid, Storage, FileId),
    ok = storage_file_manager:unlink(SFMHandle).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes given shares from oz and db.
%% @end
%%--------------------------------------------------------------------
-spec delete_shares(session:id(), [od_share:id()]) -> ok | no_return().
delete_shares(_SessId, []) ->
    ok;
delete_shares(SessId, Shares) ->
    {ok, Auth} = session:get_auth(SessId),
    [ok = share_logic:delete(Auth, ShareId) || ShareId <- Shares],
    ok.
