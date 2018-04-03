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
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/1, handle/1, cleanup/0]).
-export([request_deletion/3, request_open_file_deletion/1,
    request_remote_deletion/1]).

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
%% Request deletion of given file (delete triggered by dbsync_
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

            lists:foreach(fun(#document{key = FileUuid}) ->
                FileGuid = fslogic_uuid:uuid_to_guid(FileUuid),
                FileCtx = file_ctx:new_by_guid(FileGuid),
                UserCtx = user_ctx:new(?ROOT_SESS_ID),
                ok = fslogic_delete:remove_file_and_file_meta(FileCtx, UserCtx, false)
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
                ok = file_handles:mark_to_remove(FileCtx),
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
    {#document{key = Uuid, value = #file_meta{name = Name}} = FileDoc,
        FileCtx2} = file_ctx:get_file_doc_including_deleted(FileCtx),
    try
        UserCtx = user_ctx:new(?ROOT_SESS_ID),

        try
            {ParentCtx, FileCtx3} = file_ctx:get_parent(FileCtx2, UserCtx),
            {ParentDoc, _} = file_ctx:get_file_doc(ParentCtx),
            ok = case fslogic_path:resolve(ParentDoc, <<"/", Name/binary>>) of
                {ok, #document{key = Uuid2}} when Uuid2 =/= Uuid ->
                    ok;
                _ ->
                    sfm_utils:delete_storage_file_without_location(FileCtx3, UserCtx)
            end
        catch
            E2:E2 ->
                % Debug - parent could be deleted before
                ?debug_stacktrace("Cannot check parrent during delete ~p: ~p:~p",
                    [FileCtx, E2, E2]),
                sfm_utils:delete_storage_file_without_location(FileCtx2, UserCtx)
        end
    catch
        _:{badmatch, {error, not_found}} ->
            ?error_stacktrace("Cannot delete file at storage ~p", [FileCtx]),
            ok;
        _:{badmatch, {error, enoent}} ->
            ?debug_stacktrace("Cannot delete file at storage ~p", [FileCtx]),
            ok
    end,

    file_meta:delete_without_link(FileDoc);
handle({dbsync_deletion_request, FileCtx}) ->
    try
        FileUuid = file_ctx:get_uuid_const(FileCtx),
        fslogic_event_emitter:emit_file_removed(FileCtx, []),
        case file_handles:exists(FileUuid) of
            true ->
                ok = file_handles:mark_to_remove(FileCtx);
            false ->
                handle({open_file_deletion_request, FileCtx})
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


