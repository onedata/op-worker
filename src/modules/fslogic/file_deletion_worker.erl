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
-module(file_deletion_worker).
-behaviour(worker_plugin_behaviour).

-author("Michal Wrona").

-include("global_definitions.hrl").
-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/1, handle/1, cleanup/0]).

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
    case open_file:list() of
        {ok, Docs} ->
            RemovedFiles = lists:filter(
                fun(#document{value = #open_file{is_removed = IsRemoved}}) ->
                    IsRemoved
                end, Docs),

            lists:foreach(fun(#document{key = FileUUID}) ->
                try remove_file_and_file_meta(FileUUID, ?ROOT_SESS_ID, false)
                catch
                    T:M ->
                        ?error_stacktrace("Cannot remove file - ~p:~p", [T, M])
                end
            end, RemovedFiles),

            lists:foreach(fun(#document{key = FileUUID}) ->
                ok = open_file:delete(FileUUID)
            end, Docs);
        {error, Reason} ->
            ?error_stacktrace("Cannot clean open files descriptors - ~p", [Reason])
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
handle({fslogic_deletion_request, #fslogic_ctx{session_id = SessId, space_id = SpaceId} = CTX, FileUUID, Silent}) ->
    {ok, #document{key = FileUUID} = FileDoc} = file_meta:get(FileUUID),
    {ok, ParentDoc} = file_meta:get_parent(FileDoc),

    case open_file:exists(FileUUID) of
        true ->
            {ok, ParentPath} = fslogic_path:gen_path(ParentDoc, SessId),

            Path = <<ParentPath/binary, ?DIRECTORY_SEPARATOR, ".onedata_hidden", FileUUID/binary>>,
            #fuse_response{status = #status{code = ?OK}} = fslogic_rename:rename(
                CTX, {uuid, FileUUID}, <<ParentPath/binary, ?DIRECTORY_SEPARATOR,
                    ".onedata_hidden", FileUUID/binary>>),

            case open_file:mark_to_remove(FileUUID) of
                ok ->
                    fslogic_event:emit_file_renamed(FileUUID, SpaceId, Path, SessId);
                {error, {not_found, _}} ->
                    remove_file_and_file_meta(FileUUID, SessId, Silent)
            end;
        false ->
            remove_file_and_file_meta(FileUUID, SessId, Silent)
    end,
    ok;
handle({open_file_deletion_request, FileUUID}) ->
    remove_file_and_file_meta(FileUUID, ?ROOT_SESS_ID, false);
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
%% Removes file and file meta. If parameter Silent is true, file_removal_event
%% will not be emitted.
%% @end
%%--------------------------------------------------------------------
-spec remove_file_and_file_meta(file_meta:uuid(), session:id(), boolean()) -> ok.
remove_file_and_file_meta(FileUUID, SessId, Silent) ->
    {ok, #document{value = #file_meta{uid = UID, type = Type}} = FileDoc} =
        file_meta:get(FileUUID),
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, UID),
    {ok, ParentDoc} = file_meta:get_parent(FileDoc),

    fslogic_times:update_mtime_ctime(ParentDoc, UID),

    case Type of
        ?REGULAR_FILE_TYPE ->
            delete_file_on_storage(FileUUID, SessId, SpaceUUID);
        _ -> ok
    end,
    ok = file_meta:delete(FileDoc),

    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),

    case Silent of
        true -> ok;
        false ->
            fslogic_event:emit_file_removal(
                fslogic_uuid:to_file_guid(FileUUID, SpaceId), [SessId])
    end,
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes file from storage.
%% @end
%%--------------------------------------------------------------------
-spec delete_file_on_storage(file_meta:uuid(), session:id(), file_meta:uuid())
        -> ok.
delete_file_on_storage(FileUUID, SessId, SpaceUUID) ->
    case catch fslogic_utils:get_local_file_location({uuid, FileUUID}) of
        #document{value = #file_location{} = Location} ->
            ToDelete = fslogic_utils:get_local_storage_file_locations(Location),
            Results =
                lists:map( %% @todo: run this via task manager
                    fun({StorageId, FileId}) ->
                        case storage:get(StorageId) of
                            {ok, Storage} ->
                                SFMHandle = storage_file_manager:new_handle(
                                    SessId, SpaceUUID, FileUUID, Storage, FileId),
                                case storage_file_manager:unlink(SFMHandle) of
                                    ok -> ok;
                                    {error, Reason1} ->
                                        {{StorageId, FileId}, {error, Reason1}}
                                end ;
                            {error, Reason2} ->
                                {{StorageId, FileId}, {error, Reason2}}
                        end
                    end, ToDelete),
            case Results -- [ok] of
                [] -> ok;
                Errors ->
                    lists:foreach(
                        fun({{SID0, FID0}, {error, Reason0}}) ->
                            ?error("Cannot unlink file ~p from storage ~p due to: ~p",
                                [FID0, SID0, Reason0])
                        end, Errors)
            end;
        Reason3 ->
            ?error_stacktrace("Unable to unlink file ~p from storage due to: ~p",
                [FileUUID, Reason3])
    end,
    ok.
