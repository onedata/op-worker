%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%% @doc This module exports utility functions for logical_file_manager module.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_utils).
-author("Rafal Slota").

-include_lib("ctool/include/posix/errors.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("global_definitions.hrl").
-include("types.hrl").

%% API
-export([call_fslogic/3, ls_all/2, rmdir/2, isdir/2]).


%%--------------------------------------------------------------------
%% @doc
%% Sends given Request to fslogic_worker, recives answer and applies 'fuse_response' value to given function.
%% Returns the function's return value on success or error code returned in fslogic's response.
%% @end
%%--------------------------------------------------------------------
-spec call_fslogic(SessId :: session:id(), Request :: term(), OKHandle :: fun((Response :: term()) -> Return)) ->
    Return when Return :: term().
call_fslogic(SessId, Request, OKHandle) ->
    case worker_proxy:call(fslogic_worker, {fuse_request, SessId, Request}) of
        #fuse_response{status = #status{code = ?OK}, fuse_response = Response} ->
            OKHandle(Response);
        #fuse_response{status = #status{code = Code}} ->
            {error, Code}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Lists all contents of a directory.
%% @end
%%--------------------------------------------------------------------
-spec ls_all(CTX :: #fslogic_ctx{}, UUID :: file_uuid()) -> {ok, [{file_uuid(), file_name()}]} | error_reply().
ls_all(CTX, UUID) ->
    Chunk = application:get_env(?APP_NAME, max_children_per_request),
    ls_all(CTX, UUID, 0, Chunk).


%%--------------------------------------------------------------------
%% @doc
%% Deletes a directory with all its children.
%% @end
%%--------------------------------------------------------------------
-spec rmdir(CTX :: #fslogic_ctx{}, UUID :: file_uuid()) -> ok | error_reply().
rmdir(CTX, UUID) ->
    rm(CTX, UUID).

%%--------------------------------------------------------------------
%% @doc
%% Checks if a file is directory.
%% @end
%%--------------------------------------------------------------------
-spec isdir(CTX :: #fslogic_ctx{}, UUID :: file_uuid()) -> true | false | error_reply().
isdir(CTX, UUID) ->
    case lfm_attrs:stat(CTX, {uuid, UUID}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} -> true;
        {ok, _} -> false;
        X -> X
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Lists all contents of a directory from given offset.
%% @end
%%--------------------------------------------------------------------
-spec ls_all(CTX :: #fslogic_ctx{}, UUID :: file_uuid(), Offset :: integer(), Chunk :: integer()) ->
    {ok, [{file_uuid(), file_name()}]} | error_reply().
ls_all(CTX = #fslogic_ctx{session_id = SessId}, UUID, Offset, Chunk) ->
    case lfm_dirs:ls(SessId, {uuid, UUID}, Chunk, Offset) of
        {ok, LsResult} when length(LsResult =:= Chunk) ->
            case ls_all(CTX, UUID, Offset + Chunk, Chunk) of
                {ok, LsAll} -> {ok, LsResult ++ LsAll};
                Error -> Error
            end;
        {ok, LsResult} ->
            {ok, LsResult};
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes an object with all its children.
%% @end
%%--------------------------------------------------------------------

-spec rm(CTX :: #fslogic_ctx{}, UUID :: file_uuid()) -> ok | error_reply().
rm(CTX, UUID) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    try
        case isdir(CTX, UUID) of
            true -> ok = rm_children(CTX, UUID, Chunk);
            false -> ok
        end,
        %% delete an object
        lfm_files:unlink(CTX, {uuid, UUID})
    catch
        error:Error -> {error, Error}
    end.

%% TODO ls_all jest nieuzywana
-spec rm_children(CTX :: #fslogic_ctx{}, UUID :: file_uuid(), Chunk :: non_neg_integer())
        -> ok | error_reply().
rm_children(#fslogic_ctx{session_id = SessId} = CTX, UUID, Chunk) ->
    RemoveChild = fun({ChildUUID, _ChildName}) -> ok = rm(CTX, ChildUUID) end,
    case lfm_dirs:ls(SessId, {uuid, UUID}, Chunk, 0) of
        {ok, Children} ->
            case length(Children) of
                Chunk ->
                    lists:foreach(RemoveChild, Children),
                    rm_children(CTX, UUID, Chunk);
                _ -> %length of Children list is smaller then ls_chunk so there are no more children
                    lists:foreach(RemoveChild, Children),
                     ok
            end;
        {error, Error} -> {error, Error}
    end.
