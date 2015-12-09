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
%%
%% @end
%%--------------------------------------------------------------------
-spec ls_all(CTX :: #fslogic_ctx{}, Path :: binary()) -> {ok, [{file_uuid(), file_name()}]} | error_reply().
ls_all(CTX, Path) ->
    Chunk = application:get_env(?APP_NAME, cdmi_ls_chunk),
    {ok, ls_all(CTX, Path, 0, Chunk)}.

-spec ls_all(CTX :: #fslogic_ctx{}, Path :: binary(), Offset :: integer(), Chunk :: integer()) ->
    [{file_uuid(), file_name()}] | error_reply().
ls_all(CTX, Path, Offset, Chunk) ->
    SessID = CTX#fslogic_ctx.session_id,
    case logical_file_manager:ls(SessID, Path, Chunk, Offset) of
        {ok, LS} ->
            case length(LS) of
                Chunk ->
                    LS ++ ls_all(CTX, Path, Offset+Chunk, Chunk);
                _ ->
                    LS
            end;
        X -> X
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes a directory with all its children.
%%
%% @end
%%--------------------------------------------------------------------
-spec rmdir(CTX :: #fslogic_ctx{}, Path :: binary()) -> ok | error_reply().
rmdir(CTX, Path) ->
    try
        {ok, Children} = ls_all(CTX, Path),
        ChildDirs = lists:filter(fun(X) -> isdir(CTX, X) end, Children),
        ChildFiles = lists:subtract(Children, ChildDirs),
        %% recursively delete all subdirectories
%%         lists:map(fun(X) -> ok = rmdir(CTX, filename:join(Path, X)), ok end, ChildDirs),
        [rmdir(CTX, filename:join(Path, X)) || X <- ChildDirs],
        %% delete regular children
%%         lists:map(fun(X) -> ok = lfm_files:unlink(CTX, filename:join(Path, X)), ok end, ChildFiles),
        [lfm_files:unlink(CTX, filename:join(Path, X)) || X <- ChildFiles],
        %% delete directory itself
        ok = lfm_files:unlink(CTX, Path)
    catch
        %% return error reply from lfm
        error:{badmatch, X}  -> X
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if a file is directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec isdir(CTX :: #fslogic_ctx{}, Path :: binary()) -> true | false | error_reply().
isdir(CTX, Path) ->
    case lfm_attrs:stat(CTX, Path) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} -> true;
        {ok, _} -> false;
        X -> X
    end.
