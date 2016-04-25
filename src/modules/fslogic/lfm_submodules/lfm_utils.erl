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

%% API
-export([call_fslogic/3, rm/2, isdir/2]).


%%--------------------------------------------------------------------
%% @doc
%% Sends given Request to fslogic_worker, recives answer and applies 'fuse_response' value to given function.
%% Returns the function's return value on success or error code returned in fslogic's response.
%% @end
%%--------------------------------------------------------------------
-spec call_fslogic(SessId :: session:id(), Request :: term(), OKHandle :: fun((Response :: term()) -> Return)) ->
    Return when Return :: term().
call_fslogic(SessId, Request, OKHandle) ->
    case worker_proxy:call(fslogic_worker, {fuse_request, SessId, #fuse_request{fuse_request = Request}}) of
        #fuse_response{status = #status{code = ?OK}, fuse_response = Response} ->
            OKHandle(Response);
        #fuse_response{status = #status{code = Code}} ->
            {error, Code}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes an object with all its children.
%% @end
%%--------------------------------------------------------------------
-spec rm(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    ok | logical_file_manager:error_reply().
rm(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    {guid, GUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    try
        case isdir(CTX, GUID) of
            true -> ok = rm_children(CTX, GUID, Chunk);
            false -> ok
        end,
        %% delete an object
        logical_file_manager:unlink(SessId, {guid, GUID})
    catch
        error:{badmatch, Error2} -> Error2;
        error:Error -> {error, Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if a file is directory.
%% @end
%%--------------------------------------------------------------------
-spec isdir(CTX :: #fslogic_ctx{}, GUID :: fslogic_worker:file_guid()) ->
    true | false | logical_file_manager:error_reply().
isdir(#fslogic_ctx{session_id = SessId}, GUID) ->
    case logical_file_manager:stat(SessId, {guid, GUID}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} -> true;
        {ok, _} -> false;
        X -> X
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes all children of directory with given UUID.
%% @end
%%--------------------------------------------------------------------
-spec rm_children(CTX :: #fslogic_ctx{}, GUID :: fslogic_worker:file_guid(), Chunk :: non_neg_integer())
        -> ok | logical_file_manager:error_reply().
rm_children(#fslogic_ctx{session_id = SessId} = CTX, GUID, Chunk) ->
    RemoveChild = fun({ChildGUID, _ChildName}) -> ok = rm(SessId, {guid, ChildGUID}) end,
    case logical_file_manager:ls(SessId, {guid, GUID}, 0, Chunk) of
        {ok, Children} ->
            case length(Children) of
                Chunk ->
                    lists:foreach(RemoveChild, Children),
                    rm_children(CTX, GUID, Chunk);
                _ -> %length of Children list is smaller than ls_chunk so there are no more children
                    lists:foreach(RemoveChild, Children),
                    ok
            end;
        {error, Error} -> {error, Error}
    end.
