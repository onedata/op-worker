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

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([call_fslogic/5, call_fslogic/4, rm/2, isdir/2]).

%%--------------------------------------------------------------------
%% @doc
%% Sends given Request to fslogic_worker, recives answer and applies
%% 'fuse_response' or 'provider_response' value to given function.
%% Returns the function's return value on success or error code returned
%% in fslogic's response.
%% @end
%%--------------------------------------------------------------------
-spec call_fslogic(SessId :: session:id(), RequestType :: file_request | provider_request,
    ContextEntry :: fslogic_worker:file_guid() | undefined, Request :: term(),
    OKHandle :: fun((Response :: term()) -> Return)) ->
    Return when Return :: term().
call_fslogic(SessId, file_request, ContextGuid, Request, OKHandle) ->
    call_fslogic(SessId, fuse_request,
        #file_request{context_guid = ContextGuid, file_request = Request}, OKHandle);
call_fslogic(SessId, provider_request, ContextGuid, Request, OKHandle) ->
    case worker_proxy:call(fslogic_worker, {provider_request, SessId,
        #provider_request{context_guid = ContextGuid, provider_request = Request}}) of
        #provider_response{status = #status{code = ?OK}, provider_response = Response} ->
            OKHandle(Response);
        #provider_response{status = #status{code = Code}} ->
            {error, Code}
    end.

-spec call_fslogic(SessId :: session:id(), RequestType :: fuse_request,
    Request :: term(), OKHandle :: fun((Response :: term()) -> Return)) ->
    Return when Return :: term().
call_fslogic(SessId, fuse_request, Request, OKHandle) ->
    case worker_proxy:call(fslogic_worker, {fuse_request, SessId,
        #fuse_request{fuse_request = Request}}) of
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
    {guid, Guid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    case isdir(SessId, Guid) of
        true ->
            case rm_children(SessId, Guid, 0, Chunk, ok) of
                ok ->
                    lfm_files:unlink(SessId, {guid, Guid}, false);
                Error ->
                    Error
            end;
        false ->
            lfm_files:unlink(SessId, {guid, Guid}, false)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if a file is directory.
%% @end
%%--------------------------------------------------------------------
-spec isdir(session:id(), Guid :: fslogic_worker:file_guid()) ->
    true | false | logical_file_manager:error_reply().
isdir(SessId, Guid) ->
    case lfm_attrs:stat(SessId, {guid, Guid}) of
        {ok, #file_attr{type = ?DIRECTORY_TYPE}} -> true;
        {ok, _} -> false;
        Error -> Error
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes all children of directory with given UUID.
%% @end
%%--------------------------------------------------------------------
-spec rm_children(session:id(), Guid :: fslogic_worker:file_guid(),
    Offset :: non_neg_integer(), Chunk :: non_neg_integer(), ok | {error, term()}) ->
    ok | logical_file_manager:error_reply().
rm_children(SessId, Guid, Offset, Chunk, Answer) ->
    case lfm_dirs:ls(SessId, {guid, Guid}, Offset, Chunk) of
        {ok, Children} ->
            Answers = lists:map(fun
                ({ChildGuid, _ChildName}) ->
                    rm(SessId, {guid, ChildGuid})
            end, Children),
            {FirstError, ErrorCount} = lists:foldl(fun
                (ok, {Ans, ErrorCount}) -> {Ans, ErrorCount};
                (Error, {ok, ErrorCount}) -> {Error, ErrorCount + 1};
                (_Error, {OldError, ErrorCount}) -> {OldError, ErrorCount + 1}
            end, {Answer, 0}, Answers),

            case length(Children) of
                Chunk ->
                    rm_children(SessId, Guid, ErrorCount, Chunk, FirstError);
                _ -> % no more children
                    FirstError
            end;
        Error ->
            case Answer of
                ok -> Error;
                Other -> Other
            end
    end.
