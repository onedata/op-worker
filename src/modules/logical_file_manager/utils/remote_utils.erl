%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module exports utility functions for calling fslogic.
%%% @end
%%%-------------------------------------------------------------------
-module(remote_utils).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/proxyio_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([call_fslogic/5, call_fslogic/4]).

%%--------------------------------------------------------------------
%% @doc
%% Sends given Request to fslogic_worker, recives answer and applies
%% 'fuse_response' or 'provider_response' value to given function.
%% Returns the function's return value on success or error code returned
%% in fslogic's response.
%% @end
%%--------------------------------------------------------------------
-spec call_fslogic(SessId :: session:id(),
    RequestType :: file_request | provider_request | proxyio_request,
    ContextEntry :: fslogic_worker:file_guid() | undefined, Request :: term(),
    OKHandle :: fun((Response :: term()) -> Return)) ->
    Return when Return :: term().
call_fslogic(SessId, file_request, ContextGuid, Request, OKHandle) ->
    call_fslogic(SessId, fuse_request,
        #file_request{context_guid = ContextGuid, file_request = Request}, OKHandle);
call_fslogic(SessId, provider_request, ContextGuid, Request, OKHandle) ->
    case worker_proxy:call(fslogic_worker, {provider_request, SessId,
        #provider_request{context_guid = ContextGuid, provider_request = Request}}) of
        {ok, #provider_response{status = #status{code = ?OK}, provider_response = Response}} ->
            OKHandle(Response);
        {ok, #provider_response{status = #status{code = Code}}} ->
            {error, Code}
    end.

-spec call_fslogic(SessId :: session:id(), RequestType :: fuse_request | proxyio_request,
    Request :: term(), OKHandle :: fun((Response :: term()) -> Return)) ->
    Return when Return :: term().
call_fslogic(SessId, fuse_request, Request = #file_request{
    context_guid = FileGuid,
    file_request = FileReq
}, OKHandle) when is_record(FileReq, open_file) orelse is_record(FileReq, release) ->
    Node = consistent_hasing:get_node(FileGuid),

    case worker_proxy:call({fslogic_worker, Node}, {fuse_request, SessId,
        #fuse_request{fuse_request = Request}}) of
        {ok, #fuse_response{status = #status{code = ?OK}, fuse_response = Response}} ->
            OKHandle(Response);
        {ok, #fuse_response{status = #status{code = Code}}} ->
            {error, Code}
    end;
call_fslogic(SessId, fuse_request, Request, OKHandle) ->
    case worker_proxy:call(fslogic_worker, {fuse_request, SessId,
        #fuse_request{fuse_request = Request}}) of
        {ok, #fuse_response{status = #status{code = ?OK}, fuse_response = Response}} ->
            OKHandle(Response);
        {ok, #fuse_response{status = #status{code = Code}}} ->
            {error, Code}
    end;
call_fslogic(SessId, proxyio_request, Request = #proxyio_request{
    parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}
}, OKHandle) ->
    Node = consistent_hasing:get_node(FileGuid),
    case worker_proxy:call({fslogic_worker, Node}, {proxyio_request, SessId, Request}) of
        {ok, #proxyio_response{status = #status{code = ?OK}, proxyio_response = Response}} ->
            OKHandle(Response);
        {ok, #proxyio_response{status = #status{code = Code}}} ->
            {error, Code}
    end.