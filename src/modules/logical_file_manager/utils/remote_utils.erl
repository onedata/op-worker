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
-export([execute_on_provider/4]).

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
    Uuid = file_id:guid_to_uuid(ContextGuid),
    case worker_proxy:call({id, fslogic_worker, Uuid}, {provider_request, SessId,
        #provider_request{context_guid = ContextGuid, provider_request = Request}}) of
        {ok, #provider_response{status = #status{code = ?OK}, provider_response = Response}} ->
            OKHandle(Response);
        {ok, #provider_response{status = #status{code = Code}}} ->
            {error, Code};
        {ok, #status{code = Code}} ->
            {error, Code}
    end.

-spec call_fslogic(SessId :: session:id(), RequestType :: fuse_request | proxyio_request,
    Request :: term(), OKHandle :: fun((Response :: term()) -> Return)) ->
    Return when Return :: term().
call_fslogic(SessId, fuse_request, #file_request{context_guid = ContextGuid} = Request, OKHandle) ->
    Uuid = file_id:guid_to_uuid(ContextGuid),
    case worker_proxy:call({id, fslogic_worker, Uuid}, {fuse_request, SessId,
        #fuse_request{fuse_request = Request}}) of
        {ok, #fuse_response{status = #status{code = ?OK}, fuse_response = Response}} ->
            OKHandle(Response);
        {ok, #fuse_response{status = #status{code = Code}}} ->
            {error, Code};
        {ok, #status{code = Code}} ->
            {error, Code}
    end;
call_fslogic(SessId, fuse_request, Request, OKHandle) ->
    case worker_proxy:call(fslogic_worker, {fuse_request, SessId,
        #fuse_request{fuse_request = Request}}) of
        {ok, #fuse_response{status = #status{code = ?OK}, fuse_response = Response}} ->
            OKHandle(Response);
        {ok, #fuse_response{status = #status{code = Code}}} ->
            {error, Code};
        {ok, #status{code = Code}} ->
            {error, Code}
    end;
call_fslogic(SessId, proxyio_request, Request = #proxyio_request{
    parameters = #{?PROXYIO_PARAMETER_FILE_GUID := FileGuid}
}, OKHandle) ->
    Uuid = file_id:guid_to_uuid(FileGuid),
    case worker_proxy:call({id, fslogic_worker, Uuid}, {proxyio_request, SessId, Request}) of
        {ok, #proxyio_response{status = #status{code = ?OK}, proxyio_response = Response}} ->
            OKHandle(Response);
        {ok, #proxyio_response{status = #status{code = Code}}} ->
            {error, Code};
        {ok, #status{code = Code}} ->
            {error, Code}
    end.


%% @TODO VFS-9435 - let fslogic_worker handle routing between providers
-spec execute_on_provider(oneprovider:id(), fslogic_worker:provider_request(), session:id(), file_id:file_guid()) ->
    {ok, provider_response_type()} | {error, term()}.
execute_on_provider(ProviderId, Req, SessId, Guid) ->
    Res = case {oneprovider:is_self(ProviderId), connection:is_provider_connected(ProviderId)} of
        {true, _} ->
            worker_proxy:call(
                {id, fslogic_worker, file_id:guid_to_uuid(Guid)},
                {provider_request, SessId, Req}
            );
        {false, true} ->
            {ok, fslogic_remote:route(user_ctx:new(?ROOT_SESS_ID), ProviderId, Req)};
        {false, false} ->
            {error, ?EAGAIN}
    end,
    
    case Res of
        {ok, #provider_response{status = #status{code = ?OK}, provider_response = ProviderResponse}} ->
            {ok, ProviderResponse};
        {ok, #provider_response{status = #status{code = Error}}} ->
            {error, Error};
        {error, _} = Error ->
            Error
    end.
