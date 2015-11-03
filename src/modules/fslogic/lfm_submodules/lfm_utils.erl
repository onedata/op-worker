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

-include("proto/oneclient/fuse_messages.hrl").

%% API
-export([call_fslogic/3]).


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