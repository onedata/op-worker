%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements callback_backend_behaviour.
%%% It is used to handle RPC calls from clients with no session.
%%% @end
%%%-------------------------------------------------------------------
-module(public_rpc_backend).
-author("Lukasz Opiola").
-behaviour(rpc_backend_behaviour).

-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([handle/2]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link rpc_backend_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(FunctionId :: binary(), RequestData :: term()) ->
    ok | {ok, ResponseData :: term()} | gui_error:error_result().
% Checks if file can be downloaded (i.e. can be read by the user) and if so,
% returns download URL.
handle(<<"getPublicFileDownloadUrl">>, [{<<"fileId">>, FileId}]) ->
    PermsCheckAnswer = logical_file_manager:check_perms(
        ?GUEST_SESS_ID, {guid, FileId}, read
    ),
    case PermsCheckAnswer of
        {ok, true} ->
            Hostname = g_ctx:get_requested_hostname(),
            URL = str_utils:format_bin("https://~s/download/~s",
                [Hostname, FileId]),
            {ok, [{<<"fileUrl">>, URL}]};
        {ok, false} ->
            gui_error:report_error(<<"Permission denied">>);
        _ ->
            gui_error:internal_server_error()
    end;

handle(_, _) ->
    gui_error:report_error(<<"Not implemented">>).

