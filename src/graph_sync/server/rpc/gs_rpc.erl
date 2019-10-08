%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles gs rpc.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_rpc).
-author("Bartosz Walkowicz").

-include("op_logic.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([handle/3]).


%%%===================================================================
%%% API
%%%===================================================================


-spec handle(aai:auth(), gs_protocol:rpc_function(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
handle(Auth, RpcFun, Data) ->
    try
        handle_internal(Auth, RpcFun, Data)
    catch
        throw:Error = {error, _} ->
            Error;
        Type:Reason ->
            ?error_stacktrace("Unexpected error while processing gs file rpc "
                              "request - ~p:~p", [Type, Reason]),
            ?ERROR_INTERNAL_SERVER_ERROR
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec handle_internal(aai:auth(), gs_protocol:rpc_function(), gs_protocol:rpc_args()) ->
    gs_protocol:rpc_result().
handle_internal(Auth, <<"getDirChildren">>, Data) ->
    file_gs_rpc:ls(Auth, Data);
handle_internal(Auth, <<"initializeFileUpload">>, Data) ->
    file_gs_rpc:register_file_upload(Auth, Data);
handle_internal(Auth, <<"finalizeFileUpload">>, Data) ->
    file_gs_rpc:deregister_file_upload(Auth, Data);
handle_internal(Auth, <<"getFileDownloadUrl">>, Data) ->
    file_gs_rpc:get_file_download_url(Auth, Data);
handle_internal(Auth, <<"moveFile">>, Data) ->
    file_gs_rpc:move(Auth, Data);
handle_internal(Auth, <<"copyFile">>, Data) ->
    file_gs_rpc:copy(Auth, Data);
handle_internal(_, _, _) ->
    ?ERROR_RPC_UNDEFINED.
