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
-behaviour(rpc_backend_behaviour).
-author("Lukasz Opiola").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([handle/2]).

%%%===================================================================
%%% rpc_backend_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link rpc_backend_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(FunctionId :: binary(), RequestData :: term()) ->
    ok | {ok, ResponseData :: term()} | op_gui_error:error_result().
% Checks if file can be downloaded (i.e. can be read by the user) and if so,
% returns download URL.
handle(<<"getPublicFileDownloadUrl">>, [{<<"fileId">>, AssocId}]) ->
    {_, FileId} = op_gui_utils:association_to_ids(AssocId),
    case page_file_download:get_file_download_url(?GUEST_SESS_ID, FileId) of
        {ok, URL} ->
            {ok, [{<<"fileUrl">>, URL}]};
        ?ERROR_FORBIDDEN ->
            op_gui_error:report_error(<<"Permission denied">>);
        ?ERROR_INTERNAL_SERVER_ERROR ->
            op_gui_error:internal_server_error()
    end;

handle(<<"fetchMoreDirChildren">>, Props) ->
    file_data_backend:fetch_more_dir_children(?ROOT_SESS_ID, Props);

handle(_, _) ->
    op_gui_error:report_error(<<"Not implemented">>).
