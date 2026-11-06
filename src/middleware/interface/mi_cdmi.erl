%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for managing CDMI related file operations (requests are delegated
%%% to middleware_worker).
%%% @end
%%%-------------------------------------------------------------------
-module(mi_cdmi).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([
    get_transfer_encoding/2,
    set_transfer_encoding/3,

    get_cdmi_completion_status/2,
    set_cdmi_completion_status/3,

    get_mimetype/2,
    set_mimetype/3
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_transfer_encoding(session:id(), lfm:file_key()) ->
    custom_metadata:transfer_encoding() | no_return().
get_transfer_encoding(SessionId, FileKey) ->
    check_exec(SessionId, FileKey, #transfer_encoding_get_request{}).


-spec set_transfer_encoding(session:id(), lfm:file_key(), custom_metadata:transfer_encoding()) ->
    ok | no_return().
set_transfer_encoding(SessionId, FileKey, Encoding) ->
    check_exec(SessionId, FileKey, #transfer_encoding_set_request{value = Encoding}).


%%--------------------------------------------------------------------
%% @doc
%% Returns completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status(session:id(), lfm:file_key()) ->
    custom_metadata:cdmi_completion_status() | no_return().
get_cdmi_completion_status(SessionId, FileKey) ->
    check_exec(SessionId, FileKey, #cdmi_completion_status_get_request{}).


%%--------------------------------------------------------------------
%% @doc
%% Sets completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status(
    session:id(),
    lfm:file_key(),
    custom_metadata:cdmi_completion_status()
) ->
    ok | no_return().
set_cdmi_completion_status(SessionId, FileKey, CompletionStatus) ->
    check_exec(SessionId, FileKey, #cdmi_completion_status_set_request{value = CompletionStatus}).


-spec get_mimetype(session:id(), lfm:file_key()) ->
    custom_metadata:mimetype() | no_return().
get_mimetype(SessionId, FileKey) ->
    check_exec(SessionId, FileKey, #mimetype_get_request{}).


-spec set_mimetype(session:id(), lfm:file_key(), custom_metadata:mimetype()) ->
    ok | no_return().
set_mimetype(SessionId, FileKey, Mimetype) ->
    check_exec(SessionId, FileKey, #mimetype_set_request{value = Mimetype}).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec check_exec(session:id(), lfm:file_key(), middleware_worker:cdmi_operation()) ->
    ok | custom_metadata:cdmi_metadata() | no_return().
check_exec(SessionId, FileKey, Request) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, resolve_symlink),
    middleware_worker:check_exec(SessionId, FileGuid, Request).
