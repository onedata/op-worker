%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file's cdmi
%%% metadata.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_metadata_req).
-author("Tomasz Lichon").

-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("annotations/include/annotations.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([get_transfer_encoding/2, set_transfer_encoding/3,
    get_cdmi_completion_status/2, set_cdmi_completion_status/3, get_mimetype/2,
    set_mimetype/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns encoding suitable for rest transfer.
%% @end
%%--------------------------------------------------------------------
-spec get_transfer_encoding(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?read_attributes]).
get_transfer_encoding(_UserCtx, FileCtx) ->
    case xattr:get_by_name(FileCtx, ?TRANSFER_ENCODING_KEY) of
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #transfer_encoding{value = Val}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets encoding suitable for rest transfer.
%% @end
%%--------------------------------------------------------------------
-spec set_transfer_encoding(user_ctx:ctx(), file_ctx:ctx(),
    xattr:transfer_encoding()) -> fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?write_attributes]).
set_transfer_encoding(_UserCtx, FileCtx, Encoding) ->
    case xattr:save(FileCtx, ?TRANSFER_ENCODING_KEY, Encoding) of
        {ok, _} ->
            fslogic_times:update_ctime(FileCtx),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?read_attributes]).
get_cdmi_completion_status(_UserCtx, FileCtx) ->
    case xattr:get_by_name(FileCtx, ?CDMI_COMPLETION_STATUS_KEY) of
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #cdmi_completion_status{value = Val}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status(user_ctx:ctx(), file_ctx:ctx(),
    xattr:cdmi_completion_status()) -> fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?write_attributes]).
set_cdmi_completion_status(_UserCtx, FileCtx, CompletionStatus) ->
    case xattr:save(FileCtx, ?CDMI_COMPLETION_STATUS_KEY, CompletionStatus) of
        {ok, _} ->
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOENT}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns mimetype of file.
%% @end
%%--------------------------------------------------------------------
-spec get_mimetype(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?read_attributes]).
get_mimetype(_UserCtx, FileCtx) ->
    case xattr:get_by_name(FileCtx, ?MIMETYPE_KEY) of
        {ok, Val} ->
            #provider_response{status = #status{code = ?OK}, provider_response = #mimetype{value = Val}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sets mimetype of file.
%% @end
%%--------------------------------------------------------------------
-spec set_mimetype(user_ctx:ctx(), file_ctx:ctx(),
    xattr:mimetype()) -> fslogic_worker:provider_response().
-check_permissions([traverse_ancestors, ?write_attributes]).
set_mimetype(_UserCtx, FileCtx, Mimetype) ->
    case xattr:save(FileCtx, ?MIMETYPE_KEY, Mimetype) of
        {ok, _} ->
            fslogic_times:update_ctime(FileCtx),
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.