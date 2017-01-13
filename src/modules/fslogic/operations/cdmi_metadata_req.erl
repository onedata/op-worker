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
-check_permissions([{traverse_ancestors, 2}, {?read_attributes, 2}]).
get_transfer_encoding(_UserCtx, FileCtx) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case xattr:get_by_name(FileUuid, ?TRANSFER_ENCODING_KEY) of %todo pass file_ctx
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
-check_permissions([{traverse_ancestors, 2}, {?write_attributes, 2}]).
set_transfer_encoding(UserCtx, FileCtx, Encoding) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case xattr:save(FileUuid, ?TRANSFER_ENCODING_KEY, Encoding) of %todo pass file_ctx
        {ok, _} ->
            fslogic_times:update_ctime({uuid, FileUuid}, user_ctx:get_user_id(UserCtx)), %todo pass file_ctx
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
-check_permissions([{traverse_ancestors, 2}, {?read_attributes, 2}]).
get_cdmi_completion_status(_UserCtx, FileCtx) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case xattr:get_by_name(FileUuid, ?CDMI_COMPLETION_STATUS_KEY) of %todo pass file_ctx
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
-check_permissions([{traverse_ancestors, 2}, {?write_attributes, 2}]).
set_cdmi_completion_status(_UserCtx, FileCtx, CompletionStatus) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case xattr:save(FileUuid, ?CDMI_COMPLETION_STATUS_KEY, CompletionStatus) of %todo pass file_ctx
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
-check_permissions([{traverse_ancestors, 2}, {?read_attributes, 2}]).
get_mimetype(_UserCtx, FileCtx) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case xattr:get_by_name(FileUuid, ?MIMETYPE_KEY) of %todo pass file_ctx
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
-check_permissions([{traverse_ancestors, 2}, {?write_attributes, 2}]).
set_mimetype(UserCtx, FileCtx, Mimetype) ->
    {uuid, FileUuid} = file_ctx:get_uuid_entry_const(FileCtx),
    case xattr:save(FileUuid, ?MIMETYPE_KEY, Mimetype) of
        {ok, _} ->
            fslogic_times:update_ctime({uuid, FileUuid}, user_ctx:get_user_id(UserCtx)), %todo pass file_ctx
            #provider_response{status = #status{code = ?OK}};
        {error, {not_found, custom_metadata}} ->
            #provider_response{status = #status{code = ?ENOATTR}}
    end.