%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%% @doc This module performs attributes-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_attrs).

-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/lfm_internal.hrl").

-type file_attributes() :: #file_attr{}.

-export_type([file_attributes/0]).

%% API
-export([stat/1, stat/2, get_xattr/2, get_xattr/3, set_xattr/2, set_xattr/3,
    remove_xattr/2, remove_xattr/3, list_xattr/1, list_xattr/2]).
-export([get_transfer_encoding/2, set_transfer_encoding/3,
    get_cdmi_completion_status/2, set_cdmi_completion_status/3, get_mimetype/2,
    set_mimetype/3]).
%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes.
%%
%% @end
%%--------------------------------------------------------------------
-spec stat(logical_file_manager:handle()) ->
    {ok, file_attributes()} | logical_file_manager:error_reply().
stat(#lfm_handle{file_uuid = UUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}) ->
    stat(SessId, {uuid, UUID}).

-spec stat(SessId :: session:id(), FileEntry :: logical_file_manager:file_key()) ->
    {ok, file_attributes()} | logical_file_manager:error_reply().
stat(SessId, FileEntry) ->
    lfm_utils:call_fslogic(SessId, #get_file_attr{entry = FileEntry},
        fun(#file_attr{} = Attrs) ->
            {ok, Attrs}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Returns file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_xattr(Handle :: logical_file_manager:handle(), XattrName :: xattr:name()) ->
    {ok, #xattr{}} | logical_file_manager:error_reply().
get_xattr(#lfm_handle{file_uuid = UUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}, XattrName) ->
    get_xattr(SessId, {uuid, UUID}, XattrName).

-spec get_xattr(SessId :: session:id(), FileKey :: logical_file_manager:file_key(),
    XattrName :: xattr:name()) ->
    {ok, #xattr{}} | logical_file_manager:error_reply().
get_xattr(SessId, FileKey, XattrName) ->
    CTX = fslogic_context:new(SessId),
    {uuid, FileUuid} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #get_xattr{uuid = FileUuid, name = XattrName},
        fun(#xattr{} = Xattr) ->
            {ok, Xattr}
        end).


%%--------------------------------------------------------------------
%% @doc
%% Updates file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec set_xattr(Handle :: logical_file_manager:handle(), Xattr :: #xattr{}) ->
    ok | logical_file_manager:error_reply().
set_xattr(#lfm_handle{file_uuid = UUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}, Xattr) ->
    set_xattr(SessId, {uuid, UUID}, Xattr).

-spec set_xattr(SessId :: session:id(), FileKey :: logical_file_manager:file_key(),
    Xattr :: #xattr{}) ->
    ok | logical_file_manager:error_reply().
set_xattr(SessId, FileKey, Xattr) ->
    CTX = fslogic_context:new(SessId),
    {uuid, FileUuid} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #set_xattr{uuid = FileUuid, xattr = Xattr},
        fun(_) -> ok end).


%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(logical_file_manager:handle(), xattr:name()) ->
    ok | logical_file_manager:error_reply().
remove_xattr(#lfm_handle{file_uuid = UUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}, XattrName) ->
    remove_xattr(SessId, {uuid, UUID}, XattrName).

-spec remove_xattr(SessId :: session:id(), FileKey :: logical_file_manager:file_key(),
    XattrName :: xattr:name()) ->
    ok | logical_file_manager:error_reply().
remove_xattr(SessId, FileKey, XattrName) ->
    CTX = fslogic_context:new(SessId),
    {uuid, FileUuid} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #remove_xattr{uuid = FileUuid, name = XattrName},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(logical_file_manager:handle()) ->
    {ok, [xattr:name()]} | logical_file_manager:error_reply().
list_xattr(#lfm_handle{file_uuid = UUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}) ->
    list_xattr(SessId, {uuid, UUID}).

-spec list_xattr(session:id(), FileUuid :: logical_file_manager:file_key()) ->
    {ok, [xattr:name()]} | logical_file_manager:error_reply().
list_xattr(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    {uuid, FileUuid} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #list_xattr{uuid = FileUuid},
        fun(#xattr_list{names = Names}) ->
            {ok, Names}
        end).

%%--------------------------------------------------------------------
%% @doc Returns encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec get_transfer_encoding(session:id(), logical_file_manager:file_key()) ->
    {ok, xattr:transfer_encoding()} | logical_file_manager:error_reply().
get_transfer_encoding(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    {uuid, FileUuid} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #get_transfer_encoding{uuid = FileUuid},
        fun(#transfer_encoding{value = Val}) -> {ok, Val} end
    ).

%%--------------------------------------------------------------------
%% @doc Sets encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec set_transfer_encoding(session:id(), logical_file_manager:file_key(),
    xattr:transfer_encoding()) ->
    ok | logical_file_manager:error_reply().
set_transfer_encoding(SessId, FileKey, Encoding) ->
    CTX = fslogic_context:new(SessId),
    {uuid, FileUuid} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #set_transfer_encoding{uuid = FileUuid, value = Encoding},
        fun(_) -> ok end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Returns completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec get_cdmi_completion_status(session:id(), logical_file_manager:file_key()) ->
    {ok, xattr:cdmi_completion_status()} | logical_file_manager:error_reply().
get_cdmi_completion_status(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    {uuid, FileUuid} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #get_cdmi_completion_status{uuid = FileUuid},
        fun(#cdmi_completion_status{value = Val}) -> {ok, Val} end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Sets completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec set_cdmi_completion_status(session:id(), logical_file_manager:file_key(),
    xattr:cdmi_completion_status()) ->
    ok | logical_file_manager:error_reply().
set_cdmi_completion_status(SessId, FileKey, CompletionStatus) ->
    CTX = fslogic_context:new(SessId),
    {uuid, FileUuid} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #set_cdmi_completion_status{uuid = FileUuid, value = CompletionStatus},
        fun(_) -> ok end
    ).

%%--------------------------------------------------------------------
%% @doc Returns mimetype of file.
%%--------------------------------------------------------------------
-spec get_mimetype(session:id(), logical_file_manager:file_key()) ->
    {ok, xattr:mimetype()} | logical_file_manager:error_reply().
get_mimetype(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    {uuid, FileUuid} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #get_mimetype{uuid = FileUuid},
        fun(#mimetype{value = Val}) -> {ok, Val} end
    ).

%%--------------------------------------------------------------------
%% @doc Sets mimetype of file.
%%--------------------------------------------------------------------
-spec set_mimetype(session:id(), logical_file_manager:file_key(), xattr:mimetype()) ->
    ok | logical_file_manager:error_reply().
set_mimetype(SessId, FileKey, Mimetype) ->
    CTX = fslogic_context:new(SessId),
    {uuid, FileUuid} = fslogic_uuid:ensure_uuid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, #set_mimetype{uuid = FileUuid, value = Mimetype},
        fun(_) -> ok end
    ).
