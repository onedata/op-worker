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

-include("types.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([stat/2, get_xattr/3, set_xattr/3, remove_xattr/3, list_xattr/2]).
-export([get_transfer_encoding/2, set_transfer_encoding/3, get_completion_status/2,
    set_completion_status/3, get_mimetype/2, set_mimetype/3]).
%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns file attributes.
%%
%% @end
%%--------------------------------------------------------------------
-spec stat(fslogic_worker:ctx(), FileEntry :: file_key()) ->
    {ok, file_attributes()} | error_reply().
stat(#fslogic_ctx{session_id = SessId}, FileEntry) ->
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
-spec get_xattr(fslogic_worker:ctx(), FileUuid :: file_uuid(), XattrName :: xattr:name()) ->
    {ok, #xattr{}} | error_reply().
get_xattr(#fslogic_ctx{session_id = SessId}, FileUuid, XattrName) ->
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
-spec set_xattr(fslogic_worker:ctx(), FileUuid :: file_uuid(), Xattr :: #xattr{}) ->
    ok | error_reply().
set_xattr(#fslogic_ctx{session_id = SessId}, FileUuid, Xattr) ->
    lfm_utils:call_fslogic(SessId, #set_xattr{uuid = FileUuid, xattr = Xattr},
        fun(_) -> ok end).


%%--------------------------------------------------------------------
%% @doc
%% Removes file's extended attribute by key.
%%
%% @end
%%--------------------------------------------------------------------
-spec remove_xattr(fslogic_worker:ctx(), FileUuid :: file_uuid(), XattrName :: xattr:name()) ->
    ok | error_reply().
remove_xattr(#fslogic_ctx{session_id = SessId}, FileUuid, XattrName) ->
    lfm_utils:call_fslogic(SessId, #remove_xattr{uuid = FileUuid, name = XattrName},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Returns complete list of extended attributes of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec list_xattr(fslogic_worker:ctx(), FileUuid :: file_uuid()) ->
    {ok, [xattr:name()]} | error_reply().
list_xattr(#fslogic_ctx{session_id = SessId}, FileUuid) ->
    lfm_utils:call_fslogic(SessId, #list_xattr{uuid = FileUuid},
        fun(#xattr_list{names = Names}) ->
            {ok, Names}
        end).

%%--------------------------------------------------------------------
%% @doc Returns encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec get_transfer_encoding(session:id(), file_key()) ->
    {ok, transfer_encoding()} | error_reply().
get_transfer_encoding(#fslogic_ctx{session_id = SessId}, FileUuid) ->
    lfm_utils:call_fslogic(SessId, #get_transfer_encoding{uuid = FileUuid},
        fun(#transfer_encoding{value = Val}) -> {ok, Val} end
    ).

%%--------------------------------------------------------------------
%% @doc Sets encoding suitable for rest transfer.
%%--------------------------------------------------------------------
-spec set_transfer_encoding(session:id(), file_key(), transfer_encoding()) ->
    ok | error_reply().
set_transfer_encoding(#fslogic_ctx{session_id = SessId}, FileUuid, Encoding) ->
    lfm_utils:call_fslogic(SessId, #set_transfer_encoding{uuid = FileUuid, value = Encoding},
        fun(_) -> ok end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Returns completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec get_completion_status(session:id(), file_key()) ->
    {ok, completion_status()} | error_reply().
get_completion_status(#fslogic_ctx{session_id = SessId}, FileUuid) ->
    lfm_utils:call_fslogic(SessId, #get_completion_status{uuid = FileUuid},
        fun(#completion_status{value = Val}) -> {ok, Val} end
    ).

%%--------------------------------------------------------------------
%% @doc
%% Sets completion status, which tells if the file is under modification by
%% cdmi at the moment.
%% @end
%%--------------------------------------------------------------------
-spec set_completion_status(session:id(), file_key(), completion_status()) ->
    ok | error_reply().
set_completion_status(#fslogic_ctx{session_id = SessId}, FileUuid, CompletionStatus) ->
    lfm_utils:call_fslogic(SessId, #set_completion_status{uuid = FileUuid, value = CompletionStatus},
        fun(_) -> ok end
    ).

%%--------------------------------------------------------------------
%% @doc Returns mimetype of file.
%%--------------------------------------------------------------------
-spec get_mimetype(session:id(), file_key()) ->
    {ok, mimetype()} | error_reply().
get_mimetype(#fslogic_ctx{session_id = SessId}, FileUuid) ->
    lfm_utils:call_fslogic(SessId, #get_mimetype{uuid = FileUuid},
        fun(#mimetype{value = Val}) -> {ok, Val} end
    ).

%%--------------------------------------------------------------------
%% @doc Sets mimetype of file.
%%--------------------------------------------------------------------
-spec set_mimetype(session:id(), file_key(), mimetype()) ->
    ok | error_reply().
set_mimetype(#fslogic_ctx{session_id = SessId}, FileUuid, Mimetype) ->
    lfm_utils:call_fslogic(SessId, #set_mimetype{uuid = FileUuid, value = Mimetype},
        fun(_) -> ok end
    ).
