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

