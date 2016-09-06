%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%% @doc This module performs permissions-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_perms).

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/lfm_internal.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

-type access_control_entity() :: #accesscontrolentity{}.

-export_type([access_control_entity/0]).

%% API
-export([set_perms/3, check_perms/3, set_acl/2, set_acl/3, get_acl/1, get_acl/2,
remove_acl/1, remove_acl/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Changes the permissions of a file.
%%--------------------------------------------------------------------
-spec set_perms(SessId :: session:id(), FileKey :: logical_file_manager:file_key(),
    NewPerms :: file_meta:posix_permissions()) ->
    ok | logical_file_manager:error_reply().
set_perms(SessId, FileKey, NewPerms) ->
    CTX = fslogic_context:new(SessId),
    {guid, GUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, file_request, GUID,
        #change_mode{mode = NewPerms},
        fun(_) -> ok end).


%%--------------------------------------------------------------------
%% @doc Checks if current user has given permissions for given file.
%%--------------------------------------------------------------------
-spec check_perms(session:id(), logical_file_manager:file_key(), helpers:open_mode()) ->
    {ok, boolean()} | logical_file_manager:error_reply().
check_perms(SessId, FileKey, PermType) ->
    CTX = fslogic_context:new(SessId),
    {guid, GUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    case lfm_utils:call_fslogic(SessId, provider_request, GUID,
        #check_perms{flags = PermType}, fun(_) -> ok end)
    of
        ok ->
            {ok, true};
        {error, ?EACCES} ->
            {ok, false};
        {error, ?EPERM} ->
            {ok, false};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc Returns file's Access Control List.
%%--------------------------------------------------------------------
-spec get_acl(Handle :: logical_file_manager:handle()) ->
    {ok, [access_control_entity()]} | logical_file_manager:error_reply().
get_acl(#lfm_handle{file_guid = GUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}) ->
    get_acl(SessId, {guid, GUID}).

-spec get_acl(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, [access_control_entity()]} | logical_file_manager:error_reply().
get_acl(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    {guid, GUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, provider_request, GUID, #get_acl{},
        fun(#acl{value = Json}) ->
            Acl = fslogic_acl:from_json_fromat_to_acl(json_utils:decode(Json)), %todo store perms as integers
            {ok, Acl}
        end).


%%--------------------------------------------------------------------
%% @doc Updates file's Access Control List.
%%--------------------------------------------------------------------
-spec set_acl(logical_file_manager:handle(), EntityList :: [access_control_entity()]) ->
    ok | logical_file_manager:error_reply().
set_acl(#lfm_handle{file_guid = GUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}, EntityList) ->
    set_acl(SessId, {guid, GUID}, EntityList).

-spec set_acl(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    EntityList :: [access_control_entity()]) ->
    ok | logical_file_manager:error_reply().
set_acl(SessId, FileKey, Acl) ->
    CTX = fslogic_context:new(SessId),
    {guid, GUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    Json = json_utils:encode(fslogic_acl:from_acl_to_json_format(Acl)), %todo store perms as integers
    lfm_utils:call_fslogic(SessId, provider_request, GUID,
        #set_acl{acl = #acl{value = Json}},
        fun(_) -> ok end).


%%--------------------------------------------------------------------
%% @doc Removes file's Access Control List.
%%--------------------------------------------------------------------
-spec remove_acl(logical_file_manager:handle()) -> ok | logical_file_manager:error_reply().
remove_acl(#lfm_handle{file_guid = GUID, fslogic_ctx = #fslogic_ctx{session_id = SessId}}) ->
    remove_acl(SessId, {guid, GUID}).

-spec remove_acl(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    ok | logical_file_manager:error_reply().
remove_acl(SessId, FileKey) ->
    CTX = fslogic_context:new(SessId),
    {guid, GUID} = fslogic_uuid:ensure_guid(CTX, FileKey),
    lfm_utils:call_fslogic(SessId, provider_request, GUID, #remove_acl{},
        fun(_) -> ok end).
