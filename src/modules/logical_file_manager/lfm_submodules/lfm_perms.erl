%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module performs permissions-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_perms).

-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").

-type access_control_entity() :: #accesscontrolentity{}.

-export_type([access_control_entity/0]).

%% API
-export([set_perms/3, check_perms/3, set_acl/3, get_acl/2, remove_acl/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file.
%% @end
%%--------------------------------------------------------------------
-spec set_perms(SessId :: session:id(), FileKey :: logical_file_manager:file_key(),
    NewPerms :: file_meta:posix_permissions()) ->
    ok | logical_file_manager:error_reply().
set_perms(SessId, FileKey, NewPerms) ->
    {guid, Guid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    fslogic_utils:call_fslogic(SessId, file_request, Guid,
        #change_mode{mode = NewPerms},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Checks if current user has given permissions for given file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms(session:id(), logical_file_manager:file_key(), fslogic_worker:open_flag()) ->
    {ok, boolean()} | logical_file_manager:error_reply().
check_perms(SessId, FileKey, Flag) ->
    {guid, Guid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    case fslogic_utils:call_fslogic(SessId, provider_request, Guid,
        #check_perms{flag = Flag}, fun(_) -> ok end)
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
%% @doc
%% Returns file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec get_acl(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    {ok, [access_control_entity()]} | logical_file_manager:error_reply().
get_acl(SessId, FileKey) ->
    {guid, Guid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    fslogic_utils:call_fslogic(SessId, provider_request, Guid, #get_acl{},
        fun(#acl{value = Acl}) ->
            {ok, Acl}
        end).

%%--------------------------------------------------------------------
%% @doc
%% Updates file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec set_acl(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path(),
    EntityList :: [access_control_entity()]) ->
    ok | logical_file_manager:error_reply().
set_acl(SessId, FileKey, Acl) ->
    {guid, Guid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    fslogic_utils:call_fslogic(SessId, provider_request, Guid,
        #set_acl{acl = #acl{value = Acl}},
        fun(_) -> ok end).

%%--------------------------------------------------------------------
%% @doc
%% Removes file's Access Control List.
%% @end
%%--------------------------------------------------------------------
-spec remove_acl(SessId :: session:id(), FileKey :: fslogic_worker:file_guid_or_path()) ->
    ok | logical_file_manager:error_reply().
remove_acl(SessId, FileKey) ->
    {guid, Guid} = fslogic_uuid:ensure_guid(SessId, FileKey),
    fslogic_utils:call_fslogic(SessId, provider_request, Guid, #remove_acl{},
        fun(_) -> ok end).