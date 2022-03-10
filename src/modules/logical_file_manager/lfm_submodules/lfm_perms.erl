%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module performs permissions-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_perms).

-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([
    set_perms/3,
    check_perms/3,
    set_acl/3,
    get_acl/2,
    remove_acl/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec set_perms(session:id(), lfm:file_key(), file_meta:posix_permissions()) ->
    ok | lfm:error_reply().
set_perms(SessId, FileKey, NewPerms) ->
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(SessId, file_request, Guid,
        #change_mode{mode = NewPerms},
        fun(_) -> ok end
    ).


%%--------------------------------------------------------------------
%% @doc
%% Checks if current user has given permissions for given file.
%% @end
%%--------------------------------------------------------------------
-spec check_perms(session:id(), lfm:file_key(), fslogic_worker:open_flag()) ->
    ok | lfm:error_reply().
check_perms(SessId, FileKey, Flag) ->
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(SessId, provider_request, Guid,
        #check_perms{flag = Flag}, fun(_) -> ok end
    ).


-spec get_acl(session:id(), lfm:file_key()) ->
    {ok, acl:acl()} | lfm:error_reply().
get_acl(SessId, FileKey) ->
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(SessId, provider_request, Guid,
        #get_acl{},
        fun(#acl{value = Acl}) -> {ok, Acl} end
    ).


-spec set_acl(session:id(), lfm:file_key(), acl:acl()) ->
    ok | lfm:error_reply().
set_acl(SessId, FileKey, Acl) ->
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(SessId, provider_request, Guid,
        #set_acl{acl = #acl{value = Acl}},
        fun(_) -> ok end
    ).


-spec remove_acl(session:id(), lfm:file_key()) ->
    ok | lfm:error_reply().
remove_acl(SessId, FileKey) ->
    Guid = lfm_file_key:resolve_file_key(SessId, FileKey, do_not_resolve_symlink),

    remote_utils:call_fslogic(SessId, provider_request, Guid,
        #remove_acl{},
        fun(_) -> ok end
    ).
