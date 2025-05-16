%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for managing file permissions (requests are delegated to middleware_worker).
%%% @end
%%%-------------------------------------------------------------------
-module(mi_file_perms).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([
    get_acl/2, set_acl/3, remove_acl/2,
    check_perms/3
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_acl(session:id(), lfm:file_key()) -> acl:acl() | no_return().
get_acl(SessionId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),
    middleware_worker:check_exec(SessionId, FileGuid, #acl_get_request{}).


-spec set_acl(session:id(), lfm:file_key(), acl:acl()) -> ok | no_return().
set_acl(SessionId, FileKey, Acl) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),
    middleware_worker:check_exec(SessionId, FileGuid, #acl_set_request{
        value = Acl
    }).


-spec remove_acl(session:id(), lfm:file_key()) -> ok | no_return().
remove_acl(SessionId, FileKey) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),
    middleware_worker:check_exec(SessionId, FileGuid, #acl_remove_request{}).


-spec check_perms(session:id(), lfm:file_key(), fslogic_worker:open_flag()) -> ok | no_return().
check_perms(SessionId, FileKey, Flag) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),
    middleware_worker:check_exec(SessionId, FileGuid, #perms_check_request{
        flag = Flag
    }).
