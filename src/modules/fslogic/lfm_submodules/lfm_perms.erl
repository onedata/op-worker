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

-include("types.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

-define(CDMI_ACL_XATTR_KEY, <<"cdmi_acl">>).

%% API
-export([set_perms/2, check_perms/2, set_acl/3, get_acl/2, remove_acl/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Changes the permissions of a file.
%%--------------------------------------------------------------------
-spec set_perms(FileKey :: file_key(), NewPerms :: perms_octal()) -> ok | error_reply().
set_perms(_Path, _NewPerms) ->
    ok.


%%--------------------------------------------------------------------
%% @doc Checks if current user has given permissions for given file.
%%--------------------------------------------------------------------
-spec check_perms(FileKey :: file_key(), PermsType :: permission_type()) -> {ok, boolean()} | error_reply().
check_perms(_Path, _PermType) ->
    {ok, false}.


%%--------------------------------------------------------------------
%% @doc Returns file's Access Control List.
%%--------------------------------------------------------------------
-spec get_acl(fslogic_worker:ctx(), file_meta:uuid()) -> {ok, [access_control_entity()]} | error_reply().
get_acl(CTX, UUID) ->
    case lfm_attrs:get_xattr(CTX, UUID, ?CDMI_ACL_XATTR_KEY) of
        {ok, #xattr{value = Json}} ->
            Acl = fslogic_acl:from_json_fromat_to_acl(json_utils:decode(Json)), %todo store perms as integers
            {ok, Acl};
        Error ->
            Error
    end.


%%--------------------------------------------------------------------
%% @doc Updates file's Access Control List.
%%--------------------------------------------------------------------
-spec set_acl(fslogic_worker:ctx(), file_meta:uuid(), [access_control_entity()]) -> ok | error_reply().
set_acl(CTX, UUID, Acl) ->
    Json = json_utils:encode(fslogic_acl:from_acl_to_json_format(Acl)), %todo store perms as integers
    lfm_attrs:set_xattr(CTX, UUID, #xattr{name = ?CDMI_ACL_XATTR_KEY, value = Json}).


%%--------------------------------------------------------------------
%% @doc Removes file's Access Control List.
%%--------------------------------------------------------------------
-spec remove_acl(fslogic_worker:ctx(), file_meta:uuid()) -> ok | error_reply().
remove_acl(CTX, UUID) ->
    lfm_attrs:remove_xattr(CTX, UUID, ?CDMI_ACL_XATTR_KEY).
