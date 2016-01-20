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
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

-type access_control_entity() :: #accesscontrolentity{}.

-export_type([access_control_entity/0]).

-define(CDMI_ACL_XATTR_KEY, <<"cdmi_acl">>).

%% API
-export([set_perms/3, check_perms/2, set_acl/3, get_acl/2, remove_acl/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Changes the permissions of a file.
%%--------------------------------------------------------------------
-spec set_perms(fslogic_worker:ctx(), file_meta:uuid(), file_meta:posix_permissions()) ->
    ok | logical_file_manager:error_reply().
set_perms(#fslogic_ctx{session_id = SessId}, UUID, NewPerms) ->
    lfm_utils:call_fslogic(SessId, #change_mode{uuid = UUID, mode = NewPerms},
        fun(_) -> ok end).


%%--------------------------------------------------------------------
%% @doc Checks if current user has given permissions for given file.
%%--------------------------------------------------------------------
-spec check_perms(FileKey :: logical_file_manager:file_key(), PermsType :: check_permissions:check_type()) ->
    {ok, boolean()} | logical_file_manager:error_reply().
check_perms(_Path, _PermType) ->
    {ok, false}.


%%--------------------------------------------------------------------
%% @doc Returns file's Access Control List.
%%--------------------------------------------------------------------
-spec get_acl(fslogic_worker:ctx(), file_meta:uuid()) ->
    {ok, [access_control_entity()]} | logical_file_manager:error_reply().
get_acl(#fslogic_ctx{session_id = SessId}, UUID) ->
    lfm_utils:call_fslogic(SessId, #get_acl{uuid = UUID},
        fun(#acl{value = Json}) ->
            Acl = fslogic_acl:from_json_fromat_to_acl(json_utils:decode(Json)), %todo store perms as integers
            {ok, Acl}
        end).


%%--------------------------------------------------------------------
%% @doc Updates file's Access Control List.
%%--------------------------------------------------------------------
-spec set_acl(fslogic_worker:ctx(), file_meta:uuid(), [access_control_entity()]) ->
    ok | logical_file_manager:error_reply().
set_acl(#fslogic_ctx{session_id = SessId}, UUID, Acl) ->
    Json = json_utils:encode(fslogic_acl:from_acl_to_json_format(Acl)), %todo store perms as integers
    lfm_utils:call_fslogic(SessId, #set_acl{uuid = UUID, acl = #acl{value = Json}},
        fun(_) -> ok end).


%%--------------------------------------------------------------------
%% @doc Removes file's Access Control List.
%%--------------------------------------------------------------------
-spec remove_acl(fslogic_worker:ctx(), file_meta:uuid()) ->
    ok | logical_file_manager:error_reply().
remove_acl(#fslogic_ctx{session_id = SessId}, UUID) ->
    lfm_utils:call_fslogic(SessId, #remove_acl{uuid = UUID},
        fun(_) -> ok end).
