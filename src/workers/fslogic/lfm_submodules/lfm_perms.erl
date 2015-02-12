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
-include("errors.hrl").

%% API
-export([set_perms/2, check_perms/2, set_acl/2, get_acl/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Changes the permissions of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec set_perms(FileKey :: file_key(), NewPerms :: perms_octal()) -> ok | error_reply().
set_perms(Path, NewPerms) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Checks if current user has given permissions for given file.
%%
%% @end
%%--------------------------------------------------------------------
-spec check_perms(FileKey :: file_key(), PermsType :: permission_type()) -> {ok, boolean()} | error_reply().
check_perms(Path, PermType) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Returns file's Access Control List.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_acl(FileKey :: file_key()) -> {ok, [access_control_entity()]} | error_reply().
get_acl(Path) ->
    error(not_implemented).


%%--------------------------------------------------------------------
%% @doc
%% Updates file's Access Control List.
%%
%% @end
%%--------------------------------------------------------------------
-spec set_acl(FileKey :: file_key(), EntityList :: [access_control_entity()]) -> ok | error_reply().
set_acl(Path, EntityList) ->
    error(not_implemented).
