%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for file's metadata. Implemets low-level metadata operations such as
%%% walking through file graph.
%%% @end
%%%-------------------------------------------------------------------
-module(file_perms).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/fslogic_sufix.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").
-include_lib("ctool/include/logging.hrl").

-export([
    set_active_perms_type/2,
    get_active_perms_type/1,

    set_mode/2,
    set_acl/2, get_acl/1, clear_acl/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec set_active_perms_type(file_meta:uuid() | file_ctx:ctx(),
    file_meta:permissions_type()) -> ok | {error, term()}.
set_active_perms_type(FileUuid, posix) when is_binary(FileUuid) ->
    enable_posix_mode(FileUuid);
set_active_perms_type(FileUuid, acl) when is_binary(FileUuid) ->
    enable_acl(FileUuid);
set_active_perms_type(FileCtx, PermissionsType) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    set_active_perms_type(FileUuid, PermissionsType).


-spec get_active_perms_type(file_meta:uuid() | file_ctx:ctx()) ->
    {ok, file_meta:permissions_type()} | {error, term()}.
get_active_perms_type(FileUuid) when is_binary(FileUuid) ->
    case file_meta:get({uuid, FileUuid}) of
        {ok, #document{value = #file_meta{active_permissions_type = PermsType}}} ->
            {ok, PermsType};
        {error, _} = Error ->
            Error
    end;
get_active_perms_type(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    get_active_perms_type(FileUuid).


-spec set_mode(file_meta:uuid() | file_ctx:ctx(), file_meta:posix_permissions()) ->
    ok | {error, term()}.
set_mode(FileUuid, Mode) when is_binary(FileUuid) ->
    ?extract_ok(file_meta:update({uuid, FileUuid}, fun(FileMeta = #file_meta{}) ->
        {ok, FileMeta#file_meta{
            mode = Mode,
            active_permissions_type = posix
        }}
    end));
set_mode(FileCtx, Mode) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    set_mode(FileUuid, Mode).


-spec set_acl(file_meta:uuid() | file_ctx:ctx(), acl:acl()) ->
    ok | {error, term()}.
set_acl(FileUuid, Acl) when is_binary(FileUuid) ->
    ?extract_ok(file_meta:update({uuid, FileUuid}, fun(FileMeta = #file_meta{}) ->
        {ok, FileMeta#file_meta{
            acl = Acl,
            active_permissions_type = acl
        }}
    end));
set_acl(FileCtx, Acl) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    set_acl(FileUuid, Acl).


-spec get_acl(file_meta:uuid() | file_ctx:ctx()) ->
    {ok, acl:acl()} | {error, term()}.
get_acl(FileUuid) when is_binary(FileUuid) ->
    case file_meta:get({uuid, FileUuid}) of
        {ok, #document{value = #file_meta{acl = Acl}}} ->
            {ok, Acl};
        {error, _} = Error ->
            Error
    end;
get_acl(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    get_acl(FileUuid).


-spec clear_acl(file_meta:uuid() | file_ctx:ctx()) -> ok | {error, term()}.
clear_acl(FileUuid) when is_binary(FileUuid) ->
    ?extract_ok(file_meta:update({uuid, FileUuid}, fun(FileMeta = #file_meta{}) ->
        {ok, FileMeta#file_meta{
            acl = [],
            active_permissions_type = posix
        }}
    end));
clear_acl(FileCtx) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    clear_acl(FileUuid).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec enable_posix_mode(file_meta:uuid()) -> ok | {error, term()}.
enable_posix_mode(FileUuid) ->
    ?extract_ok(file_meta:update({uuid, FileUuid}, fun(FileMeta = #file_meta{}) ->
        {ok, FileMeta#file_meta{active_permissions_type = posix}}
    end)).


%% @private
-spec enable_acl(file_meta:uuid()) -> ok | {error, term()}.
enable_acl(FileUuid) ->
    ?extract_ok(file_meta:update({uuid, FileUuid}, fun(FileMeta = #file_meta{}) ->
        {ok, FileMeta#file_meta{active_permissions_type = acl}}
    end)).
