%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on file access
%%% control lists.
%%% Note: this module operates on referenced uuids - all operations on hardlinks
%%% are treated as operations on original file. Thus, acls are shared
%%% between hardlinks and original file.
%%% @end
%%%--------------------------------------------------------------------
-module(acl_req).
-author("Tomasz Lichon").

-include("modules/fslogic/data_access_control.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([get_acl/2, set_acl/3, remove_acl/2]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_acl(user_ctx:ctx(), file_ctx:ctx()) -> {ok, acl:acl()} | no_return().
get_acl(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?read_acl_mask)]
    ),

    {Acl, _} = file_ctx:get_acl(file_ctx:ensure_based_on_referenced_guid(FileCtx1)),
    % ACLs are kept in database without names, as they might change. Resolve the names here.
    {ok, acl:add_names(Acl)}.


-spec set_acl(user_ctx:ctx(), file_ctx:ctx(), acl:acl()) -> ok | errors:error().
set_acl(UserCtx, FileCtx0, Acl) ->
    file_ctx:assert_not_trash_dir_const(FileCtx0),
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?write_acl_mask)]
    ),
    set_acl_insecure(UserCtx, FileCtx1, Acl).


-spec remove_acl(user_ctx:ctx(), file_ctx:ctx()) -> ok | errors:error().
remove_acl(UserCtx, FileCtx0) ->
    file_ctx:assert_not_trash_dir_const(FileCtx0),
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS, ?OPERATIONS(?write_acl_mask)]
    ),
    remove_acl_insecure(UserCtx, FileCtx1).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec set_acl_insecure(user_ctx:ctx(), file_ctx:ctx(), acl:acl()) ->
    ok | errors:error().
set_acl_insecure(_UserCtx, FileCtx, Acl) ->
    {IsDir, FileCtx2} = file_ctx:is_dir(FileCtx),

    case IsDir of
        true -> acl:validate(Acl, dir);
        false -> acl:validate(Acl, file)
    end,

    % ACLs are kept in database without names, as they might change.
    % Strip the names here.
    AclWithoutNames = acl:strip_names(Acl),
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    case file_meta:update_acl(FileUuid, AclWithoutNames) of
        ok ->
            ok = permissions_cache:invalidate(),
            times_api:touch(FileCtx2, [?attr_ctime]),
            fslogic_event_emitter:emit_file_perm_changed(FileCtx2),
            ok;
        {error, not_found} ->
            ?ERR_POSIX(?err_ctx(), ?ENOENT)
    end.


%% @private
-spec remove_acl_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    ok | errors:error().
remove_acl_insecure(_UserCtx, FileCtx) ->
    FileUuid = file_ctx:get_referenced_uuid_const(FileCtx),
    case file_meta:update_acl(FileUuid, []) of
        ok ->
            ok = permissions_cache:invalidate(),
            times_api:touch(FileCtx, [?attr_ctime]),
            fslogic_event_emitter:emit_file_perm_changed(FileCtx),
            ok;
        {error, not_found} ->
            ?ERR_POSIX(?err_ctx(), ?ENOENT)
    end.
