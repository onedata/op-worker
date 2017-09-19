%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Opaque cache similar to file_ctx. Supports limited functionality compared
%%% to file_ctx, it provides basic functions for files that are not supported
%%% locally. Whole API works also with normal file_ctx.
%%% @end
%%%--------------------------------------------------------------------
-module(file_partial_ctx).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

-record(file_partial_ctx, {
    canonical_path :: file_meta:path()
}).

-type ctx() :: #file_partial_ctx{} | file_ctx:ctx().

%% API
-export([new_by_guid/1, new_by_logical_path/2, new_by_canonical_path/2]).
-export([get_space_id_const/1, is_space_dir_const/1, is_user_root_dir_const/2]).
-export([get_parent/2, get_canonical_path/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new file partial context using file's GUID.
%% @end
%%--------------------------------------------------------------------
-spec new_by_guid(fslogic_worker:file_guid()) -> ctx().
new_by_guid(Guid)->
    file_ctx:new_by_guid(Guid).

%%--------------------------------------------------------------------
%% @doc
%% Creates new file partial context using file logical path.
%% The context is called partial, as the file may not be available locally and
%% its available methods are limited to getting space_id and parent.
%% @end
%%--------------------------------------------------------------------
-spec new_by_logical_path(user_ctx:ctx(), file_meta:path()) -> ctx().
new_by_logical_path(UserCtx, Path) ->
    {ok, Tokens} = fslogic_path:split_skipping_dots(Path),
    case session:is_special(user_ctx:get_session_id(UserCtx)) of
        true ->
            throw({invalid_request, <<"Path resolution requested in the context"
            " of special session. You may only operate on guids in this context.">>});
        false ->
            case Tokens of
                [<<"/">>] ->
                    UserId = user_ctx:get_user_id(UserCtx),
                    UserRootDirGuid = fslogic_uuid:user_root_dir_guid(UserId),
                    file_ctx:new_by_guid(UserRootDirGuid);
                [<<"/">>, SpaceName | Rest] ->
                    SpaceId = get_space_id_from_user_spaces(SpaceName, UserCtx),
                    #file_partial_ctx{
                        canonical_path = filename:join([<<"/">>, SpaceId | Rest])
                    }
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates new file partial context using file canonical path.
%% The context is called partial, as the file may not be available locally and
%% its available methods are limited to getting space_id and parent.
%% @end
%%--------------------------------------------------------------------
-spec new_by_canonical_path(user_ctx:ctx(), file_meta:path()) ->
    ctx().
new_by_canonical_path(UserCtx, Path) ->
    UserId = user_ctx:get_user_id(UserCtx),
    {ok, Tokens} = fslogic_path:split_skipping_dots(Path),
    case Tokens of
        [<<"/">>] ->
            UserRootDirGuid = fslogic_uuid:user_root_dir_guid(UserId),
            file_ctx:new_by_guid(UserRootDirGuid);
        [<<"/">>, SpaceId | Rest] ->
            #file_partial_ctx{
                canonical_path = filename:join([<<"/">>, SpaceId | Rest])
            }
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns parent's file partial context.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(ctx(), user_ctx:ctx() | undefined) ->
    {ParentFileCtx :: ctx(), NewFileCtx :: ctx()}.
get_parent(FilePartialCtx = #file_partial_ctx{
    canonical_path = CanonicalPath
}, UserCtx) ->
    ParentPartialCtx = new_by_canonical_path(UserCtx, filename:dirname(CanonicalPath)),
    {ParentPartialCtx, FilePartialCtx};
get_parent(FileCtx, UserCtx) ->
    file_ctx:get_parent(FileCtx, UserCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's canonical path (starting with "/SpaceId/...").
%% @end
%%--------------------------------------------------------------------
-spec get_canonical_path(ctx()) -> {file_meta:path(), ctx()}.
get_canonical_path(FilePartialCtx = #file_partial_ctx{canonical_path = CanonicalPath}) ->
    {CanonicalPath, FilePartialCtx};
get_canonical_path(FileCtx) ->
    file_ctx:get_canonical_path(FileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's space ID.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id_const(ctx()) -> od_space:id() | undefined.
get_space_id_const(#file_partial_ctx{canonical_path = Path}) ->
    case fslogic_path:split(Path) of
        [<<"/">>, SpaceId | _] ->
            SpaceId;
        _ ->
            undefined
    end;
get_space_id_const(FileCtx) ->
    file_ctx:get_space_id_const(FileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Checks if file is a space root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_space_dir_const(ctx()) -> boolean().
is_space_dir_const(#file_partial_ctx{canonical_path = Path}) ->
    case fslogic_path:split(Path) of
        [<<"/">>, _SpaceId] ->
            true;
        _ ->
            false
    end;
is_space_dir_const(FileCtx) ->
    file_ctx:is_space_dir_const(FileCtx).

%%--------------------------------------------------------------------
%% @doc
%% Checks if file is an user root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_user_root_dir_const(ctx(), user_ctx:ctx()) -> boolean().
is_user_root_dir_const(#file_partial_ctx{canonical_path = Path}, _UserCtx) ->
    Path =:= <<"/">>;
is_user_root_dir_const(FileCtx, UserCtx) ->
    file_ctx:is_user_root_dir_const(FileCtx, UserCtx).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Gets space alias and space id, in user context according to given space name.
%% @end
%%--------------------------------------------------------------------
-spec get_space_id_from_user_spaces(od_space:name(), user_ctx:ctx()) ->
    od_space:alias().
get_space_id_from_user_spaces(SpaceName, UserCtx) ->
    #document{value = #od_user{space_aliases = Spaces}} =
        user_ctx:get_user(UserCtx),
    case lists:keyfind(SpaceName, 2, Spaces) of
        false ->
            throw(?ENOENT);
        {SpaceId, SpaceName} ->
            SpaceId
    end.