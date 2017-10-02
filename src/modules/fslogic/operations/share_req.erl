%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests modifying shares.
%%% @end
%%%--------------------------------------------------------------------
-module(share_req).
-author("Tomasz Lichon").

-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([create_share/3, remove_share/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv create_share_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec create_share(user_ctx:ctx(), file_ctx:ctx(), od_share:name()) ->
    fslogic_worker:provider_response().
create_share(UserCtx, FileCtx, Name) ->
    check_permissions:execute(
        [traverse_ancestors],
        [UserCtx, FileCtx, Name],
        fun create_share_insecure/3).

%%--------------------------------------------------------------------
%% @equiv remove_share_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_share(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
remove_share(UserCtx, FileCtx) ->
    check_permissions:execute(
        [traverse_ancestors],
        [UserCtx, FileCtx],
        fun remove_share_insecure/2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Shares a given file.
%% @end
%%--------------------------------------------------------------------
-spec create_share_insecure(user_ctx:ctx(), file_ctx:ctx(), od_share:name()) ->
    fslogic_worker:provider_response().
create_share_insecure(UserCtx, FileCtx, Name) ->
    Guid = file_ctx:get_guid_const(FileCtx),
    ShareId = datastore_utils:gen_key(),
    ShareGuid = fslogic_uuid:guid_to_share_guid(Guid, ShareId),
    SessionId = user_ctx:get_session_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, _} = share_logic:create(SessionId, ShareId, Name, SpaceId, ShareGuid),
    {ok, _} = file_meta:add_share(FileCtx, ShareId),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #share{
            share_id = ShareId,
            share_file_guid = ShareGuid
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Stops sharing a given file.
%% @end
%%--------------------------------------------------------------------
-spec remove_share_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
remove_share_insecure(UserCtx, FileCtx) ->
    ShareId = file_ctx:get_share_id_const(FileCtx),
    SessionId = user_ctx:get_session_id(UserCtx),
    ok = share_logic:delete(SessionId, ShareId),
    {ok, _} = file_meta:remove_share(FileCtx, ShareId),
    ok = permissions_cache:invalidate(),
    #provider_response{status = #status{code = ?OK}}.