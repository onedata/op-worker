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
-include_lib("ctool/include/privileges.hrl").

-define(ERROR(Error), throw({error, Error})).

-define(CATCH_ERRORS(Expr), try
    Expr
catch
    {error, Error} -> % Catch only the errors thrown via ?ERROR macro
        #provider_response{status = #status{code = Error}}
end).

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
        fun create_share_internal/3).

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
        fun remove_share_internal/2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Shares a given file, catches known errors.
%% @end
%%--------------------------------------------------------------------
-spec create_share_internal(user_ctx:ctx(), file_ctx:ctx(), od_share:name()) ->
    fslogic_worker:provider_response().
create_share_internal(UserCtx, FileCtx, Name) ->
    ?CATCH_ERRORS(create_share_insecure(UserCtx, FileCtx, Name)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Stops sharing a given file, catches known errors.
%% @end
%%--------------------------------------------------------------------
-spec remove_share_internal(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
remove_share_internal(UserCtx, FileCtx) ->
    ?CATCH_ERRORS(remove_share_insecure(UserCtx, FileCtx)).

%% @private
-spec create_share_insecure(user_ctx:ctx(), file_ctx:ctx(), od_share:name()) ->
    fslogic_worker:provider_response().
create_share_insecure(UserCtx, FileCtx, Name) ->
    Guid = file_ctx:get_guid_const(FileCtx),
    ShareId = datastore_utils:gen_key(),
    ShareGuid = file_id:guid_to_share_guid(Guid, ShareId),
    SessionId = user_ctx:get_session_id(UserCtx),
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    check_is_dir(FileCtx),
    check_can_manage_shares(SessionId, SpaceId, UserId),

    case share_logic:create(SessionId, ShareId, Name, SpaceId, ShareGuid) of
        {ok, _} ->
            case file_meta:add_share(FileCtx, ShareId) of
                {error, already_exists} ->
                    ok = share_logic:delete(SessionId, ShareId),
                    ?ERROR(?EEXIST);
                {ok, _} ->
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #share{
                            share_id = ShareId,
                            share_file_guid = ShareGuid
                        }
                    }
            end;
        _ ->
            ?ERROR(?EAGAIN)
    end.

%% @private
-spec remove_share_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
remove_share_insecure(UserCtx, FileCtx) ->
    ShareId = file_ctx:get_share_id_const(FileCtx),
    SessionId = user_ctx:get_session_id(UserCtx),
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    check_is_dir(FileCtx),
    check_can_manage_shares(SessionId, SpaceId, UserId),

    case file_meta:remove_share(FileCtx, ShareId) of
        {error, not_found} ->
            ?ERROR(?ENOENT);
        {ok, _} ->
            ok = share_logic:delete(SessionId, ShareId),
            ok = permissions_cache:invalidate(),
            #provider_response{status = #status{code = ?OK}}
    end.

%% @private
-spec check_is_dir(file_ctx:ctx()) -> ok | no_return().
check_is_dir(FileCtx) ->
    case file_ctx:is_dir(FileCtx) of
        {false, _} -> ?ERROR(?EINVAL);
        _ -> ok
    end.

%% @private
-spec check_can_manage_shares(session:id(), od_space:id(), od_user:id()) ->
    boolean() | no_return().
check_can_manage_shares(SessionId, SpaceId, UserId) ->
    false == space_logic:has_eff_privilege(
        SessionId, SpaceId, UserId, ?SPACE_MANAGE_SHARES
    ) andalso ?ERROR(?EACCES).