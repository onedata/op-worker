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

-include("global_definitions.hrl").
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
-export([create_share/3, remove_share/3]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @equiv create_share_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec create_share(user_ctx:ctx(), file_ctx:ctx(), od_share:name()) ->
    fslogic_worker:provider_response().
create_share(UserCtx, FileCtx0, Name) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors]
    ),
    create_share_internal(UserCtx, FileCtx1, Name).


%%--------------------------------------------------------------------
%% @equiv remove_share_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_share(user_ctx:ctx(), file_ctx:ctx(), od_share:id()) ->
    fslogic_worker:provider_response().
remove_share(UserCtx, FileCtx0, ShareId) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors]
    ),
    remove_share_internal(UserCtx, FileCtx1, ShareId).


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
-spec remove_share_internal(user_ctx:ctx(), file_ctx:ctx(), od_share:id()) ->
    fslogic_worker:provider_response().
remove_share_internal(UserCtx, FileCtx, ShareId) ->
    ?CATCH_ERRORS(remove_share_insecure(UserCtx, FileCtx, ShareId)).


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

    assert_has_space_privilege(SpaceId, UserId, ?SPACE_MANAGE_SHARES),

    case share_logic:create(SessionId, ShareId, Name, SpaceId, ShareGuid) of
        {ok, _} ->
            case file_meta:add_share(FileCtx, ShareId) of
                {error, _} ->
                    ok = share_logic:delete(SessionId, ShareId),
                    ?ERROR(?EAGAIN);
                {ok, _} ->
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #share{
                            share_id = ShareId,
                            root_file_guid = ShareGuid
                        }
                    }
            end;
        _ ->
            ?ERROR(?EAGAIN)
    end.


%% @private
-spec remove_share_insecure(user_ctx:ctx(), file_ctx:ctx(), od_share:id()) ->
    fslogic_worker:provider_response().
remove_share_insecure(UserCtx, FileCtx, ShareId) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    assert_has_space_privilege(SpaceId, UserId, ?SPACE_MANAGE_SHARES),

    case file_meta:remove_share(FileCtx, ShareId) of
        {error, not_found} ->
            ?ERROR(?ENOENT);
        {ok, _} ->
            ok = share_logic:delete(SessionId, ShareId),
            ok = permissions_cache:invalidate(),
            #provider_response{status = #status{code = ?OK}}
    end.


%% @private
-spec assert_has_space_privilege(od_space:id(), od_user:id(),
    privileges:space_privilege()) -> ok | no_return().
assert_has_space_privilege(SpaceId, UserId, Privilege) ->
    case space_logic:has_eff_privilege(SpaceId, UserId, Privilege) of
        true ->
            ok;
        false ->
            ?ERROR(?EACCES)
    end.
