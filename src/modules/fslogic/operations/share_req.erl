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
-include("modules/fslogic/data_access_control.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/privileges.hrl").

-define(ERROR(Error), throw({error, Error})).

-define(CATCH_ERRORS(Expr), try
    Expr
catch
    {error, Error} -> % Catch only the errors thrown via ?ERROR macro
        #provider_response{status = #status{code = Error}}
end).

%% API
-export([create_share/4, remove_share/3]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @equiv create_share_insecure/3 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec create_share(user_ctx:ctx(), file_ctx:ctx(), od_share:name(), od_share:description()) ->
    fslogic_worker:provider_response().
create_share(UserCtx, FileCtx0, Name, Description) ->
    file_ctx:assert_not_trash_dir_const(FileCtx0),
    data_constraints:assert_not_readonly_mode(UserCtx),

    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS]
    ),
    create_share_internal(UserCtx, FileCtx1, Name, Description).


%%--------------------------------------------------------------------
%% @equiv remove_share_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_share(user_ctx:ctx(), file_ctx:ctx(), od_share:id()) ->
    fslogic_worker:provider_response().
remove_share(UserCtx, FileCtx0, ShareId) ->
    data_constraints:assert_not_readonly_mode(UserCtx),
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [?TRAVERSE_ANCESTORS]
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
-spec create_share_internal(user_ctx:ctx(), file_ctx:ctx(), od_share:name(), od_share:description()) ->
    fslogic_worker:provider_response().
create_share_internal(UserCtx, FileCtx, Name, Description) ->
    ?CATCH_ERRORS(create_share_insecure(UserCtx, FileCtx, Name, Description)).


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
-spec create_share_insecure(user_ctx:ctx(), file_ctx:ctx(), od_share:name(), od_share:description()) ->
    fslogic_worker:provider_response().
create_share_insecure(UserCtx, FileCtx0, Name, Description) ->
    Guid = file_ctx:get_logical_guid_const(FileCtx0),
    ShareId = datastore_key:new(),
    ShareGuid = file_id:guid_to_share_guid(Guid, ShareId),
    SessionId = user_ctx:get_session_id(UserCtx),
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx0),

    {IsDir, FileCtx1} = file_ctx:is_dir(FileCtx0),
    FileType = case IsDir of
        true -> dir;
        false -> file
    end,

    space_logic:assert_has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_SHARES),

    case share_logic:create(SessionId, ShareId, Name, Description, SpaceId, ShareGuid, FileType) of
        {ok, _} ->
            case file_meta:add_share(FileCtx1, ShareId) of
                {error, _} ->
                    ok = share_logic:delete(SessionId, ShareId),
                    ?ERROR(?EAGAIN);
                ok ->
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #share{share_id = ShareId}
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

    space_logic:assert_has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_SHARES),

    case file_meta:remove_share(FileCtx, ShareId) of
        {error, not_found} ->
            ?ERROR(?ENOENT);
        ok ->
            ok = share_logic:delete(SessionId, ShareId),
            ok = permissions_cache:invalidate(),
            #provider_response{status = #status{code = ?OK}}
    end.