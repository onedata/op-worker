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
    {ok, od_share:id()} | no_return().
create_share(UserCtx, FileCtx0, Name, Description) ->
    file_ctx:assert_not_trash_or_tmp_dir_const(FileCtx0),
    FileCtx1 = file_ctx:assert_synchronization_enabled(FileCtx0),
    data_constraints:assert_not_readonly_mode(UserCtx),

    FileCtx2 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx1,
        [?TRAVERSE_ANCESTORS]
    ),
    create_share_internal(UserCtx, FileCtx2, Name, Description).


%%--------------------------------------------------------------------
%% @equiv remove_share_insecure/2 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec remove_share(user_ctx:ctx(), file_ctx:ctx(), od_share:id()) ->
    ok | no_return().
remove_share(UserCtx, FileCtx, ShareId) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx),

    data_constraints:assert_no_constraints(UserCtx),
    space_logic:assert_has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_SHARES),

    ok = file_meta:remove_share(FileCtx, ShareId),
    ok = share_logic:delete(SessionId, ShareId),
    ok = permissions_cache:invalidate().


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_share_internal(user_ctx:ctx(), file_ctx:ctx(), od_share:name(), od_share:description()) ->
    {ok, od_share:id()} | no_return().
create_share_internal(UserCtx, FileCtx0, Name, Description) ->
    Guid = file_ctx:get_logical_guid_const(FileCtx0),
    ShareId = datastore_key:new(),
    ShareGuid = file_id:guid_to_share_guid(Guid, ShareId),
    SessionId = user_ctx:get_session_id(UserCtx),
    UserId = user_ctx:get_user_id(UserCtx),
    SpaceId = file_ctx:get_space_id_const(FileCtx0),

    {FileType, FileCtx1} = file_ctx:get_effective_type(FileCtx0),
    lists:member(FileType, [?REGULAR_FILE_TYPE, ?DIRECTORY_TYPE]) orelse throw({error, ?EINVAL}),

    space_logic:assert_has_eff_privilege(SpaceId, UserId, ?SPACE_MANAGE_SHARES),

    case share_logic:create(SessionId, ShareId, Name, Description, SpaceId, ShareGuid, FileType) of
        {ok, _} ->
            case file_meta:add_share(FileCtx1, ShareId) of
                {error, _} ->
                    ok = share_logic:delete(SessionId, ShareId),
                    throw({error, ?EAGAIN});
                ok ->
                    {ok, ShareId}
            end;
        _ ->
            throw({error, ?EAGAIN})
    end.
