%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests starting transfers.
%%% @end
%%%-------------------------------------------------------------------
-module(transfer_req).
-author("Bartosz Walkowicz").

-include("proto/oneprovider/provider_messages.hrl").

%% API
-export([
    schedule_file_transfer/5,
    schedule_view_transfer/7
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec schedule_file_transfer(
    user_ctx:ctx(), file_ctx:ctx(),
    ReplicatingProviderId :: undefined | od_provider:id(),
    EvictingProviderId :: undefined | od_provider:id(),
    transfer:callback()
) ->
    sync_req:provider_response().
schedule_file_transfer(
    UserCtx, FileCtx0,
    ReplicatingProviderId, EvictingProviderId,
    Callback
) ->
    assert_not_a_trash_dir_replication(ReplicatingProviderId, FileCtx0),
    data_constraints:assert_not_readonly_mode(UserCtx),

    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors]
    ),
    schedule_transfer_insecure(
        UserCtx, FileCtx1,
        ReplicatingProviderId, EvictingProviderId,
        undefined, [],
        Callback
    ).


-spec schedule_view_transfer(
    user_ctx:ctx(), file_ctx:ctx(),
    ReplicatingProviderId :: undefined | od_provider:id(),
    EvictingProviderId :: undefined | od_provider:id(),
    transfer:view_name(), transfer:query_view_params(),
    transfer:callback()
) ->
    fslogic_worker:provider_response().
schedule_view_transfer(
    UserCtx, SpaceDirCtx0,
    ReplicatingProviderId, EvictingProviderId,
    ViewName, QueryViewParams,
    Callback
) ->
    data_constraints:assert_not_readonly_mode(UserCtx),

    SpaceDirCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, SpaceDirCtx0,
        [traverse_ancestors]
    ),
    schedule_transfer_insecure(
        UserCtx, SpaceDirCtx1,
        ReplicatingProviderId, EvictingProviderId,
        ViewName, QueryViewParams,
        Callback
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec schedule_transfer_insecure(
    user_ctx:ctx(), file_ctx:ctx(),
    ReplicatingProviderId :: undefined | od_provider:id(),
    EvictingProviderId :: undefined | od_provider:id(),
    transfer:view_name(), transfer:query_view_params(),
    transfer:callback()
) ->
    fslogic_worker:provider_response().
schedule_transfer_insecure(
    UserCtx, SpaceDirCtx,
    ReplicatingProviderId, EvictingProviderId,
    ViewName, QueryViewParams,
    Callback
) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(SpaceDirCtx),
    {FilePath, _} = file_ctx:get_logical_path(SpaceDirCtx, UserCtx),

    {ok, TransferId} = transfer:start(
        SessionId, FileGuid, FilePath,
        EvictingProviderId, ReplicatingProviderId, Callback,
        ViewName, QueryViewParams
    ),
    ?PROVIDER_OK_RESP(#scheduled_transfer{transfer_id = TransferId}).


-spec assert_not_a_trash_dir_replication(undefined | od_provider:id(), file_ctx:ctx()) -> ok.
assert_not_a_trash_dir_replication(undefined, _) ->
    ok;
assert_not_a_trash_dir_replication(_ReplicatingProviderId, FileCtx0) ->
    file_ctx:assert_not_trash_dir_const(FileCtx0).
