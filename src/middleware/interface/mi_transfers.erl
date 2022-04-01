%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Interface for managing transfers (requests are delegated to middleware_worker).
%%% @end
%%%-------------------------------------------------------------------
-module(mi_transfers).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").

%% API
-export([
    schedule_file_transfer/5,
    schedule_view_transfer/7
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec schedule_file_transfer(
    session:id(),
    lfm:file_key(),
    ReplicatingProviderId :: undefined | od_provider:id(),
    EvictingProviderId :: undefined | od_provider:id(),
    transfer:callback()
) ->
    transfer:id() | no_return().
schedule_file_transfer(SessionId, FileKey, ReplicatingProviderId, EvictingProviderId, Callback) ->
    FileGuid = lfm_file_key:resolve_file_key(SessionId, FileKey, do_not_resolve_symlink),

    middleware_worker:check_exec(SessionId, FileGuid, #schedule_file_transfer{
        replicating_provider_id = ReplicatingProviderId,
        evicting_provider_id = EvictingProviderId,
        callback = Callback
    }).


-spec schedule_view_transfer(
    session:id(),
    od_space:id(),
    transfer:view_name(),
    transfer:query_view_params(),
    ReplicatingProviderId :: undefined | od_provider:id(),
    EvictingProviderId :: undefined | od_provider:id(),
    transfer:callback()
) ->
    transfer:id() | no_return().
schedule_view_transfer(
    SessionId, SpaceId, ViewName, QueryViewParams,
    ReplicatingProviderId, EvictingProviderId, Callback
) ->
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    middleware_worker:check_exec(SessionId, SpaceGuid, #schedule_view_transfer{
        replicating_provider_id = ReplicatingProviderId,
        evicting_provider_id = EvictingProviderId,
        view_name = ViewName,
        query_view_params = QueryViewParams,
        callback = Callback
    }).
