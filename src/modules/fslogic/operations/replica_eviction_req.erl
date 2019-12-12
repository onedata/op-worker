%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests evicting file
%%% replicas (including whole file trees).
%%% @end
%%%-------------------------------------------------------------------
-module(replica_eviction_req).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([schedule_replica_eviction/6]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Schedules eviction of replica by creating transfer doc.
%% Returns the id of the created transfer doc wrapped in
%% 'scheduled_transfer' provider response. Resolves file path
%% based on file guid.
%% @end
%%--------------------------------------------------------------------
-spec schedule_replica_eviction(user_ctx:ctx(), file_ctx:ctx(),
    SourceProviderId :: sync_req:provider_id(),
    MigrationProviderId :: sync_req:provider_id(), transfer:view_name(),
    sync_req:query_view_params()) -> sync_req:provider_response().
schedule_replica_eviction(
    UserCtx, FileCtx0, SourceProviderId,
    MigrationProviderId, ViewName, QueryViewParams
) ->
    data_constraints:in_readonly_mode(UserCtx) andalso throw(?EACCES),

    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors] %todo VFS-4844
    ),
    schedule_replica_eviction_insecure(
        UserCtx, FileCtx1,
        SourceProviderId, MigrationProviderId,
        ViewName, QueryViewParams
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules eviction of replica, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% @end
%%--------------------------------------------------------------------
-spec schedule_replica_eviction_insecure(user_ctx:ctx(), file_ctx:ctx(),
    sync_req:provider_id(), sync_req:provider_id(), transfer:view_name(),
    sync_req:query_view_params()) -> sync_req:provider_response().
schedule_replica_eviction_insecure(UserCtx, FileCtx, SourceProviderId,
    MigrationProviderId, ViewName, QueryViewParams
) ->
    {FilePath, _} = file_ctx:get_logical_path(FileCtx, UserCtx),
    SessionId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ok, TransferId} = transfer:start(SessionId, FileGuid, FilePath,
        SourceProviderId, MigrationProviderId, undefined, ViewName, QueryViewParams),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #scheduled_transfer{
            transfer_id = TransferId
        }
    }.
