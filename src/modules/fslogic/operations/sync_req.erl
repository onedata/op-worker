%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handling requests synchronizing and getting
%%% synchronization state of files.
%%% @end
%%%--------------------------------------------------------------------
-module(sync_req).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/transfer.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").

-type block() :: undefined | fslogic_blocks:block().
-type transfer_id() :: undefined | transfer:id().
-type provider_id() :: undefined | od_provider:id().
-type query_view_params() :: transfer:query_view_params().
-type fuse_response() :: fslogic_worker:fuse_response().
-type provider_response() :: fslogic_worker:provider_response().

-export_type([block/0, transfer_id/0, provider_id/0, query_view_params/0, provider_response/0]).

%% API
-export([
    synchronize_block/6,
    request_block_synchronization/6,
    synchronize_block_and_compute_checksum/5,
    get_file_distribution/2
]).

-export([
    schedule_file_replication/6
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block(user_ctx:ctx(), file_ctx:ctx(), block(),
    Prefetch :: boolean(), transfer_id(), non_neg_integer()) -> fuse_response().
synchronize_block(UserCtx, FileCtx, undefined, Prefetch, TransferId, Priority) ->
    % trigger file_location creation
    {_, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx, false),
    {Size, FileCtx3} = file_ctx:get_file_size(FileCtx2),
    synchronize_block(UserCtx, FileCtx3, #file_block{offset = 0, size = Size},
        Prefetch, TransferId, Priority);
synchronize_block(UserCtx, FileCtx, Block, Prefetch, TransferId, Priority) ->
    case replica_synchronizer:synchronize(UserCtx, FileCtx, Block,
        Prefetch, TransferId, Priority) of
        {ok, Ans} ->
            #fuse_response{status = #status{code = ?OK}, fuse_response = Ans};
        {error, Reason} ->
            throw(Reason)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Requests synchronization of given block with remote replicas.
%% Does not wait for sync.
%% @end
%%--------------------------------------------------------------------
-spec request_block_synchronization(user_ctx:ctx(), file_ctx:ctx(), block(),
    Prefetch :: boolean(), transfer_id(), non_neg_integer()) -> fuse_response().
request_block_synchronization(UserCtx, FileCtx, undefined, Prefetch, TransferId, Priority) ->
    % trigger file_location creation
    {_, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx, false),
    {Size, FileCtx3} = file_ctx:get_file_size(FileCtx2),
    request_block_synchronization(UserCtx, FileCtx3, #file_block{offset = 0, size = Size},
        Prefetch, TransferId, Priority);
request_block_synchronization(UserCtx, FileCtx, Block, Prefetch, TransferId, Priority) ->
    case replica_synchronizer:request_synchronization(UserCtx, FileCtx, Block,
        Prefetch, TransferId, Priority) of
        ok ->
            #fuse_response{status = #status{code = ?OK}};
        {error, Reason} ->
            throw(Reason)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas and returns checksum of
%% synchronized data.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block_and_compute_checksum(user_ctx:ctx(), file_ctx:ctx(),
    block(), boolean(), non_neg_integer()) -> fuse_response().
synchronize_block_and_compute_checksum(UserCtx, FileCtx,
    Range = #file_block{offset = Offset, size = Size}, Prefetch, Priority
) ->
    SessId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),

    {ok, Ans} = replica_synchronizer:synchronize(UserCtx, FileCtx, Range,
        Prefetch, undefined, Priority),

    %todo do not use lfm, operate on fslogic directly
    {ok, Handle} = lfm:open(SessId, {guid, FileGuid}, read),
    % does sync internally
    {ok, _, Data} = lfm_files:read_without_events(Handle, Offset, Size, off),
    lfm:release(Handle),

    Checksum = crypto:hash(md4, Data),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #sync_response{
            checksum = Checksum,
            file_location_changed = Ans
        }
    }.


%%--------------------------------------------------------------------
%% @doc
%% get_file_distribution_insecure/2 with permission checks.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(user_ctx:ctx(), file_ctx:ctx()) -> provider_response().
get_file_distribution(UserCtx, FileCtx0) ->
    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?read_metadata]
    ),
    get_file_distribution_insecure(UserCtx, FileCtx1).


%%--------------------------------------------------------------------
%% @doc
%% Schedules file or dir replication, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% Resolves file path based on file guid.
%% TODO VFS-6365 remove deprecated replicas endpoints
%% @end
%%--------------------------------------------------------------------
-spec schedule_file_replication(user_ctx:ctx(), file_ctx:ctx(),
    od_provider:id(), transfer:callback(), transfer:view_name(),
    query_view_params()) -> provider_response().
schedule_file_replication(
    UserCtx, FileCtx0, TargetProviderId, Callback,
    ViewName, QueryViewParams
) ->
    data_constraints:assert_not_readonly_mode(UserCtx),

    FileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, FileCtx0,
        [traverse_ancestors]
    ),

    {FilePath, _} = file_ctx:get_logical_path(FileCtx1, UserCtx),
    schedule_file_replication_insecure(
        UserCtx, FileCtx1, FilePath, TargetProviderId,
        Callback, ViewName, QueryViewParams
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets distribution of file over providers' storages.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution_insecure(user_ctx:ctx(), file_ctx:ctx()) ->
    provider_response().
get_file_distribution_insecure(_UserCtx, FileCtx) ->
    {Locations, _FileCtx2} = file_ctx:get_file_location_docs(FileCtx),
    ProviderDistributions = lists:map(fun(#document{
        value = #file_location{provider_id = ProviderId}
    } = FL) ->
        #provider_file_distribution{
            provider_id = ProviderId,
            blocks = fslogic_location_cache:get_blocks(FL)
        }
    end, Locations),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #file_distribution{
            provider_file_distributions = ProviderDistributions
        }
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Schedules file or dir replication, returns the id of created transfer doc
%% wrapped in 'scheduled_transfer' provider response.
%% @end
%%--------------------------------------------------------------------
-spec schedule_file_replication_insecure(user_ctx:ctx(), file_ctx:ctx(),
    file_meta:path(), od_provider:id(), transfer:callback(),
    transfer:view_name(), query_view_params()) -> provider_response().
schedule_file_replication_insecure(
    UserCtx, FileCtx, FilePath, TargetProviderId,
    Callback, ViewName, QueryViewParams
) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ok, TransferId} = transfer:start(SessionId, FileGuid, FilePath, undefined,
        TargetProviderId, Callback, ViewName, QueryViewParams),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #scheduled_transfer{
            transfer_id = TransferId
        }
    }.
