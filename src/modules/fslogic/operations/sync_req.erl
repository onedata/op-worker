%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests synchronizing and getting
%%% synchronization state of files.
%%% @end
%%%--------------------------------------------------------------------
-module(sync_req).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([synchronize_block/4, synchronize_block_and_compute_checksum/3,
    get_file_distribution/2, replicate_file/3, invalidate_file_replica/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block(user_ctx:ctx(), file_ctx:ctx(), fslogic_blocks:block(), Prefetch :: boolean()) ->
    fslogic_worker:fuse_response().
synchronize_block(UserCtx, FileCtx, undefined, Prefetch) ->
    {_, FileCtx2} = file_ctx:get_or_create_local_file_location_doc(FileCtx), % trigger file_location creation
    {Size, FileCtx3} = file_ctx:get_file_size(FileCtx2),
    synchronize_block(UserCtx, FileCtx3, #file_block{offset = 0, size = Size}, Prefetch);
synchronize_block(UserCtx, FileCtx, Block, Prefetch) ->
    ok = replica_synchronizer:synchronize(UserCtx, FileCtx, Block, Prefetch),
    #fuse_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas and returns checksum of
%% synchronized data.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block_and_compute_checksum(user_ctx:ctx(),
    file_ctx:ctx(), fslogic_blocks:block()) -> fslogic_worker:fuse_response().
synchronize_block_and_compute_checksum(UserCtx, FileCtx, Range = #file_block{offset = Offset, size = Size}) ->
    SessId = user_ctx:get_session_id(UserCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    {ok, Handle} = lfm_files:open(SessId, {guid, FileGuid}, read), %todo do not use lfm, operate on fslogic directly
    {ok, _, Data} = lfm_files:read_without_events(Handle, Offset, Size), % does sync internally
    lfm_files:release(Handle),

    Checksum = crypto:hash(md4, Data),
    {LocationToSend, _FileCtx2} = file_ctx:get_file_location_with_filled_gaps(FileCtx, Range),
    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = #sync_response{
            checksum = Checksum,
            file_location = LocationToSend
        }
    }.

%%--------------------------------------------------------------------
%% @doc
%% Gets distribution of file over providers' storages.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(user_ctx:ctx(), file_ctx:ctx()) ->
    fslogic_worker:provider_response().
get_file_distribution(_UserCtx, FileCtx) ->
    {Locations, _FileCtx2} = file_ctx:get_file_location_docs(FileCtx),
    ProviderDistributions = lists:map(fun(#document{
        value = #file_location{
            provider_id = ProviderId,
            blocks = Blocks
        }
    }) ->
        #provider_file_distribution{
            provider_id = ProviderId,
            blocks = Blocks
        }
    end, Locations),
    #provider_response{
        status = #status{code = ?OK},
        provider_response = #file_distribution{
            provider_file_distributions = ProviderDistributions
        }
    }.

%%--------------------------------------------------------------------
%% @equiv replicate_file_insecure/3 with permission check
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(user_ctx:ctx(), file_ctx:ctx(), fslogic_blocks:block()) ->
    fslogic_worker:provider_response().
replicate_file(UserCtx, FileCtx, Block) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_object],
        [UserCtx, FileCtx, Block, 0],
        fun replicate_file_insecure/4).

%%--------------------------------------------------------------------
%% @equiv invalidate_file_replica_insecure/3 with permission check
%% @end
%%--------------------------------------------------------------------
-spec invalidate_file_replica(user_ctx:ctx(), file_ctx:ctx(), undefined | oneprovider:id()) ->
    fslogic_worker:provider_response().
invalidate_file_replica(UserCtx, FileCtx, MigrationProviderId) ->
    check_permissions:execute(
        [traverse_ancestors, ?write_object],
        [UserCtx, FileCtx, MigrationProviderId, 0],
        fun invalidate_file_replica_insecure/4).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicates given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec replicate_file_insecure(user_ctx:ctx(), file_ctx:ctx(),
    fslogic_blocks:block(), non_neg_integer()) ->
    fslogic_worker:provider_response().
replicate_file_insecure(UserCtx, FileCtx, Block, Offset) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    case file_ctx:is_dir(FileCtx) of
        {true, FileCtx2} ->
            case file_ctx:get_file_children(FileCtx2, UserCtx, Offset, Chunk) of
                {Children, _FileCtx3} when length(Children) < Chunk ->
                    replicate_children(UserCtx, Children, Block),
                    #provider_response{status = #status{code = ?OK}};
                {Children, FileCtx3} ->
                    replicate_children(UserCtx, Children, Block),
                    replicate_file_insecure(UserCtx, FileCtx3, Block, Offset + Chunk)
            end;
        {false, FileCtx2} ->
            #fuse_response{status = Status} =
                synchronize_block(UserCtx, FileCtx2, Block, false),
            #provider_response{status = Status}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicates children list.
%% @end
%%--------------------------------------------------------------------
-spec replicate_children(user_ctx:ctx(), [file_ctx:ctx()],
    fslogic_blocks:block()) -> ok.
replicate_children(UserCtx, Children, Block) ->
    utils:pforeach(fun(ChildCtx) ->
        replicate_file(UserCtx, ChildCtx, Block)
    end, Children).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Invalidates replica of given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec invalidate_file_replica_insecure(user_ctx:ctx(), file_ctx:ctx(),
    oneprovider:id(), non_neg_integer()) ->
    fslogic_worker:provider_response().
invalidate_file_replica_insecure(UserCtx, FileCtx, undefined, Offset) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    {ok, #document{value = #od_space{providers = Providers}}} =
        od_space:get(SpaceId),
    case Providers -- [oneprovider:get_provider_id()] of
        [] ->
            #provider_response{status = #status{code = ?OK}};
        ExternalProviders ->
            MigrationProviderId = utils:random_element(ExternalProviders),
            invalidate_file_replica_insecure(UserCtx, FileCtx, MigrationProviderId, Offset)
    end;
invalidate_file_replica_insecure(UserCtx, FileCtx, MigrationProviderId, Offset) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    case file_ctx:is_dir(FileCtx) of
        {true, FileCtx2} ->
            case file_ctx:get_file_children(FileCtx2, UserCtx, Offset, Chunk) of
                {Children, _FileCtx3} when length(Children) < Chunk ->
                    invalidate_children_replicas(UserCtx, Children, MigrationProviderId),
                    #provider_response{status = #status{code = ?OK}};
                {Children, FileCtx3} ->
                    invalidate_children_replicas(UserCtx, Children, MigrationProviderId),
                    invalidate_file_replica_insecure(UserCtx, FileCtx3, MigrationProviderId, Offset + Chunk)
            end;
        {false, FileCtx2} ->
            SessionId = user_ctx:get_session_id(UserCtx),
            FileGuid = file_ctx:get_guid_const(FileCtx),
            ok = logical_file_manager:replicate_file(SessionId, {guid, FileGuid}, MigrationProviderId),
            #fuse_response{status = #status{code = ?OK}} =
                truncate_req:truncate_insecure(UserCtx, FileCtx, 0, false),
            FileUuid = file_ctx:get_uuid_const(FileCtx2),
            LocalFileId = file_location:local_id(FileUuid),
            case file_location:update(LocalFileId, #{blocks => []}) of
                {ok, _} -> ok;
                {error, {not_found, _}} -> ok
            end,
            #provider_response{status = #status{code = ?OK}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Invalidates replicas of children list.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_children_replicas(user_ctx:ctx(), [file_ctx:ctx()],
    oneprovider:id()) -> ok.
invalidate_children_replicas(UserCtx, Children, MigrationProviderId) ->
    utils:pforeach(fun(ChildCtx) ->
        invalidate_file_replica(UserCtx, ChildCtx, MigrationProviderId)
    end, Children).

