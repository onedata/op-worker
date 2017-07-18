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
    get_file_distribution/2, replicate_file/3]).

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
    {Size, _FileCtx2} = file_ctx:get_file_size(FileCtx),
    synchronize_block(UserCtx, FileCtx, #file_block{offset = 0, size = Size}, Prefetch);
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
