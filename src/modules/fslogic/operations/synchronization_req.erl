%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests synchronizing and getting synchronization state of files.
%%% @end
%%%--------------------------------------------------------------------
-module(synchronization_req).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("annotations/include/annotations.hrl").
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
-spec synchronize_block(fslogic_context:ctx(), file_info:file_info(), fslogic_blocks:block(), Prefetch :: boolean()) ->
    fslogic_worker:fuse_response().
synchronize_block(Ctx, File, undefined, Prefetch) ->
    {FileEntry, File2} = file_info:get_uuid_entry(File),
    Size = fslogic_blocks:get_file_size(FileEntry), %todo pass file_info
    synchronize_block(Ctx, File2, #file_block{offset = 0, size = Size}, Prefetch);
synchronize_block(Ctx, File, Block, Prefetch) ->
    ok = replica_synchronizer:synchronize(Ctx, File, Block, Prefetch),
    #fuse_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas and returns checksum of
%% synchronized data.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block_and_compute_checksum(fslogic_context:ctx(),
    file_info:file_info(), fslogic_blocks:block()) -> fslogic_worker:fuse_response().
synchronize_block_and_compute_checksum(Ctx, File, Range = #file_block{offset = Offset, size = Size}) ->
    SessId = fslogic_context:get_session_id(Ctx),
    {FileGuid, File2} = file_info:get_guid(File),
    {ok, Handle} = lfm_files:open(SessId, {guid, FileGuid}, read), %todo do not use lfm, operate on fslogic directly
    {ok, _, Data} = lfm_files:read_without_events(Handle, Offset, Size), % does sync internally

    {FileEntry, _File3} = file_info:get_uuid_entry(File2),
    Checksum = crypto:hash(md4, Data),
    LocationToSend =
        fslogic_file_location:prepare_location_for_client(FileEntry, Range), %todo pass file_info
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #sync_response{checksum = Checksum, file_location = LocationToSend}}.

%%--------------------------------------------------------------------
%% @doc
%% Get distribution of file over providers' storages.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(fslogic_context:ctx(), file_info:file_info()) ->
    fslogic_worker:provider_response().
get_file_distribution(_Ctx, File) ->
    {Locations, _File2} = file_info:get_file_location_ids(File),
    ProviderDistributions = lists:map( %todo VFS-2813 support multi location
        fun(LocationId) ->
            {ok, #document{value = #file_location{
                provider_id = ProviderId,
                blocks = Blocks
            }}} = file_location:get(LocationId),

            #provider_file_distribution{
                provider_id = ProviderId,
                blocks = Blocks
            }
        end, Locations),
    #provider_response{status = #status{code = ?OK}, provider_response =
    #file_distribution{provider_file_distributions = ProviderDistributions}}.

%%--------------------------------------------------------------------
%% @equiv replicate_file(Ctx, {uuid, Uuid}, Block, 0)
%%--------------------------------------------------------------------
-spec replicate_file(fslogic_context:ctx(), file_info:file_info(), fslogic_blocks:block()) ->
    fslogic_worker:provider_response().
replicate_file(Ctx, File, Block) ->
    replicate_file(Ctx, File, Block, 0).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicate given dir or file on current provider
%% (the space has to be locally supported).
%% @end
%%--------------------------------------------------------------------
-spec replicate_file(fslogic_context:ctx(), file_info:file_info(),
    fslogic_blocks:block(), non_neg_integer()) ->
    fslogic_worker:provider_response().
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
replicate_file(Ctx, File, Block, Offset) ->
    {ok, Chunk} = application:get_env(?APP_NAME, ls_chunk_size),
    case file_info:is_dir(File) of
        {true, File2} ->
            case file_info:get_file_children(File2, Ctx, Offset, Chunk) of
                {Children, _File3} when is_list(Children) andalso length(Children) < Chunk ->
                    replicate_children(Ctx, Children, Block),
                    #provider_response{status = #status{code = ?OK}};
                {Children, File3} when is_list(Children) ->
                    replicate_children(Ctx, Children, Block),
                    replicate_file(Ctx, File3, Block, Offset + Chunk)
            end;
        {false, File2} ->
            #fuse_response{status = Status} = synchronize_block(Ctx, File2, Block, false),
            #provider_response{status = Status}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replicate children list
%% @end
%%--------------------------------------------------------------------
-spec replicate_children(fslogic_context:ctx(), [file_info:file_info()], fslogic_blocks:block()) -> ok.
replicate_children(Ctx, Children, Block) ->
    utils:pforeach(
        fun(Child) ->
            replicate_file(Ctx, Child, Block)
        end, Children).
