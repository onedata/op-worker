%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc FSLogic request handlers for regular files.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_req_regular).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([get_file_location/2, truncate/3]).
-export([get_parent/2, synchronize_block/4, synchronize_block_and_compute_checksum/3,
    get_file_distribution/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Truncates file on storage and returns only if operation is complete. Does not change file size in
%%      #file_meta model. Model's size should be changed by write events.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec truncate(fslogic_context:ctx(), File :: fslogic_worker:file(), Size :: non_neg_integer()) ->
    FuseResponse :: #fuse_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}, {?write_object, 2}]).
truncate(Ctx, Entry, Size) ->
    SessId = fslogic_context:get_session_id(Ctx),
    {ok, #document{key = FileUUID} = FileDoc} = file_meta:get(Entry),
    {ok, #document{key = SpaceUUID}} = fslogic_spaces:get_space(FileDoc, fslogic_context:get_user_id(Ctx)),

    %% START -> Quota check
    SpaceId = fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUUID),
    OldSize = fslogic_blocks:get_file_size(FileDoc),
    ok = space_quota:assert_write(SpaceId, Size - OldSize),
    %% END   -> Quota check

    Results = lists:map(
        fun({SID, FID} = Loc) ->
            {ok, Storage} = storage:get(SID),
            SFMHandle = storage_file_manager:new_handle(SessId, SpaceUUID, FileUUID, Storage, FID),
            case storage_file_manager:open(SFMHandle, write) of
                {ok, Handle} ->
                    {Loc, storage_file_manager:truncate(Handle, Size)};
                Error ->
                    {Loc, Error}
            end
        end, fslogic_utils:get_local_storage_file_locations(Entry)),

    case [{Loc, Error} || {Loc, {error, _} = Error} <- Results] of
        [] -> ok;
        Errors ->
            [?error("Unable to truncate [FileId: ~p] [StoragId: ~p] to size ~p due to: ~p", [FID, SID, Size, Reason])
                || {{SID, FID}, {error, Reason}} <- Errors],
            ok
    end,

    fslogic_times:update_mtime_ctime(FileDoc, fslogic_context:get_user_id(Ctx)),
    #fuse_response{status = #status{code = ?OK}}.

%%--------------------------------------------------------------------
%% @doc Returns file location.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(fslogic_context:ctx(), File :: fslogic_worker:file()) ->
    no_return() | #fuse_response{}.
-check_permissions([{traverse_ancestors, 2}]).
get_file_location(Ctx, File) ->
    SpaceId = fslogic_context:get_space_id(Ctx),
    {ok, #document{key = FileUUID}} = file_meta:get(File),
    {ok, #document{key = StorageId}} = fslogic_storage:select_storage(SpaceId),
    #document{value = #file_location{
        blocks = Blocks, file_id = FileId
    }} = fslogic_utils:get_local_file_location({uuid, FileUUID}), %todo VFS-2813 support multi location

    #fuse_response{
        status = #status{code = ?OK},
        fuse_response = file_location:ensure_blocks_not_empty(#file_location{
            uuid = fslogic_uuid:uuid_to_guid(FileUUID, SpaceId),
            provider_id = oneprovider:get_provider_id(),
            storage_id = StorageId,
            file_id = FileId,
            blocks = Blocks,
            space_id = SpaceId
        })
    }.

%%--------------------------------------------------------------------
%% @doc Gets parent of file
%% @end
%%--------------------------------------------------------------------
-spec get_parent(Ctx :: fslogic_context:ctx(), File :: fslogic_worker:file()) ->
    ProviderResponse :: #provider_response{} | no_return().
-check_permissions([{traverse_ancestors, 2}]).
get_parent(Ctx, File) ->
    ShareId = file_info:get_share_id(Ctx),
    case ShareId of
        undefined ->
            SpacesBaseDirUUID = ?ROOT_DIR_UUID,
            {ok, #document{key = ParentUUID}} = file_meta:get_parent(File),
            case ParentUUID of
                SpacesBaseDirUUID ->
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #dir{uuid =
                        fslogic_uuid:uuid_to_guid(fslogic_uuid:user_root_dir_uuid(fslogic_context:get_user_id(Ctx)), undefined)}
                    };
                _ ->
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #dir{uuid = fslogic_uuid:uuid_to_guid(ParentUUID)}
                    }
            end;
        _ ->
            SpaceId = fslogic_context:get_space_id(Ctx),
            {ok, #document{key = FileUuid, value = #file_meta{shares = Shares}}} = file_meta:get(File),
            case lists:member(ShareId, Shares) of
                true ->
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #dir{uuid =
                        fslogic_uuid:uuid_to_share_guid(FileUuid, SpaceId, ShareId)}
                    };
                false ->
                    {ok, #document{key = ParentUUID}} = file_meta:get_parent(File),
                    #provider_response{
                        status = #status{code = ?OK},
                        provider_response = #dir{uuid =
                        fslogic_uuid:uuid_to_share_guid(ParentUUID, SpaceId, ShareId)}
                    }
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block(fslogic_context:ctx(), {uuid, file_meta:uuid()}, fslogic_blocks:block(), boolean()) ->
    #fuse_response{}.
synchronize_block(Ctx, {uuid, FileUUID}, undefined, Prefetch) ->
    Size = fslogic_blocks:get_file_size({uuid, FileUUID}),
    synchronize_block(Ctx, {uuid, FileUUID}, #file_block{offset = 0, size = Size}, Prefetch);
synchronize_block(Ctx, {uuid, FileUUID}, Block, Prefetch) ->
    ok = replica_synchronizer:synchronize(Ctx, FileUUID, Block, Prefetch),
    #fuse_response{status = #status{code = ?OK}}.


%%--------------------------------------------------------------------
%% @doc
%% Synchronizes given block with remote replicas and returns checksum of
%% synchronized data.
%% @end
%%--------------------------------------------------------------------
-spec synchronize_block_and_compute_checksum(fslogic_context:ctx(),
    {uuid, file_meta:uuid()}, fslogic_blocks:block()) -> #fuse_response{}.
synchronize_block_and_compute_checksum(Ctx, {uuid, FileUUID},
    Range = #file_block{offset = Offset, size = Size}) ->

    SessId = fslogic_context:get_session_id(Ctx),
    {ok, Handle} = lfm_files:open(SessId, {guid, fslogic_uuid:uuid_to_guid(FileUUID)}, read),
    {ok, _, Data} = lfm_files:read_without_events(Handle, Offset, Size), % does sync internally
    Checksum = crypto:hash(md4, Data),
    LocationToSend =
        fslogic_file_location:prepare_location_for_client({uuid, FileUUID}, Range),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #sync_response{checksum = Checksum, file_location = LocationToSend}}.


%%--------------------------------------------------------------------
%% @doc
%% Get distribution of file over providers' storages.
%% @end
%%--------------------------------------------------------------------
-spec get_file_distribution(fslogic_context:ctx(), {uuid, file_meta:uuid()}) ->
    #provider_response{}.
get_file_distribution(_Ctx, {uuid, UUID}) ->
    {ok, Locations} = file_meta:get_locations({uuid, UUID}),
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

%%%===================================================================
%%% Internal functions
%%%===================================================================