%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Module for updating blocks of file replicas.
%%% @end
%%%--------------------------------------------------------------------
-module(replica_updater).
-author("Tomasz Lichon").

-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([update/4]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Appends given blocks to the file's local location and invalidates those blocks in its remote locations.
%% This function in synchronized on the file.
%% FileSize argument may be used to truncate file's blocks if needed.
%% Return value tells whether file size has been changed by this call.
%% @end
%%--------------------------------------------------------------------
-spec update(FileUUID :: file_meta:uuid(), Blocks :: fslogic_blocks:blocks(),
    FileSize :: non_neg_integer() | undefined, BumpVersion :: boolean()) ->
    {ok, size_changed} | {ok, size_not_changed} | {error, Reason :: term()}.
update(FileUUID, Blocks, FileSize, BumpVersion) ->
    file_location:run_synchronized(FileUUID,
        fun() ->
            try
                [Location | _] = fslogic_utils:get_local_file_locations({uuid, FileUUID}), %todo get location as argument, insted operating on first one
                FullBlocks = fill_blocks_with_storage_info(Blocks, Location),

                ok = append([Location], FullBlocks, BumpVersion),

                case FileSize of
                    undefined ->
                        case fslogic_blocks:upper(FullBlocks) > fslogic_blocks:calculate_file_size(Location) of
                            true -> {ok, size_changed};
                            false -> {ok, size_not_changed}
                        end;
                    _ ->
                        do_local_truncate(FileSize, Location),
                        {ok, size_changed}
                end
                % todo reconcile other local replicas according to this one
            catch
                _:Reason ->
                    ?error_stacktrace("Failed to update blocks for file ~p (blocks ~p, file_size ~p) due to: ~p", [FileUUID, Blocks, FileSize, Reason]),
                    {error, Reason}
            end
        end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Truncates blocks from given location. Works for both shrinking and growing file.
%% @end
%%--------------------------------------------------------------------
-spec do_local_truncate(FileSize :: non_neg_integer(), datastore:document()) -> ok | no_return().
do_local_truncate(FileSize, #document{value = #file_location{size = FileSize}}) ->
    ok;
do_local_truncate(FileSize, #document{value = #file_location{size = LocalSize, file_id = FileId, storage_id = StorageId}} = LocalLocation) when LocalSize < FileSize ->
    append(LocalLocation, [#file_block{offset = LocalSize, size = FileSize - LocalSize, file_id = FileId, storage_id = StorageId}], true);
do_local_truncate(FileSize, #document{value = #file_location{size = LocalSize}} = LocalLocation) when LocalSize > FileSize ->
    shrink(LocalLocation, [#file_block{offset = FileSize, size = LocalSize - FileSize}], FileSize).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Appends given blocks to given locations and updates file size for those locations.
%% @end
%%--------------------------------------------------------------------
-spec append(datastore:document() | [datastore:document()], fslogic_blocks:blocks(), boolean()) -> ok | no_return().
append([], _Blocks, _) ->
    ok;
append([Location | T], Blocks, BumpVersion) ->
    ok = append(Location, Blocks, BumpVersion),
    ok = append(T, Blocks, BumpVersion);
append(_, [], _) ->
    ok;
append(#document{value = #file_location{blocks = OldBlocks, size = OldSize} = Loc} = Doc, Blocks, BumpVersion) ->
    ?debug("OldBlocks ~p, NewBlocks ~p", [OldBlocks, Blocks]),
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, Blocks) ++ Blocks,
    NewSize = fslogic_blocks:upper(Blocks),
    ?debug("NewBlocks ~p", [NewBlocks]),
    NewBlocks1 = fslogic_blocks:consolidate(lists:sort(NewBlocks)),
    ?debug("NewBlocks1 ~p", [NewBlocks1]),
    case BumpVersion of
        true ->
            {ok, _} = file_location:save_and_bump_version(
                fslogic_file_location:add_change(
                    Doc#document{rev = undefined, value =
                    Loc#file_location{blocks = NewBlocks1, size = max(OldSize, NewSize)}},
                    {update, Blocks}
                )
            );
        false ->
            {ok, _} = file_location:save(
                Doc#document{rev = undefined, value =
                Loc#file_location{blocks = NewBlocks1, size = max(OldSize, NewSize)}}
            )
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Inavlidates given blocks in given locations. File size is also updated.
%% @end
%%--------------------------------------------------------------------
-spec shrink(datastore:document() | [datastore:document()], Blocks :: fslogic_blocks:blocks(), NewSize :: non_neg_integer()) ->
    ok | no_return().
shrink([Location | T], Blocks, NewSize) ->
    ok = shrink(Location, Blocks, NewSize),
    ok = shrink(T, Blocks, NewSize);
shrink(#document{value = #file_location{size = NewSize}}, [], NewSize) ->
    ok;
shrink(#document{value = #file_location{blocks = OldBlocks} = Loc} = Doc, Blocks, NewSize) ->
    ?debug("OldBlocks shrink ~p, new ~p", [OldBlocks, Blocks]),
    NewBlocks = fslogic_blocks:invalidate(OldBlocks, Blocks),
    ?debug("NewBlocks shrink ~p", [NewBlocks]),
    NewBlocks1 = fslogic_blocks:consolidate(NewBlocks),
    ?debug("NewBlocks1 shrink ~p", [NewBlocks1]),
    {ok, _} = file_location:save_and_bump_version(
        fslogic_file_location:add_change(
            Doc#document{rev = undefined, value =
            Loc#file_location{blocks = NewBlocks1, size = NewSize}},
            {shrink, NewSize}
        )
    ),
    ok;
shrink([], _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Make sure that storage_id and file_id are set (assume defaults form location
%% if not)
%% @end
%%--------------------------------------------------------------------
-spec fill_blocks_with_storage_info(fslogic_blocks:blocks(), file_location:doc()) ->
    fslogic_blocks:blocks().
fill_blocks_with_storage_info(Blocks, #document{value = #file_location{storage_id = DSID, file_id = DFID}}) ->
    FilledBlocks = lists:map(
        fun(#file_block{storage_id = SID, file_id = FID} = B) ->
            NewSID =
                case SID of
                    undefined -> DSID;
                    _ -> SID
                end,
            NewFID =
                case FID of
                    undefined -> DFID;
                    _ -> FID
                end,

            B#file_block{storage_id = NewSID, file_id = NewFID}
        end, Blocks),

    fslogic_blocks:consolidate(lists:usort(FilledBlocks)).