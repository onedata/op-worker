%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module provides cache in process memory
%%% for file system logic elements.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_cache).
-author("Michał Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

% Control API
-export([init/1, is_current_proc_cache/0, flush/0, flush/1, flush/2, check_flush/0,
    verify_flush_ans/3]).
% File/UUID API
-export([get_uuid/0, get_local_location/0, get_all_locations/0,
    cache_location_change/2, clear_location_changes/0]).
% Doc API
-export([get_doc/1, save_doc/1, cache_doc/1, delete_doc/1, attach_blocks/1,
    attach_local_blocks/1, attach_public_blocks/1, merge_local_blocks/1]).
% Block API
-export([get_blocks/1, save_blocks/2, cache_blocks/2, check_blocks/1,
    get_blocks_tree/1, use_blocks/2, finish_blocks_usage/1, get_changed_blocks/1,
    mark_changed_blocks/1, mark_changed_blocks/5, set_local_change/1,
    get_public_blocks/1]).
% Size API
-export([get_local_size/1, update_size/2]).

%%%===================================================================
%%% Macros
%%%===================================================================

-define(MAIN_KEY, fslogic_cache).

-define(FLUSH_TIMER, fslogic_cache_flush_timer).
-define(IS_FLUSH_PLANNED, fslogic_cache_flush_planned).
-define(CHECK_FLUSH, fslogic_cache_check_flush).
-define(FLUSH_PID, fslogic_cache_flush_pid).
-define(FLUSH_CONFIRMATION, fslogic_cache_flushed).

-define(DOCS, fslogic_cache_docs).
-define(FLUSHED_DOCS, fslogic_cache_flushed_docs).
-define(BLOCKS, fslogic_cache_blocks).
-define(PUBLIC_BLOCKS, fslogic_cache_public_blocks).
-define(SIZES, fslogic_cache_sizes).
-define(SIZE_CHANGES, fslogic_cache_size_changes).
-define(SPACE_IDS, fslogic_cache_space_ids).

-define(KEYS, fslogic_cache_keys).
-define(KEYS_MODIFIED, fslogic_cache_modified_keys).
-define(KEYS_BLOCKS_MODIFIED, fslogic_cache_modified_blocks_keys).
-define(BLOCKS_IN_USE, fslogic_cache_blocks_in_use).

-define(LOCATION_CHANGES_CACHE, fslogic_location_changes).

-define(SAVED_BLOCKS, fslogic_cache_saved_blocks).
-define(DELETED_BLOCKS, fslogic_cache_deleted_blocks).
-define(RESET_BLOCKS, fslogic_cache_reset_blocks).
-define(LOCAL_CHANGES, fslogic_cache_local_changes).

-define(LOCAL_BLOCKS_STORE,
    op_worker:get_env(local_blocks_store, doc)).
-define(LOCAL_BLOCKS_FLUSH,
    op_worker:get_env(local_blocks_flush, on_terminate)).
-define(BLOCKS_FLUSH_DELAY_MILLIS,
    op_worker:get_env(blocks_flush_delay, timer:seconds(3))).

-type flush_type() :: sync | async | terminate.

%%%===================================================================
%%% Control API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Initializes cache in process.
%% @end
%%-------------------------------------------------------------------
-spec init(file_location:id()) -> ok.
init(Uuid) ->
    put(?MAIN_KEY, Uuid),
    % init with a dummy timer that is already expired (flush will be triggered during next check)
    put(?FLUSH_TIMER, countdown_timer:start_millis(0)),
    put(?KEYS, []),
    put(?KEYS_MODIFIED,[]),
    put(?KEYS_BLOCKS_MODIFIED,[]),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Checks if process is used as cache.
%% @end
%%-------------------------------------------------------------------
-spec is_current_proc_cache() -> boolean().
is_current_proc_cache() ->
    get(?MAIN_KEY) =/= undefined.

%%-------------------------------------------------------------------
%% @doc
%% @equiv flush(sync).
%% @end
%%-------------------------------------------------------------------
-spec flush() -> ok | flush_error.
flush() ->
    flush(sync).

%%-------------------------------------------------------------------
%% @doc
%% Flushes cache.
%% @end
%%-------------------------------------------------------------------
-spec flush(flush_type()) -> ok | flush_error.
flush(Type) ->
    KM = get(?KEYS_MODIFIED),
    KBM = get(?KEYS_BLOCKS_MODIFIED),
    KeysToFlush = case Type of
        terminate -> get(?KEYS);
        _ ->
            KM ++ (KBM -- KM)
    end,

    Saved = lists:foldl(fun(Key, Acc) ->
        case flush_key(Key, Type) of
            ok ->
                [Key | Acc];
            FlushError ->
                ?warning("Fslogic cache flush error: ~p for key ~p", [FlushError, Key]),
                Acc
        end
    end, [], KeysToFlush),
    NewKM = KM -- Saved,
    NewKBM = KBM -- Saved,
    put(?KEYS_MODIFIED, NewKM),
    put(?KEYS_BLOCKS_MODIFIED, NewKBM),
    put(?FLUSH_TIMER, countdown_timer:start_millis(?BLOCKS_FLUSH_DELAY_MILLIS)),
    erase(?IS_FLUSH_PLANNED),
    case length(NewKM) + length(NewKBM) of
        0 ->
            ok;
        _ ->
            ?warning("Fslogic cache not flushed keys: ~p", [NewKM ++ (NewKBM -- NewKM)]),
            init_flush_check(),
            flush_error
    end.

%%-------------------------------------------------------------------
%% @doc
%% Flushes cache for a key.
%% @end
%%-------------------------------------------------------------------
-spec flush(file_location:id(), boolean()) -> ok | {error, term()}.
% TODO VFS-4743 - Second arg to be used in next step of refactoring
flush(Key, _FlushBlocks) ->
    KM = get(?KEYS_MODIFIED),
    KBM = get(?KEYS_BLOCKS_MODIFIED),
    case lists:member(Key, KM) orelse lists:member(Key, KBM) of
        true ->
            case flush_key(Key, sync) of
                ok ->
                    put(?KEYS_MODIFIED, KM -- [Key]),
                    put(?KEYS_BLOCKS_MODIFIED, KBM -- [Key]),
                    ok;
                Error ->
                    Error
            end;
        _ ->
            ok
    end.

%%-------------------------------------------------------------------
%% @doc
%% Checks if flush should be performed and flushes cache if needed.
%% @end
%%-------------------------------------------------------------------
-spec check_flush() -> ok.
check_flush() ->
    FlushTimer = get(?FLUSH_TIMER),
    case {countdown_timer:is_expired(FlushTimer), get(?IS_FLUSH_PLANNED)} of
        {true, _} ->
            flush(async);
        {_, true} ->
            erlang:send_after(?BLOCKS_FLUSH_DELAY_MILLIS, self(), ?CHECK_FLUSH),
            ok;
        _ ->
            ok
    end.

%%-------------------------------------------------------------------
%% @doc
%% Checks if flush ended successfully.
%% @end
%%-------------------------------------------------------------------
-spec verify_flush_ans(file_location:id(), list(), list()) ->
    ok | [{error, term()}].
verify_flush_ans(Key, Check1, Check2) ->
    erase(?FLUSH_PID),
    case Check1 of
        [] ->
            erase({?DELETED_BLOCKS, Key}),
            erase({?RESET_BLOCKS, Key}),

            case Check2 of
                [] ->
                    erase({?SAVED_BLOCKS, Key}),
                    ok;
                _ ->
                    ?error("Local blocks flush failed for key"
                    " ~p: ~p", [Key, Check2]),
                    Check2
            end;
        _ ->
            ?error("Local blocks del failed for key"
            " ~p: ~p", [Key, Check1]),
            Check1
    end.

%%%===================================================================
%%% File/Uuid API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns uuid of the file.
%% @end
%%-------------------------------------------------------------------
-spec get_uuid() -> file_meta:uuid().
get_uuid() ->
    get(?MAIN_KEY).

%%-------------------------------------------------------------------
%% @doc
%% Returns local file location.
%% @end
%%-------------------------------------------------------------------
-spec get_local_location() -> file_location:doc() | {error, not_found}.
get_local_location() ->
    get_doc(file_location:local_id(get(?MAIN_KEY))).

%%-------------------------------------------------------------------
%% @doc
%% Returns all file locations.
%% @end
%%-------------------------------------------------------------------
-spec get_all_locations() -> [file_location:doc()].
% TODO VFS-4689 - handle space leave
get_all_locations() ->
    lists:map(fun(Key) -> get_doc(Key) end, get(?KEYS)).

%%-------------------------------------------------------------------
%% @doc
%% Caches location change to be produce event in future.
%% @end
%%-------------------------------------------------------------------
-spec cache_location_change([session:id()], replica_updater:location_change_description()) -> ok.
cache_location_change(SessionIds, LocationChange) ->
    case get(?LOCATION_CHANGES_CACHE) of
        undefined ->
            put(?LOCATION_CHANGES_CACHE, [{SessionIds, [LocationChange]}]);
        CachedChanges ->
            SessionCachedChanges = proplists:get_value(SessionIds, CachedChanges, []),
            CachedChanges2 = proplists:delete(SessionIds, CachedChanges),
            put(?LOCATION_CHANGES_CACHE, [{SessionIds, [LocationChange | SessionCachedChanges]} | CachedChanges2])
    end,

    ok.

%%-------------------------------------------------------------------
%% @doc
%% Clears location changes' cache and returns its content.
%% @end
%%-------------------------------------------------------------------
-spec clear_location_changes() -> replica_updater:location_changes_description().
clear_location_changes() ->
    Ans = case get(?LOCATION_CHANGES_CACHE) of
        undefined -> [];
        Value -> Value
    end,
    erase(?LOCATION_CHANGES_CACHE),
    Ans.

%%%===================================================================
%%% Doc API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns file location.
%% @end
%%-------------------------------------------------------------------
-spec get_doc(file_location:id()) -> file_location:doc() | {error, not_found}.
get_doc(undefined) ->
    {error, not_found};
get_doc(Key) ->
    case get({?DOCS, Key}) of
        undefined ->
            case file_location:get(Key) of
                {ok, LocationDoc = #document{
                    key = Key,
                    value = Location = #file_location{blocks = PublicBlocks}}
                } ->
                    LocationDoc2 = LocationDoc#document{
                        value = Location#file_location{blocks = []}
                    },
                    cache_doc(LocationDoc),

                    {Blocks, Sorted} = merge_local_blocks(LocationDoc),
                    put({?BLOCKS, Key}, blocks_to_tree(Blocks, Sorted)),

                    put({?PUBLIC_BLOCKS, Key}, PublicBlocks),
                    LocationDoc2;
                {error, not_found} = ENF ->
                    ENF;
                Error ->
                    ?error("Fslogic cache error: ~p", [Error]),
                    throw({fslogic_cache_error, Error})
            end;
        Doc ->
            Doc
    end.

%%-------------------------------------------------------------------
%% @doc
%% Saves file location (marks it to be flushed).
%% @end
%%-------------------------------------------------------------------
-spec save_doc(file_location:doc()) -> {ok, file_location:id()}.
save_doc(#document{key = Key} = LocationDoc) ->
    Keys = get(?KEYS_MODIFIED),
    put(?KEYS_MODIFIED, [Key | (Keys -- [Key])]),
    init_flush_check(),
    store_doc(LocationDoc).

%%-------------------------------------------------------------------
%% @doc
%% Caches file location (document will not be flushed).
%% @end
%%-------------------------------------------------------------------
-spec cache_doc(file_location:doc()) -> {ok, file_location:id()}.
cache_doc(#document{key = Key} = LocationDoc) ->
    put({?FLUSHED_DOCS, Key}, LocationDoc),
    store_doc(LocationDoc).

%%-------------------------------------------------------------------
%% @doc
%% Deletes file location.
%% @end
%%-------------------------------------------------------------------
-spec delete_doc(file_location:id()) -> ok | {error, term()}.
delete_doc(Key) ->
    Ans = file_location:delete(Key),

    case get_doc(Key) of
        #document{value = #file_location{uuid = FileUuid, space_id = SpaceId, storage_id = StorageId}} ->
            Size = get_local_size(Key),
            cache_size_change(Key, SpaceId, StorageId, -Size),
            apply_size_change(Key, FileUuid);
        _ ->
            ok
    end,
    
    delete_local_blocks(Key),

    delete_local_blocks(Key),

    erase({?DOCS, Key}),
    erase({?FLUSHED_DOCS, Key}),
    erase({?BLOCKS, Key}),
    erase({?PUBLIC_BLOCKS, Key}),
    erase({?SIZES, Key}),
    erase({?SIZE_CHANGES, Key}),
    erase({?SPACE_IDS, Key}),
    erase({?BLOCKS_IN_USE, Key}),

    Keys = get(?KEYS_MODIFIED),
    put(?KEYS_MODIFIED, Keys -- [Key]),
    Keys2 = get(?KEYS_BLOCKS_MODIFIED),
    put(?KEYS_BLOCKS_MODIFIED, Keys2 -- [Key]),
    Keys3 = get(?KEYS),
    put(?KEYS, Keys3 -- [Key]),

    Ans.

%%-------------------------------------------------------------------
%% @doc
%% Attaches blocks to document.
%% @end
%%-------------------------------------------------------------------
-spec attach_blocks(file_location:doc()) -> file_location:doc().
attach_blocks(#document{key = Key, value = Location} = LocationDoc) ->
    Blocks = get_blocks(Key),
    LocationDoc#document{value = Location#file_location{blocks = Blocks}}.

%%-------------------------------------------------------------------
%% @doc
%% Attaches public blocks to document.
%% @end
%%-------------------------------------------------------------------
-spec attach_public_blocks(file_location:doc()) -> file_location:doc().
attach_public_blocks(#document{key = Key, value = Location} = LocationDoc) ->
    Blocks = get_public_blocks(Key),
    LocationDoc#document{value = Location#file_location{blocks = Blocks}}.

%%-------------------------------------------------------------------
%% @doc
%% Attaches local blocks to public blocks.
%% @end
%%-------------------------------------------------------------------
-spec attach_local_blocks(file_location:doc()) -> file_location:doc().
attach_local_blocks(#document{value = Location} = LocationDoc) ->
    {Blocks, Sorted} = merge_local_blocks(LocationDoc),
    Blocks2 = case Sorted of
        true -> Blocks;
        _ -> lists:sort(Blocks)
    end,
    LocationDoc#document{value =
    Location#file_location{blocks = Blocks2}}.

%%%===================================================================
%%% Block API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns blocks of location.
%% @end
%%-------------------------------------------------------------------
-spec get_blocks(file_location:id()) -> fslogic_blocks:blocks().
get_blocks(Key) ->
    tree_to_blocks(get_blocks_tree(Key)).

%%-------------------------------------------------------------------
%% @doc
%% Returns public blocks of location.
%% @end
%%-------------------------------------------------------------------
-spec get_public_blocks(file_location:id()) -> fslogic_blocks:blocks().
get_public_blocks(Key) ->
    case get({?PUBLIC_BLOCKS, Key}) of
        undefined ->
            case get_doc(Key) of
                #document{} ->
                    get_public_blocks(Key);
                _ ->
                    ?warning("Get public blocks for not existing key ~p", [Key]),
                    []
            end;
        Blocks ->
            Blocks
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns blocks of location as tree.
%% @end
%%-------------------------------------------------------------------
-spec get_blocks_tree(file_location:id()) -> fslogic_location_cache:blocks_tree().
get_blocks_tree(Key) ->
    case get({?BLOCKS, Key}) of
        undefined ->
            case get_doc(Key) of
                #document{} ->
                    get_blocks_tree(Key);
                _ ->
                    ?warning("Get blocks for not existing key ~p", [Key]),
                    gb_sets:new()
            end;
        Blocks ->
            Blocks
    end.

%%-------------------------------------------------------------------
%% @doc
%% Saves blocks (marks it to be flushed).
%% @end
%%-------------------------------------------------------------------
-spec save_blocks(file_location:id(), fslogic_location_cache:stored_blocks()) -> ok.
save_blocks(Key, Blocks) ->
    init_flush_check(),
    Keys = get(?KEYS_BLOCKS_MODIFIED),
    put(?KEYS_BLOCKS_MODIFIED, [Key | (Keys -- [Key])]),
    put({?BLOCKS, Key}, blocks_to_tree(Blocks)),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Caches blocks (blocks will not be flushed).
%% @end
%%-------------------------------------------------------------------
-spec cache_blocks(file_location:id(), fslogic_location_cache:stored_blocks()) -> ok.
cache_blocks(Key, Blocks) ->
    put({?BLOCKS, Key}, blocks_to_tree(Blocks, true)),
    put({?PUBLIC_BLOCKS, Key}, Blocks),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Checks if blocks are cached and loads it to cache if needed.
%% @end
%%-------------------------------------------------------------------
-spec check_blocks(file_location:doc()) -> ok.
check_blocks(LocationDoc = #document{
    key = Key,
    value = #file_location{blocks = PublicBlocks}
}) ->
    case get({?BLOCKS, Key}) of
        undefined ->
            {Blocks, Sorted} = merge_local_blocks(LocationDoc),
            put({?BLOCKS, Key}, blocks_to_tree(Blocks, Sorted)),
            put({?PUBLIC_BLOCKS, Key}, PublicBlocks),
            ok;
        _ ->
            ok
    end.

%%-------------------------------------------------------------------
%% @doc
%% Marks blocks as "blocks in use". Argument is a list of blocks' lists because
%% the blocks will be used in groups (each element of outer list at once).
%% @end
%%-------------------------------------------------------------------
-spec use_blocks(file_location:id(), [fslogic_blocks:blocks()]) -> ok.
use_blocks(Key, Blocks) ->
    put({?BLOCKS_IN_USE, Key}, Blocks),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Returns "blocks in use" and deletes their marking.
%% @end
%%-------------------------------------------------------------------
-spec finish_blocks_usage(file_location:id()) -> fslogic_blocks:blocks().
finish_blocks_usage(Key) ->
    Ans = get({?BLOCKS_IN_USE, Key}),
    case Ans of
        undefined ->
            ?warning("Attepmted to finish usage of blocks that were not previously "
                "declared for the key ~p", [Key]),
            [];
        [] ->
            ?warning("Empty list of blocks to use declared for the key ~p", [Key]),
            [];
        [Head] ->
            erase({?BLOCKS_IN_USE, Key}),
            Head;
        [Head | Tail] ->
            use_blocks(Key, Tail),
            Head
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns blocks changed since last flush.
%% @end
%%-------------------------------------------------------------------
-spec get_changed_blocks(file_location:id()) -> {sets:set(), sets:set()}.
get_changed_blocks(Key) ->
    Saved = get_set({?SAVED_BLOCKS, Key}),
    Deleted = get_set({?DELETED_BLOCKS, Key}),
    {Saved, Deleted}.

%%-------------------------------------------------------------------
%% @doc
%% @equiv mark_changed_blocks(Key, all, all, [], []).
%% @end
%%-------------------------------------------------------------------
-spec mark_changed_blocks(file_location:id()) -> ok.
mark_changed_blocks(Key) ->
    mark_changed_blocks(Key, all, all, [], []).

%%-------------------------------------------------------------------
%% @doc
%% Marks blocks as changed.
%% @end
%%-------------------------------------------------------------------
-spec mark_changed_blocks(file_location:id(), sets:set() | all,
    sets:set() | all, fslogic_blocks:blocks(), fslogic_blocks:blocks()) -> ok.
mark_changed_blocks(Key, all, all, _, _) ->
    erase({?SAVED_BLOCKS, Key}),
    erase({?DELETED_BLOCKS, Key}),
    put({?RESET_BLOCKS, Key}, true),
    ok;
mark_changed_blocks(Key, Saved, Deleted, LastSaved, LastDeleted) ->
    AreChangesLocal = get(?LOCAL_CHANGES),
    PublicBlocks = get({?PUBLIC_BLOCKS, Key}),
    WasDeletedFromPublic = lists:any(fun(Block) ->
        lists:member(Block, PublicBlocks)
    end, LastDeleted),

    case WasDeletedFromPublic orelse (AreChangesLocal =/= true) of
        true ->
            put({?PUBLIC_BLOCKS, Key}, (PublicBlocks -- LastDeleted) ++ LastSaved);
        _ ->
            ok
    end,

    put({?SAVED_BLOCKS, Key}, Saved),
    put({?DELETED_BLOCKS, Key}, Deleted),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Sets change as local or public.
%% @end
%%-------------------------------------------------------------------
-spec set_local_change(boolean()) -> ok.
set_local_change(false) ->
    erase(?LOCAL_CHANGES),
    ok;
set_local_change(Value) ->
    put(?LOCAL_CHANGES, Value),
    UpdatedDoc = file_location:set_last_replication_timestamp(
        get_local_location(), global_clock:timestamp_seconds()),
    save_doc(UpdatedDoc),
    ok.

%%%===================================================================
%%% Size API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% Returns size of location.
%% @end
%%-------------------------------------------------------------------
-spec get_local_size(file_location:id()) -> non_neg_integer().
get_local_size(Key) ->
    case get({?SIZES, Key}) of
        undefined ->
            Size = fslogic_blocks:size(get_blocks(Key)),
            put({?SIZES, Key}, Size),
            Size;
        Size ->
            Size
    end.

%%-------------------------------------------------------------------
%% @doc
%% Updates size of location.
%% @end
%%-------------------------------------------------------------------
-spec update_size(file_location:doc(), non_neg_integer()) -> ok.
% TODO VFS-4743 - do we use size of any other replica than local
update_size(#document{key = Key, value = #file_location{space_id = SpaceId, storage_id = StorageId}}, Change) ->
    Size2 = get_local_size(Key) + Change,
    put({?SIZES, Key}, Size2),
    cache_size_change(Key, SpaceId, StorageId, Change).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Initializes flush procedure.
%% @end
%%-------------------------------------------------------------------
-spec init_flush_check() -> ok.
init_flush_check() ->
    case get(?IS_FLUSH_PLANNED) of
        undefined ->
            put(?IS_FLUSH_PLANNED, true),
            erlang:send_after(?BLOCKS_FLUSH_DELAY_MILLIS, self(), ?CHECK_FLUSH),
            ok;
        _ ->
            ok
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Flushes location and blocks for a key.
%% @end
%%-------------------------------------------------------------------
-spec flush_key(file_location:id(), flush_type()) ->
    ok | {error, term()} | [{error, term()}].
% TODO VFS-4743 - do we save any other location than local?
% TODO VFS-4743 - do not save location when only blocks differ
% TODO VFS-4743 - save doc and blocks in separate functions
flush_key(Key, Type) ->
    case get({?DOCS, Key}) of
        undefined ->
            ok;
        #document{key = Key, value = #file_location{uuid = FileUuid,
            size = Size0} = Location} = Doc ->
            {DocToSave = #document{value = #file_location{blocks = BlocksToSave}}, AddBlocks, DelBlocks} =
                case get({?RESET_BLOCKS, Key}) of
                    true ->
                        % TODO VFS-4743 - makes all blocks public
                        {attach_blocks(Doc), [], all};
                    _ ->
                        Size = case Size0 of
                            undefined -> 0;
                            _ -> Size0
                        end,
                        SizeThreshold = application:get_env(?APP_NAME,
                            public_block_size_threshold, 104857600),
                        PercentThreshold = application:get_env(?APP_NAME,
                            public_block_percent_threshold, 10),

                        SavedBlocks = get_set({?SAVED_BLOCKS, Key}),
                        PublicBlocks = get({?PUBLIC_BLOCKS, Key}),
                        SavedBlocksWithoutPublic = sets:subtract(SavedBlocks,
                            sets:from_list(PublicBlocks)),

                        {LocalBlocks, MergedPublicBlocks} = sets:fold(
                            fun(#file_block{size = S} = Block, {TmpLocalBlocks, TmpPublicBlocks}) ->
                                case (S >= SizeThreshold) orelse (S >= (Size * PercentThreshold / 100)) of
                                    true ->
                                        {TmpLocalBlocks, [Block | TmpPublicBlocks]};
                                    _ ->
                                        {[Block | TmpLocalBlocks], TmpPublicBlocks}
                                end
                            end, {[], PublicBlocks}, SavedBlocksWithoutPublic),

                        DeletedBlocks = sets:to_list(get_set({?DELETED_BLOCKS, Key})),

                        {ResultPublicBlocks, ResultLocalBlocks} = case
                            {lists:sort(MergedPublicBlocks), lists:sort(LocalBlocks)} of
                            {[], [FirstLocal | LocalBlocksTail]} ->
                                {[FirstLocal], LocalBlocksTail};
                            {Public, Local} ->
                                {Public, Local}
                        end,

                        {Doc#document{value = Location#file_location{
                            blocks = ResultPublicBlocks}}, ResultLocalBlocks, DeletedBlocks}
            end,

            put({?PUBLIC_BLOCKS, Key}, BlocksToSave),
            case get(?FLUSH_PID) of
                undefined ->
                    ok;
                FlushPid ->
                    wait_for_flush(Key, FlushPid, true)
            end,

            Ans = case get({?FLUSHED_DOCS, Key}) =:= DocToSave of
                true ->
                    flush_local_blocks(DocToSave, DelBlocks, AddBlocks, Type);
                _ ->
                    case file_location:save(DocToSave) of
                        {ok, _} ->
                            put({?FLUSHED_DOCS, Key}, DocToSave),
                            flush_local_blocks(DocToSave, DelBlocks, AddBlocks, Type);
                        Error ->
                            ?error("Flush failed for key ~p: ~p", [Key, Error]),
                            Error
                    end
            end,

            case Ans of
                ok ->
                    apply_size_change(Key, FileUuid);
                _ ->
                    Ans
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Flushes local blocks.
%% @end
%%-------------------------------------------------------------------
-spec flush_local_blocks(file_location:doc(), list(), list(), flush_type()) ->
    ok | {error, term()} | [{error, term()}].
flush_local_blocks(_, [], [], _) ->
    ok;
flush_local_blocks(#document{key = Key,
    value = #file_location{blocks = PublicBlocks}}, DelBlocks, AddBlocks, Type) ->
    Proceed = case ?LOCAL_BLOCKS_FLUSH of
        always -> true;
        on_terminate -> Type =:= terminate
    end,
    case {Proceed, ?LOCAL_BLOCKS_STORE} of
        {true, links} ->
            case Type of
                spawn ->
                    Master = self(),
                    Pid = spawn(fun() ->
                        {Check1, Check2} = flush_local_links(Key, DelBlocks, AddBlocks),
                        Master ! {?FLUSH_CONFIRMATION, Key, Check1, Check2}
                    end),
                    put(?FLUSH_PID, Pid),
                    ok;
                _ ->
                    {Check1, Check2} = flush_local_links(Key, DelBlocks, AddBlocks),
                    verify_flush_ans(Key, Check1, Check2)
            end;
        {true, doc} ->
            case file_local_blocks:update(Key, get_blocks(Key) -- PublicBlocks) of
                ok ->
                    erase({?DELETED_BLOCKS, Key}),
                    erase({?RESET_BLOCKS, Key}),
                    erase({?SAVED_BLOCKS, Key}),
                    ok;
                Error -> Error
            end;
        _ ->
            erase({?DELETED_BLOCKS, Key}),
            erase({?RESET_BLOCKS, Key}),
            erase({?SAVED_BLOCKS, Key}),
            ok
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Flushes local blocks.
%% @end
%%-------------------------------------------------------------------
-spec flush_local_links(file_location:id(), list(), list()) ->
    {[{error, term()}], [{error, term()}]}.
flush_local_links(Key, DelBlocks, AddBlocks) ->
    Check1 = case file_local_blocks:delete_local_blocks(Key, DelBlocks) of
        ok ->
            [];
        List1 ->
            lists:filter(fun
                (ok) -> false;
                ({error, not_found}) -> false;
                (_) -> true
            end, List1)
    end,

    Check2 = case file_local_blocks:save_local_blocks(Key, AddBlocks) of
        ok ->
            [];
        List2 ->
            lists:filter(fun
                ({ok, _}) -> false;
                ({error, already_exists}) -> false;
                (_) -> true
            end, List2)
    end,

    {Check1, Check2}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Waits for flush confirmation.
%% @end
%%-------------------------------------------------------------------
-spec wait_for_flush(file_location:id(), pid(), boolean()) -> ok.
wait_for_flush(Key, FlushPid, CheckAndRetry) ->
    receive
        {?FLUSH_CONFIRMATION, Key, Check1, Check2} ->
            verify_flush_ans(Key, Check1, Check2),
            ok
    after
        1000 ->
            case {CheckAndRetry, erlang:is_process_alive(FlushPid)} of
                {true, true} ->
                    wait_for_flush(Key, FlushPid, CheckAndRetry);
                {true, _} ->
                    wait_for_flush(Key, FlushPid, false); % retry last time to prevent race between
                                                          % confirmation sending / process terminating
                _ ->
                    ok
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Applies size change (updates quota).
%% @end
%%-------------------------------------------------------------------
-spec apply_size_change(file_location:id(), file_meta:uuid()) -> ok.
apply_size_change(Key, FileUuid) ->
    case get({?SIZE_CHANGES, Key}) of
        undefined ->
            ok;
        [] ->
            ok;
        Changes ->
            try
                {ok, UserId} = file_location:get_owner_id(FileUuid),
                lists:foreach(fun({{SpaceId, StorageId}, ChangeSize}) ->
                    % TODO VFS-8835 - cache parent when rename works properly
                    dir_size_stats:report_reg_file_size_changed(
                        file_id:pack_guid(FileUuid, SpaceId), {on_storage, StorageId}, ChangeSize),
                    space_quota:apply_size_change_and_maybe_emit(SpaceId, ChangeSize),
                    monitoring_event_emitter:emit_storage_used_updated(
                        SpaceId, UserId, ChangeSize)
                end, Changes),

                put({?SIZE_CHANGES, Key}, []),
                ok
            catch
                Error:Reason:Stacktrace ->
                    ?error_stacktrace("Apply quota error ~p:~p", [Error, Reason], Stacktrace),
                    {error, Reason}
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% @equiv blocks_to_tree(Blocks, true).
%% @end
%%-------------------------------------------------------------------
-spec blocks_to_tree(fslogic_location_cache:stored_blocks()) -> fslogic_location_cache:blocks_tree().
blocks_to_tree(Blocks) ->
    blocks_to_tree(Blocks, true).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Translates blocks (list or tree) to tree.
%% @end
%%-------------------------------------------------------------------
-spec blocks_to_tree(fslogic_location_cache:stored_blocks(), boolean()) ->
    fslogic_location_cache:blocks_tree().
blocks_to_tree(Blocks, true) when is_list(Blocks) ->
    gb_sets:from_ordset(lists:map(
        fun(#file_block{offset = O, size = S}) ->
            #file_block{offset = O+S, size = S}
        end, Blocks));
blocks_to_tree(Blocks, _) when is_list(Blocks) ->
    gb_sets:from_list(lists:map(
        fun(#file_block{offset = O, size = S}) ->
            #file_block{offset = O+S, size = S}
        end, Blocks));
blocks_to_tree(Blocks, _) ->
    Blocks.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Translates blocks (list or tree) to blocks list.
%% @end
%%-------------------------------------------------------------------
-spec tree_to_blocks(fslogic_location_cache:stored_blocks()) -> fslogic_blocks:blocks().
tree_to_blocks(Tree) ->
    lists:map(
        fun(#file_block{offset = O, size = S}) ->
            #file_block{offset = O-S, size = S}
        end, gb_sets:to_list(Tree)).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns set from memory or empty one.
%% @end
%%-------------------------------------------------------------------
-spec get_set(term()) -> sets:set().
get_set(Key) ->
    case get(Key) of
        undefined -> sets:new();
        Value -> Value
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Attaches local blocks to public blocks.
%% @end
%%-------------------------------------------------------------------
-spec merge_local_blocks(file_location:doc()) ->
    {fslogic_blocks:blocks(), Sorted :: boolean()}.
merge_local_blocks(#document{key = Key,
    value = #file_location{uuid = Uuid, blocks = PublicBlocks}}) ->
    case file_location:local_id(Uuid) of
        Key ->
            case ?LOCAL_BLOCKS_STORE of
                links ->
                    {ok, LocalBlocks} = file_local_blocks:get_local_blocks(Key),
                    {PublicBlocks ++ LocalBlocks, false};
                doc ->
                    {ok, LocalBlocks} = file_local_blocks:get(Key),
                    {PublicBlocks ++ LocalBlocks, false};
                none ->
                    {PublicBlocks, true}
            end;
        _ ->
            {PublicBlocks, true}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Stores doc in memory.
%% @end
%%-------------------------------------------------------------------
-spec store_doc(file_location:doc()) -> {ok, file_location:id()}.
store_doc(#document{key = Key, value = #file_location{space_id = SpaceId, storage_id = StorageId} =
    Location} = LocationDoc) ->
    LocationDoc2 = LocationDoc#document{value =
    Location#file_location{blocks = []}},
    put({?DOCS, Key}, LocationDoc2),

    Keys = get(?KEYS),
    put(?KEYS, [Key | (Keys -- [Key])]),

    case get({?SPACE_IDS, Key}) of
        undefined ->
            ok;
        SpaceId ->
            ok;
        OldSpaceId ->
            cache_size_on_space_change(Key, SpaceId, OldSpaceId, StorageId)
    end,
    put({?SPACE_IDS, Key}, SpaceId),

    {ok, Key}.

%% @private
-spec cache_size_change(file_location:id(), od_space:id(), storage:id(), integer()) -> ok.
cache_size_change(LocationKey, SpaceId, StorageId, SizeChange) ->
    Changes = case get({?SIZE_CHANGES, LocationKey}) of
        undefined -> [];
        Value -> Value
    end,
    ProplistKey = {SpaceId, StorageId},
    SpaceChange = proplists:get_value(ProplistKey, Changes, 0),
    put({?SIZE_CHANGES, LocationKey}, [{ProplistKey, SpaceChange + SizeChange} |
        proplists:delete(ProplistKey, Changes)]),
    ok.

%% @private
-spec cache_size_on_space_change(file_location:id(), od_space:id(), od_space:id(), storage:id()) -> ok.
cache_size_on_space_change(LocationKey, SpaceId, OldSpaceId, StorageId) ->
    Size = get_local_size(LocationKey),
    Changes = case get({?SIZE_CHANGES, LocationKey}) of
        undefined -> [];
        Value -> Value
    end,
    ProplistKey = {SpaceId, StorageId},
    SpaceChange = proplists:get_value(ProplistKey, Changes, 0),
    case lists:filter(fun({{SId, _}, _}) -> SId =:= OldSpaceId end, Changes) of
        [] ->
            put({?SIZE_CHANGES, LocationKey}, [{ProplistKey, Size + SpaceChange} |
                proplists:delete(ProplistKey, Changes)]);
        [{OldSpaceProplistKey, OldSpaceChange} | _] ->
            put({?SIZE_CHANGES, LocationKey}, [{OldSpaceProplistKey, -Size + OldSpaceChange},
                {ProplistKey, Size + OldSpaceChange + SpaceChange} |
                proplists:delete(OldSpaceProplistKey, proplists:delete(ProplistKey, Changes))])
    end,
    ok.

%% @private
-spec delete_local_blocks(file_location:id()) -> ok.
delete_local_blocks(Key) ->
    case ?LOCAL_BLOCKS_STORE of
        doc -> file_local_blocks:delete(Key);
        link -> file_local_blocks:delete_local_blocks(Key, all);
        none -> ok
    end.