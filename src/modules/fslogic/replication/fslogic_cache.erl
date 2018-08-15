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
-export([init/1, is_current_proc_cache/0, flush/0, flush/2, check_flush/0]).
% File/UUID API
-export([get_uuid/0, get_local_location/0, get_all_locations/0,
    cache_event/2, clear_events/0]).
% Doc API
-export([get_doc/1, save_doc/1, cache_doc/1, delete_doc/1, attach_blocks/1]).
% Block API
-export([get_blocks/1, save_blocks/2, cache_blocks/2, get_blocks_tree/1,
    use_blocks/2, finish_blocks_usage/1, get_changed_blocks/1,
    mark_changed_blocks/3, set_local_change/1]).
% Size API
-export([get_local_size/1, update_size/2]).

%%%===================================================================
%%% Macros
%%%===================================================================

-define(MAIN_KEY, fslogic_cache).

-define(FLUSH_TIME, fslogic_cache_flush_time).
-define(IS_FLUSH_PLANNED, fslogic_cache_flush_planned).
-define(CHECK_FLUSH, check_flush).

-define(DOCS, fslogic_cache_docs).
-define(BLOCKS, fslogic_cache_blocks).
-define(PUBLIC_BLOCKS, fslogic_cache_public_blocks).
-define(SIZES, fslogic_cache_sizes).
-define(SIZE_CHANGES, fslogic_cache_size_changes).
-define(SPACE_IDS, fslogic_cache_space_ids).

-define(KEYS, fslogic_cache_keys).
-define(KEYS_MODIFIED, fslogic_cache_modified_keys).
-define(KEYS_BLOCKS_MODIFIED, fslogic_cache_modified_blocks_keys).
-define(BLOCKS_IN_USE, fslogic_cache_blocks_in_use).

-define(EVENTS_CACHE, fslogic_cache_events).

-define(SAVED_BLOCKS, fslogic_cache_saved_blocks).
-define(DELETED_BLOCKS, fslogic_cache_deleted_blocks).
-define(RESET_BLOCKS, fslogic_cache_reset_blocks).
-define(LOCAL_CHANGES, fslogic_cache_local_changes).

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
    put(?FLUSH_TIME, {0,0,0}),
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
%% Flushes cache.
%% @end
%%-------------------------------------------------------------------
-spec flush() -> ok | flush_error.
flush() ->
    KM = get(?KEYS_MODIFIED),
    KBM = get(?KEYS_BLOCKS_MODIFIED),
    Saved = lists:foldl(fun(Key, Acc) ->
        case flush_key(Key) of
            ok -> [Key | Acc];
            _ -> Acc
        end
    end, [], KM ++ (KBM -- KM)),
    NewKM = KM -- Saved,
    NewKBM = KBM -- Saved,
    put(?KEYS_MODIFIED, NewKM),
    put(?KEYS_BLOCKS_MODIFIED, NewKBM),
    put(?FLUSH_TIME, os:timestamp()),
    erase(?IS_FLUSH_PLANNED),
    case length(NewKM) + length(NewKBM) of
        0 ->
            ok;
        _ ->
            ?warning("Not flushed keys: ~p", [NewKM ++ (NewKBM -- NewKM)]),
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
    case lists:member(Key, KM) orelse lists:member(Key, KM) of
        true ->
            case flush_key(Key) of
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
    FlushTime = get(?FLUSH_TIME),
    Now = os:timestamp(),
    Delay = application:get_env(?APP_NAME, blocks_flush_delay, timer:seconds(3)),
    case {timer:now_diff(Now, FlushTime) > Delay * 1000, get(?IS_FLUSH_PLANNED)} of
        {true, _} ->
            flush();
        {_, true} ->
            erlang:send_after(Delay, self(), ?CHECK_FLUSH),
            ok;
        _ ->
            ok
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
%% Caches events to be sent.
%% @end
%%-------------------------------------------------------------------
-spec cache_event([session:id()], term()) -> ok.
cache_event(SessionIds, Event) ->
    case get(?EVENTS_CACHE) of
        undefined ->
            put(?EVENTS_CACHE, [{SessionIds, [Event]}]);
        Events ->
            TmpEvents = proplists:get_value(SessionIds, Events, []),
            Events2 = proplists:delete(SessionIds, Events),
            put(?EVENTS_CACHE, [{SessionIds, [Event | TmpEvents]} | Events2])
    end,

    ok.

%%-------------------------------------------------------------------
%% @doc
%% Clears events' cache and returns its content.
%% @end
%%-------------------------------------------------------------------
-spec clear_events() -> list().
clear_events() ->
    Ans = case get(?EVENTS_CACHE) of
        undefined -> [];
        Value -> Value
    end,
    erase(?EVENTS_CACHE),
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
                {ok, #document{key = Key, value = #file_location{blocks = PublicBlocks}
                    = Location} = LocationDoc} ->
                    LocationDoc2 = LocationDoc#document{value =
                    Location#file_location{blocks = []}},
                    cache_doc(LocationDoc2),

                    {ok, LocalBlocks} = file_location:get_local_blocks(Key),

                    put({?BLOCKS, Key}, blocks_to_tree(PublicBlocks ++ LocalBlocks, false)),
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

    cache_doc(LocationDoc).

%%-------------------------------------------------------------------
%% @doc
%% Caches file location (document will not be flushed).
%% @end
%%-------------------------------------------------------------------
-spec cache_doc(file_location:doc()) -> {ok, file_location:id()}.
cache_doc(#document{key = Key, value = #file_location{space_id = SpaceId} =
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
            Changes = case get({?SIZE_CHANGES, Key}) of
                undefined -> [];
                Value -> Value
            end,
            SpaceChange = proplists:get_value(SpaceId, Changes, 0),
            OldSpaceChange = proplists:get_value(OldSpaceId, Changes, 0),
            Size = get_local_size(Key),

            put({?SIZE_CHANGES, Key}, [{OldSpaceId, -1 * Size + OldSpaceChange},
                {SpaceId, Size + OldSpaceChange + SpaceChange} |
                proplists:delete(OldSpaceId, proplists:delete(SpaceId, Changes))])
    end,
    put({?SPACE_IDS, Key}, SpaceId),

    {ok, Key}.

%%-------------------------------------------------------------------
%% @doc
%% Deletes file location.
%% @end
%%-------------------------------------------------------------------
-spec delete_doc(file_location:id()) -> ok | {error, term()}.
delete_doc(Key) ->
    Ans = file_location:delete(Key),

    case get_doc(Key) of
        #document{value = #file_location{uuid = FileUuid, space_id = SpaceId}} ->
            Changes = case get({?SIZE_CHANGES, Key}) of
                undefined -> [];
                Value -> Value
            end,
            SpaceChange = proplists:get_value(SpaceId, Changes, 0),
            Size = get_local_size(Key),
            put({?SIZE_CHANGES, Key}, [{SpaceId, SpaceChange - Size} |
                proplists:delete(SpaceId, Changes)]),
            apply_size_change(Key, FileUuid);
        _ ->
            ok
    end,

    erase({?DOCS, Key}),
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
%% Attaches blocks to document as list or tree.
%% @end
%%-------------------------------------------------------------------
-spec attach_blocks(file_location:doc()) -> file_location:doc().
attach_blocks(#document{key = Key, value = Location} = LocationDoc) ->
    Blocks = get_blocks(Key),
    LocationDoc#document{value = Location#file_location{blocks = Blocks}}.

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
%% Returns blocks of location as tree.
%% @end
%%-------------------------------------------------------------------
-spec get_blocks_tree(file_location:id()) -> fslogic_blocks:blocks_tree().
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
-spec save_blocks(file_location:id(), fslogic_blocks:stored_blocks()) -> ok.
save_blocks(Key, Blocks) ->
    Keys = get(?KEYS_BLOCKS_MODIFIED),
    put(?KEYS_BLOCKS_MODIFIED, [Key | (Keys -- [Key])]),
    put({?BLOCKS, Key}, blocks_to_tree(Blocks)),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Caches blocks (blocks will not be flushed).
%% @end
%%-------------------------------------------------------------------
-spec cache_blocks(file_location:id(), fslogic_blocks:stored_blocks()) -> ok.
cache_blocks(Key, Blocks) ->
    put({?BLOCKS, Key}, blocks_to_tree(Blocks)),
    put({?PUBLIC_BLOCKS, Key}, Blocks),
    ok.

%%-------------------------------------------------------------------
%% @doc
%% Marks blocks as "blocks in use".
%% @end
%%-------------------------------------------------------------------
-spec use_blocks(file_location:id(), fslogic_blocks:blocks()) -> ok.
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
    erase({?BLOCKS_IN_USE, Key}),
    case Ans of
        undefined ->
            ?warning("Attepmted to finish usage of blocks that were not previously "
                "declared for the key ~p", [Key]),
            [];
        _ ->
            Ans
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns blocks changed since last flush.
%% @end
%%-------------------------------------------------------------------
-spec get_changed_blocks(file_location:id()) -> {sets:set(), sets:set()}.
get_changed_blocks(Key) ->
    Local = get(?LOCAL_CHANGES),
    Saved = get_set({?SAVED_BLOCKS, Key, Local}),
    Deleted = get_set({?DELETED_BLOCKS, Key}),
    {Saved, Deleted}.

%%-------------------------------------------------------------------
%% @doc
%% Marks blocks as changed.
%% @end
%%-------------------------------------------------------------------
-spec mark_changed_blocks(file_location:id(), sets:set() | all,
    sets:set() | all) -> ok.
mark_changed_blocks(Key, all, all) ->
    erase({?SAVED_BLOCKS, Key, true}),
    erase({?SAVED_BLOCKS, Key, undefined}),
    erase({?DELETED_BLOCKS, Key}),
    put({?RESET_BLOCKS, Key}, true),
    ok;
mark_changed_blocks(Key, Saved, Deleted) ->
    Local = get(?LOCAL_CHANGES),
    put({?SAVED_BLOCKS, Key, Local}, Saved),
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
-spec update_size(file_location:id(), non_neg_integer()) -> ok.
% TODO VFS-4743 - do we use size of any other replica than local
update_size(Key, Change) ->
    Size2 = get_local_size(Key) + Change,
    put({?SIZES, Key}, Size2),

    SpaceId = get({?SPACE_IDS, Key}),
    Changes = case get({?SIZE_CHANGES, Key}) of
        undefined -> [];
        Value -> Value
    end,
    SpaceChange = proplists:get_value(SpaceId, Changes, 0),
    put({?SIZE_CHANGES, Key}, [{SpaceId, SpaceChange + Change} |
        proplists:delete(SpaceId, Changes)]),
    ok.

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
            Delay = application:get_env(?APP_NAME,
                blocks_flush_delay, timer:seconds(3)),
            erlang:send_after(Delay, self(), ?CHECK_FLUSH),
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
-spec flush_key(file_location:id()) -> ok | {error, term()}.
% TODO VFS-4743 - do we save any other location than local?
% TODO VFS-4743 - do not save location when only blocks differ
% TODO VFS-4743 - save doc and blocks in separate functions
flush_key(Key) ->
    case get({?DOCS, Key}) of
        undefined ->
            ok;
        #document{key = Key, value = #file_location{uuid = FileUuid,
            size = Size0} = Location} = Doc ->
            {DocToSave, AddBlocks, DelBlocks} = case get({?RESET_BLOCKS, Key}) of
                true ->
                    % TODO VFS-4743 - makes all blocks public
                    {attach_blocks(Doc), [], all};
                _ ->
                    Size = case Size0 of
                        undefined -> 0;
                        _ -> Size0
                    end,
                    SizeThreshold = application:get_env(?APP_NAME,
                        public_block_size_treshold, 104857600),
                    PercentThreshold = application:get_env(?APP_NAME,
                        public_block_percent_treshold, 10),

                    PublicBlocks = get_set({?SAVED_BLOCKS, Key, undefined}),
                    % Warning - LocalBlocks are not sorted
                    {LocalBlocks, PublicBlocks2} = sets:fold(
                        fun(#file_block{size = S} = Block, {TmpLocalBlocks, TmpPublicBlocks}) ->
                            case (S >= SizeThreshold) orelse (S >= (Size * PercentThreshold / 100)) of
                                true ->
                                    {TmpLocalBlocks, sets:add_element(Block, TmpPublicBlocks)};
                                _ ->
                                    {[Block | TmpLocalBlocks], TmpPublicBlocks}
                            end
                        end, {[], PublicBlocks}, get_set({?SAVED_BLOCKS, Key, true})),

                    DeletedBlocksSet = get_set({?DELETED_BLOCKS, Key}),
                    DeletedBlocks = sets:to_list(DeletedBlocksSet),
                    BlocksToSave = get({?PUBLIC_BLOCKS, Key}),
                    BlocksToSave2 = BlocksToSave -- DeletedBlocks,
                    BlocksToSave3 = BlocksToSave2 ++ sets:to_list(PublicBlocks2),
                    BlocksToSave4 = lists:sort(BlocksToSave3),

                    put({?PUBLIC_BLOCKS, Key}, BlocksToSave4),
                    {Doc#document{value = Location#file_location{
                        blocks = BlocksToSave4}}, LocalBlocks, DeletedBlocks}
            end,

            apply_size_change(Key, FileUuid),
            case file_location:save(DocToSave) of
                {ok, _} ->
                    Check1 = case file_location:delete_local_blocks(Key, DelBlocks) of
                        ok ->
                            [];
                        List1 ->
                            lists:filter(fun
                                (ok) -> false;
                                ({error, not_found}) -> false;
                                (_) -> true
                            end, List1)
                    end,

                    case Check1 of
                        [] ->
                            erase({?DELETED_BLOCKS, Key}),
                            erase({?RESET_BLOCKS, Key}),

                            Check2 = case file_location:save_local_blocks(Key, AddBlocks) of
                                ok ->
                                    [];
                                List2 ->
                                    lists:filter(fun
                                        ({ok, _}) -> false;
                                        ({error, already_exists}) -> false;
                                        (_) -> true
                                    end, List2)
                            end,

                            case Check2 of
                                [] ->
                                    erase({?SAVED_BLOCKS, Key, true}),
                                    erase({?SAVED_BLOCKS, Key, undefined}),

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
                    end;
                Error ->
                    ?error("Flush failed for key ~p: ~p", [Key, Error]),
                    Error
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
            {ok, UserId} = file_location:get_owner_id(FileUuid),
            lists:foreach(fun({SpaceId, ChangeSize}) ->
                space_quota:apply_size_change_and_maybe_emit(SpaceId, ChangeSize),
                monitoring_event:emit_storage_used_updated(SpaceId, UserId, ChangeSize)
            end, Changes),

            put({?SIZE_CHANGES, Key}, []),
            ok
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% @equiv blocks_to_tree(Blocks, true).
%% @end
%%-------------------------------------------------------------------
-spec blocks_to_tree(fslogic_blocks:stored_blocks()) -> fslogic_blocks:blocks_tree().
blocks_to_tree(Blocks) ->
    blocks_to_tree(Blocks, true).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Translates blocks (list or tree) to tree.
%% @end
%%-------------------------------------------------------------------
-spec blocks_to_tree(fslogic_blocks:stored_blocks(), boolean()) ->
    fslogic_blocks:blocks_tree().
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
-spec tree_to_blocks(fslogic_blocks:stored_blocks()) -> fslogic_blocks:blocks().
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