%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Executor of PES framework that gathers statistics about directories.
%%% Executor for each directory gathers statistics and sends them to
%%% parent. Thus, each directory possess statistics about all nested
%%% directories and files.
%%% @end
%%%-------------------------------------------------------------------
-module(files_tree_gatherer_pes_executor).
-author("Michal Wrzeszcz").


-behavior(pes_executor_behaviour).


-include("modules/tree_gatherer/files_tree_gatherer.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([call/1, handle_missing_doc_appearance/3, flush/1]).
%% pes_callback behaviour callbacks
-export([init/0, terminate/2, get_root_supervisor/0,
    handle_call/2, handle_cast/2, get_mode/0]).



-record(state, {
    cached_entries = #{} :: cached_entries(),
    last_flush_succeeded = true :: boolean(),
    updates_from_last_flush = false :: boolean(),
    next_flush_num = 1 :: non_neg_integer(),
    flush_timer_ref :: reference() | undefined
}).

-record(cache_entry, {
    values = #{} :: values_map(),
    diff_to_flush = #{} :: diff_map(),
    last_flush_succeeded = true :: boolean(),
    last_used = 0 :: non_neg_integer(), % number of flush from @state{} record is used to determine when entry
                                        % was used (entry was used between `last_used - 1` and `last_used` flush
    parent :: file_id:file_guid() | root_dir | undefined % undefined before first usage
                                                         % (when it is needed, it is checked and cached here)
}).


-type state() :: #state{}.
-type request() :: #ftg_update_request{} | #ftg_get_request{}.
-type handler_module() :: module().

-type parameter() :: term().
-type parameter_value() :: term().
-type parameter_diff() :: term().
-type values_map() :: #{parameter() => parameter_value()}.
-type diff_map() :: #{parameter() => parameter_diff()}.

-type cache_entry_key() :: {file_id:file_guid(), handler_module()}.
-type cache_entry() :: #cache_entry{}.
-type cached_entries() :: #{cache_entry_key() => cache_entry()}.

-export_type([handler_module/0, parameter/0, parameter_value/0, values_map/0, diff_map/0]).


-define(FLUSH_PERIOD, 5000).
-define(NOT_USED_PERIODS_TO_FORGET, 3). % value is deleted from state's cached_entries map when it is not used for
                                        % ?NOT_USED_PERIODS_TO_FORGET periods
-define(SUPERVISOR_NAME, files_tree_gatherer_worker_sup).
-define(SUBMIT_FLUSH, submit_flush).
-define(FLUSH_INTERNAL_MESSAGE, flush_internal).


%%%===================================================================
%%% API
%%%===================================================================

-spec call(request()) -> ok | {ok, values_map()} | {error, Reason :: term()}.
call(#ftg_update_request{guid = Guid} = Request) ->
    Node = consistent_hashing:get_assigned_node(Guid),
    rpc:call(Node, pes, check_cast, [?MODULE, Guid, Request]);
call(#ftg_get_request{guid = Guid} = Request) ->
    Node = consistent_hashing:get_assigned_node(Guid),
    rpc:call(Node, pes, submit_and_await, [?MODULE, Guid, Request]).


-spec handle_missing_doc_appearance(file_id:file_guid(), handler_module(), diff_map()) -> ok.
handle_missing_doc_appearance(Guid, HandlerModule, DiffToFlush) ->
    Parent = get_parent(Guid),
    case Parent of
        root_dir ->
            ok;
        _ ->
            call(#ftg_update_request{
                guid = Parent,
                handler_module = HandlerModule,
                diff_map = DiffToFlush
            })
    end.


-spec flush(file_id:file_guid()) -> ok.
flush(Guid) ->
    Node = consistent_hashing:get_assigned_node(Guid),
    case rpc:call(Node, pes, submit_and_await, [?MODULE, Guid, ?SUBMIT_FLUSH, #{expect_alive => true}]) of
        ok -> ok;
        {error, not_alive} -> ok
    end.


%%%===================================================================
%%% Callbacks for initialization and termination.
%%%===================================================================

-spec init() -> state().
init() ->
    #state{}.


-spec terminate(pes:termination_reason(), state()) -> {ok | abort, state()}.
terminate(_, #state{last_flush_succeeded = true, updates_from_last_flush = false} = State) ->
    {ok, State};
terminate(termination_request, State) ->
    UpdatedState = flush_internal(State),
    case UpdatedState#state.last_flush_succeeded of
        true -> {ok, UpdatedState};
        false -> {abort, UpdatedState}
    end;
terminate(Reason, State) ->
    ?error("Files tree gatherer terminate of not flushed process, reason: ~p", [Reason]),
    UpdatedState = flush_internal(State),
    case UpdatedState#state.last_flush_succeeded of
        true ->
            {ok, UpdatedState};
        false ->
            NotFlushed = lists:foldl(fun
                ({_Key, #cache_entry{last_flush_succeeded = true}}, Acc) ->
                    Acc;
                ({Key, #cache_entry{last_flush_succeeded = false}}, Acc) ->
                    [Key | Acc]
            end, [], maps:to_list(UpdatedState#state.cached_entries)),
            ?critical("Files tree gatherer terminate of not flushed process, lost data for: ~p", [NotFlushed]),
            {ok, UpdatedState}
    end.


%%%===================================================================
%%% Callback providing name of root supervisor.
%%%===================================================================

-spec get_root_supervisor() -> pes_supervisor:name().
get_root_supervisor() ->
    ?SUPERVISOR_NAME.


%%%===================================================================
%%% Callbacks handling requests inside pes_server_slave.
%%%===================================================================

-spec handle_call(#ftg_get_request{} | ?SUBMIT_FLUSH, state()) -> {{ok, values_map()} | noreply, state()}.
handle_call(#ftg_get_request{
    guid = Guid,
    handler_module = HandlerModule,
    parameters = Parameters
}, #state{
    cached_entries = CachedEntries,
    next_flush_num = NextFlushNum
} = State) ->
    CacheEntryKey = {Guid, HandlerModule},
    #cache_entry{values = Values} = CachedEntry = case maps:find(CacheEntryKey, CachedEntries) of
        {ok, Entry} -> Entry;
        error -> new_entry(Guid, HandlerModule)
    end,

    UpdatedCachedEntries = CachedEntries#{CacheEntryKey => CachedEntry#cache_entry{last_used = NextFlushNum}},
    {{ok,  maps:with(Parameters, Values)}, State#state{cached_entries = UpdatedCachedEntries}};

handle_call(?SUBMIT_FLUSH, State) ->
    {ok, flush_internal(State#state{flush_timer_ref = undefined})}.


-spec handle_cast(#ftg_update_request{} | ?FLUSH_INTERNAL_MESSAGE, state()) -> state().
handle_cast(#ftg_update_request{
    guid = Guid,
    handler_module = HandlerModule,
    diff_map = DiffMap
}, #state{
    cached_entries = CachedEntries,
    next_flush_num = NextFlushNum
} = State) ->
    CacheEntryKey = {Guid, HandlerModule},
    #cache_entry{values = Values, diff_to_flush = DiffToFlush} = CacheEntry =
        case maps:find(CacheEntryKey, CachedEntries) of
            {ok, Entry} -> Entry;
            error -> new_entry(Guid, HandlerModule)
        end,

    UpdatedValues = apply_diff_map(HandlerModule, Values, DiffMap),
    UpdatedDiffToFlush = apply_diff_map(HandlerModule, DiffToFlush, DiffMap),
    schedule_flush(State#state{
        updates_from_last_flush = true,
        cached_entries = CachedEntries#{
            CacheEntryKey => CacheEntry#cache_entry{
                values = UpdatedValues,
                diff_to_flush = UpdatedDiffToFlush,
                last_used = NextFlushNum
            }
        }
    });

handle_cast(?FLUSH_INTERNAL_MESSAGE, State) ->
    flush_internal(State#state{flush_timer_ref = undefined});

handle_cast(Info, State) ->
    % This message should not appear - log it
    ?log_bad_request(Info),
    State.


%%%===================================================================
%%% Optional callback setting mode (see pes_server.erl).
%%%===================================================================

-spec get_mode() -> pes:mode().
get_mode() ->
    async.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec flush_internal(state()) -> state().
flush_internal(#state{
    cached_entries = CachedEntries,
    next_flush_num = FlushNum
} = State) ->
    {NewCachedEntries, HasAllSucceeded} = maps:fold(fun({Guid, HandlerModule} = CacheEntryKey, #cache_entry{
        values = Values,
        diff_to_flush = DiffToFlush,
        last_flush_succeeded = LastFlushSucceeded,
        last_used = LastUsed
    } = CacheEntry, {EntriesAcc, HasSucceededAcc} = Acc) ->
        case LastFlushSucceeded andalso maps:size(DiffToFlush) =:= 0 of
            true ->
                case FlushNum - LastUsed >= ?NOT_USED_PERIODS_TO_FORGET of
                    true -> Acc;
                    false -> {EntriesAcc#{CacheEntryKey => CacheEntry}, HasSucceededAcc}
                end;
            false ->
                case HandlerModule:save(Guid, Values) of
                    ok ->
                        #cache_entry{last_flush_succeeded = HasSucceeded} = UpdatedCacheEntry =
                            flush_diff_to_parent(Guid, HandlerModule, CacheEntry),
                        {EntriesAcc#{CacheEntryKey => UpdatedCacheEntry#cache_entry{last_used = FlushNum}},
                            HasSucceeded and HasSucceededAcc};
                    {error, Error} ->
                        ?error("Files tree gatherer save error for handler: ~p and guid ~p: ~p",
                            [HandlerModule, Guid, Error]),
                        {EntriesAcc#{CacheEntryKey => CacheEntry#cache_entry{
                            last_flush_succeeded = false,
                            last_used = FlushNum
                        }}, false}
                end     
        end
    end, {#{}, true}, CachedEntries),

    State#state{
        cached_entries = NewCachedEntries,
        next_flush_num = FlushNum + 1,
        last_flush_succeeded = HasAllSucceeded,
        updates_from_last_flush = false
    }.


-spec schedule_flush(state()) -> state().
schedule_flush(State = #state{flush_timer_ref = undefined}) ->
    State#state{flush_timer_ref = erlang:send_after(?FLUSH_PERIOD, self(), ?FLUSH_INTERNAL_MESSAGE)};
schedule_flush(State) ->
    State.


-spec new_entry(file_id:file_guid(), handler_module()) -> cache_entry() | no_return().
new_entry(Guid, HandlerModule) ->
    case HandlerModule:init_cache(Guid) of
        {ok, Values} ->
            #cache_entry{values = Values};
        {error, Error} ->
            ?error("Tree gatherer init_cache error for handler: ~p and guid ~p: ~p",
                [HandlerModule, Guid, Error]),
            throw(Error)
    end.


-spec apply_diff_map(handler_module(), values_map() | diff_map(), diff_map()) -> values_map() | diff_map().
apply_diff_map(HandlerModule, OriginalMap, DiffMap) ->
    maps:merge_with(fun(Parameter, Value1, Value2) ->
        HandlerModule:merge(Parameter, Value1, Value2)
    end, OriginalMap, DiffMap).


-spec flush_diff_to_parent(file_id:file_guid(), handler_module(), cache_entry()) -> cache_entry().
flush_diff_to_parent(Guid, HandlerModule, #cache_entry{
    diff_to_flush = DiffToFlush,
    parent = CachedParent
} = CacheEntry) ->
    {Parent, UpdatedCacheEntry} = case CachedParent of
        undefined ->
            try
                ParentGuidToCache = get_parent(Guid),
                {ParentGuidToCache, CacheEntry#cache_entry{parent = ParentGuidToCache}}
            catch
                _:{badmatch, {error, not_found}} ->
                    {not_found, CacheEntry}
            end;
        _ ->
            {CachedParent, CacheEntry}
    end,

    FlushResult = case Parent of
        root_dir ->
            ok;
        not_found ->
            add_hook_for_missing_doc(Guid, HandlerModule, DiffToFlush);
        _ ->
            call(#ftg_update_request{
                guid = Parent,
                handler_module = HandlerModule,
                diff_map = DiffToFlush
            })
    end,

    case FlushResult of
        ok ->
            UpdatedCacheEntry#cache_entry{last_flush_succeeded = true, diff_to_flush = #{}};
        {error, Error} ->
            ?error("Files tree gatherer diff flush error for handler: ~p and guid ~p: ~p",
                [HandlerModule, Guid, Error]),
            UpdatedCacheEntry#cache_entry{last_flush_succeeded = false}
    end.


-spec get_parent(file_id:file_guid()) -> file_id:file_guid() | root_dir.
get_parent(Guid) ->
    {ParentFileCtx, _} = files_tree:get_parent(file_ctx:new_by_guid(Guid), undefined),
    CalculatedParentGuid = file_ctx:get_logical_guid_const(ParentFileCtx),
    case file_ctx:is_root_dir_const(file_ctx:new_by_guid(CalculatedParentGuid)) of
        true -> root_dir;
        _ -> CalculatedParentGuid
    end.


-spec add_hook_for_missing_doc(file_id:file_guid(), handler_module(), diff_map()) -> ok | {error, term()}.
add_hook_for_missing_doc(Guid, HandlerModule, DiffToFlush) ->
    {Uuid, _SpaceId} = file_id:unpack_guid(Guid),
    file_meta_posthooks:add_hook(Uuid, generator:gen_name(), 
        ?MODULE, handle_missing_doc_appearance, [Guid, HandlerModule, DiffToFlush]).