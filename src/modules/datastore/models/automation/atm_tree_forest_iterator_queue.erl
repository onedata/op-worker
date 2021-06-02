%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements persistent state of a single tree forest iteration. 
%%% It is designed to be used as a fifo queue - entries should be peeked by increasing 
%%% indices. Peeking an entry does not remove it from the queue to allow for later 
%%% peek in case of provider restart. 
%%% It is assumed that new entries are derived from previous one, hence OriginIndex must 
%%% be provided to push function. If OriginIndex is lower than currently processed 
%%% index (highest one that have been peeked), such push is ignored (entries resulting 
%%% from this index were already pushed). In order to avoid duplication between pushes 
%%% from the same OriginIndex discriminator keeps entry name of last provided entry. 
%%% Entries with name lower than the one kept by discriminator are ignored.
%%% Therefore it is assumed, that entries are sorted ascending by these names. 
%%% 
%%% For each new tree in forest empty entry is "added" - this is simply done by increasing 
%%% entries counter. This simulates pushing tree root to the queue and immediately peeking it. 
%%% It must be done in order to distinguish between pushing entries from new tree 
%%% and restarting iteration in the previous one.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_tree_forest_iterator_queue).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([push/3, peek/2, report_new_tree/2, prune/2, destroy/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).

-type id() :: datastore:key().
-type index() :: non_neg_integer().
-type entry_count() :: non_neg_integer().
-type value() :: binary().
-type entry_name() :: binary().
-type entry() :: {value(), entry_name()}.
-type discriminator() :: {index(), entry_name()}.
-type values() :: #{index() => value()}.
-type doc_num() :: non_neg_integer().
-type record() :: #atm_tree_forest_iterator_queue{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-export_type([id/0, index/0, entry_count/0, values/0, entry_name/0, discriminator/0, doc_num/0]).

-define(CTX, #{model => ?MODULE}).

-define(MAX_VALUES_PER_DOC, op_worker:get_env(atm_tree_forest_iterator_queue_max_values_per_doc, 10000)).


%%%===================================================================
%%% API
%%%===================================================================

-spec push(id(), [entry()], index()) -> ok | {error, term()}.
push(_Id, [], _OriginIndex) -> ok;
push(Id, Entries, OriginIndex) -> 
    critical_section:run({?MODULE, Id}, fun() -> push_unsafe(Id, Entries, OriginIndex) end).


-spec peek(id(), index()) -> {ok, value() | undefined} | {error, term()}.
peek(Id, Index) ->
    DocNum = Index div ?MAX_VALUES_PER_DOC,
    case get_record(Id, DocNum) of
        {ok, #atm_tree_forest_iterator_queue{values = Values}} ->
            case maps:get(Index, Values, undefined) of
                undefined -> 
                    {ok, undefined};
                Value ->
                    UpdateFirstDocFun = fun(#atm_tree_forest_iterator_queue{
                        currently_processed_index = ProcessedIndex
                    } = Record) ->
                        {ok, Record#atm_tree_forest_iterator_queue{
                            currently_processed_index = max(Index, ProcessedIndex)}
                        }
                    end,
                    {ok, _} = update_record(Id, 0, UpdateFirstDocFun),
                    {ok, Value}
            end;
        {error, not_found} ->
            {ok, undefined};
        {error, _} = Error ->
            Error
    end.


-spec report_new_tree(id(), index()) -> ok | {error, term()}.
report_new_tree(Id, Index) ->
    UpdateFun = fun(#atm_tree_forest_iterator_queue{entry_count = EntryCount} = Q) ->
        case Index of
            EntryCount ->
                {ok, Q#atm_tree_forest_iterator_queue{entry_count = EntryCount + 1}};
            _ ->
                {ok, Q}
        end
    end, 
    ?extract_ok(update_record(Id, 0, UpdateFun, #atm_tree_forest_iterator_queue{})).


-spec prune(id(), index()) -> ok | {error, term()}.
prune(Id, Index) ->
    LastToPruneDocNum = Index div ?MAX_VALUES_PER_DOC,
    case get_record(Id, 0) of
        {ok, #atm_tree_forest_iterator_queue{last_pruned_doc_num = StartDocNum}} ->
            lists:foreach(fun(Num) ->
                delete_record(Id, Num)
            end, lists:seq(StartDocNum, max(StartDocNum - 1, LastToPruneDocNum - 1)) -- [0]), % never delete 0th doc
            UpdateFinalDocFun = fun(#atm_tree_forest_iterator_queue{values = Values} = Record) ->
                {ok, Record#atm_tree_forest_iterator_queue{
                    values = prune_values(LastToPruneDocNum * ?MAX_VALUES_PER_DOC, max(Index, 0), Values)
                }}
            end,
            ok = ?extract_ok(update_record(Id, LastToPruneDocNum, UpdateFinalDocFun)),
            case LastToPruneDocNum of
                0 -> 
                    ok;
                _ ->
                    UpdateFirstDocFun = fun(#atm_tree_forest_iterator_queue{} = Record) ->
                        {ok, Record#atm_tree_forest_iterator_queue{
                            last_pruned_doc_num = LastToPruneDocNum, values = #{}
                        }}
                    end,
                    ?extract_ok(update_record(Id, 0, UpdateFirstDocFun))
            end;
        {error, _} = Error ->
            Error
    end.


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    case get_record(Id, 0) of
        {ok, #atm_tree_forest_iterator_queue{entry_count = EntryCount}} ->
            lists:foreach(fun(Num) ->
                delete_record(Id, Num)
            end, lists:seq(0, EntryCount div ?MAX_VALUES_PER_DOC));
        {error, _} = Error ->
            Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec push_unsafe(id(), [entry()], index()) -> ok | {error, term()}.
push_unsafe(Id, Entries, OriginIndex) ->
    case get_record(Id, 0) of
        {ok, #atm_tree_forest_iterator_queue{
            currently_processed_index = ProcessedIndex
        } = FirstRecord} when ProcessedIndex =< OriginIndex ->
            #atm_tree_forest_iterator_queue{
                discriminator = Discriminator
            } = FirstRecord,
            FilteredEntries = filter_by_discriminator(Discriminator, OriginIndex, Entries),
            push_unsafe(Id, FirstRecord, OriginIndex, FilteredEntries);
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.


%% @private
-spec push_unsafe(id(), record(), index(), [entry()]) -> ok.
push_unsafe(_Id, _FirstRecord, _OriginIndex, []) -> ok;
push_unsafe(Id, FirstRecord, OriginIndex, Entries) ->
    #atm_tree_forest_iterator_queue{values = ValuesBefore, entry_count = EntryCount } = FirstRecord,
    {NewEntryCount, [{LowestDocNum, LowestDocValues} | EntriesPerDocTail]} =
        prepare_values(EntryCount, Entries),
    {_, Name} = lists:last(Entries),
    UpdatedFirstRecord = case LowestDocNum of
        0 ->
            FirstRecord#atm_tree_forest_iterator_queue{
                entry_count = NewEntryCount,
                discriminator = {OriginIndex, Name},
                values = maps:merge(ValuesBefore, LowestDocValues)
            };
        _ ->
            {ok, _} = datastore_model:update(?CTX, get_doc_id(Id, LowestDocNum),
                fun(#atm_tree_forest_iterator_queue{values = Values} = Q) ->
                    {ok, Q#atm_tree_forest_iterator_queue{
                        values = maps:merge(Values, LowestDocValues)
                    }}
                end, #atm_tree_forest_iterator_queue{values = LowestDocValues}),
            FirstRecord#atm_tree_forest_iterator_queue{
                entry_count = NewEntryCount,
                discriminator = {OriginIndex, Name}
            }
    end,
    {ok, _} = datastore_model:update(?CTX, get_doc_id(Id, 0), fun(_) -> {ok, UpdatedFirstRecord} end),
    lists:foreach(fun({DocNum, DocValues}) ->
        {ok, _} = datastore_model:save(?CTX, 
            #document{
                key = get_doc_id(Id, DocNum), 
                value = #atm_tree_forest_iterator_queue{values = DocValues}
            })
    end, EntriesPerDocTail).


%% @private
-spec filter_by_discriminator(discriminator(), index(), [entry()]) -> [entry()].
filter_by_discriminator({OriginIndex, DiscriminatorName}, OriginIndex, Entries) ->
    lists:filter(fun({_, EntryName}) ->
        EntryName > DiscriminatorName
    end, Entries);
filter_by_discriminator(_, _OriginIndex, Entries) ->
    Entries.


%% @private
-spec prepare_values(entry_count(), [entry()]) -> 
    {entry_count(), [{entry_count(), values()}]}.
prepare_values(EntryCount, Entries) ->
    {FinalEntryCount, ReversedEntriesPerDoc} = lists:foldl(
        fun({Value, _}, {CurrentIndex, [{DocNum, Map} | Tail] = Acc}) ->
            NewIndex = CurrentIndex + 1,
            case NewIndex div ?MAX_VALUES_PER_DOC of
                DocNum ->
                    NewMap = Map#{NewIndex => Value},
                    {NewIndex, [{DocNum, NewMap} | Tail]};
                NewDocNum ->
                    {NewIndex, [{NewDocNum, #{NewIndex => Value}} | Acc]}
            end
        end, {EntryCount, [{(EntryCount + 1) div ?MAX_VALUES_PER_DOC, #{}}]}, Entries),
    {FinalEntryCount, lists:reverse(ReversedEntriesPerDoc)}.


%% @private
-spec prune_values(index(), index(), values()) -> values().
prune_values(StartIndex, EndIndex, Values) ->
    maps:without(lists:seq(StartIndex, EndIndex), Values).


%%%===================================================================
%%% datastore_model API
%%%===================================================================

%% @private
-spec get_record(id(), doc_num()) -> {ok, record()} | {error, term()}.
get_record(Id, Num) ->
    case datastore_model:get(?CTX, get_doc_id(Id, Num)) of
        {ok, #document{value = Record}} -> {ok, Record};
        {error, _} = Error -> Error
    end.


%% @private
-spec update_record(id(), doc_num(), diff()) -> {ok, doc()} | {error, term()}.
update_record(Id, Num, UpdateFun) ->
    datastore_model:update(?CTX, get_doc_id(Id, Num), UpdateFun).


%% @private
-spec update_record(id(), doc_num(), diff(), record()) -> {ok, doc()} | {error, term()}.
update_record(Id, Num, UpdateFun, Default) ->
    datastore_model:update(?CTX, get_doc_id(Id, Num), UpdateFun, Default).


%% @private
-spec delete_record(id(), doc_num()) -> ok | {error, term()}.
delete_record(Id, Num) ->
    datastore_model:delete(?CTX, get_doc_id(Id, Num)).


%% @private
-spec get_doc_id(id(), doc_num()) -> id().
get_doc_id(Id, Num) ->
    datastore_key:adjacent_from_digest(Num, Id).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {values, #{integer => string}},
        {entry_count, integer},
        {processed_index, integer},
        {discriminator, {integer, binary}},
        {last_pruned_doc_num, integer}
    ]}.

