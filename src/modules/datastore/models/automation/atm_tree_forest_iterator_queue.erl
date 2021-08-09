%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements persistent state of a single tree forest iteration. 
%%% It is designed to be used as a fifo queue - values should be peeked by increasing 
%%% indices. Peeking an entry does not remove it from the queue to allow for later 
%%% peek in case of provider restart. 
%%% It is assumed that new entries are derived from previous one, hence OriginIndex must 
%%% be provided to push function. If OriginIndex is lower than highest one that have been 
%%% peeked, such push is ignored (entries resulting from this index were already pushed). 
%%% In order to avoid duplication between pushes from the same OriginIndex discriminator 
%%% keeps entry name of last provided entry. Entries with name lower than the one kept 
%%% by discriminator are ignored. Therefore it is assumed, that entries are sorted 
%%% ascending by these names. 
%%% 
%%% For each new tree in forest empty entry is "added" - this is simply done by increasing 
%%% `last_pushed_value_index`. This simulates pushing tree root to the queue and immediately 
%%% peeking and pruning it. It must be done in order to distinguish between pushing entries 
%%% from new tree and restarting iteration in the previous one. 
%%% 
%%% In order to avoid storing to many values in one datastore document whole structure is 
%%% stored between multiple nodes saved in individual datastore documents. Division of 
%%% values between nodes is as follows -> value with index I is stored in node number 
%%% I div MaxValuesPerNode. MaxValuesPerNode is provided during queue init.
%%% All structure statistics are kept in node number 0 which is never pruned.
%%%
%%% NOTE: this module does NOT provide any security for concurrent usage. 
%%% It must be provided by higher level modules.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_tree_forest_iterator_queue).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/1, push/3, peek/2, report_new_tree/2, prune/2, destroy/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type id() :: datastore:key().
-type index() :: non_neg_integer().
-type value() :: binary().
-type entry_name() :: binary().
-type entry() :: {value(), entry_name()}.
-type discriminator() :: {index(), entry_name()}.
-type values() :: #{index() => value()}.
-type node_num() :: non_neg_integer().
-type record() :: #atm_tree_forest_iterator_queue{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-export_type([id/0, index/0, values/0, entry_name/0, discriminator/0, node_num/0]).

-define(CTX, #{model => ?MODULE}).


%%%===================================================================
%%% API
%%%===================================================================

-spec init(pos_integer()) -> {ok, id()} | {error, term()}.
init(MaxValuesPerNode) ->
    Id = datastore_key:new(),
    Document = #document{
        key = get_node_id(Id, 0),
        value = #atm_tree_forest_iterator_queue{max_values_per_node = MaxValuesPerNode
    }},
    case datastore_model:create(?CTX, Document) of
        {ok, _} -> {ok, Id};
        {error, _} = Error -> Error
    end.
    

-spec push(id(), [entry()], index()) -> ok | {error, term()}.
push(_Id, [], _OriginIndex) -> ok;
push(Id, Entries, OriginIndex) ->
    case get_record(Id, 0) of
        {ok, #atm_tree_forest_iterator_queue{
            highest_peeked_value_index = HighestPeekedIndex,
            max_values_per_node = MaxValuesPerNode
        } = FirstRecord} when HighestPeekedIndex =< OriginIndex ->
            #atm_tree_forest_iterator_queue{
                discriminator = Discriminator
            } = FirstRecord,
            FilteredEntries = filter_by_discriminator(Discriminator, OriginIndex, Entries),
            push(Id, FirstRecord, OriginIndex, FilteredEntries, MaxValuesPerNode);
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.


-spec peek(id(), index()) -> {ok, value() | undefined} | {error, term()}.
peek(Id, Index) ->
    case get_record(Id, 0) of
        {ok, #atm_tree_forest_iterator_queue{max_values_per_node = MaxValuesPerNode} = FirstRecord} ->
            NodeNum = Index div MaxValuesPerNode,
            GetRecordResult = case NodeNum of
                0 -> 
                    {ok, FirstRecord};
                _ ->
                    get_record(Id, NodeNum) 
            end,
            case GetRecordResult of
                {ok, Record} ->
                    peek_internal(Id, Index, Record);
                {error, not_found} ->
                    {ok, undefined};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.


-spec report_new_tree(id(), index()) -> ok | {error, term()}.
report_new_tree(Id, Index) ->
    UpdateFun = fun(#atm_tree_forest_iterator_queue{last_pushed_value_index = LastEntryIndex} = Q) ->
        case Index of
            LastEntryIndex ->
                {ok, Q#atm_tree_forest_iterator_queue{last_pushed_value_index = LastEntryIndex + 1}};
            _ ->
                {error, no_change}
        end
    end, 
    case update_record(Id, 0, UpdateFun) of
        {ok, _} -> ok;
        {error, no_change} -> ok;
        {error, _} = Error -> Error
    end.


-spec prune(id(), index()) -> ok | {error, term()}.
prune(Id, Index) ->
    case get_record(Id, 0) of
        {ok, #atm_tree_forest_iterator_queue{
            last_pruned_node_num = StartNodeNum, 
            max_values_per_node = MaxValuesPerNode
        }} ->
            LastToPruneNodeNum = Index div MaxValuesPerNode,
            lists:foreach(fun(Num) ->
                delete_record(Id, Num)
            end, lists:seq(StartNodeNum, max(StartNodeNum - 1, LastToPruneNodeNum - 1)) -- [0]), % do not delete 0th node
            UpdateFinalNodeFun = fun(#atm_tree_forest_iterator_queue{values = Values} = Record) ->
                {ok, Record#atm_tree_forest_iterator_queue{
                    values = prune_values(LastToPruneNodeNum * MaxValuesPerNode, max(Index, 0), Values)
                }}
            end,
            ok = ?ok_if_not_found(?extract_ok(update_record(Id, LastToPruneNodeNum, UpdateFinalNodeFun))),
            case LastToPruneNodeNum of
                0 -> 
                    ok;
                _ ->
                    UpdateFirstNodeFun = fun(#atm_tree_forest_iterator_queue{
                        last_pruned_node_num = PrevLastPrunedNodeNum
                    } = Record) ->
                        {ok, Record#atm_tree_forest_iterator_queue{
                            last_pruned_node_num = max(LastToPruneNodeNum, PrevLastPrunedNodeNum),
                            values = #{}
                        }}
                    end,
                    ?ok_if_not_found(?extract_ok(update_record(Id, 0, UpdateFirstNodeFun)))
            end;
        {error, _} = Error ->
            ?ok_if_not_found(Error)
    end.


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    case get_record(Id, 0) of
        {ok, #atm_tree_forest_iterator_queue{
            last_pushed_value_index = LastEntryIndex, 
            max_values_per_node = MaxValuesPerNode
        }} ->
            lists:foreach(fun(Num) ->
                delete_record(Id, Num)
            end, lists:seq(0, LastEntryIndex div MaxValuesPerNode));
        {error, _} = Error ->
            ?ok_if_not_found(Error)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec push(id(), record(), index(), [entry()], pos_integer()) -> ok.
push(_Id, _FirstRecord, _OriginIndex, [], _MaxValuesPerNode) -> ok;
push(Id, FirstRecord, OriginIndex, Entries, MaxValuesPerNode) ->
    #atm_tree_forest_iterator_queue{values = ValuesBefore, last_pushed_value_index = LastEntryIndex} = FirstRecord,
    {UpdatedLastValueIndex, [{LowestNodeNum, LowestNodeValues} | ValuesPerNodeTail]} =
        prepare_values(LastEntryIndex, Entries, MaxValuesPerNode),
    {_, Name} = lists:last(Entries),
    UpdatedFirstRecord = case LowestNodeNum of
        0 ->
            FirstRecord#atm_tree_forest_iterator_queue{
                last_pushed_value_index = UpdatedLastValueIndex,
                discriminator = {OriginIndex, Name},
                values = maps:merge(ValuesBefore, LowestNodeValues)
            };
        _ ->
            {ok, _} = datastore_model:update(?CTX, get_node_id(Id, LowestNodeNum),
                fun(#atm_tree_forest_iterator_queue{values = Values} = Q) ->
                    {ok, Q#atm_tree_forest_iterator_queue{
                        values = maps:merge(Values, LowestNodeValues)
                    }}
                end, #atm_tree_forest_iterator_queue{values = LowestNodeValues}),
            FirstRecord#atm_tree_forest_iterator_queue{
                last_pushed_value_index = UpdatedLastValueIndex,
                discriminator = {OriginIndex, Name}
            }
    end,
    {ok, _} = datastore_model:update(?CTX, get_node_id(Id, 0), fun(_) -> {ok, UpdatedFirstRecord} end),
    lists:foreach(fun({NodeNum, NodeValues}) ->
        {ok, _} = datastore_model:save(?CTX, 
            #document{
                key = get_node_id(Id, NodeNum), 
                value = #atm_tree_forest_iterator_queue{values = NodeValues}
            })
    end, ValuesPerNodeTail).


%% @private
-spec filter_by_discriminator(discriminator(), index(), [entry()]) -> [entry()].
filter_by_discriminator({OriginIndex, DiscriminatorName}, OriginIndex, Entries) ->
    lists:filter(fun({_, EntryName}) ->
        EntryName > DiscriminatorName
    end, Entries);
filter_by_discriminator(_, _OriginIndex, Entries) ->
    Entries.


%% @private
-spec prepare_values(index(), [entry()], pos_integer()) -> 
    {index(), [{node_num(), values()}]}.
prepare_values(LastEntryIndex, Entries, MaxValuesPerNode) ->
    {FinalLastEntryIndex, ReversedValuesPerNode} = lists:foldl(
        fun({Value, _}, {CurrentIndex, [{NodeNum, Map} | Tail] = Acc}) ->
            NewIndex = CurrentIndex + 1,
            case NewIndex div MaxValuesPerNode of
                NodeNum ->
                    NewMap = Map#{NewIndex => Value},
                    {NewIndex, [{NodeNum, NewMap} | Tail]};
                NewNodeNum ->
                    {NewIndex, [{NewNodeNum, #{NewIndex => Value}} | Acc]}
            end
        end, {LastEntryIndex, [{(LastEntryIndex + 1) div MaxValuesPerNode, #{}}]}, Entries),
    {FinalLastEntryIndex, lists:reverse(ReversedValuesPerNode)}.


%% @private
-spec peek_internal(id(), index(), record()) -> {ok, value() | undefined}.
peek_internal(Id, Index, #atm_tree_forest_iterator_queue{values = Values}) ->
    case maps:get(Index, Values, undefined) of
        undefined ->
            {ok, undefined};
        Value ->
            ok = update_highest_peeked_value_index(Id, Index),
            {ok, Value}
    end.


%% @private
-spec update_highest_peeked_value_index(id(), index()) -> ok.
update_highest_peeked_value_index(Id, Index) ->
    UpdateFirstNodeFun = fun(#atm_tree_forest_iterator_queue{
        highest_peeked_value_index = HighestPeekedIndex
    } = Record) ->
        {ok, Record#atm_tree_forest_iterator_queue{
            highest_peeked_value_index = max(Index, HighestPeekedIndex)}
        }
    end,
    ?extract_ok(update_record(Id, 0, UpdateFirstNodeFun)).


%% @private
-spec prune_values(index(), index(), values()) -> values().
prune_values(StartIndex, EndIndex, Entries) ->
    maps:without(lists:seq(StartIndex, EndIndex), Entries).


%%%===================================================================
%%% datastore_model API
%%%===================================================================

%% @private
-spec get_record(id(), node_num()) -> {ok, record()} | {error, term()}.
get_record(Id, Num) ->
    case datastore_model:get(?CTX, get_node_id(Id, Num)) of
        {ok, #document{value = Record}} -> {ok, Record};
        {error, _} = Error -> Error
    end.


%% @private
-spec update_record(id(), node_num(), diff()) -> {ok, doc()} | {error, term()}.
update_record(Id, Num, UpdateFun) ->
    datastore_model:update(?CTX, get_node_id(Id, Num), UpdateFun).


%% @private
-spec delete_record(id(), node_num()) -> ok | {error, term()}.
delete_record(Id, Num) ->
    datastore_model:delete(?CTX, get_node_id(Id, Num)).


%% @private
-spec get_node_id(id(), node_num()) -> id().
get_node_id(Id, Num) ->
    datastore_key:adjacent_from_digest(Num, Id).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {values, #{integer => string}},
        {last_pushed_value_index , integer},
        {highest_peeked_value_index , integer},
        {discriminator, {integer, binary}},
        {last_pruned_node_num , integer}
    ]};
get_record_struct(2) ->
    {record, [
        {values, #{integer => string}},
        {last_pushed_value_index , integer},
        {highest_peeked_value_index , integer},
        {discriminator, {integer, binary}},
        {last_pruned_node_num , integer},
        {max_values_per_node, integer}
    ]}.


-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, 
    {?MODULE, Values, LastPushedValueIndex, HighestPeekedValueIndex, Discriminator, LastPrunedNodeNum}
) ->
    {2, {?MODULE, 
        Values, 
        LastPushedValueIndex, 
        HighestPeekedValueIndex, 
        Discriminator, 
        LastPrunedNodeNum, 
        10000
    }}.

