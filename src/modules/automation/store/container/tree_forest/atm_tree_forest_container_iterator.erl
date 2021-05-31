%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides `atm_container_iterator` functionality for
%%% `atm_tree_forest_container`.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_tree_forest_container_iterator).
-author("Michal Stanisz").

-behaviour(atm_container_iterator).
-behaviour(persistent_record).

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/errors.hrl"). 
-include_lib("ctool/include/logging.hrl").

%% API
-export([build/2]).

% atm_container_iterator callbacks
-export([get_next_batch/2, mark_exhausted/1]).

%% persistent_record callbacks
-export([version/0, db_encode/2, db_decode/2]).


-type item() :: json_utils:json_term().
-type object_id() :: binary().
-type list_opts() :: map().

-record(queue_info, {
    id :: atm_tree_forest_iterator_queue:id(),
    current_queue_index = -1 :: integer()
}).

-type queue_info() :: #queue_info{}.

-record(atm_tree_forest_container_iterator, {
    type_specific_module :: module(),
    current_object = undefined :: undefined | object_id(),
    tree_list_opts :: list_opts(),
    roots_iterator :: atm_list_container_iterator:record(),
    queue_info :: queue_info()
}).
-type record() :: #atm_tree_forest_container_iterator{}.

-export_type([item/0, list_opts/0, record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================

-callback list_children(object_id(), list_opts()) -> 
    {[{object_id(), binary()}], [object_id()], list_opts()} | no_return().

-callback check_object_existence(object_id()) -> boolean().

%%%===================================================================
%%% API
%%%===================================================================

-spec build(atm_list_container_iterator:record(), atm_data_spec:record()) -> record().
build(RootsIterator, DataSpec) ->
    Module = type_specific_module(atm_data_spec:get_type(DataSpec)),
    #atm_tree_forest_container_iterator{
        type_specific_module = Module,
        roots_iterator = RootsIterator,
        tree_list_opts = default_tree_list_options(),
        queue_info = #queue_info{id = datastore_key:new()}
    }.


%%%===================================================================
%%% atm_container_iterator callbacks
%%%===================================================================

-spec get_next_batch(atm_container_iterator:batch_size(), record()) ->
    {ok, [item()], record()} | stop.
get_next_batch(BatchSize, #atm_tree_forest_container_iterator{} = Record) ->
    get_next_batch(BatchSize, Record, []).


-spec mark_exhausted(record()) -> ok.
mark_exhausted(#atm_tree_forest_container_iterator{queue_info = Q} = Iterator) -> 
    case get_next_batch(1, Iterator) of
        stop -> 
            destroy_queue(Q);
        _ ->
            prune_queue(Q)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec get_next_batch(atm_container_iterator:batch_size(), record(), [item()]) ->
    {ok, [item()], record()} | stop.
get_next_batch(BatchSize, #atm_tree_forest_container_iterator{} = Record, ForestAcc) when BatchSize =< 0 ->
    {ok, ForestAcc, Record};
get_next_batch(BatchSize, #atm_tree_forest_container_iterator{current_object = undefined} = Record, ForestAcc) ->
    #atm_tree_forest_container_iterator{type_specific_module = Module, roots_iterator = ListIterator, queue_info = Q} = Record,
    case atm_list_container_iterator:get_next_batch(1, ListIterator) of
        {ok, [ObjectId], NextRootsIterator} ->
            UpdatedRecord = Record#atm_tree_forest_container_iterator{
                current_object = ObjectId,
                roots_iterator = NextRootsIterator,
                tree_list_opts = default_tree_list_options(),
                queue_info = queue_report_new_tree(Q)
            },
            case Module:check_object_existence(ObjectId) of
                true ->
                    get_next_batch(BatchSize - 1, UpdatedRecord, [ObjectId | ForestAcc]);
                false ->
                    get_next_batch(BatchSize, UpdatedRecord#atm_tree_forest_container_iterator{
                        current_object = undefined
                    }, ForestAcc)
            end;
        stop ->
            case length(ForestAcc) of
                0 -> stop;
                _ -> {ok, ForestAcc, Record}
            end
    end;
get_next_batch(BatchSize, #atm_tree_forest_container_iterator{} = Record, ForestAcc) ->
    {TreeAcc, NewRecord} = get_next_batch_from_single_tree(BatchSize, Record, ForestAcc),
    get_next_batch(BatchSize - length(TreeAcc), NewRecord, TreeAcc).


%% @private
-spec get_next_batch_from_single_tree(atm_container_iterator:batch_size(), record(), [item()]) ->
    {[item()], record()}.
get_next_batch_from_single_tree(_BatchSize, #atm_tree_forest_container_iterator{current_object = undefined} = Record, Acc) ->
    {Acc, Record};
get_next_batch_from_single_tree(BatchSize, #atm_tree_forest_container_iterator{} = Record, Acc) when BatchSize =< 0 ->
    {Acc, Record};
get_next_batch_from_single_tree(BatchSize, #atm_tree_forest_container_iterator{} = Record, Acc) ->
    #atm_tree_forest_container_iterator{
        type_specific_module = Module,
        queue_info = QueueInfo,
        current_object = CurrentObject,
        tree_list_opts = ListOpts
    } = Record,
    {ChildDirs, ChildFiles, ListExtendedInfo} =
        Module:list_children(CurrentObject, ListOpts#{size => BatchSize}),
    UpdatedQueueInfo = add_to_queue(QueueInfo, ChildDirs),
    ChildDirsGuids = lists:map(fun({Guid, _}) -> Guid end, ChildDirs),
    
    UpdatedRecord = case maps:get(is_last, ListExtendedInfo) of
        true ->
            {NextObject, QueueInfo2} = get_from_queue(UpdatedQueueInfo),
            Record#atm_tree_forest_container_iterator{
                queue_info = QueueInfo2,
                current_object = NextObject,
                tree_list_opts = default_tree_list_options()
            };
        false ->
            Record#atm_tree_forest_container_iterator{
                queue_info = UpdatedQueueInfo,
                tree_list_opts = #{
                    last_name => maps:get(last_name, ListExtendedInfo, <<>>),
                    last_tree => maps:get(last_tree, ListExtendedInfo, <<>>)
                }
            }
    end,
    Result = ChildDirsGuids ++ ChildFiles,
    get_next_batch_from_single_tree(BatchSize - length(Result), UpdatedRecord, Result ++ Acc).


%% @private
-spec default_tree_list_options() -> list_opts().
default_tree_list_options() ->
    #{
        last_name => <<>>,
        last_tree => <<>>
    }.


%% @private
-spec add_to_queue(queue_info(), [{object_id(), binary()}]) -> queue_info() | no_return().
add_to_queue(#queue_info{id = Id, current_queue_index = Index} = Q, Batch) ->
    case atm_tree_forest_iterator_queue:push(Id, Batch, Index) of
        ok -> Q;
        {error, _} = Error -> throw(Error)
    end.


%% @private
-spec get_from_queue(queue_info()) -> {object_id() | undefined, queue_info()} | no_return().
get_from_queue(#queue_info{id = Id, current_queue_index = Index} = Q) ->
    case atm_tree_forest_iterator_queue:get(Id, Index + 1) of
        {ok, undefined} ->
            {undefined, Q#queue_info{current_queue_index = Index}};
        {ok, Value} ->
            {Value, Q#queue_info{current_queue_index = Index + 1}};
        {error, _} = Error ->
            throw(Error)
    end.


%% @private
-spec queue_report_new_tree(queue_info()) -> queue_info() | no_return().
queue_report_new_tree(#queue_info{id = Id, current_queue_index = Index} = Q) ->
    case atm_tree_forest_iterator_queue:report_new_tree(Id, Index) of
        ok -> Q#queue_info{current_queue_index = Index + 1};
        {error, _} = Error -> throw(Error)
    end.


%% @private
-spec prune_queue(queue_info()) -> ok | no_return().
prune_queue(#queue_info{id = Id, current_queue_index = Index}) ->
    case atm_tree_forest_iterator_queue:prune(Id, Index) of
        ok -> ok;
        {error, _} = Error -> throw(Error)
    end.


%% @private
-spec destroy_queue(queue_info()) -> ok | no_return().
destroy_queue(#queue_info{id = Id}) ->
    case atm_tree_forest_iterator_queue:destroy(Id) of
        ok -> ok;
        {error, _} = Error -> throw(Error)
    end.

%%%===================================================================
%%% persistent_record callbacks
%%%===================================================================

-spec version() -> persistent_record:record_version().
version() ->
    1.


-spec db_encode(record(), persistent_record:nested_record_encoder()) ->
    json_utils:json_term().
db_encode(#atm_tree_forest_container_iterator{
    type_specific_module = Module,
    current_object = CurrentObject,
    tree_list_opts = TreeListOpts,
    roots_iterator = RootsIterator,
    queue_info = QueueInfo
}, NestedRecordEncoder) ->
    #{
        <<"typeSpecificModule">> => atom_to_binary(Module, utf8),
        <<"currentObject">> => utils:undefined_to_null(CurrentObject),
        <<"treeListOpts">> => 
            json_utils:encode(maps:fold(fun(Key, Value, Acc) -> 
                Acc#{atom_to_binary(Key, utf8) => Value} 
            end, #{}, TreeListOpts)),
        <<"rootsIterator">> => atm_list_container_iterator:db_encode(RootsIterator, NestedRecordEncoder),
        <<"queueInfo">> => encode_queue_info(QueueInfo)
    }.


-spec db_decode(json_utils:json_term(), persistent_record:nested_record_decoder()) ->
    record().
db_decode(#{
    <<"typeSpecificModule">> := Module,
    <<"currentObject">> := CurrentObject,
    <<"treeListOpts">> := TreeListOpts,
    <<"rootsIterator">> := RootsIterator,
    <<"queueInfo">> := QueueInfo
}, NestedRecordDecoder) ->
    #atm_tree_forest_container_iterator{
        type_specific_module = binary_to_atom(Module, utf8),
        current_object = utils:null_to_undefined(CurrentObject),
        tree_list_opts = 
            maps:fold(fun(Key, Value, Acc) -> 
                Acc#{binary_to_atom(Key, utf8) => Value} 
            end, #{}, json_utils:decode(TreeListOpts)),
        roots_iterator = atm_list_container_iterator:db_decode(RootsIterator, NestedRecordDecoder),
        queue_info = decode_queue_info(QueueInfo)
    }.


%% @private
-spec encode_queue_info(queue_info()) -> json_utils:json_term().
encode_queue_info(#queue_info{id = QueueId, current_queue_index = CurrentIndex}) ->
   #{
        <<"queueId">> => QueueId,
        <<"currentIndex">> => CurrentIndex
    }.


%% @private
-spec decode_queue_info(json_utils:json_term()) -> queue_info().
decode_queue_info(#{<<"queueId">> := QueueId, <<"currentIndex">> := CurrentIndex}) ->
    #queue_info{
        id = QueueId,
        current_queue_index = CurrentIndex
    }.

%%%===================================================================
%%% atm_tree_forest_container_iterator behaviour 
%%%===================================================================

%% @private
-spec type_specific_module(atm_data_type:type()) -> module().
type_specific_module(atm_file_type) ->
    atm_file_value.
