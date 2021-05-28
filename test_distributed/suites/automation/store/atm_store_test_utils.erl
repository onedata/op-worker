%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module with utility functions for automation store tests
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_test_utils).
-author("Michal Stanisz").

-include("modules/automation/atm_tmp.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([
    create_store/4,
    apply_operation/6,
    acquire_store_iterator/3, 
    iterator_get_next/2
]).
-export([
    split_into_chunks/3
]).
-export([example_data/1, example_bad_data/1, all_data_types/0]).

-type item() :: json_utils:json_term().

%%%===================================================================
%%% API
%%%===================================================================

-spec create_store(
    node(),
    atm_workflow_execution_ctx:record(),
    atm_store_api:initial_value(),
    atm_store_schema:record()
) ->
    {ok, atm_store:id()} | {error, term()}.
create_store(Node, AtmWorkflowExecutionCtx, InitialValue, AtmStoreSchema) ->
    ?extract_key(rpc:call(Node, atm_store_api, create, [
        AtmWorkflowExecutionCtx, InitialValue, AtmStoreSchema
    ])).


-spec apply_operation(
    node(),
    atm_workflow_execution_ctx:record(),
    atm_container:operation(),
    atm_api:item(),
    atm_container:operation_options(),
    atm_store:id()
) ->
    ok | {error, term()}.
apply_operation(Node, AtmWorkflowExecutionCtx, Operation, Item, Options, AtmStoreId) ->
    rpc:call(Node, atm_store_api, apply_operation, [
        AtmWorkflowExecutionCtx, Operation, Item, Options, AtmStoreId
    ]).


-spec acquire_store_iterator(
    node(),
    atm_workflow_execution_env:record(),
    atm_store_iterator_spec:record()
) ->
    atm_store_iterator:record().
acquire_store_iterator(Node, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec) ->
    rpc:call(Node, atm_store_api, acquire_iterator, [
        AtmWorkflowExecutionEnv, AtmStoreIteratorSpec
    ]).


-spec iterator_get_next(node(), iterator:iterator()) ->
    {ok, iterator:item(), iterator:iterato()} | stop.
iterator_get_next(Node, Iterator) ->
    rpc:call(Node, iterator, get_next, [Iterator]).


-spec split_into_chunks(pos_integer(), [[item()]], [item()]) ->
    [[item()]].
split_into_chunks(_Size, Acc, []) ->
    lists:reverse(Acc);
split_into_chunks(Size, Acc, [_ | _] = Items) ->
    Chunk = lists:sublist(Items, 1, Size),
    split_into_chunks(Size, [Chunk | Acc], Items -- Chunk).


-spec example_data(atm_data_type:type()) -> atm_api:item().
example_data(atm_integer_type) -> 
    rand:uniform(1000000);
example_data(atm_string_type) -> 
    str_utils:rand_hex(32);
example_data(atm_object_type) -> 
    lists:foldl(fun(_, Acc) ->
        Key = example_data(atm_string_type),
        Value = example_data(lists_utils:random_element(all_data_types())),
        Acc#{Key => Value}
    end, #{}, lists:seq(1, rand:uniform(3) - 1)).


-spec example_bad_data(atm_data_type:type()) -> atm_api:item().
example_bad_data(Type) -> 
    example_data(lists_utils:random_element(all_data_types() -- [Type])).


-spec all_data_types() -> [atm_data_type:type()].
all_data_types() -> [
    atm_integer_type,
    atm_string_type,
    atm_object_type
].
