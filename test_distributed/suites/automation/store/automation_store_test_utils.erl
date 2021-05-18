%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module with utility functions for automation tests
%%% @end
%%%-------------------------------------------------------------------
-module(automation_store_test_utils).
-author("Michal Stanisz").

-include("modules/automation/atm_tmp.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([
    create_store/3, 
    update_store/5,
    acquire_store_iterator/3, 
    iterator_get_next/2, 
    iterator_jump_to/3
]).
-export([
    split_into_chunks/3
]).
-export([example_data/1, example_bad_data/1, all_data_types/0]).

-type item() :: json_utils:json_term().

%%%===================================================================
%%% API
%%%===================================================================

-spec create_store(node(), atm_store_api:initial_value(), atm_store_schema:record()) ->
    {ok, atm_store:id()} | {error, term()}.
create_store(Node, InitialValue, AtmStoreSchema) ->
    ?extract_key(rpc:call(Node, atm_store_api, create, [
        <<"dummyId">>, InitialValue, AtmStoreSchema
    ])).


-spec update_store(node(), atm_store:id(), atm_container:update_operation(), 
    atm_container:update_options(), json_utils:json_term()) -> ok | {error, term()}.
update_store(Node, AtmStoreId, Operation, Options, Item) ->
    rpc:call(Node, atm_store_api, update, [AtmStoreId, Operation, Options, Item]).


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
    {ok, iterator:item(), iterator:cursor(), iterator:iterato()} | stop.
iterator_get_next(Node, Iterator) ->
    rpc:call(Node, iterator, get_next, [Iterator]).


-spec iterator_jump_to(node(), iterator:cursor(), iterator:iterator()) ->
    iterator:iterato().
iterator_jump_to(Node, Cursor, Iterator) ->
    rpc:call(Node, iterator, jump_to, [Cursor, Iterator]).


-spec split_into_chunks(pos_integer(), [[item()]], [item()]) ->
    [[item()]].
split_into_chunks(_Size, Acc, []) ->
    lists:reverse(Acc);
split_into_chunks(Size, Acc, [_ | _] = Items) ->
    Chunk = lists:sublist(Items, 1, Size),
    split_into_chunks(Size, [Chunk | Acc], Items -- Chunk).


-spec example_data(atm_data_type:type()) -> json_utils:json_term().
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


-spec example_bad_data(atm_data_type:type()) -> json_utils:json_term().
example_bad_data(Type) -> 
    example_data(lists_utils:random_element(all_data_types() -- [Type])).


-spec all_data_types() -> [atm_data_type:type()].
all_data_types() -> [
    atm_integer_type,
    atm_string_type,
    atm_object_type
].
