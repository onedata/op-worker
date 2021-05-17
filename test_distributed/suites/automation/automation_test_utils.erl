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
-module(automation_test_utils).
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
