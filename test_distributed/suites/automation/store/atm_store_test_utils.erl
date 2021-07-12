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
-export([create_workflow_execution_ctx/3]).
-export([
    create_store/4,
    apply_operation/6,
    get/2,
    browse_content/4,
    acquire_store_iterator/3, 
    iterator_get_next/3,
    iterator_forget_before/2,
    iterator_mark_exhausted/2
]).
-export([
    split_into_chunks/3
]).
-export([example_data/1, example_bad_data/1, all_data_types/0]).

-type item() :: json_utils:json_term().

%%%===================================================================
%%% API
%%%===================================================================


-spec create_workflow_execution_ctx(
    oct_background:entity_selector(),
    oct_background:entity_selector(),
    oct_background:entity_selector()
) ->
    atm_workflow_execution_ctx:record() | no_return().
create_workflow_execution_ctx(ProviderSelector, UserSelector, SpaceSelector) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),

    AtmWorkflowExecutionId = str_utils:rand_hex(32),

    SessionId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    UserCtx = rpc:call(Node, user_ctx, new, [SessionId]),
    ok = rpc:call(Node, atm_workflow_execution_session, init, [AtmWorkflowExecutionId, UserCtx]),

    SpaceId = oct_background:get_space_id(SpaceSelector),
    rpc:call(Node, atm_workflow_execution_ctx, build, [SpaceId, AtmWorkflowExecutionId, UserCtx]).


-spec create_store(
    oct_background:entity_selector(),
    atm_workflow_execution_ctx:record(),
    atm_store_api:initial_value(),
    atm_store_schema:record()
) ->
    {ok, atm_store:id()} | {error, term()}.
create_store(ProviderSelector, AtmWorkflowExecutionCtx, InitialValue, AtmStoreSchema) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    ?extract_key(rpc:call(Node, atm_store_api, create, [
        AtmWorkflowExecutionCtx, InitialValue, AtmStoreSchema
    ])).


-spec apply_operation(
    oct_background:entity_selector(),
    atm_workflow_execution_ctx:record(),
    atm_store_container:operation(),
    automation:item(),
    atm_store_container:operation_options(),
    atm_store:id()
) ->
    ok | {error, term()}.
apply_operation(ProviderSelector, AtmWorkflowExecutionCtx, Operation, Item, Options, AtmStoreId) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    rpc:call(Node, atm_store_api, apply_operation, [
        AtmWorkflowExecutionCtx, Operation, Item, Options, AtmStoreId
    ]).


-spec get(
    oct_background:entity_selector(),
    atm_store:id()
) ->
    {ok, atm_store:record()} | {error, term()}.
get(ProviderSelector, AtmStoreId) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    rpc:call(Node, atm_store_api, get, [
        AtmStoreId
    ]).


-spec browse_content(
    oct_background:entity_selector(),
    atm_workflow_execution_ctx:record(),
    atm_store_api:browse_opts(),
    atm_store:record()
) ->
    atm_store_api:browse_result() | {error, term()}.
browse_content(ProviderSelector, AtmWorkflowExecutionCtx, BrowseOptions, AtmStore) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    rpc:call(Node, atm_store_api, browse_content, [
        AtmWorkflowExecutionCtx, BrowseOptions, AtmStore
    ]).


-spec acquire_store_iterator(
    oct_background:entity_selector(),
    atm_workflow_execution_env:record(),
    atm_store_iterator_spec:record()
) ->
    atm_store_iterator:record().
acquire_store_iterator(ProviderSelector, AtmWorkflowExecutionEnv, AtmStoreIteratorSpec) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    rpc:call(Node, atm_store_api, acquire_iterator, [
        AtmWorkflowExecutionEnv, AtmStoreIteratorSpec
    ]).


-spec iterator_get_next(oct_background:entity_selector(), atm_workflow_execution_env:record(), iterator:iterator()) ->
    {ok, iterator:item(), iterator:iterator()} | stop.
iterator_get_next(ProviderSelector, AtmWorkflowExecutionEnv, Iterator) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    rpc:call(Node, iterator, get_next, [AtmWorkflowExecutionEnv, Iterator]).


-spec iterator_forget_before(oct_background:entity_selector(), iterator:iterator()) -> ok.
iterator_forget_before(ProviderSelector, Iterator) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    rpc:call(Node, iterator, forget_before, [Iterator]).


-spec iterator_mark_exhausted(oct_background:entity_selector(), iterator:iterator()) -> ok.
iterator_mark_exhausted(ProviderSelector, Iterator) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    rpc:call(Node, iterator, mark_exhausted, [Iterator]).


-spec split_into_chunks(pos_integer(), [[item()]], [item()]) ->
    [[item()]].
split_into_chunks(_Size, Acc, []) ->
    lists:reverse(Acc);
split_into_chunks(Size, Acc, [_ | _] = Items) ->
    Chunk = lists:sublist(Items, 1, Size),
    split_into_chunks(Size, [Chunk | Acc], Items -- Chunk).


-spec example_data(atm_data_type:type()) -> automation:item().
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


-spec example_bad_data(atm_data_type:type()) -> automation:item().
example_bad_data(Type) -> 
    example_data(lists_utils:random_element(all_data_types() -- [Type])).


-spec all_data_types() -> [atm_data_type:type()].
all_data_types() -> [
    atm_integer_type,
    atm_string_type,
    atm_object_type
].
