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

-include("modules/automation/atm_execution.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("test_rpc.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([create_workflow_execution_auth/3]).
-export([
    ensure_fully_expanded_data/4,
    gen_valid_data/3,
    gen_invalid_data/3
]).
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

-define(RAND_STR(Bytes), str_utils:rand_hex(Bytes)).
-define(RAND_INT(From, To), From + rand:uniform(To - From + 1) - 1).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_workflow_execution_auth(
    oct_background:entity_selector(),
    oct_background:entity_selector(),
    oct_background:entity_selector()
) ->
    atm_workflow_execution_auth:record() | no_return().
create_workflow_execution_auth(ProviderSelector, UserSelector, SpaceSelector) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),

    AtmWorkflowExecutionId = str_utils:rand_hex(32),

    SessionId = oct_background:get_user_session_id(UserSelector, ProviderSelector),
    UserCtx = rpc:call(Node, user_ctx, new, [SessionId]),
    ok = rpc:call(Node, atm_workflow_execution_session, init, [AtmWorkflowExecutionId, UserCtx]),

    SpaceId = oct_background:get_space_id(SpaceSelector),
    rpc:call(Node, atm_workflow_execution_auth, build, [SpaceId, AtmWorkflowExecutionId, UserCtx]).


-spec ensure_fully_expanded_data(
    oct_background:node_selector(),
    atm_workflow_execution_auth:record(),
    atm_value:expanded(),
    atm_store:id()
) ->
    atm_value:expanded().
ensure_fully_expanded_data(ProviderSelector, AtmWorkflowExecutionAuth, Data, AtmDataSpec) ->
    {ok, ExpandedData} = ?rpc(ProviderSelector, atm_value:expand(
        AtmWorkflowExecutionAuth,
        atm_value:compress(Data, AtmDataSpec),
        AtmDataSpec
    )),
    ExpandedData.


-spec gen_valid_data(
    oct_background:node_selector(),
    atm_workflow_execution_auth:record(),
    atm_data_spec:record()
) ->
    atm_value:expanded().
gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_array_type,
    value_constraints = #{item_data_spec := ItemDataSpec}
}) ->
    lists:map(
        fun(_) -> gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, ItemDataSpec) end,
        lists:seq(1, ?RAND_INT(5, 10))
    );

gen_valid_data(_ProviderSelector, _AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_dataset_type
}) ->
    ?RAND_STR(32);  %% TODO

gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_file_type
}) ->
    SessionId = atm_workflow_execution_auth:get_session_id(AtmWorkflowExecutionAuth),
    SpaceId = atm_workflow_execution_auth:get_space_id(AtmWorkflowExecutionAuth),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    {ok, FileGuid} = ?rpc(ProviderSelector, lfm:create(   %% TODO random file type?
        SessionId, SpaceGuid, str_utils:rand_hex(32), undefined
    )),
    {ok, ObjectId} = file_id:guid_to_objectid(FileGuid),
    #{<<"file_id">> => ObjectId};

gen_valid_data(_ProviderSelector, _AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_integer_type
}) ->
    rand:uniform(1000000);

gen_valid_data(_ProviderSelector, _AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_object_type
}) ->
    lists:foldl(fun(_, Acc) ->
        Acc#{?RAND_STR(32) => lists_utils:random_element([?RAND_STR(32), rand:uniform(1000000)])}
    end, #{}, lists:seq(1, ?RAND_INT(3, 5)));

gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_onedatafs_credentials_type
}) ->
    #{
        <<"host">> => ?rpc(ProviderSelector, oneprovider:get_domain()),
        <<"accessToken">> => atm_workflow_execution_auth:get_access_token(AtmWorkflowExecutionAuth)
    };

gen_valid_data(_ProviderSelector, _AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_string_type
}) ->
    ?RAND_STR(32).

%%example_valid_data(_AtmWorkflowExecutionAuth, #atm_data_spec{type = atm_time_series_measurements_type}) ->
%%    str_utils:rand_hex(32).


-spec gen_invalid_data(
    oct_background:node_selector(),
    atm_workflow_execution_auth:record(),
    atm_data_spec:record()
) ->
    atm_value:expanded().
gen_invalid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_array_type,
    value_constraints = #{item_data_spec := #atm_data_spec{type = ItemDataType}}
}) ->
    InvalidItemDataSpec = lists_utils:random_element(all_basic_data_types() -- [ItemDataType]),

    lists:map(
        fun(_) -> gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, InvalidItemDataSpec) end,
        lists:seq(1, ?RAND_INT(5, 10))
    );

gen_invalid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = atm_object_type
}) ->
    gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
        type = lists_utils:random_element([atm_integer_type, atm_string_type])
    });

gen_invalid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
    type = AtmDataType
}) ->
    gen_valid_data(ProviderSelector, AtmWorkflowExecutionAuth, #atm_data_spec{
        type = lists_utils:random_element(all_basic_data_types() -- [AtmDataType])
    }).


%% @private
-spec all_basic_data_types() -> [atm_data_type:type()].
all_basic_data_types() -> [
%%    atm_dataset_type,
    atm_file_type,
    atm_integer_type,
    atm_object_type,
    atm_onedatafs_credentials_type,
    atm_string_type
].


-spec create_store(
    oct_background:entity_selector(),
    atm_workflow_execution_auth:record(),
    atm_store_api:initial_content(),
    atm_store_schema:record()
) ->
    {ok, atm_store:id()} | {error, term()}.
create_store(ProviderSelector, AtmWorkflowExecutionAuth, InitialValue, AtmStoreSchema) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    ?extract_key(rpc:call(Node, atm_store_api, create, [
        AtmWorkflowExecutionAuth, InitialValue, AtmStoreSchema
    ])).


-spec apply_operation(
    oct_background:entity_selector(),
    atm_workflow_execution_auth:record(),
    atm_store_container:operation(),
    automation:item(),
    atm_store_container:operation_options(),
    atm_store:id()
) ->
    ok | {error, term()}.
apply_operation(ProviderSelector, AtmWorkflowExecutionAuth, Operation, Item, Options, AtmStoreId) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    rpc:call(Node, atm_store_api, apply_operation, [
        AtmWorkflowExecutionAuth, Operation, Item, Options, AtmStoreId
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
    atm_workflow_execution_auth:record(),
    atm_store_api:browse_options(),
    atm_store:record()
) ->
    atm_store_api:browse_result() | {error, term()}.
browse_content(ProviderSelector, AtmWorkflowExecutionAuth, BrowseOptions, AtmStore) ->
    Node = oct_background:get_random_provider_node(ProviderSelector),
    rpc:call(Node, atm_store_api, browse_content, [
        AtmWorkflowExecutionAuth, BrowseOptions, AtmStore
    ]).


-spec acquire_store_iterator(
    oct_background:entity_selector(),
    atm_store:id(),
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
