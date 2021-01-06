%%%-------------------------------------------------------------------
%%% @author Piotr Duleba
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module exposing op-worker functions, that are used in tests.
%%% @end
%%%-------------------------------------------------------------------
-module(test_rpc_api).
-author("Piotr Duleba").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").

-export([
    get_storage_ids/0,
    get_local_storage_id/1,
    get_local_storage_ids/1,
    describe_storage/1,
    is_storage_imported/1,

    get_space_ids/0,
    get_space_document/1,
    get_autocleaning_status/1,
    get_space_support_size/1,
    get_space_providers/1,
    support_space/3,
    revoke_space_support/1
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_storage_ids() -> {ok, [storage:id()]} | {error, term()}.
get_storage_ids() ->
    rpc_api:storage_list_ids().


-spec get_local_storage_id(od_space:id()) -> {ok, storage:id()}.
get_local_storage_id(SpaceID) ->
    rpc_api:space_logic_get_storage_id(SpaceID).


-spec get_local_storage_ids(od_space:id()) -> {ok, [storage:id()]}.
get_local_storage_ids(SpaceID) ->
    rpc_api:space_logic_get_storage_ids(SpaceID).


-spec describe_storage(storage:id()) -> {ok, #{binary() := binary() | boolean() | undefined}} | {error, term()}.
describe_storage(StorageId) ->
    rpc_api:storage_describe(StorageId).


-spec is_storage_imported(storage:id()) -> boolean().
is_storage_imported(StorageId) ->
    rpc_api:storage_is_imported_storage(StorageId).


-spec get_space_ids() -> {ok, [od_space:id()]} | errors:error().
get_space_ids() ->
    rpc_api:get_spaces().


-spec get_space_document(od_space:id()) -> {ok, #{atom() := term()}} | errors:error().
get_space_document(SpaceId) ->
    rpc_api:get_space_details(SpaceId).


-spec get_autocleaning_status(od_space:id()) -> map().
get_autocleaning_status(SpaceId) ->
    rpc_api:autocleaning_status(SpaceId).


-spec get_space_support_size(od_space:id()) -> {ok, integer()} | errors:error().
get_space_support_size(SpaceId) ->
    rpc_api:get_space_support_size(SpaceId).


-spec get_space_providers(od_space:id()) -> {ok, [od_provider:id()]}.
get_space_providers(SpaceId) ->
    rpc_api:space_logic_get_provider_ids(SpaceId).


-spec support_space(storage:id(), tokens:serialized(), SupportSize :: integer()) -> {ok, od_space:id()} | errors:error().
support_space(StorageId, Token, SupportSize) ->
    rpc_api:support_space(StorageId, Token, SupportSize).


-spec revoke_space_support(od_space:id()) -> ok | {error, term()}.
revoke_space_support(SpaceId) ->
    rpc_api:revoke_space_support(SpaceId).
