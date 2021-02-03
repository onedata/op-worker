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
    create_fuse_session/3,
    build_token_credentials/5,

    get_storages/0,
    storage_describe/1,
    is_storage_imported/1,

    get_spaces/0,
    get_space_details/1,
    get_space_local_storages/1,
    get_space_capacity_usage/1,
    get_autocleaning_status/1,
    get_support_size/1,
    get_space_providers/1,
    supports_space/1,
    support_space/3,
    revoke_space_support/1,

    get_provider_id/0,
    get_provider_domain/0,
    get_provider_name/0,
    get_provider_eff_users/0,

    get_cert_chain_ders/0,
    gs_protocol_supported_versions/0
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_fuse_session(binary(), aai:subject(),
    auth_manager:token_credentials()) -> {ok, session:id()} | no_return().
create_fuse_session(Nonce, Identity, TokenCredentials) ->
    session_manager:reuse_or_create_fuse_session(Nonce, Identity, TokenCredentials).


-spec build_token_credentials(
    binary(),
    undefined | binary(),
    undefined | ip_utils:ip(),
    undefined | cv_interface:interface(),
    data_access_caveats:policy()
) ->
    auth_manager:token_credentials().
build_token_credentials(AccessToken, ConsumerToken, PeerIp, Interface, DataAccessCaveatsPolicy) ->
    auth_manager:build_token_credentials(AccessToken, ConsumerToken, PeerIp, Interface, DataAccessCaveatsPolicy).


-spec get_storages() -> {ok, [storage:id()]} | {error, term()}.
get_storages() ->
    rpc_api:get_storages().


-spec storage_describe(storage:id()) -> {ok, #{binary() := binary() | boolean() | undefined}} | {error, term()}.
storage_describe(StorageId) ->
    rpc_api:storage_describe(StorageId).


-spec is_storage_imported(storage:id()) -> boolean().
is_storage_imported(StorageId) ->
    rpc_api:storage_is_imported_storage(StorageId).


-spec get_spaces() -> {ok, [od_space:id()]} | errors:error().
get_spaces() ->
    rpc_api:get_spaces().


-spec get_space_details(od_space:id()) -> {ok, #{atom() := term()}} | errors:error().
get_space_details(SpaceId) ->
    rpc_api:get_space_details(SpaceId).


-spec get_space_local_storages(od_space:id()) -> {ok, [storage:id()]}.
get_space_local_storages(SpaceId) ->
    rpc_api:space_logic_get_storages(SpaceId).


-spec get_space_capacity_usage(od_space:id()) -> integer().
get_space_capacity_usage(SpaceId) ->
    space_quota:current_size(SpaceId).


-spec get_autocleaning_status(od_space:id()) -> map().
get_autocleaning_status(SpaceId) ->
    rpc_api:autocleaning_status(SpaceId).


-spec get_support_size(od_space:id()) -> {ok, integer()} | errors:error().
get_support_size(SpaceId) ->
    provider_logic:get_support_size(SpaceId).


-spec get_space_providers(od_space:id()) -> {ok, [od_provider:id()]}.
get_space_providers(SpaceId) ->
    space_logic:get_provider_ids(SpaceId).


-spec supports_space(od_space:id()) -> boolean().
supports_space(SpaceId) ->
    provider_logic:supports_space(SpaceId).


-spec support_space(storage:id(), tokens:serialized(), SupportSize :: integer()) -> {ok, od_space:id()} | errors:error().
support_space(StorageId, Token, SupportSize) ->
    rpc_api:support_space(StorageId, Token, SupportSize).


-spec revoke_space_support(od_space:id()) -> ok | {error, term()}.
revoke_space_support(SpaceId) ->
    rpc_api:revoke_space_support(SpaceId).


-spec get_provider_id() -> binary() | no_return().
get_provider_id() ->
    oneprovider:get_id().


-spec get_provider_domain() -> binary() | no_return().
get_provider_domain() ->
    oneprovider:get_domain().


-spec get_provider_name() -> {ok, od_provider:name()} | errors:error().
get_provider_name() ->
    provider_logic:get_name().


-spec get_provider_eff_users() -> {ok, [od_user:id()]} | errors:error().
get_provider_eff_users() ->
    provider_logic:get_eff_users().


-spec get_cert_chain_ders() -> [public_key:der_encoded()] | no_return().
get_cert_chain_ders() ->
    https_listener:get_cert_chain_ders().


-spec gs_protocol_supported_versions() -> [gs_protocol:protocol_version()].
gs_protocol_supported_versions() ->
    gs_protocol:supported_versions().
