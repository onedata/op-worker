%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Oneprovider RPC api that should be sued by tests (the shouldn't directly
%%% call op as having everything in one place will help with refactoring).
%%% @end
%%%-------------------------------------------------------------------
-module(op_test_rpc).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/test/test_utils.hrl").

%% API
-export([
    get_cert_chain_ders/1,
    gs_protocol_supported_versions/1,

    get_provider_id/1,
    get_provider_domain/1,

    supports_space/2,
    get_supporting_storage_id/2,
    space_capacity_usage/2,

    create_fuse_session/4
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_cert_chain_ders(node()) -> [public_key:der_encoded()] | no_return().
get_cert_chain_ders(Node) ->
    ?assertMatch([_ | _], rpc:call(Node, https_listener, get_cert_chain_ders, [])).


-spec gs_protocol_supported_versions(node()) -> [gs_protocol:protocol_version()].
gs_protocol_supported_versions(Node) ->
    ?assertMatch([_ | _], rpc:call(Node, gs_protocol, supported_versions, [])).


-spec get_provider_id(node()) -> binary() | no_return().
get_provider_id(Node) ->
    ?assertMatch(<<_/binary>>, rpc:call(Node, oneprovider, get_id, [])).


-spec get_provider_domain(node()) -> binary() | no_return().
get_provider_domain(Node) ->
    ?assertMatch(<<_/binary>>, rpc:call(Node, oneprovider, get_domain, [])).


-spec supports_space(node(), od_space:id()) -> boolean().
supports_space(Node, SpaceId) ->
    rpc:call(Node, provider_logic, supports_space, [SpaceId]).


-spec get_supporting_storage_id(node(), od_space:id()) -> storage:id().
get_supporting_storage_id(Node, SpaceId) ->
    {ok, [StorageId]} = rpc:call(Node, space_logic, get_local_storage_ids, [SpaceId]),
    StorageId.


-spec space_capacity_usage(node(), od_space:id()) -> non_neg_integer().
space_capacity_usage(Node, SpaceId) ->
    rpc:call(Node, space_quota, current_size, [SpaceId]).


-spec create_fuse_session(node(), binary(), aai:subject(),
    auth_manager:token_credentials()) -> {ok, session:id()} | no_return().
create_fuse_session(Node, Nonce, Identity, TokenCredentials) ->
    ?assertMatch({ok, _}, rpc:call(
        Node,
        session_manager,
        reuse_or_create_fuse_session,
        [Nonce, Identity, TokenCredentials]
    )).
