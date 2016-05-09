%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Multi provider rest tests
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_rest_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include("http/rest/http_status.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    file_distribution/1
]).

all() ->
    ?ALL([
        file_distribution
    ]).

-define(TIMEOUT, timer:seconds(5)).


%%%===================================================================
%%% Test functions
%%%===================================================================

file_distribution(Config) ->
    [_WorkerP1, WorkerP2] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, <<"user1">>}, Config),
    File = <<"/file">>,
    {ok, FileGuid} = lfm_proxy:create(WorkerP2, SessionId, File, 8#700),
    {ok, Handle} = lfm_proxy:open(WorkerP2, SessionId, {guid, FileGuid}, write),
    lfm_proxy:write(WorkerP2, Handle, 0, <<"test">>),
    lfm_proxy:fsync(WorkerP2, Handle),

    % when
    {ok, 200, _, Body} = do_request(WorkerP2, <<"file_distribution/file">>, get, [user_1_token_header(Config)], []),

    % then
    ct:print("~p", Body).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(_, Config) ->
    application:start(ssl2),
    hackney:start(),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    initializer:enable_grpca_based_communication(Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config),
     %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:disable_grpca_based_communication(Config),
    hackney:stop(),
    application:stop(ssl2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_request(Node, URL, Method, Headers, Body) ->
    http_client:request(Method, <<(rest_endpoint(Node))/binary,  URL/binary>>, Headers, Body, [insecure]).

rest_endpoint(Node) ->
    Port =
        case get(port) of
            undefined ->
                {ok, P} = test_utils:get_env(Node, ?APP_NAME, rest_port),
                PStr = integer_to_binary(P),
                PStr;
            P -> P
        end,
    <<"https://", (list_to_binary(utils:get_host(Node)))/binary, ":", Port/binary, "/rest/latest/">>.


user_1_token_header(Config) ->
    #auth{macaroon = Macaroon} = ?config({auth, <<"user1">>}, Config),
    {ok, Srlzd} = macaroon:serialize(Macaroon),
    {<<"X-Auth-Token">>, Srlzd}.