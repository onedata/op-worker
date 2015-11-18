%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% CDMI tests
%%% @end
%%%-------------------------------------------------------------------
-module(cdmi_test_SUITE).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([choose_adequate_handler/1, use_supported_cdmi_version/1,
    use_unsupported_cdmi_version/1, create_dir_test/1]).

-performance({test_cases, []}).
all() -> [choose_adequate_handler, use_supported_cdmi_version,
    use_unsupported_cdmi_version, create_dir_test].

-define(MACAROON, "macaroon").
-define(TIMEOUT, timer:seconds(5)).

-define(USER_1_TOKEN_HEADER, {"X-Auth-Token", "1"}).
-define(CDMI_VERSION_HEADER, {"X-CDMI-Specification-Version", "1.1.1"}).

%%%===================================================================
%%% Test functions
%%%===================================================================

choose_adequate_handler(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    File = "file",
    Dir = "dir/",

    % when
    {ok, _, _, _} = do_request(Worker, File, get, [], []),
    % then
    ?assert(rpc:call(Worker, meck, called, [cdmi_object_handler, rest_init, '_'])),

    % when
    {ok, _, _, _} = do_request(Worker, Dir, get, [], []),
    % then
    ?assert(rpc:call(Worker, meck, called, [cdmi_container_handler, rest_init, '_'])).

use_supported_cdmi_version(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [?CDMI_VERSION_HEADER, ?USER_1_TOKEN_HEADER],

    % when
    {ok, Code, _ResponseHeaders, _Response} = do_request(Worker, "/random", get, RequestHeaders, []),

    % then
    ?assertEqual("404", Code).

use_unsupported_cdmi_version(Config) ->
    % given
    [Worker | _] = ?config(op_worker_nodes, Config),
    RequestHeaders = [{"X-CDMI-Specification-Version", "1.0.2"}],

    % when
    {ok, Code, _ResponseHeaders, _Response} = do_request(Worker, "/random", get, RequestHeaders, []),

    % then
    ?assertEqual("400", Code).

% Tests dir creation (cdmi container PUT), remember that every container URI ends
% with '/'
create_dir_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    DirName = "toCreate/",

    %%------ non-cdmi create -------
    ?assert(not object_exists(Config, DirName)),

    {ok, Code1, _Headers1, _Response1} = do_request(Worker, DirName, put, [?USER_1_TOKEN_HEADER], []),
    ?assertEqual("201",Code1),

    ?assert(object_exists(Config, DirName)).
    %%------------------------------

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json")),
    initializer:setup_storage(ConfigWithNodes).
end_per_suite(Config) ->
    tracer:stop(),
    initializer:teardown_storage(Config),
    test_node_starter:clean_environment(Config).

init_per_testcase(choose_adequate_handler, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, [cdmi_object_handler, cdmi_container_handler]),
    init_per_testcase(default, Config);
init_per_testcase(_, Config) ->
    ssl:start(),
    ibrowse:start(),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(Config),
    mock_user_auth(ConfigWithSessionInfo),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(choose_adequate_handler, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, [cdmi_object_handler, cdmi_container_handler]),
    test_utils:mock_unload(Workers, [cdmi_object_handler, cdmi_container_handler]),
    end_per_testcase(default, Config);
end_per_testcase(_, Config) ->
    lfm_proxy:teardown(Config),
    unmock_user_auth(Config),
    initializer:clean_test_users_and_spaces(Config),
    ibrowse:stop(),
    ssl:stop(),
    timer:sleep(timer:seconds(10)). %todo fix datastore 'no_file_meta' error, and remove this sleep

%%%===================================================================
%%% Internal functions
%%%===================================================================

% Performs a single request using ibrowse
do_request(Node, RestSubpath, Method, Headers, Body) ->
    ibrowse:send_req(
            cdmi_endpoint(Node) ++ RestSubpath,
            Headers,
            Method,
            Body,
            [{ssl_options, [{reuse_sessions, false}]}]
        ).

cdmi_endpoint(Node) ->
    Port =
        case get(port) of
            undefined ->
                {ok, P} = rpc:call(Node, application, get_env, [?APP_NAME, http_worker_rest_port]),
                PStr = integer_to_list(P),
                put(port, PStr),
                PStr;
            P -> P
        end,
    string:join(["https://", utils:get_host(Node), ":", Port, "/cdmi/"], "").

mock_user_auth(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_new(Workers, rest_auth),
    test_utils:mock_expect(Workers, rest_auth, is_authorized,
        fun(Req, State) ->
            case cowboy_req:header(<<"x-auth-token">>, Req) of
                {undefined, NewReq} ->
                    {{false, <<"authentication_error">>}, NewReq, State};
                {Token, NewReq} ->
                    UserId = ?config({user_id, binary_to_integer(Token)}, Config),
                    {true, NewReq, State#{identity => #identity{user_id = UserId}}}
            end
        end
    ).

unmock_user_auth(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_validate(Workers, rest_auth),
    test_utils:mock_unload(Workers, rest_auth).

object_exists(Config, Path) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, 1}, Config),

    case lfm_proxy:stat(Worker, SessionId, {path, utils:ensure_unicode_binary(Path)}) of
        {ok, _} ->
            true;
        {error, ?ENOENT} ->
            false
    end.