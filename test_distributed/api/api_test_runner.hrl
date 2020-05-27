%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions of macros and records used in API (REST + gs) tests.
%%% @end
%%%-------------------------------------------------------------------
-author("Bartosz Walkowicz").

-ifndef(API_TEST_UTILS_HRL).
-define(API_TEST_UTILS_HRL, 1).

-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-type scenario_type() ::
    % Standard rest scenario - using fileId in path so that no lookup
    % takes place
    rest |
    % Rest scenario using file path in URL - causes fileId lookup in
    % rest_handler. If path can't be resolved (this file/space is not
    % supported by specific provider) rather then concrete error a
    % ?ERROR_BAD_VALUE_IDENTIFIER(<<"urlFilePath">>) will be returned
    rest_with_file_path |
    % Rest scenario that results in ?ERROR_NOT_SUPPORTED regardless
    % of request auth and parameters.
    rest_not_supported |
    % Standard graph sync scenario
    gs |
    % Gs scenario with gri.scope == private and gri.id == SharedGuid.
    % Such requests should be rejected at auth steps resulting in
    % ?ERROR_UNAUTHORIZED.
    gs_with_shared_guid_and_aspect_private |
    % Gs scenario that results in ?ERROR_NOT_SUPPORTED regardless
    % of test case due to for example invalid aspect.
    gs_not_supported.

% List of nodes to which api calls can be directed. However only one node
% from this list will be chosen (randomly) for each test case.
% It greatly shortens time needed to run tests and also allows to test e.g.
% setting some value on one node and updating it on another node.
% Checks whether value set on one node was synced with other nodes can be
% performed using `verify_fun` callback.
-type target_nodes() :: [node()].

-record(client_spec, {
    correct = [] :: [aai:auth()],
    unauthorized = [] :: [aai:auth()],
    forbidden_not_in_space = [] :: [aai:auth()],
    forbidden_in_space = [] :: [aai:auth()],
    supported_clients_per_node :: #{node() => [aai:auth()]}
}).
-type client_spec() :: #client_spec{}.

-record(data_spec, {
    required = [] :: [Key :: binary()],
    optional = [] :: [Key :: binary()],
    at_least_one = [] :: [Key :: binary()],
    correct_values = #{} :: #{Key :: binary() => Values :: [binary()]},
    bad_values = [] :: [{Key :: binary(), Value :: term(), errors:error()}]
}).
-type data_spec() :: #data_spec{}.

-record(rest_args, {
    method :: get | patch | post | put | delete,
    path :: binary(),
    headers = #{} :: #{Key :: binary() => Value :: binary()},
    body = <<>> :: binary()
}).
-type rest_args() :: #rest_args{}.

-record(gs_args, {
    operation :: gs_protocol:operation(),
    gri :: gri:gri(),
    auth_hint = undefined :: gs_protocol:auth_hint(),
    data = undefined :: undefined | map()
}).
-type gs_args() :: #gs_args{}.

-record(api_test_ctx, {
    scenario :: scenario_type(),
    node :: node(),
    client :: aai:auth(),
    env :: api_test_env(),
    data :: map()
}).
-type api_test_ctx() :: #api_test_ctx{}.

-type api_test_env() :: map().

% Function called before testcase. Can be used to create ephemeral
% things used in it as returned api_test_env() will be passed to
% test functions
-type env_setup_fun() :: fun(() -> api_test_env()).
% Function called after testcase. Can be used to clear up things
% created in env_setup_fun()
-type env_teardown_fun() :: fun((api_test_env()) -> ok).
% Function called after testcase. Can be used to check if test had
% desired effect on environment (e.g. check if resource deleted during
% test was truly deleted).
% First argument tells whether request made during testcase should succeed
-type env_verify_fun() :: fun(
    (RequestResultExpectation :: expected_success | expected_failure, api_test_env()) ->
        boolean()
).

% Function called during testcase to prepare call/request arguments
-type prepare_args_fun() :: fun((api_test_ctx()) -> rest_args() | gs_args()).
% Function called after testcase to validate returned call/request result
-type validate_call_result_fun() :: fun((api_test_ctx(), Result :: term()) -> ok | no_return()).

-record(scenario_spec, {
    name :: binary(),
    type :: scenario_type(),
    target_nodes :: target_nodes(),
    client_spec :: client_spec(),

    setup_fun = fun() -> #{} end :: env_setup_fun(),
    teardown_fun = fun(_) -> ok end :: env_teardown_fun(),
    verify_fun = fun(_, _) -> true end :: env_verify_fun(),

    prepare_args_fun :: prepare_args_fun(),
    validate_result_fun :: validate_call_result_fun(),

    data_spec = undefined :: undefined | data_spec()
}).
-type scenario_spec() :: #scenario_spec{}.

% Template used to create scenario_spec(). It contains scenario specific data
% while args/params repeated for all scenarios of some type/group can be
% extracted and kept in e.g. suite_spec().
-record(scenario_template, {
    name :: binary(),
    type :: scenario_type(),
    prepare_args_fun :: prepare_args_fun(),
    validate_result_fun :: validate_call_result_fun()
}).
-type scenario_template() :: #scenario_template{}.

% Record used to group scenarios having common parameters like target nodes, client spec
% or check functions. List of scenario_spec() will be created from that common params as
% well as scenario specific data contained in each scenario_template().
-record(suite_spec, {
    target_nodes :: target_nodes(),
    client_spec :: client_spec(),

    setup_fun = fun() -> #{} end :: env_setup_fun(),
    teardown_fun = fun(_) -> ok end :: env_teardown_fun(),
    verify_fun = fun(_, _) -> true end :: env_verify_fun(),

    scenario_templates = [] :: [scenario_template()],

    data_spec = undefined :: undefined | data_spec()
}).
-type suite_spec() :: #suite_spec{}.

-define(ATTEMPTS, 30).
-define(SCENARIO_NAME, atom_to_binary(?FUNCTION_NAME, utf8)).

-define(SPACE_1, <<"space1">>).
-define(SPACE_2, <<"space2">>).

-define(USER_IN_SPACE_1, <<"user1">>).
-define(USER_IN_SPACE_1_AUTH, ?USER(?USER_IN_SPACE_1)).

-define(USER_IN_SPACE_2, <<"user3">>).
-define(USER_IN_SPACE_2_AUTH, ?USER(?USER_IN_SPACE_2)).

-define(USER_IN_BOTH_SPACES, <<"user2">>).
-define(USER_IN_BOTH_SPACES_AUTH, ?USER(?USER_IN_BOTH_SPACES)).

-define(SUPPORTED_CLIENTS_PER_NODE(__CONFIG), (fun() ->
    [Provider2, Provider1] = ?config(op_worker_nodes, __CONFIG),
    #{
        Provider1 => [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        Provider2 => [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH]
    }
end)()).

-define(CLIENT_SPEC_FOR_SPACE_1_SCENARIOS(__CONFIG), #client_spec{
    correct = [?USER_IN_SPACE_1_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
    unauthorized = [?NOBODY],
    forbidden_not_in_space = [?USER_IN_SPACE_2_AUTH],
    supported_clients_per_node = ?SUPPORTED_CLIENTS_PER_NODE(__CONFIG)
}).
-define(CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(__CONFIG), #client_spec{
    correct = [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
    unauthorized = [?NOBODY],
    forbidden_not_in_space = [?USER_IN_SPACE_1_AUTH],
    supported_clients_per_node = ?SUPPORTED_CLIENTS_PER_NODE(__CONFIG)
}).
% Special case -> any user can make requests for shares but if request is
% being made using credentials by user not supported on specific provider
% ?ERROR_USER_NOT_SUPPORTED should be returned
-define(CLIENT_SPEC_FOR_SHARE_SCENARIOS(__CONFIG), #client_spec{
    correct = [?NOBODY, ?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
    unauthorized = [],
    forbidden_not_in_space = [],
    supported_clients_per_node = ?SUPPORTED_CLIENTS_PER_NODE(__CONFIG)
}).

-define(SESS_ID(__USER, __NODE, __CONFIG),
    ?config({session_id, {__USER, ?GET_DOMAIN(__NODE)}}, __CONFIG)
).
-define(USER_IN_BOTH_SPACES_SESS_ID(__NODE, __CONFIG),
    ?SESS_ID(?USER_IN_BOTH_SPACES, __NODE, __CONFIG)
).

-define(REST_ERROR(__ERROR), #{<<"error">> => errors:to_json(__ERROR)}).

-define(RANDOM_FILE_NAME(), generator:gen_name()).
-define(RANDOM_FILE_TYPE(), lists_utils:random_element([<<"file">>, <<"dir">>])).


-endif.
