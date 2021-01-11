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


-record(client_spec, {
    correct = [] :: [aai:auth() | onenv_api_test_runner:client_placeholder()],
    % list of clients unauthorized to perform operation. By default it is assumed
    % that ?ERROR_UNAUTHORIZED is returned when executing operation on their behalf
    % but it is possible to specify concrete error if necessary (edge cases).
    unauthorized = [] :: [
        aai:auth() |
        onenv_api_test_runner:client_placeholder() |
        {aai:auth() | onenv_api_test_runner:client_placeholder(), errors:error()}
    ],
    % list of clients (members of space in context of which operation is performed)
    % forbidden to perform operation. By default it is assumed that ?ERROR_FORBIDDEN
    % is returned when executing operation on their behalf but it is possible to
    % specify concrete error if necessary (edge cases).
    forbidden_in_space = [] :: [
        aai:auth() |
        onenv_api_test_runner:client_placeholder() |
        {aai:auth() | onenv_api_test_runner:client_placeholder(), errors:error()}
    ],
    % list of clients (not in space in context of which operation is performed)
    % forbidden to perform operation. By default it is assumed that ?ERROR_FORBIDDEN
    % is returned when executing operation on their behalf but it is possible to
    % specify concrete error if necessary (edge cases).
    forbidden_not_in_space = [] :: [
        aai:auth() |
        onenv_api_test_runner:client_placeholder() |
        {aai:auth() | onenv_api_test_runner:client_placeholder(), errors:error()}
    ],
    supported_clients_per_node :: #{node() => [aai:auth()]}
}).

-record(data_spec, {
    required = [] :: [Key :: binary()],
    optional = [] :: [Key :: binary()],
    at_least_one = [] :: [Key :: binary()],
    correct_values = #{} :: #{Key :: binary() => Values :: [binary()]},
    bad_values = [] :: [{
        Key :: binary(),
        Value :: term(),
        errors:error() | {onenv_api_test_runner:scenario_type(), errors:error()}
    }]
}).

-record(rest_args, {
    method :: get | patch | post | put | delete,
    path :: binary(),
    headers = #{} :: #{Key :: binary() => Value :: binary()},
    body = <<>> :: binary()
}).

-record(gs_args, {
    operation :: gs_protocol:operation(),
    gri :: gri:gri(),
    auth_hint = undefined :: gs_protocol:auth_hint(),
    data = undefined :: undefined | map()
}).

-record(api_test_ctx, {
    scenario_name :: binary(),
    scenario_type :: onenv_api_test_runner:scenario_type(),
    node :: node(),
    client :: aai:auth(),
    data :: map()
}).

-record(scenario_spec, {
    name :: binary(),
    type :: onenv_api_test_runner:scenario_type(),
    target_nodes :: onenv_api_test_runner:target_nodes(),
    client_spec :: onenv_api_test_runner:client_spec(),

    setup_fun = fun() -> ok end :: onenv_api_test_runner:setup_fun(),
    teardown_fun = fun() -> ok end :: onenv_api_test_runner:teardown_fun(),
    verify_fun = fun(_, _) -> true end :: onenv_api_test_runner:verify_fun(),

    prepare_args_fun :: onenv_api_test_runner:prepare_args_fun(),
    validate_result_fun :: onenv_api_test_runner:validate_call_result_fun(),

    data_spec = undefined :: undefined | onenv_api_test_runner:data_spec()
}).

-record(scenario_template, {
    name :: binary(),
    type :: onenv_api_test_runner:scenario_type(),
    prepare_args_fun :: onenv_api_test_runner:prepare_args_fun(),
    validate_result_fun :: onenv_api_test_runner:validate_call_result_fun()
}).

-record(suite_spec, {
    target_nodes :: onenv_api_test_runner:target_nodes(),
    client_spec :: onenv_api_test_runner:client_spec(),

    setup_fun = fun() -> ok end :: onenv_api_test_runner:setup_fun(),
    teardown_fun = fun() -> ok end :: onenv_api_test_runner:teardown_fun(),
    verify_fun = fun(_, _) -> true end :: onenv_api_test_runner:verify_fun(),

    scenario_templates = [] :: [onenv_api_test_runner:scenario_template()],
    % If set then instead of running all scenarios for all clients and data sets
    % only one scenario will be drawn from 'scenario_templates' for client and
    % data set combination. For 2 clients (client1, client2), 2 data sets (data1,
    % data2), 2 providers (provider1, provider2) and 2 scenario_templates (A, B)
    % only 4 testcase will be run (instead of 8 in case of flag not set), e.g.:
    % - client1 makes call with data1 on provider1 using scenario B
    % - client1 makes call with data2 on provider2 using scenario A
    % - client2 makes call with data1 on provider2 using scenario A
    % - client2 makes call with data2 on provider1 using scenario B
    randomly_select_scenarios = false,

    data_spec = undefined :: undefined | onenv_api_test_runner:data_spec()
}).

-define(SCENARIO_NAME, atom_to_binary(?FUNCTION_NAME, utf8)).

-define(SPACE_1, <<"space1">>).
-define(SPACE_2, <<"space2">>).
-define(SPACE_KRK, <<"space_krk">>).
-define(SPACE_KRK_PAR, <<"space_krk_par">>).

-define(USER_IN_SPACE_1, <<"user1">>).
-define(USER_IN_SPACE_1_AUTH, ?USER(?USER_IN_SPACE_1)).
-define(USER_IN_SPACE_KRK, <<"user1">>).
-define(USER_IN_SPACE_KRK_AUTH, ?USER(?USER_IN_SPACE_KRK)).

-define(USER_IN_SPACE_2, <<"user3">>).
-define(USER_IN_SPACE_2_AUTH, ?USER(?USER_IN_SPACE_2)).
-define(USER_IN_SPACE_KRK_PAR, <<"user3">>).
-define(USER_IN_SPACE_KRK_PAR_AUTH, ?USER(?USER_IN_SPACE_KRK_PAR)).

-define(USER_IN_BOTH_SPACES, <<"user2">>).
-define(USER_IN_BOTH_SPACES_AUTH, ?USER(?USER_IN_BOTH_SPACES)).

-define(SUPPORTED_CLIENTS_PER_NODE(__CONFIG), (fun() ->
    [Provider2, Provider1] = ?config(op_worker_nodes, __CONFIG),
    #{
        Provider1 => [?USER_IN_SPACE_KRK_AUTH, ?USER_IN_SPACE_KRK_PAR_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        Provider2 => [?USER_IN_SPACE_KRK_PAR_AUTH, ?USER_IN_BOTH_SPACES_AUTH]
    }
end)()).

-define(CLIENT_SPEC_FOR_SPACE_KRK_SCENARIOS(__CONFIG), #client_spec{
    correct = [?USER_IN_SPACE_KRK_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
    unauthorized = [?NOBODY],
    forbidden_not_in_space = [?USER_IN_SPACE_KRK_PAR_AUTH],
    supported_clients_per_node = ?SUPPORTED_CLIENTS_PER_NODE(__CONFIG)
}).
-define(CLIENT_SPEC_FOR_SPACE_KRK_PAR_SCENARIOS(__CONFIG), #client_spec{
    correct = [?USER_IN_SPACE_KRK_PAR_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
    unauthorized = [?NOBODY],
    forbidden_not_in_space = [?USER_IN_SPACE_KRK_AUTH],
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
% ?ERROR_UNAUTHORIZED(?ERROR_USER_NOT_SUPPORTED) should be returned
-define(CLIENT_SPEC_FOR_SHARE_SCENARIOS(__CONFIG), #client_spec{
    correct = [?NOBODY, ?USER_IN_SPACE_KRK_AUTH, ?USER_IN_SPACE_KRK_PAR_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
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
