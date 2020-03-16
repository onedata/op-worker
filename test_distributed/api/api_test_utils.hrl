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

-include_lib("ctool/include/errors.hrl").

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
    % of test case.
    rest_not_supported |
    % Standard graph sync scenario
    gs.

-type api_test_env() :: map().

-record(client_spec, {
    correct = [] :: [aai:auth()],
    unauthorized = [] :: [aai:auth()],
    forbidden = [] :: [aai:auth()],
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
    path = <<"/">> :: binary(),
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
    node :: node(),
    client :: aai:auth(),
    env :: api_test_env(),
    data :: map()
}).
-type api_test_ctx() :: #api_test_ctx{}.

-record(scenario_spec, {
    name :: binary(),
    type :: scenario_type(),
    target_nodes :: [node()],
    client_spec = undefined :: undefined | client_spec(),

    % Function called before every testcase. Can be used to create ephemeral
    % things used in testcase as returned api_test_env() will be passed to
    % below test functions
    setup_fun = fun() -> #{} end :: fun(() -> api_test_env()),
    % Function called after every testcase. Can be used to clear up things
    % created in `setup_fun`
    teardown_fun = fun(_) -> ok end :: fun((api_test_env()) -> ok),
    % Function called after avery testcase. Can be used to check if test had
    % desired effect on environment (e.g. check if resource deleted during
    % test was truly deleted).
    % First argument tells whether request made during testcase should succeed
    verify_fun = fun(_, _) -> true end :: fun((ShouldSucceed :: boolean(), api_test_env()) -> ok),

    % Function called during testcase to prepare request arguments
    prepare_args_fun :: fun((api_test_ctx()) -> rest_args() | gs_args()),
    % Function called after testcases that should succeed to validate returned result
    validate_result_fun :: fun((api_test_ctx(), Result :: term()) -> ok | no_return()),
    data_spec = undefined :: undefined | data_spec()
}).

-define(REST_ERROR(__ERROR), #{<<"error">> => errors:to_json(__ERROR)}).

-endif.
