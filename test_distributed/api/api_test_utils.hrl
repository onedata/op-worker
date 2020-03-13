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

-type client() :: nobody | root | {user, UserId :: binary()}.
-type scenario_type() :: rest | rest_with_file_path | rest_not_supported | gs.

-record(client_spec, {
    correct = [] :: [client()],
    unauthorized = [] :: [client()],
    forbidden = [] :: [client()],
    supported_clients_per_node :: #{node() => [client()]}
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
    method = get :: get | patch | post | put | delete,
    path = <<"/">> :: binary(),
    headers = undefined :: undefined | #{Key :: binary() => Value :: binary()},
    body = <<>> :: binary()
}).
-type rest_args() :: #rest_args{}.

-record(gs_args, {
    operation = get :: gs_protocol:operation(),
    gri :: gri:gri(),
    subscribe = false :: boolean(),
    auth_hint = undefined :: gs_protocol:auth_hint(),
    data = undefined :: undefined | map()
}).
-type gs_args() :: #gs_args{}.

-record(api_test_ctx, {
    node :: node(),
    client :: client(),
    env :: map(),
    data :: map()
}).
-type api_test_ctx() :: #api_test_ctx{}.

-record(scenario_spec, {
    name :: binary(),
    type :: scenario_type(),
    target_nodes :: [node()],
    client_spec = undefined :: undefined | client_spec(),

    % Function called before every testcase
    setup_fun = fun() -> #{} end :: fun(() -> TestEnv :: map()),
    teardown_fun = fun(_) -> ok end :: fun((TestEnv :: map()) -> ok),
    % Function called after avery testcase to
    verify_fun = fun(_, _, _) -> true end,

    prepare_args_fun :: fun((api_test_ctx()) -> rest_args() | gs_args()),
    validate_result_fun :: fun((api_test_ctx(), Result :: term()) -> ok | no_return()),
    data_spec = undefined :: undefined | data_spec()
}).

-endif.
