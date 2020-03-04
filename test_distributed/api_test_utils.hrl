%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @doc
%%% Definitions of macros and records used in API (REST + gs) tests.
%%% @end
%%%-------------------------------------------------------------------
-author("Bartosz Walkowicz").

-ifndef(API_TEST_UTILS_HRL).
-define(API_TEST_UTILS_HRL, 1).

-include_lib("ctool/include/errors.hrl").

-type client() :: nobody | root | {user, UserId :: binary()}.
-type scenario_type() :: rest | gs.

-record(client_spec, {
    correct = [] :: [client()],
    unauthorized = [] :: [client()],
    forbidden = [] :: [client()]
}).

-record(params_spec, {
    required = [] :: [Key :: binary()],
    optional = [] :: [Key :: binary()],
    at_least_one = [] :: [Key :: binary()],
    correct_values = #{} :: #{Key :: binary() => Values :: [binary()]},
    bad_values = [] :: [{Key :: binary(), Value :: term(), errors:error()}]
}).

-record(rest_args, {
    method = get :: get | patch | post | put | delete,
    path = <<"/">> :: binary(),
    headers = undefined :: undefined | #{Key :: binary() => Value :: binary()},
    body = <<>> :: binary()
}).

-record(scenario_spec, {
    type :: scenario_type(),
    target_node :: node(),
    client_spec = undefined :: undefined | #client_spec{},

    setup_fun = fun() -> #{} end :: fun(() -> TestEnv :: map()),
    teardown_fun = fun(_) -> ok end :: fun((TestEnv :: map()) -> ok),
    verify_fun = fun(_, _, _) -> ok end,

    prepare_args_fun :: fun((Env :: map(), Data :: map()) -> #rest_args{}),
    validate_result_fun :: fun((Result :: term()) -> ok | no_return()),
    params_spec = undefined :: undefined | #params_spec{}
}).

-endif.
