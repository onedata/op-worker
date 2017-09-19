%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Logic modules test suite.
%%% @end
%%%-------------------------------------------------------------------
-module(logic_modules_test_SUITE).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([all/0]).

-export([
    providers_with_common_support_retrieval_test/1
]).


all() -> ?ALL([
    providers_with_common_support_retrieval_test
]).


%%%===================================================================
%%% Test functions
%%%===================================================================

providers_with_common_support_retrieval_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    save(Worker, <<"s1">>, #od_space{providers_supports = [{<<"we">>, 1}, {<<"p1">>, 1}, {<<"p2">>, 1}]}),
    save(Worker, <<"s2">>, #od_space{providers_supports = [{<<"we">>, 1}, {<<"p3">>, 1}]}),
    save(Worker, <<"s3">>, #od_space{providers_supports = [{<<"p4">>, 1}]}),
    save(Worker, <<"p1">>, #od_provider{spaces = [<<"s1">>]}),
    save(Worker, <<"p2">>, #od_provider{spaces = [<<"s1">>]}),
    save(Worker, <<"p3">>, #od_provider{spaces = [<<"s2">>]}),
    save(Worker, <<"p4">>, #od_provider{spaces = [<<"s3">>]}),
    save(Worker, <<"we">>, #od_provider{spaces = [<<"s1">>, <<"s2">>]}),
    set_own_provider_id(Worker, <<"we">>),

    % proper state
    ?assertMatch([<<"p1">>, <<"p2">>, <<"p3">>, <<"we">>], get_providers_with_common_support(Worker)),

    % no public info about this provider
    save(Worker, <<"we">>, #od_provider{spaces = [], public_only = true}),
    ?assertMatch({error, no_private_info}, get_providers_with_common_support(Worker)),

    % no info about this provider at all
    delete_document(Worker, od_provider, <<"we">>),
    ?assertMatch({error, no_info}, get_providers_with_common_support(Worker)),

    % missing space info
    save(Worker, <<"we">>, #od_provider{spaces = [<<"s1">>, <<"s2">>]}),
    delete_document(Worker, od_space, <<"s2">>),
    ?assertMatch({error, no_od_space}, get_providers_with_common_support(Worker)),

    % missing provider info
    save(Worker, <<"s2">>, #od_space{providers_supports = [{<<"we">>, 1}, {<<"p3">>, 1}]}),
    delete_document(Worker, od_provider, <<"p1">>),
    ?assertMatch({error, no_public_provider_info}, get_providers_with_common_support(Worker)),

    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

set_own_provider_id(Worker, ID) ->
    rpc:call(Worker, application, set_env, [?APP_NAME, provider_id, ID]).

save(Node, ID, Value) ->
    ?assertMatch({ok, ID}, rpc:call(Node, element(1, Value), save,
        [#document{key = ID, value = Value}])).

delete_document(Node, Model, ID) ->
    ?assertMatch(ok, rpc:call(Node, Model, delete, [ID])).

get_providers_with_common_support(Worker) ->
    case rpc:call(Worker, provider_logic, get_providers_with_common_support, []) of
        {ok, Docs} -> lists:usort(lists:map(fun(#document{key = ID}) -> ID end, Docs));
        Error -> Error
    end.