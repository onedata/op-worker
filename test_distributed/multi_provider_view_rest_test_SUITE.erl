%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Multi provider view REST tests
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_view_rest_test_SUITE).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("http/rest.hrl").
-include("proto/common/credentials.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("rest_test_utils.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").


%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_get_update_delete_view/1,
    creating_view_with_invalid_params_should_fail/1,
    updating_view_with_invalid_params_should_fail/1,
    overwriting_view_should_fail/1,
    create_get_delete_reduce_fun/1,
    getting_nonexistent_view_should_fail/1,
    getting_view_of_not_supported_space_should_fail/1,
    list_views/1,
    query_view/1,
    quering_view_with_invalid_params_should_fail/1,
    create_geospatial_view/1,
    query_geospatial_view/1,
    query_file_popularity_view/1,
    spatial_flag_test/1,
    file_removal_test/1,
    create_duplicated_views_on_remote_providers/1]).

all() ->
    ?ALL([
        create_get_update_delete_view,
        creating_view_with_invalid_params_should_fail,
        updating_view_with_invalid_params_should_fail,
        overwriting_view_should_fail,
        create_get_delete_reduce_fun,
        getting_nonexistent_view_should_fail,
        getting_view_of_not_supported_space_should_fail,
        list_views,
        query_view,
        quering_view_with_invalid_params_should_fail,
        create_geospatial_view,
        query_geospatial_view,
        query_file_popularity_view,
        spatial_flag_test,
        file_removal_test,
        create_duplicated_views_on_remote_providers
    ]).

-define(ATTEMPTS, 100).

-define(SPACE_ID, <<"space1">>).
-define(PROVIDER_ID(__Node), rpc:call(__Node, oneprovider, get_id, [])).

-define(USER_1_AUTH_HEADERS(Config), ?USER_1_AUTH_HEADERS(Config, [])).
-define(USER_1_AUTH_HEADERS(Config, OtherHeaders),
    ?USER_AUTH_HEADERS(Config, <<"user1">>, OtherHeaders)).

-define(FILE_POPULARITY_VIEW(__FunctionName), begin
    FunctionNameBin = str_utils:to_binary(__FunctionName),
    RandomIntBin = integer_to_binary(erlang:unique_integer([positive])),
    <<"view_", FunctionNameBin/binary, RandomIntBin/binary>>
end).

-define(VIEW_PATH(__SpaceId, __ViewName),
    <<"spaces/", __SpaceId/binary, "/views/", __ViewName/binary>>
).

-define(XATTR_NAME, begin
    __RandInt = rand:uniform(1073741824),
    <<"onexattr_", (integer_to_binary(__RandInt))/binary>>
end).
-define(XATTR(__XattrName, __Val), #xattr{name = __XattrName, value = __Val}).

-define(MAP_FUNCTION(XattrName),
    <<"function (id, type, meta, ctx) {
        if(type == 'custom_metadata' && meta['", XattrName/binary,"']) {
            return [meta['", XattrName/binary,"'], id];
        }
        return null;
    }">>
).
-define(MAP_FUNCTION2(XattrName),
    <<"function (id, type, meta, ctx) {
        if(type == 'custom_metadata' && meta['", XattrName/binary,"']) {
            return [id, meta['", XattrName/binary,"']];
        }
        return null;
    }">>
).
-define(GEOSPATIAL_MAP_FUNCTION,
    <<"function (id, type, meta, ctx) {
        if(type == 'custom_metadata' && meta['onedata_json'] && meta['onedata_json']['loc']) {
            return [meta['onedata_json']['loc'], id];
        }
        return null;
    }">>
).

-define(REDUCE_FUNCTION(XattrName),
    <<"function (id, meta) {
        if(meta['", XattrName/binary,"']) {
            return [meta['", XattrName/binary,"'], id];
        }
        return null;
    }">>
).

-define(VIEW@(ViewName, ProviderId),
    <<ViewName/binary, "@", (ProviderId)/binary >>).


%%%===================================================================
%%% Test functions
%%%===================================================================


create_get_update_delete_view(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    Provider1 = ?PROVIDER_ID(WorkerP1),
    Provider2 = ?PROVIDER_ID(WorkerP2),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,
    UserId = <<"user1">>,

    AllPrivs = privileges:space_privileges(),
    ErrorForbidden = rest_test_utils:get_rest_error(?ERROR_FORBIDDEN),

    %% CREATE

    Options1 = #{<<"update_min_changes">> => 10000},

    % creating view without SPACE_MANAGE_VIEWS privilege should fail
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, AllPrivs -- [?SPACE_MANAGE_VIEWS]),
    ?assertMatch([], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100)),
    ?assertMatch(ErrorForbidden, create_view_via_rest(
        Config, WorkerP1, ?SPACE_ID, ViewName, ?MAP_FUNCTION(XattrName), false, [], Options1
    )),
    ?assertMatch([], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100)),

    % creating view with SPACE_MANAGE_VIEWS privilege should succeed
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, [?SPACE_MANAGE_VIEWS, ?SPACE_VIEW_VIEWS]),
    ?assertMatch(ok, create_view_via_rest(
        Config, WorkerP1, ?SPACE_ID, ViewName, ?MAP_FUNCTION(XattrName), false, [], Options1
    )),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    %% GET

    % viewing view without SPACE_VIEW_VIEWS should fail
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, AllPrivs -- [?SPACE_VIEW_VIEWS]),
    lists:foreach(fun(Worker) ->
        ?assertMatch(ErrorForbidden, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers),

    % viewing view with SPACE_VIEW_VIEWS should succeed
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, [?SPACE_VIEW_VIEWS]),
    ExpMapFun = view_utils:escape_js_function(?MAP_FUNCTION(XattrName)),
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := Options1,
            <<"viewOptions">> := Options1,
            <<"providers">> := [Provider1],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers),

    %% UPDATE

    Options2 = #{<<"replica_update_min_changes">> => 100},

    % updating view without SPACE_MANAGE_VIEWS privilege should fail
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, AllPrivs -- [?SPACE_MANAGE_VIEWS]),
    ?assertMatch(ErrorForbidden, update_view_via_rest(
        Config, WorkerP1, ?SPACE_ID, ViewName,
        <<>>, Options2#{providers => [Provider2]}
    )),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % assert nothing was changed
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := Options1,
            <<"viewOptions">> := Options1,
            <<"providers">> := [Provider1],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers),

    % updating view (without overriding map function) with SPACE_MANAGE_VIEWS privilege should succeed
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, [?SPACE_MANAGE_VIEWS, ?SPACE_VIEW_VIEWS]),
    ?assertMatch(ok, update_view_via_rest(
        Config, WorkerP1, ?SPACE_ID, ViewName,
        <<>>, Options2#{providers => [Provider2]}
    )),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % get on both after update
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := Options2,
            <<"viewOptions">> := Options2,
            <<"providers">> := [Provider2],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers),

    % updating view (with overriding map function) with SPACE_MANAGE_VIEWS privilege should succeed
    ?assertMatch(ok, update_view_via_rest(
        Config, WorkerP1, ?SPACE_ID, ViewName, ?GEOSPATIAL_MAP_FUNCTION, #{}
    )),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % get on both after update
    ExpMapFun2 = view_utils:escape_js_function(?GEOSPATIAL_MAP_FUNCTION),
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := Options2,
            <<"viewOptions">> := Options2,
            <<"providers">> := [Provider2],
            <<"mapFunction">> := ExpMapFun2,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers),

    %% DELETE

    % deleting view without SPACE_MANAGE_VIEWS privilege should fail
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, AllPrivs -- [?SPACE_MANAGE_VIEWS]),
    ?assertMatch(ErrorForbidden, remove_view_via_rest(Config, WorkerP2, ?SPACE_ID, ViewName)),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % deleting view with SPACE_MANAGE_VIEWS privilege should succeed
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, [?SPACE_MANAGE_VIEWS, ?SPACE_VIEW_VIEWS]),
    ?assertMatch(ok, remove_view_via_rest(Config, WorkerP2, ?SPACE_ID, ViewName)),
    ?assertMatch([], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS).


creating_view_with_invalid_params_should_fail(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    lists:foreach(fun({Options, ExpError}) ->
        Worker = lists:nth(rand:uniform(length(Workers)), Workers),
        ExpRestError = rest_test_utils:get_rest_error(ExpError),
        ?assertMatch(ExpRestError, create_view_via_rest(
            Config, Worker, ?SPACE_ID, ViewName, ?MAP_FUNCTION(XattrName),
            maps:get(spatial, Options, false), maps:get(providers, Options, []),
            Options
        )),
        ?assertMatch([], list_views_via_rest(Config, Worker, ?SPACE_ID, 100))
    end, [
        {#{update_min_changes => ok}, ?ERROR_BAD_VALUE_INTEGER(<<"update_min_changes">>)},
        {#{update_min_changes => -3}, ?ERROR_BAD_VALUE_TOO_LOW(<<"update_min_changes">>, 1)},
        {#{update_min_changes => 15.2}, ?ERROR_BAD_VALUE_INTEGER(<<"update_min_changes">>)},
        {#{update_min_changes => 0}, ?ERROR_BAD_VALUE_TOO_LOW(<<"update_min_changes">>, 1)},

        {#{replica_update_min_changes => ok}, ?ERROR_BAD_VALUE_INTEGER(<<"replica_update_min_changes">>)},
        {#{replica_update_min_changes => -3}, ?ERROR_BAD_VALUE_TOO_LOW(<<"replica_update_min_changes">>, 1)},
        {#{replica_update_min_changes => 15.2}, ?ERROR_BAD_VALUE_INTEGER(<<"replica_update_min_changes">>)},
        {#{replica_update_min_changes => 0}, ?ERROR_BAD_VALUE_TOO_LOW(<<"replica_update_min_changes">>, 1)},

        {#{spatial => 1}, ?ERROR_BAD_VALUE_BOOLEAN(<<"spatial">>)},
        {#{spatial => -3}, ?ERROR_BAD_VALUE_BOOLEAN(<<"spatial">>)},
        {#{spatial => ok}, ?ERROR_BAD_VALUE_BOOLEAN(<<"spatial">>)},

        {#{providers => [ok]}, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"ok">>)},
        {#{providers => [<<"ASD">>]}, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"ASD">>)}
    ]).


updating_view_with_invalid_params_should_fail(Config) ->
    Workers = [WorkerP1 | _] = ?config(op_worker_nodes, Config),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    % create view
    InitialOptions = #{<<"update_min_changes">> => 10000},
    ?assertMatch(ok, create_view_via_rest(
        Config, WorkerP1, ?SPACE_ID, ViewName,
        ?MAP_FUNCTION(XattrName), false, [], InitialOptions
    )),
    ExpView = #{
        <<"indexOptions">> => InitialOptions,
        <<"viewOptions">> => InitialOptions,
        <<"providers">> => [?PROVIDER_ID(WorkerP1)],
        <<"mapFunction">> => view_utils:escape_js_function(?MAP_FUNCTION(XattrName)),
        <<"reduceFunction">> => null,
        <<"spatial">> => false
    },
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, ExpView}, get_view_via_rest(
            Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS
        )
    end, Workers),

    lists:foreach(fun({Options, ExpError}) ->
        Worker = lists:nth(rand:uniform(length(Workers)), Workers),
        ExpRestError = rest_test_utils:get_rest_error(ExpError),
        ?assertMatch(ExpRestError, update_view_via_rest(
            Config, Worker, ?SPACE_ID, ViewName, <<>>, Options
        )),
        ?assertMatch({ok, ExpView}, get_view_via_rest(
            Config, Worker, ?SPACE_ID, ViewName)
        )
    end, [
        {#{update_min_changes => ok}, ?ERROR_BAD_VALUE_INTEGER(<<"update_min_changes">>)},
        {#{update_min_changes => -3}, ?ERROR_BAD_VALUE_TOO_LOW(<<"update_min_changes">>, 1)},
        {#{update_min_changes => 15.2}, ?ERROR_BAD_VALUE_INTEGER(<<"update_min_changes">>)},
        {#{update_min_changes => 0}, ?ERROR_BAD_VALUE_TOO_LOW(<<"update_min_changes">>, 1)},

        {#{replica_update_min_changes => ok}, ?ERROR_BAD_VALUE_INTEGER(<<"replica_update_min_changes">>)},
        {#{replica_update_min_changes => -3}, ?ERROR_BAD_VALUE_TOO_LOW(<<"replica_update_min_changes">>, 1)},
        {#{replica_update_min_changes => 15.2}, ?ERROR_BAD_VALUE_INTEGER(<<"replica_update_min_changes">>)},
        {#{replica_update_min_changes => 0}, ?ERROR_BAD_VALUE_TOO_LOW(<<"replica_update_min_changes">>, 1)},

        {#{spatial => 1}, ?ERROR_BAD_VALUE_BOOLEAN(<<"spatial">>)},
        {#{spatial => -3}, ?ERROR_BAD_VALUE_BOOLEAN(<<"spatial">>)},
        {#{spatial => ok}, ?ERROR_BAD_VALUE_BOOLEAN(<<"spatial">>)},

        {#{providers => [ok]}, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"ok">>)},
        {#{providers => [<<"ASD">>]}, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"ASD">>)}
    ]).


overwriting_view_should_fail(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    Provider2 = ?PROVIDER_ID(WorkerP2),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    % create
    ?assertMatch([], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100)),
    Options = #{
        <<"update_min_changes">> => 10000,
        <<"replica_update_min_changes">> => 100
    },
    ?assertMatch(ok, create_view_via_rest(
        Config, WorkerP1, ?SPACE_ID, ViewName,
        ?MAP_FUNCTION(XattrName), false, [Provider2], Options
    )),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    ExpMapFun1 = view_utils:escape_js_function(?MAP_FUNCTION(XattrName)),
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := Options,
            <<"viewOptions">> := Options,
            <<"providers">> := [Provider2],
            <<"mapFunction">> := ExpMapFun1,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers),

    % overwrite
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_ALREADY_EXISTS),
    ?assertMatch(ExpRestError, create_view_via_rest(
        Config, WorkerP1, ?SPACE_ID, ViewName,
        ?GEOSPATIAL_MAP_FUNCTION, true, [], #{}
    )),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),


    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := Options,
            <<"viewOptions">> := Options,
            <<"providers">> := [Provider2],
            <<"mapFunction">> := ExpMapFun1,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers),

    % delete
    ?assertMatch(ok, remove_view_via_rest(Config, WorkerP1, ?SPACE_ID, ViewName)),
    ?assertMatch([], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS).


create_get_delete_reduce_fun(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    Provider1 = ?PROVIDER_ID(WorkerP1),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,
    UserId = <<"user1">>,

    AllPrivs = privileges:space_privileges(),
    ErrorForbidden = rest_test_utils:get_rest_error(?ERROR_FORBIDDEN),

    % create on one provider
    ?assertMatch([], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100)),
    ?assertMatch(ok, create_view_via_rest(
        Config, WorkerP1, ?SPACE_ID, ViewName, ?MAP_FUNCTION(XattrName), false
    )),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % get on both
    ExpMapFun = view_utils:escape_js_function(?MAP_FUNCTION(XattrName)),
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := #{},
            <<"viewOptions">> := #{},
            <<"providers">> := [Provider1],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers),

    %% CREATE

    % adding view reduce fun without SPACE_MANAGE_VIEWS privilege should fail
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, AllPrivs -- [?SPACE_MANAGE_VIEWS]),
    ?assertMatch(ErrorForbidden, add_reduce_fun_via_rest(
        Config, WorkerP2, ?SPACE_ID, ViewName, ?REDUCE_FUNCTION(XattrName)
    )),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % view should not change
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := #{},
            <<"viewOptions">> := #{},
            <<"providers">> := [Provider1],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers),

    % adding view reduce fun with SPACE_MANAGE_VIEWS privilege should succeed
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, [?SPACE_MANAGE_VIEWS, ?SPACE_VIEW_VIEWS]),
    ?assertMatch(ok, add_reduce_fun_via_rest(
        Config, WorkerP2, ?SPACE_ID, ViewName, ?REDUCE_FUNCTION(XattrName)
    )),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % get on both after adding reduce
    ExpReduceFun = view_utils:escape_js_function(?REDUCE_FUNCTION(XattrName)),
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := #{},
            <<"viewOptions">> := #{},
            <<"providers">> := [Provider1],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := ExpReduceFun,
            <<"spatial">> := false
        }}, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers),

    %% DELETE

    % deleting view reduce fun without SPACE_MANAGE_VIEWS privilege should fail
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, AllPrivs -- [?SPACE_MANAGE_VIEWS]),
    ?assertMatch(ErrorForbidden, remove_reduce_fun_via_rest(Config, WorkerP1, ?SPACE_ID, ViewName)),

    % reduce fun should stay
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := #{},
            <<"viewOptions">> := #{},
            <<"providers">> := [Provider1],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := ExpReduceFun,
            <<"spatial">> := false
        }}, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers),

    % deleting view reduce fun with SPACE_MANAGE_VIEWS privilege should succeed
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, [?SPACE_MANAGE_VIEWS, ?SPACE_VIEW_VIEWS]),
    ?assertMatch(ok, remove_reduce_fun_via_rest(Config, WorkerP1, ?SPACE_ID, ViewName)),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % get on both after adding reduce
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := #{},
            <<"viewOptions">> := #{},
            <<"providers">> := [Provider1],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers),

    % delete on other provider
    ?assertMatch(ok, remove_view_via_rest(Config, WorkerP1, ?SPACE_ID, ViewName)),
    ?assertMatch([], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS).


getting_nonexistent_view_should_fail(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_NOT_FOUND),

    lists:foreach(fun(Worker) ->
        ?assertMatch(ExpRestError, get_view_via_rest(
            Config, Worker, ?SPACE_ID, ViewName
        ))
    end, Workers).


getting_view_of_not_supported_space_should_fail(Config) ->
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),
    SpaceId = <<"space2">>,
    XattrName = ?XATTR_NAME,

    ?assertMatch(ok, create_view_via_rest(
        Config, WorkerP2, SpaceId, ViewName, ?MAP_FUNCTION(XattrName), false
    )),

    ExpMapFun = view_utils:escape_js_function(?MAP_FUNCTION(XattrName)),
    ExpProviders = [?PROVIDER_ID(WorkerP2)],
    ?assertMatch({ok, #{
        <<"indexOptions">> := #{},
        <<"viewOptions">> := #{},
        <<"providers">> := ExpProviders,
        <<"mapFunction">> := ExpMapFun,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_view_via_rest(Config, WorkerP2, SpaceId, ViewName), ?ATTEMPTS),

    ExpRestError = rest_test_utils:get_rest_error(
        ?ERROR_SPACE_NOT_SUPPORTED_BY(?GET_DOMAIN_BIN(WorkerP1))
    ),
    ?assertMatch(ExpRestError, get_view_via_rest(
        Config, WorkerP1, SpaceId, ViewName
    )).


list_views(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    ViewNum = 20,
    Chunk = rand:uniform(ViewNum),

    AllPrivs = privileges:space_privileges(),
    ErrorForbidden = rest_test_utils:get_rest_error(?ERROR_FORBIDDEN),

    ?assertMatch([], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, Chunk)),
    ?assertMatch([], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, Chunk)),

    ViewNames = lists:sort(lists:map(fun(Num) ->
        Worker = lists:nth(rand:uniform(length(Workers)), Workers),
        ViewName = <<"view_name_", (integer_to_binary(Num))/binary>>,
        XattrName = ?XATTR_NAME,
        ?assertMatch(ok, create_view_via_rest(
            Config, Worker, ?SPACE_ID, ViewName, ?MAP_FUNCTION(XattrName)
        )),
        ViewName
    end, lists:seq(1, ViewNum))),

    ?assertMatch(ViewNames, list_views_via_rest(Config, WorkerP1, ?SPACE_ID, Chunk), ?ATTEMPTS),
    ?assertMatch(ViewNames, list_views_via_rest(Config, WorkerP2, ?SPACE_ID, Chunk), ?ATTEMPTS),

    % listing views without SPACE_VIEW_VIEWS privilege should fail
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, <<"user1">>, AllPrivs -- [?SPACE_VIEW_VIEWS]),
    ?assertMatch(ErrorForbidden, list_views_via_rest(Config, WorkerP1, ?SPACE_ID, Chunk), ?ATTEMPTS),
    ?assertMatch(ErrorForbidden, list_views_via_rest(Config, WorkerP2, ?SPACE_ID, Chunk), ?ATTEMPTS).


query_view(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,
    UserId = <<"user1">>,

    AllPrivs = privileges:space_privileges(),
    Query = query_filter(Config, SpaceId, ViewName),
    FilePrefix = atom_to_list(?FUNCTION_NAME),
    Guids = create_files_with_xattrs(WorkerP1, SessionId, SpaceName, FilePrefix, 5, XattrName),
    ErrorNotFound = rest_test_utils:get_rest_error(?ERROR_NOT_FOUND),
    ErrorForbidden = rest_test_utils:get_rest_error(?ERROR_FORBIDDEN),
    ExpGuids = lists:sort(Guids),

    % support view only by one provider; other should return error on query
    ?assertMatch(ok, create_view_via_rest(
        Config, WorkerP1, SpaceId, ViewName, ?MAP_FUNCTION(XattrName),
        false, [?PROVIDER_ID(WorkerP2)], #{}
    )),
    ?assertMatch(ErrorNotFound, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query(WorkerP2, #{}), ?ATTEMPTS),

    % support view on both providers and check that they returns correct results
    ?assertMatch(ok, update_view_via_rest(
        Config, WorkerP2, ?SPACE_ID, ViewName, <<>>,
        #{providers => [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)]}
    ), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query(WorkerP2, #{}), ?ATTEMPTS),

    % remove support for view on one provider,
    % which should then remove it from db
    ?assertMatch(ok, update_view_via_rest(
        Config, WorkerP2, SpaceId, ViewName, <<>>,
        #{providers => [?PROVIDER_ID(WorkerP2)]}
    )),
    ?assertEqual(ErrorNotFound, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertMatch(ExpGuids, Query(WorkerP2, #{}), ?ATTEMPTS),

    % add view again on both providers and check that they
    % returns correct results
    ?assertMatch(ok, update_view_via_rest(
        Config, WorkerP1, ?SPACE_ID, ViewName, <<>>,
        #{providers => [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)]}
    )),
    ?assertEqual(ExpGuids, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query(WorkerP2, #{}), ?ATTEMPTS),

    % querying view without SPACE_QUERY_VIEWS privilege should fail
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, AllPrivs -- [?SPACE_QUERY_VIEWS]),
    ?assertEqual(ErrorForbidden, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ErrorForbidden, Query(WorkerP2, #{}), ?ATTEMPTS).


quering_view_with_invalid_params_should_fail(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    Query = query_filter(Config, SpaceId, ViewName),
    FilePrefix = atom_to_list(?FUNCTION_NAME),
    Guids = create_files_with_xattrs(WorkerP1, SessionId, SpaceName, FilePrefix, 5, XattrName),
    ExpGuids = lists:sort(Guids),

    % support view only by one provider; other should return error on query
    ?assertMatch(ok, create_view_via_rest(
        Config, WorkerP1, SpaceId, ViewName, ?MAP_FUNCTION(XattrName),
        false, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)], #{}
    )),
    ?assertEqual(ExpGuids, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query(WorkerP2, #{}), ?ATTEMPTS),

    lists:foreach(fun({Options, ExpError}) ->
        ExpRestError = rest_test_utils:get_rest_error(ExpError),
        Worker = lists:nth(rand:uniform(length(Workers)), Workers),
        ?assertMatch(ExpRestError, Query(Worker, Options))
    end, [
        {#{bbox => ok}, ?ERROR_BAD_DATA(<<"bbox">>)},
        {#{bbox => 1}, ?ERROR_BAD_DATA(<<"bbox">>)},

        {#{descending => ok}, ?ERROR_BAD_VALUE_BOOLEAN(<<"descending">>)},
        {#{descending => 1}, ?ERROR_BAD_VALUE_BOOLEAN(<<"descending">>)},
        {#{descending => -15.6}, ?ERROR_BAD_VALUE_BOOLEAN(<<"descending">>)},

        {#{inclusive_end => ok}, ?ERROR_BAD_VALUE_BOOLEAN(<<"inclusive_end">>)},
        {#{inclusive_end => 1}, ?ERROR_BAD_VALUE_BOOLEAN(<<"inclusive_end">>)},
        {#{inclusive_end => -15.6}, ?ERROR_BAD_VALUE_BOOLEAN(<<"inclusive_end">>)},

        {#{keys => ok}, ?ERROR_BAD_VALUE_JSON(<<"keys">>)},
        {#{keys => 1}, ?ERROR_BAD_VALUE_JSON(<<"keys">>)},
        {#{keys => -15.6}, ?ERROR_BAD_VALUE_JSON(<<"keys">>)},

        {#{limit => ok}, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
        {#{limit => -3}, ?ERROR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},
        {#{limit => 15.2}, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
        {#{limit => 0}, ?ERROR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},

        {#{skip => ok}, ?ERROR_BAD_VALUE_INTEGER(<<"skip">>)},
        {#{skip => -3}, ?ERROR_BAD_VALUE_TOO_LOW(<<"skip">>, 1)},
        {#{skip => 15.2}, ?ERROR_BAD_VALUE_INTEGER(<<"skip">>)},
        {#{skip => 0}, ?ERROR_BAD_VALUE_TOO_LOW(<<"skip">>, 1)},

        {#{stale => da}, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"stale">>, [<<"ok">>, <<"update_after">>, <<"false">>])},
        {#{stale => -3}, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"stale">>, [<<"ok">>, <<"update_after">>, <<"false">>])},
        {#{stale => 15.2}, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"stale">>, [<<"ok">>, <<"update_after">>, <<"false">>])},
        {#{stale => 0}, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"stale">>, [<<"ok">>, <<"update_after">>, <<"false">>])},

        {#{spatial => 1}, ?ERROR_BAD_VALUE_BOOLEAN(<<"spatial">>)},
        {#{spatial => -3}, ?ERROR_BAD_VALUE_BOOLEAN(<<"spatial">>)},
        {#{spatial => ok}, ?ERROR_BAD_VALUE_BOOLEAN(<<"spatial">>)}
    ]).


create_geospatial_view(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),

    create_view_via_rest(Config, WorkerP1, ?SPACE_ID, ViewName, ?GEOSPATIAL_MAP_FUNCTION, true),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    ExpMapFun = view_utils:escape_js_function(?GEOSPATIAL_MAP_FUNCTION),
    ExpProviders = [?PROVIDER_ID(WorkerP1)],
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := #{},
            <<"viewOptions">> := #{},
            <<"providers">> := ExpProviders,
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := null,
            <<"spatial">> := true
        }}, get_view_via_rest(Config, Worker, ?SPACE_ID, ViewName), ?ATTEMPTS)
    end, Workers).


query_geospatial_view(Config) ->
    Workers = [WorkerP1, WorkerP2, WorkerP3 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),

    Path0 = filename:join(["/", SpaceName, "f0"]),
    Path1 = filename:join(["/", SpaceName, "f1"]),
    Path2 = filename:join(["/", SpaceName, "f2"]),
    Path3 = filename:join(["/", SpaceName, "f3"]),
    {ok, _Guid0} = lfm_proxy:create(WorkerP1, SessionId, Path0, 8#777),
    {ok, Guid1} = lfm_proxy:create(WorkerP1, SessionId, Path1, 8#777),
    {ok, Guid2} = lfm_proxy:create(WorkerP1, SessionId, Path2, 8#777),
    {ok, Guid3} = lfm_proxy:create(WorkerP1, SessionId, Path3, 8#777),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid1}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [5.1, 10.22]}, [<<"loc">>]),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid2}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [0, 0]}, [<<"loc">>]),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid3}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [10, 5]}, [<<"loc">>]),

    ?assertMatch(ok, create_view_via_rest(
        Config, WorkerP1, SpaceId, ViewName,
        ?GEOSPATIAL_MAP_FUNCTION, true,
        [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2), ?PROVIDER_ID(WorkerP3)], #{}
    )),

    Query = query_filter(Config, SpaceId, ViewName),

    lists:foreach(fun(Worker) ->
        QueryOptions1 = #{spatial => true, stale => false},
        ?assertEqual(lists:sort([Guid1, Guid2, Guid3]), Query(Worker, QueryOptions1), ?ATTEMPTS),

        QueryOptions2 = QueryOptions1#{
            start_range => <<"[0,0]">>,
            end_range => <<"[5.5,10.5]">>
        },
        ?assertEqual(lists:sort([Guid1, Guid2]), Query(Worker, QueryOptions2), ?ATTEMPTS)
    end, Workers).


query_file_popularity_view(Config) ->
    [WorkerP1 | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ViewName = <<"file-popularity">>,
    Options = #{
        spatial => false,
        stale => false
    },
    ?assertMatch({ok, _}, query_view_via_rest(Config, WorkerP1, SpaceId, ViewName, Options)).


spatial_flag_test(Config) ->
    [WorkerP1 | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),

    ?assertMatch(ok, create_view_via_rest(
        Config, WorkerP1, SpaceId, ViewName,
        ?GEOSPATIAL_MAP_FUNCTION, true,
        [?PROVIDER_ID(WorkerP1)], #{}
    )),

    ?assertMatch({404, _}, query_view_via_rest(Config, WorkerP1, SpaceId, ViewName, [])),
    ?assertMatch({ok, _}, query_view_via_rest(Config, WorkerP1, SpaceId, ViewName, [spatial])),
    ?assertMatch({ok, _}, query_view_via_rest(Config, WorkerP1, SpaceId, ViewName, [{spatial, true}])).


file_removal_test(Config) ->
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    Query = query_filter(Config, SpaceId, ViewName),
    FilePrefix = atom_to_list(?FUNCTION_NAME),
    Guids = create_files_with_xattrs(WorkerP1, SessionId, SpaceName, FilePrefix, 5, XattrName),
    ExpGuids = lists:sort(Guids),

    % support view on both providers and check that they returns correct results
    ?assertMatch(ok, create_view_via_rest(
        Config, WorkerP1, SpaceId, ViewName, ?MAP_FUNCTION(XattrName),
        false, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)], #{}
    )),
    ?assertEqual(ExpGuids, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query(WorkerP2, #{}), ?ATTEMPTS),

    % remove files
    remove_files(WorkerP1, SessionId, Guids),

    % they should not be included in query results any more
    ?assertEqual([], Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual([], Query(WorkerP1, #{}), ?ATTEMPTS).


create_duplicated_views_on_remote_providers(Config) ->
    [WorkerP1, WorkerP2, WorkerP3 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Provider1 = ?PROVIDER_ID(WorkerP1),
    Provider2 = ?PROVIDER_ID(WorkerP2),
    ViewName = ?FILE_POPULARITY_VIEW(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    FilePrefix = atom_to_list(?FUNCTION_NAME),
    Guids = create_files_with_xattrs(WorkerP1, SessionId, SpaceName, FilePrefix, 5, XattrName),
    ExpGuids = lists:sort(Guids),

    % create view on P1 and P2
    ?assertMatch([], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100)),
    Options1 = #{<<"update_min_changes">> => 10000},
    ?assertMatch(ok, create_view_via_rest(
        Config, WorkerP1, ?SPACE_ID, ViewName, ?MAP_FUNCTION(XattrName), false,
        [Provider1, Provider2], Options1
    )),
    % create different view on P2 but with the same name
    ?assertMatch(ok, create_view_via_rest(
        Config, WorkerP2, ?SPACE_ID, ViewName, ?MAP_FUNCTION2(XattrName), false, [], Options1
    )),

    ViewName@P1 = ?VIEW@(ViewName, Provider1),
    ViewName@P2 = ?VIEW@(ViewName, Provider2),

    ViewName@P1Short = ?VIEW@(ViewName, binary_part(Provider1, 0, 4)),
    ViewName@P2Short = ?VIEW@(ViewName, binary_part(Provider2, 0, 4)),

    ?assertMatch([ViewName@P2, ViewName], list_views_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName@P1, ViewName], list_views_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([ViewName@P1Short, ViewName@P2Short], list_views_via_rest(Config, WorkerP3, ?SPACE_ID, 100), ?ATTEMPTS),

    ExpMapFun = view_utils:escape_js_function(?MAP_FUNCTION(XattrName)),
    ExpMapFun2 = view_utils:escape_js_function(?MAP_FUNCTION2(XattrName)),

    ExpError = rest_test_utils:get_rest_error(?ERROR_NOT_FOUND),
    ExpError2 = rest_test_utils:get_rest_error(?ERROR_BAD_VALUE_AMBIGUOUS_ID(<<"view_name">>)),

    % get by simple name
    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"viewOptions">> := Options1,
        <<"providers">> := [Provider1, Provider2],
        <<"mapFunction">> := ExpMapFun,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_view_via_rest(Config, WorkerP1, ?SPACE_ID, ViewName), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"viewOptions">> := Options1,
        <<"providers">> := [Provider2],
        <<"mapFunction">> := ExpMapFun2,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_view_via_rest(Config, WorkerP2, ?SPACE_ID, ViewName), ?ATTEMPTS),

    ?assertMatch(ExpError2, get_view_via_rest(Config, WorkerP3, ?SPACE_ID, ViewName), ?ATTEMPTS),

    % get by extended name
    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"viewOptions">> := Options1,
        <<"providers">> := [Provider1, Provider2],
        <<"mapFunction">> := ExpMapFun,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_view_via_rest(Config, WorkerP1, ?SPACE_ID, ViewName@P1), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"viewOptions">> := Options1,
        <<"providers">> := [Provider2],
        <<"mapFunction">> := ExpMapFun2,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_view_via_rest(Config, WorkerP2, ?SPACE_ID, ViewName@P2), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"viewOptions">> := Options1,
        <<"providers">> := [Provider1, Provider2],
        <<"mapFunction">> := ExpMapFun,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_view_via_rest(Config, WorkerP3, ?SPACE_ID, ViewName@P1), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"viewOptions">> := Options1,
        <<"providers">> := [Provider2],
        <<"mapFunction">> := ExpMapFun2,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_view_via_rest(Config, WorkerP3, ?SPACE_ID, ViewName@P2), ?ATTEMPTS),

    % get by shortened extended name
    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"viewOptions">> := Options1,
        <<"providers">> := [Provider1, Provider2],
        <<"mapFunction">> := ExpMapFun,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_view_via_rest(Config, WorkerP1, ?SPACE_ID, ViewName@P1Short), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"viewOptions">> := Options1,
        <<"providers">> := [Provider2],
        <<"mapFunction">> := ExpMapFun2,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_view_via_rest(Config, WorkerP2, ?SPACE_ID, ViewName@P2Short), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"viewOptions">> := Options1,
        <<"providers">> := [Provider1, Provider2],
        <<"mapFunction">> := ExpMapFun,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_view_via_rest(Config, WorkerP3, ?SPACE_ID, ViewName@P1Short), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"viewOptions">> := Options1,
        <<"providers">> := [Provider2],
        <<"mapFunction">> := ExpMapFun2,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_view_via_rest(Config, WorkerP3, ?SPACE_ID, ViewName@P2Short), ?ATTEMPTS),


    % query the views by simple name
    Query = query_filter(Config, SpaceId, ViewName),
    Query2 = query_filter2(Config, SpaceId, ViewName),

    ?assertMatch(ExpGuids, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query2(WorkerP2, #{}), ?ATTEMPTS),
    ?assertEqual(ExpError2, Query(WorkerP3, #{}), ?ATTEMPTS),

    % query the views by extended name
    Query@P1 = query_filter(Config, SpaceId, ViewName@P1),
    Query@P2 = query_filter2(Config, SpaceId, ViewName@P2),

    ?assertMatch(ExpGuids, Query@P1(WorkerP1, #{}), ?ATTEMPTS),
    ?assertMatch(ExpError, Query@P2(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query@P1(WorkerP2, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query@P2(WorkerP2, #{}), ?ATTEMPTS),
    ?assertEqual(ExpError, Query@P1(WorkerP3, #{}), ?ATTEMPTS),
    ?assertEqual(ExpError, Query@P2(WorkerP3, #{}), ?ATTEMPTS),

    % query the views by shortened extended name
    Query@P1Short = query_filter(Config, SpaceId, ViewName@P1Short),
    Query@P2Short = query_filter2(Config, SpaceId, ViewName@P2Short),

    ?assertMatch(ExpGuids, Query@P1Short(WorkerP1, #{}), ?ATTEMPTS),
    ?assertMatch(ExpError, Query@P2Short(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query@P1Short(WorkerP2, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query@P2Short(WorkerP2, #{}), ?ATTEMPTS),
    ?assertEqual(ExpError, Query@P1Short(WorkerP3, #{}), ?ATTEMPTS),
    ?assertEqual(ExpError, Query@P2Short(WorkerP3, #{}), ?ATTEMPTS).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)), % TODO - change to 2 seconds
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off),
            test_utils:set_env(Worker, ?APP_NAME, public_block_size_treshold, 0),
            test_utils:set_env(Worker, ?APP_NAME, public_block_percent_treshold, 0)
        end, ?config(op_worker_nodes, NewConfig2)),

        application:start(ssl),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2),
        NewConfig3
    end,
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, multi_provider_view_rest_test_SUITE]}
        | Config
    ].

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).

init_per_testcase(Case, Config) when
    Case =:= create_get_update_delete_view;
    Case =:= create_get_delete_reduce_fun;
    Case =:= list_views;
    Case =:= query_view
->
    [WorkerP1, _| _] = ?config(op_worker_nodes, Config),
    OldPrivs = rpc:call(WorkerP1, initializer, node_get_mocked_space_user_privileges, [?SPACE_ID, <<"user1">>]),
    init_per_testcase(all, [{old_privs, OldPrivs} | Config]);

init_per_testcase(query_file_popularity_view, Config) ->
    Config2 = sort_workers(Config),
    [WorkerP1, WorkerP2| _] = ?config(op_worker_nodes, Config2),
    ok = rpc:call(WorkerP1, file_popularity_api, enable, [?SPACE_ID]),
    ok = rpc:call(WorkerP2, file_popularity_api, enable, [?SPACE_ID]),
    init_per_testcase(all, Config2);

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(sort_workers(Config)).

end_per_testcase(Case, Config) when
    Case =:= create_get_update_delete_view;
    Case =:= create_get_delete_reduce_fun;
    Case =:= list_views;
    Case =:= query_view
->
    UserId = <<"user1">>,
    Workers = ?config(op_worker_nodes, Config),
    OldPrivs = ?config(old_privs, Config),
    initializer:testmaster_mock_space_user_privileges(Workers, ?SPACE_ID, UserId, OldPrivs),
    end_per_testcase(all, Config);

end_per_testcase(query_file_popularity_view, Config) ->
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    rpc:call(WorkerP1, file_popularity_api, disable, [?SPACE_ID]),
    rpc:call(WorkerP2, file_popularity_api, disable, [?SPACE_ID]),
    end_per_testcase(all, Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    remove_all_views(Workers, <<"space1">>),
    remove_all_views(Workers, <<"space2">>),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_view_via_rest(Config, Worker, SpaceId, ViewName, MapFunction) ->
    create_view_via_rest(
        Config, Worker, SpaceId, ViewName, MapFunction, false
    ).

create_view_via_rest(Config, Worker, SpaceId, ViewName, MapFunction, Spatial) ->
    create_view_via_rest(
        Config, Worker, SpaceId, ViewName, MapFunction,
        Spatial, [], #{}
    ).

create_view_via_rest(Config, Worker, SpaceId, ViewName, MapFunction, Spatial, Providers, Options) ->
    QueryString = create_query_string(Options#{
        spatial => Spatial,
        providers => Providers
    }),
    Path = <<(?VIEW_PATH(SpaceId, ViewName))/binary, QueryString/binary>>,

    Headers = ?USER_1_AUTH_HEADERS(Config, [
        {?HDR_CONTENT_TYPE, <<"application/javascript">>}
    ]),

    case rest_test_utils:request(Worker, Path, put, Headers, MapFunction) of
        {ok, 204, _, _} ->
            ok;
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

update_view_via_rest(Config, Worker, SpaceId, ViewName, MapFunction, Options) ->
    QueryString = create_query_string(Options),
    Path = <<(?VIEW_PATH(SpaceId, ViewName))/binary, QueryString/binary>>,

    Headers = ?USER_1_AUTH_HEADERS(Config, [
        {?HDR_CONTENT_TYPE, <<"application/javascript">>}
    ]),

    case rest_test_utils:request(Worker, Path, patch, Headers, MapFunction) of
        {ok, 204, _, _} ->
            ok;
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

add_reduce_fun_via_rest(Config, Worker, SpaceId, ViewName, ReduceFunction) ->
    Path = <<(?VIEW_PATH(SpaceId, ViewName))/binary, "/reduce">>,
    Headers = ?USER_1_AUTH_HEADERS(Config, [
        {?HDR_CONTENT_TYPE, <<"application/javascript">>}
    ]),

    case rest_test_utils:request(Worker, Path, put, Headers, ReduceFunction) of
        {ok, 204, _, _} ->
            ok;
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

remove_reduce_fun_via_rest(Config, Worker, SpaceId, ViewName) ->
    Path = <<(?VIEW_PATH(SpaceId, ViewName))/binary, "/reduce">>,
    Headers = ?USER_1_AUTH_HEADERS(Config, [
        {?HDR_CONTENT_TYPE, <<"application/javascript">>}
    ]),

    case rest_test_utils:request(Worker, Path, delete, Headers, []) of
        {ok, 204, _, _} ->
            ok;
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

get_view_via_rest(Config, Worker, SpaceId, ViewName) ->
    Path = ?VIEW_PATH(SpaceId, ViewName),
    Headers = ?USER_1_AUTH_HEADERS(Config, [{?HDR_ACCEPT, <<"application/json">>}]),
    case rest_test_utils:request(Worker, Path, get, Headers, []) of
        {ok, 200, _, Body} ->
            {ok, json_utils:decode(Body)};
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

remove_view_via_rest(Config, Worker, SpaceId, ViewName) ->
    Path = ?VIEW_PATH(SpaceId, ViewName),
    Headers = ?USER_1_AUTH_HEADERS(Config),
    case rest_test_utils:request(Worker, Path, delete, Headers, []) of
        {ok, 204, _, _} ->
            ok;
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

query_view_via_rest(Config, Worker, SpaceId, ViewName, Options) ->
    QueryString = create_query_string(Options),
    Path = <<(?VIEW_PATH(SpaceId, ViewName))/binary, "/query", QueryString/binary>>,
    Headers = ?USER_1_AUTH_HEADERS(Config),
    case rest_test_utils:request(Worker, Path, get, Headers, []) of
        {ok, 200, _, Body} ->
            {ok, json_utils:decode(Body)};
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

list_views_via_rest(Config, Worker, Space, ChunkSize) ->
    case list_views_via_rest(Config, Worker, Space, ChunkSize, <<"null">>, []) of
        Result when is_list(Result) ->
            % Make sure there are no duplicates
            ?assertEqual(lists:sort(Result), lists:usort(Result)),
            Result;
        Error ->
            Error
    end.

list_views_via_rest(Config, Worker, Space, ChunkSize, StartId, Acc) ->
    case list_views_via_rest(Config, Worker, Space, StartId, ChunkSize) of
        {ok, {Views, NextPageToken}} ->
            case NextPageToken of
                <<"null">> ->
                    Acc ++ Views;
                _ ->
                    ?assertMatch(ChunkSize, length(Views)),
                    list_views_via_rest(Config, Worker, Space, ChunkSize, NextPageToken, Acc ++ Views)
            end;
        Error ->
            Error
    end.

list_views_via_rest(Config, Worker, Space, StartId, LimitOrUndef) ->
    TokenParam = case StartId of
        <<"null">> -> <<"">>;
        Token -> <<"&page_token=", Token/binary>>
    end,
    LimitParam = case LimitOrUndef of
        undefined ->
            <<"">>;
        Int when is_integer(Int) ->
            <<"&limit=", (integer_to_binary(Int))/binary>>
    end,
    Url = str_utils:format_bin("spaces/~s/views?~s~s", [
        Space, TokenParam, LimitParam
    ]),
    case rest_test_utils:request(Worker, Url, get, ?USER_1_AUTH_HEADERS(Config), <<>>) of
        {ok, 200, _, Body} ->
            ParsedBody = json_utils:decode(Body),
            Views = maps:get(<<"views">>, ParsedBody),
            NextPageToken = maps:get(<<"nextPageToken">>, ParsedBody, <<"null">>),
            {ok, {Views, NextPageToken}};
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

remove_all_views(Nodes, SpaceId) ->
    lists:foreach(fun(Node) ->
        case rpc:call(Node, index, list, [SpaceId]) of
            {ok, ViewNames} ->
                lists:foreach(fun(ViewName) ->
                    ok = rpc:call(Node, index, delete, [SpaceId, ViewName])
                end, ViewNames);
            {error, not_found} ->
                ok
        end
    end, Nodes).

create_query_string(undefined) ->
    <<>>;
create_query_string(Options) when is_map(Options) ->
    create_query_string(maps:to_list(Options));
create_query_string(Options) ->
    lists:foldl(fun(Option, AccQuery) ->
        OptionBin = case Option of
            {Key, Values} when is_list(Values) ->
                KeyBin = binary_from_term(Key),
                lists:foldl(fun(Val, Acc) ->
                    ValBin = binary_from_term(Val),
                    <<Acc/binary, "&", KeyBin/binary, "[]=", ValBin/binary>>
                end, <<>>, Values);
            {Key, Val} ->
                KeyBin = binary_from_term(Key),
                ValBin = binary_from_term(Val),
                <<"&", KeyBin/binary, "=", ValBin/binary>>;
            _ ->
                <<"&", (binary_from_term(Option))/binary>>
        end,
        <<AccQuery/binary, OptionBin/binary>>
    end, <<"?">>, Options).

binary_from_term(Val) when is_binary(Val) ->
    Val;
binary_from_term(Val) when is_integer(Val) ->
    integer_to_binary(Val);
binary_from_term(Val) when is_float(Val) ->
    float_to_binary(Val);
binary_from_term(Val) when is_atom(Val) ->
    atom_to_binary(Val, utf8).

objectids_to_guids(ObjectIds) ->
    lists:map(fun(ObjectId) ->
        {ok, Guid} = file_id:objectid_to_guid(ObjectId),
        Guid
    end, ObjectIds).

query_filter(Config, SpaceId, ViewName) ->
    fun(Node, Options) ->
        case query_view_via_rest(Config, Node, SpaceId, ViewName, Options) of
            {ok, Body} when is_list(Body) ->
                ObjectIds = lists:map(fun(#{<<"value">> := ObjectId}) ->
                    ObjectId
                end, Body),
                lists:sort(objectids_to_guids(ObjectIds));
            Error ->
                Error
        end
    end.

query_filter2(Config, SpaceId, ViewName) ->
    fun(Node, Options) ->
        case query_view_via_rest(Config, Node, SpaceId, ViewName, Options) of
            {ok, Body} ->
                ObjectIds = lists:map(fun(#{<<"key">> := ObjectId}) ->
                    ObjectId
                end, Body),
                lists:sort(objectids_to_guids(ObjectIds));
            Error ->
                Error
        end
    end.

create_files_with_xattrs(Node, SessionId, SpaceName, Prefix, Num, XattrName) ->
    lists:map(fun(X) ->
        Path = filename:join(["/", SpaceName, Prefix ++ integer_to_list(X)]),
        {ok, Guid} = lfm_proxy:create(Node, SessionId, Path, 8#777),
        ok = lfm_proxy:set_xattr(Node, SessionId, {guid, Guid}, ?XATTR(XattrName, X)),
        Guid
    end, lists:seq(1, Num)).

remove_files(Node, SessionId, Guids) ->
    lists:foreach(fun(G) ->
        ok = lfm_proxy:unlink(Node, SessionId, {guid, G})
    end, Guids).

sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).
