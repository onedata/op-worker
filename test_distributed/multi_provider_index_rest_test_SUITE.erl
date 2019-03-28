%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Multi provider index REST tests
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_index_rest_test_SUITE).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("http/rest/cdmi/cdmi_errors.hrl").
-include("http/rest/cdmi/cdmi_capabilities.hrl").
-include("http/rest/http_status.hrl").
-include("proto/common/credentials.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("rest_test_utils.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include("http/rest/rest_api/rest_errors.hrl").


%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_get_update_delete_index/1,
    creating_index_with_invalid_params_should_fail/1,
    updating_index_with_invalid_params_should_fail/1,
    overwriting_index_should_fail/1,
    create_get_delete_reduce_fun/1,
    getting_nonexistent_index_should_fail/1,
    getting_index_of_not_supported_space_should_fail/1,
    list_indexes/1,
    query_index/1,
    quering_index_with_invalid_params_should_fail/1,
    create_geospatial_index/1,
    query_geospatial_index/1,
    query_file_popularity_index/1,
    spatial_flag_test/1,
    file_removal_test/1,
    create_duplicated_indexes_on_remote_providers/1]).

all() ->
    ?ALL([
        create_get_update_delete_index,
        creating_index_with_invalid_params_should_fail,
        updating_index_with_invalid_params_should_fail,
        overwriting_index_should_fail,
        create_get_delete_reduce_fun,
        getting_nonexistent_index_should_fail,
        getting_index_of_not_supported_space_should_fail,
        list_indexes,
        query_index,
        quering_index_with_invalid_params_should_fail,
        create_geospatial_index,
        query_geospatial_index,
        query_file_popularity_index,
        spatial_flag_test,
        file_removal_test,
        create_duplicated_indexes_on_remote_providers
    ]).

-define(ATTEMPTS, 100).

-define(SPACE_ID, <<"space1">>).
-define(PROVIDER_ID(__Node), rpc:call(__Node, oneprovider, get_id, [])).

-define(USER_1_AUTH_HEADERS(Config), ?USER_1_AUTH_HEADERS(Config, [])).
-define(USER_1_AUTH_HEADERS(Config, OtherHeaders),
    ?USER_AUTH_HEADERS(Config, <<"user1">>, OtherHeaders)).

-define(INDEX_NAME(__FunctionName), begin
    FunctionNameBin = str_utils:to_binary(__FunctionName),
    RandomIntBin = integer_to_binary(erlang:unique_integer([positive])),
    <<"index_", FunctionNameBin/binary, RandomIntBin/binary>>
end).

-define(INDEX_PATH(__SpaceId, __IndexName),
    <<"spaces/", __SpaceId/binary, "/indexes/", __IndexName/binary>>
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

-define(INDEX@(IndexName, ProviderId),
    <<IndexName/binary, "@", (ProviderId)/binary >>).

%%%===================================================================
%%% Test functions
%%%===================================================================

create_get_update_delete_index(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    Provider1 = ?PROVIDER_ID(WorkerP1),
    Provider2 = ?PROVIDER_ID(WorkerP2),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    % create index
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100)),
    Options1 = #{<<"update_min_changes">> => 10000},
    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP1, ?SPACE_ID, IndexName, ?MAP_FUNCTION(XattrName), false, [], Options1
    )),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % get on both
    ExpMapFun = index_utils:escape_js_function(?MAP_FUNCTION(XattrName)),
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := Options1,
            <<"providers">> := [Provider1],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_index_via_rest(Config, Worker, ?SPACE_ID, IndexName), ?ATTEMPTS)
    end, Workers),

    % update index without overriding map function
    Options2 = #{<<"replica_update_min_changes">> => 100},
    ?assertMatch(ok, update_index_via_rest(
        Config, WorkerP1, ?SPACE_ID, IndexName,
        <<>>, Options2#{providers => [Provider2]}
    )),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % get on both after update
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := Options2,
            <<"providers">> := [Provider2],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_index_via_rest(Config, Worker, ?SPACE_ID, IndexName), ?ATTEMPTS)
    end, Workers),

    % update index with overriding map function
    ?assertMatch(ok, update_index_via_rest(
        Config, WorkerP1, ?SPACE_ID, IndexName, ?GEOSPATIAL_MAP_FUNCTION, #{}
    )),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % get on both after update
    ExpMapFun2 = index_utils:escape_js_function(?GEOSPATIAL_MAP_FUNCTION),
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := Options2,
            <<"providers">> := [Provider2],
            <<"mapFunction">> := ExpMapFun2,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_index_via_rest(Config, Worker, ?SPACE_ID, IndexName), ?ATTEMPTS)
    end, Workers),

    % delete on other provider
    ?assertMatch(ok, remove_index_via_rest(Config, WorkerP2, ?SPACE_ID, IndexName)),
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS).

creating_index_with_invalid_params_should_fail(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    lists:foreach(fun({Options, ExpError}) ->
        Worker = lists:nth(rand:uniform(length(Workers)), Workers),
        ?assertMatch(ExpError, create_index_via_rest(
            Config, Worker, ?SPACE_ID, IndexName, ?MAP_FUNCTION(XattrName),
            maps:get(spatial, Options, false), maps:get(providers, Options, []),
            Options
        )),
        ?assertMatch([], list_indexes_via_rest(Config, Worker, ?SPACE_ID, 100))
    end, [
        {#{update_min_changes => ok}, ?ERROR_INVALID_UPDATE_MIN_CHANGES},
        {#{update_min_changes => -3}, ?ERROR_INVALID_UPDATE_MIN_CHANGES},
        {#{update_min_changes => 15.2}, ?ERROR_INVALID_UPDATE_MIN_CHANGES},
        {#{update_min_changes => 0}, ?ERROR_INVALID_UPDATE_MIN_CHANGES},

        {#{replica_update_min_changes => ok}, ?ERROR_INVALID_REPLICA_UPDATE_MIN_CHANGES},
        {#{replica_update_min_changes => -3}, ?ERROR_INVALID_REPLICA_UPDATE_MIN_CHANGES},
        {#{replica_update_min_changes => 15.2}, ?ERROR_INVALID_REPLICA_UPDATE_MIN_CHANGES},
        {#{replica_update_min_changes => 0}, ?ERROR_INVALID_REPLICA_UPDATE_MIN_CHANGES},

        {#{spatial => 1}, ?ERROR_INVALID_SPATIAL_FLAG},
        {#{spatial => -3}, ?ERROR_INVALID_SPATIAL_FLAG},
        {#{spatial => ok}, ?ERROR_INVALID_SPATIAL_FLAG},

        {#{providers => [ok]}, ?ERROR_PROVIDER_NOT_SUPPORTING_SPACE(<<"ok">>)},
        {#{providers => [<<"ASD">>]}, ?ERROR_PROVIDER_NOT_SUPPORTING_SPACE(<<"ASD">>)}
    ]).

updating_index_with_invalid_params_should_fail(Config) ->
    Workers = [WorkerP1 | _] = ?config(op_worker_nodes, Config),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    % create index
    InitialOptions = #{<<"update_min_changes">> => 10000},
    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP1, ?SPACE_ID, IndexName,
        ?MAP_FUNCTION(XattrName), false, [], InitialOptions
    )),
    ExpIndex = #{
        <<"indexOptions">> => InitialOptions,
        <<"providers">> => [?PROVIDER_ID(WorkerP1)],
        <<"mapFunction">> => index_utils:escape_js_function(?MAP_FUNCTION(XattrName)),
        <<"reduceFunction">> => null,
        <<"spatial">> => false
    },
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, ExpIndex}, get_index_via_rest(
            Config, Worker, ?SPACE_ID, IndexName), ?ATTEMPTS
        )
    end, Workers),

    lists:foreach(fun({Options, ExpError}) ->
        Worker = lists:nth(rand:uniform(length(Workers)), Workers),
        ?assertMatch(ExpError, update_index_via_rest(
            Config, Worker, ?SPACE_ID, IndexName, <<>>, Options
        )),
        ?assertMatch({ok, ExpIndex}, get_index_via_rest(
            Config, Worker, ?SPACE_ID, IndexName)
        )
    end, [
        {#{update_min_changes => ok}, ?ERROR_INVALID_UPDATE_MIN_CHANGES},
        {#{update_min_changes => -3}, ?ERROR_INVALID_UPDATE_MIN_CHANGES},
        {#{update_min_changes => 15.2}, ?ERROR_INVALID_UPDATE_MIN_CHANGES},
        {#{update_min_changes => 0}, ?ERROR_INVALID_UPDATE_MIN_CHANGES},

        {#{replica_update_min_changes => ok}, ?ERROR_INVALID_REPLICA_UPDATE_MIN_CHANGES},
        {#{replica_update_min_changes => -3}, ?ERROR_INVALID_REPLICA_UPDATE_MIN_CHANGES},
        {#{replica_update_min_changes => 15.2}, ?ERROR_INVALID_REPLICA_UPDATE_MIN_CHANGES},
        {#{replica_update_min_changes => 0}, ?ERROR_INVALID_REPLICA_UPDATE_MIN_CHANGES},

        {#{spatial => 1}, ?ERROR_INVALID_SPATIAL_FLAG},
        {#{spatial => -3}, ?ERROR_INVALID_SPATIAL_FLAG},
        {#{spatial => ok}, ?ERROR_INVALID_SPATIAL_FLAG},

        {#{providers => [ok]}, ?ERROR_PROVIDER_NOT_SUPPORTING_SPACE(<<"ok">>)},
        {#{providers => [<<"ASD">>]}, ?ERROR_PROVIDER_NOT_SUPPORTING_SPACE(<<"ASD">>)}
    ]).

overwriting_index_should_fail(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    Provider2 = ?PROVIDER_ID(WorkerP2),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    % create
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100)),
    Options = #{
        <<"update_min_changes">> => 10000,
        <<"replica_update_min_changes">> => 100
    },
    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP1, ?SPACE_ID, IndexName,
        ?MAP_FUNCTION(XattrName), false, [Provider2], Options
    )),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    ExpMapFun1 = index_utils:escape_js_function(?MAP_FUNCTION(XattrName)),
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := Options,
            <<"providers">> := [Provider2],
            <<"mapFunction">> := ExpMapFun1,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_index_via_rest(Config, Worker, ?SPACE_ID, IndexName), ?ATTEMPTS)
    end, Workers),

    % overwrite
    ?assertMatch({?HTTP_409_CONFLICT, _}, create_index_via_rest(
        Config, WorkerP1, ?SPACE_ID, IndexName,
        ?GEOSPATIAL_MAP_FUNCTION, true, [], #{}
    )),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),


    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := Options,
            <<"providers">> := [Provider2],
            <<"mapFunction">> := ExpMapFun1,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_index_via_rest(Config, Worker, ?SPACE_ID, IndexName), ?ATTEMPTS)
    end, Workers),

    % delete
    ?assertMatch(ok, remove_index_via_rest(Config, WorkerP1, ?SPACE_ID, IndexName)),
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS).

create_get_delete_reduce_fun(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    Provider1 = ?PROVIDER_ID(WorkerP1),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    % create on one provider
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100)),
    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP1, ?SPACE_ID, IndexName, ?MAP_FUNCTION(XattrName), false
    )),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % get on both
    ExpMapFun = index_utils:escape_js_function(?MAP_FUNCTION(XattrName)),
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := #{},
            <<"providers">> := [Provider1],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_index_via_rest(Config, Worker, ?SPACE_ID, IndexName), ?ATTEMPTS)
    end, Workers),

    % add reduce function on other provider
    ?assertMatch(ok, add_reduce_fun_via_rest(
        Config, WorkerP2, ?SPACE_ID, IndexName, ?REDUCE_FUNCTION(XattrName)
    )),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % get on both after adding reduce
    ExpReduceFun = index_utils:escape_js_function(?REDUCE_FUNCTION(XattrName)),
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := #{},
            <<"providers">> := [Provider1],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := ExpReduceFun,
            <<"spatial">> := false
        }}, get_index_via_rest(Config, Worker, ?SPACE_ID, IndexName), ?ATTEMPTS)
    end, Workers),

    % remove reduce function
    ?assertMatch(ok, remove_reduce_fun_via_rest(Config, WorkerP1, ?SPACE_ID, IndexName)),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % get on both after adding reduce
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := #{},
            <<"providers">> := [Provider1],
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := null,
            <<"spatial">> := false
        }}, get_index_via_rest(Config, Worker, ?SPACE_ID, IndexName), ?ATTEMPTS)
    end, Workers),

    % delete on other provider
    ?assertMatch(ok, remove_index_via_rest(Config, WorkerP1, ?SPACE_ID, IndexName)),
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS).

getting_nonexistent_index_should_fail(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),
    ExpError = ?ERROR_INDEX_NOT_FOUND,

    lists:foreach(fun(Worker) ->
        ?assertMatch(ExpError, get_index_via_rest(
            Config, Worker, ?SPACE_ID, IndexName
        ))
    end, Workers).

getting_index_of_not_supported_space_should_fail(Config) ->
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),
    SpaceId = <<"space2">>,
    XattrName = ?XATTR_NAME,

    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP2, SpaceId, IndexName, ?MAP_FUNCTION(XattrName), false
    )),

    ExpMapFun = index_utils:escape_js_function(?MAP_FUNCTION(XattrName)),
    ExpProviders = [?PROVIDER_ID(WorkerP2)],
    ?assertMatch({ok, #{
        <<"indexOptions">> := #{},
        <<"providers">> := ExpProviders,
        <<"mapFunction">> := ExpMapFun,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_index_via_rest(Config, WorkerP2, SpaceId, IndexName), ?ATTEMPTS),

    ExpError = ?ERROR_INDEX_NOT_FOUND,
    ?assertMatch(ExpError, get_index_via_rest(
        Config, WorkerP1, SpaceId, IndexName
    )).

list_indexes(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    IndexNum = 20,
    Chunk = rand:uniform(IndexNum),

    ?assertMatch([], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, Chunk)),
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, Chunk)),

    IndexNames = lists:sort(lists:map(fun(Num) ->
        Worker = lists:nth(rand:uniform(length(Workers)), Workers),
        IndexName = <<"index_name_", (integer_to_binary(Num))/binary>>,
        XattrName = ?XATTR_NAME,
        ?assertMatch(ok, create_index_via_rest(
            Config, Worker, ?SPACE_ID, IndexName, ?MAP_FUNCTION(XattrName)
        )),
        IndexName
    end, lists:seq(1, IndexNum))),

    ?assertMatch(IndexNames, list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, Chunk), ?ATTEMPTS),
    ?assertMatch(IndexNames, list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, Chunk), ?ATTEMPTS).

query_index(Config) ->
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    Query = query_filter(Config, SpaceId, IndexName),
    FilePrefix = atom_to_list(?FUNCTION_NAME),
    Guids = create_files_with_xattrs(WorkerP1, SessionId, SpaceName, FilePrefix, 5, XattrName),
    ExpError = ?ERROR_INDEX_NOT_FOUND,
    ExpGuids = lists:sort(Guids),

    % support index only by one provider; other should return error on query
    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP1, SpaceId, IndexName, ?MAP_FUNCTION(XattrName),
        false, [?PROVIDER_ID(WorkerP2)], #{}
    )),
    ?assertMatch(ExpError, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query(WorkerP2, #{}), ?ATTEMPTS),

    % support index on both providers and check that they returns correct results
    ?assertMatch(ok, update_index_via_rest(
        Config, WorkerP2, ?SPACE_ID, IndexName, <<>>,
        #{providers => [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)]}
    ), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query(WorkerP2, #{}), ?ATTEMPTS),

    % remove support for index on one provider,
    % which should then remove it from db
    ?assertMatch(ok, update_index_via_rest(
        Config, WorkerP2, SpaceId, IndexName, <<>>,
        #{providers => [?PROVIDER_ID(WorkerP2)]}
    )),
    ?assertEqual(ExpError, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertMatch(ExpGuids, Query(WorkerP2, #{}), ?ATTEMPTS),

    % lastly add index again on both providers and check that they
    % returns correct results
    ?assertMatch(ok, update_index_via_rest(
        Config, WorkerP1, ?SPACE_ID, IndexName, <<>>,
        #{providers => [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)]}
    )),
    ?assertEqual(ExpGuids, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query(WorkerP2, #{}), ?ATTEMPTS).

quering_index_with_invalid_params_should_fail(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    Query = query_filter(Config, SpaceId, IndexName),
    FilePrefix = atom_to_list(?FUNCTION_NAME),
    Guids = create_files_with_xattrs(WorkerP1, SessionId, SpaceName, FilePrefix, 5, XattrName),
    ExpGuids = lists:sort(Guids),

    % support index only by one provider; other should return error on query
    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP1, SpaceId, IndexName, ?MAP_FUNCTION(XattrName),
        false, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)], #{}
    )),
    ?assertEqual(ExpGuids, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query(WorkerP2, #{}), ?ATTEMPTS),

    lists:foreach(fun({Options, ExpError}) ->
        Worker = lists:nth(rand:uniform(length(Workers)), Workers),
        ?assertMatch(ExpError, Query(Worker, Options))
    end, [
        {#{bbox => ok}, ?ERROR_INVALID_BBOX},
        {#{bbox => 1}, ?ERROR_INVALID_BBOX},

        {#{descending => ok}, ?ERROR_INVALID_DESCENDING},
        {#{descending => 1}, ?ERROR_INVALID_DESCENDING},
        {#{descending => -15.6}, ?ERROR_INVALID_DESCENDING},

        {#{inclusive_end => ok}, ?ERROR_INVALID_INCLUSIVE_END},
        {#{inclusive_end => 1}, ?ERROR_INVALID_INCLUSIVE_END},
        {#{inclusive_end => -15.6}, ?ERROR_INVALID_INCLUSIVE_END},

        {#{keys => ok}, ?ERROR_INVALID_KEYS},
        {#{keys => 1}, ?ERROR_INVALID_KEYS},
        {#{keys => -15.6}, ?ERROR_INVALID_KEYS},

        {#{limit => ok}, ?ERROR_INVALID_LIMIT},
        {#{limit => -3}, ?ERROR_INVALID_LIMIT},
        {#{limit => 15.2}, ?ERROR_INVALID_LIMIT},
        {#{limit => 0}, ?ERROR_INVALID_LIMIT},

        {#{skip => ok}, ?ERROR_INVALID_SKIP},
        {#{skip => -3}, ?ERROR_INVALID_SKIP},
        {#{skip => 15.2}, ?ERROR_INVALID_SKIP},
        {#{skip => 0}, ?ERROR_INVALID_SKIP},

        {#{stale => da}, ?ERROR_INVALID_STALE},
        {#{stale => -3}, ?ERROR_INVALID_STALE},
        {#{stale => 15.2}, ?ERROR_INVALID_STALE},
        {#{stale => 0}, ?ERROR_INVALID_STALE},

        {#{spatial => 1}, ?ERROR_INVALID_SPATIAL_FLAG},
        {#{spatial => -3}, ?ERROR_INVALID_SPATIAL_FLAG},
        {#{spatial => ok}, ?ERROR_INVALID_SPATIAL_FLAG}
    ]).

create_geospatial_index(Config) ->
    Workers = [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),

    create_index_via_rest(Config, WorkerP1, ?SPACE_ID, IndexName, ?GEOSPATIAL_MAP_FUNCTION, true),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    ExpMapFun = index_utils:escape_js_function(?GEOSPATIAL_MAP_FUNCTION),
    ExpProviders = [?PROVIDER_ID(WorkerP1)],
    lists:foreach(fun(Worker) ->
        ?assertMatch({ok, #{
            <<"indexOptions">> := #{},
            <<"providers">> := ExpProviders,
            <<"mapFunction">> := ExpMapFun,
            <<"reduceFunction">> := null,
            <<"spatial">> := true
        }}, get_index_via_rest(Config, Worker, ?SPACE_ID, IndexName), ?ATTEMPTS)
    end, Workers).

query_geospatial_index(Config) ->
    Workers = [WorkerP1, WorkerP2, WorkerP3 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),

    Path0 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f0"])),
    Path1 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f1"])),
    Path2 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f2"])),
    Path3 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f3"])),
    {ok, _Guid0} = lfm_proxy:create(WorkerP1, SessionId, Path0, 8#777),
    {ok, Guid1} = lfm_proxy:create(WorkerP1, SessionId, Path1, 8#777),
    {ok, Guid2} = lfm_proxy:create(WorkerP1, SessionId, Path2, 8#777),
    {ok, Guid3} = lfm_proxy:create(WorkerP1, SessionId, Path3, 8#777),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid1}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [5.1, 10.22]}, [<<"loc">>]),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid2}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [0, 0]}, [<<"loc">>]),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid3}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [10, 5]}, [<<"loc">>]),

    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP1, SpaceId, IndexName,
        ?GEOSPATIAL_MAP_FUNCTION, true,
        [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2), ?PROVIDER_ID(WorkerP3)], #{}
    )),

    Query = query_filter(Config, SpaceId, IndexName),

    lists:foreach(fun(Worker) ->
        QueryOptions1 = #{spatial => true, stale => false},
        ?assertEqual(lists:sort([Guid1, Guid2, Guid3]), Query(Worker, QueryOptions1), ?ATTEMPTS),

        QueryOptions2 = QueryOptions1#{
            start_range => <<"[0,0]">>,
            end_range => <<"[5.5,10.5]">>
        },
        ?assertEqual(lists:sort([Guid1, Guid2]), Query(Worker, QueryOptions2), ?ATTEMPTS)
    end, Workers).

query_file_popularity_index(Config) ->
    [WorkerP1 | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    IndexName = <<"file-popularity">>,
    Options = #{
        spatial => false,
        stale => false
    },
    ?assertMatch({ok, _}, query_index_via_rest(Config, WorkerP1, SpaceId, IndexName, Options)).

spatial_flag_test(Config) ->
    [WorkerP1 | _] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),

    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP1, SpaceId, IndexName,
        ?GEOSPATIAL_MAP_FUNCTION, true,
        [?PROVIDER_ID(WorkerP1)], #{}
    )),

    ?assertMatch({404, _}, query_index_via_rest(Config, WorkerP1, SpaceId, IndexName, [])),
    ?assertMatch({400, _}, query_index_via_rest(Config, WorkerP1, SpaceId, IndexName, [spatial])),
    ?assertMatch({ok, _}, query_index_via_rest(Config, WorkerP1, SpaceId, IndexName, [{spatial, true}])).

file_removal_test(Config) ->
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    Query = query_filter(Config, SpaceId, IndexName),
    FilePrefix = atom_to_list(?FUNCTION_NAME),
    Guids = create_files_with_xattrs(WorkerP1, SessionId, SpaceName, FilePrefix, 5, XattrName),
    ExpGuids = lists:sort(Guids),

    % support index on both providers and check that they returns correct results
    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP1, SpaceId, IndexName, ?MAP_FUNCTION(XattrName),
        false, [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)], #{}
    )),
    ?assertEqual(ExpGuids, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query(WorkerP2, #{}), ?ATTEMPTS),

    % remove files
    remove_files(WorkerP1, SessionId, Guids),

    % they should not be included in query results any more
    ?assertEqual([], Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual([], Query(WorkerP1, #{}), ?ATTEMPTS).

create_duplicated_indexes_on_remote_providers(Config) ->
    [WorkerP1, WorkerP2, WorkerP3 | _] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Provider1 = ?PROVIDER_ID(WorkerP1),
    Provider2 = ?PROVIDER_ID(WorkerP2),
    IndexName = ?INDEX_NAME(?FUNCTION_NAME),
    XattrName = ?XATTR_NAME,

    FilePrefix = atom_to_list(?FUNCTION_NAME),
    Guids = create_files_with_xattrs(WorkerP1, SessionId, SpaceName, FilePrefix, 5, XattrName),
    ExpGuids = lists:sort(Guids),

    % create index on P1 and P2
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100)),
    Options1 = #{<<"update_min_changes">> => 10000},
    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP1, ?SPACE_ID, IndexName, ?MAP_FUNCTION(XattrName), false,
        [Provider1, Provider2], Options1
    )),
    % create different index on P2 but with the same name
    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP2, ?SPACE_ID, IndexName, ?MAP_FUNCTION2(XattrName), false, [], Options1
    )),

    IndexName@P1 = ?INDEX@(IndexName, Provider1),
    IndexName@P2 = ?INDEX@(IndexName, Provider2),

    IndexName@P1Short = ?INDEX@(IndexName, binary_part(Provider1, 0, 4)),
    IndexName@P2Short = ?INDEX@(IndexName, binary_part(Provider2, 0, 4)),

    ?assertMatch([IndexName@P2, IndexName], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName@P1, IndexName], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName@P1Short, IndexName@P2Short], list_indexes_via_rest(Config, WorkerP3, ?SPACE_ID, 100), ?ATTEMPTS),

    ExpMapFun = index_utils:escape_js_function(?MAP_FUNCTION(XattrName)),
    ExpMapFun2 = index_utils:escape_js_function(?MAP_FUNCTION2(XattrName)),
    ExpError = ?ERROR_INDEX_NOT_FOUND,
    ExpError2 = ?ERROR_AMBIGUOUS_INDEX_NAME,

    % get by simple name
    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"providers">> := [Provider1, Provider2],
        <<"mapFunction">> := ExpMapFun,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_index_via_rest(Config, WorkerP1, ?SPACE_ID, IndexName), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"providers">> := [Provider2],
        <<"mapFunction">> := ExpMapFun2,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_index_via_rest(Config, WorkerP2, ?SPACE_ID, IndexName), ?ATTEMPTS),

    ?assertMatch(ExpError2, get_index_via_rest(Config, WorkerP3, ?SPACE_ID, IndexName), ?ATTEMPTS),

    % get by extended name
    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"providers">> := [Provider1, Provider2],
        <<"mapFunction">> := ExpMapFun,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_index_via_rest(Config, WorkerP1, ?SPACE_ID, IndexName@P1), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"providers">> := [Provider2],
        <<"mapFunction">> := ExpMapFun2,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_index_via_rest(Config, WorkerP2, ?SPACE_ID, IndexName@P2), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"providers">> := [Provider1, Provider2],
        <<"mapFunction">> := ExpMapFun,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_index_via_rest(Config, WorkerP3, ?SPACE_ID, IndexName@P1), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"providers">> := [Provider2],
        <<"mapFunction">> := ExpMapFun2,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_index_via_rest(Config, WorkerP3, ?SPACE_ID, IndexName@P2), ?ATTEMPTS),

    % get by shortened extended name
    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"providers">> := [Provider1, Provider2],
        <<"mapFunction">> := ExpMapFun,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_index_via_rest(Config, WorkerP1, ?SPACE_ID, IndexName@P1Short), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"providers">> := [Provider2],
        <<"mapFunction">> := ExpMapFun2,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_index_via_rest(Config, WorkerP2, ?SPACE_ID, IndexName@P2Short), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"providers">> := [Provider1, Provider2],
        <<"mapFunction">> := ExpMapFun,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_index_via_rest(Config, WorkerP3, ?SPACE_ID, IndexName@P1Short), ?ATTEMPTS),

    ?assertMatch({ok, #{
        <<"indexOptions">> := Options1,
        <<"providers">> := [Provider2],
        <<"mapFunction">> := ExpMapFun2,
        <<"reduceFunction">> := null,
        <<"spatial">> := false
    }}, get_index_via_rest(Config, WorkerP3, ?SPACE_ID, IndexName@P2Short), ?ATTEMPTS),


    % query the indexes by simple name
    Query = query_filter(Config, SpaceId, IndexName),
    Query2 = query_filter2(Config, SpaceId, IndexName),

    ?assertMatch(ExpGuids, Query(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query2(WorkerP2, #{}), ?ATTEMPTS),
    ?assertEqual(ExpError2, Query(WorkerP3, #{}), ?ATTEMPTS),

    % query the indexes by extended name
    Query@P1 = query_filter(Config, SpaceId, IndexName@P1),
    Query@P2 = query_filter2(Config, SpaceId, IndexName@P2),

    ?assertMatch(ExpGuids, Query@P1(WorkerP1, #{}), ?ATTEMPTS),
    ?assertMatch(ExpError, Query@P2(WorkerP1, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query@P1(WorkerP2, #{}), ?ATTEMPTS),
    ?assertEqual(ExpGuids, Query@P2(WorkerP2, #{}), ?ATTEMPTS),
    ?assertEqual(ExpError, Query@P1(WorkerP3, #{}), ?ATTEMPTS),
    ?assertEqual(ExpError, Query@P2(WorkerP3, #{}), ?ATTEMPTS),

    % query the indexes by shortened extended name
    Query@P1Short = query_filter(Config, SpaceId, IndexName@P1Short),
    Query@P2Short = query_filter2(Config, SpaceId, IndexName@P2Short),

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
        {?LOAD_MODULES, [initializer, multi_provider_index_rest_test_SUITE]}
        | Config
    ].

end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).

init_per_testcase(query_file_popularity_index, Config) ->
    Config2 = sort_workers(Config),
    [WorkerP1, WorkerP2| _] = ?config(op_worker_nodes, Config2),
    ok = rpc:call(WorkerP1, file_popularity_api, enable, [?SPACE_ID]),
    ok = rpc:call(WorkerP2, file_popularity_api, enable, [?SPACE_ID]),
    init_per_testcase(all, Config2);

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(sort_workers(Config)).

end_per_testcase(query_file_popularity_index, Config) ->
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    rpc:call(WorkerP1, file_popularity_api, disable, [?SPACE_ID]),
    rpc:call(WorkerP2, file_popularity_api, disable, [?SPACE_ID]),
    end_per_testcase(all, Config);

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    remove_all_indexes(Workers, <<"space1">>),
    remove_all_indexes(Workers, <<"space2">>),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_index_via_rest(Config, Worker, SpaceId, IndexName, MapFunction) ->
    create_index_via_rest(
        Config, Worker, SpaceId, IndexName, MapFunction, false
    ).

create_index_via_rest(Config, Worker, SpaceId, IndexName, MapFunction, Spatial) ->
    create_index_via_rest(
        Config, Worker, SpaceId, IndexName, MapFunction,
        Spatial, [], #{}
    ).

create_index_via_rest(Config, Worker, SpaceId, IndexName, MapFunction, Spatial, Providers, Options) ->
    QueryString = create_query_string(Options#{
        spatial => Spatial,
        providers => Providers
    }),
    Path = <<(?INDEX_PATH(SpaceId, IndexName))/binary, QueryString/binary>>,

    Headers = ?USER_1_AUTH_HEADERS(Config, [
        {<<"content-type">>, <<"application/javascript">>}
    ]),

    case rest_test_utils:request(Worker, Path, put, Headers, MapFunction) of
        {ok, 200, _, _} ->
            ok;
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

update_index_via_rest(Config, Worker, SpaceId, IndexName, MapFunction, Options) ->
    QueryString = create_query_string(Options),
    Path = <<(?INDEX_PATH(SpaceId, IndexName))/binary, QueryString/binary>>,

    Headers = ?USER_1_AUTH_HEADERS(Config, [
        {<<"content-type">>, <<"application/javascript">>}
    ]),

    case rest_test_utils:request(Worker, Path, patch, Headers, MapFunction) of
        {ok, 200, _, _} ->
            ok;
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

add_reduce_fun_via_rest(Config, Worker, SpaceId, IndexName, ReduceFunction) ->
    Path = <<(?INDEX_PATH(SpaceId, IndexName))/binary, "/reduce">>,
    Headers = ?USER_1_AUTH_HEADERS(Config, [
        {<<"content-type">>, <<"application/javascript">>}
    ]),

    case rest_test_utils:request(Worker, Path, put, Headers, ReduceFunction) of
        {ok, 200, _, _} ->
            ok;
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

remove_reduce_fun_via_rest(Config, Worker, SpaceId, IndexName) ->
    Path = <<(?INDEX_PATH(SpaceId, IndexName))/binary, "/reduce">>,
    Headers = ?USER_1_AUTH_HEADERS(Config, [
        {<<"content-type">>, <<"application/javascript">>}
    ]),

    case rest_test_utils:request(Worker, Path, delete, Headers, []) of
        {ok, 204, _, _} ->
            ok;
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

get_index_via_rest(Config, Worker, SpaceId, IndexName) ->
    Path = ?INDEX_PATH(SpaceId, IndexName),
    Headers = ?USER_1_AUTH_HEADERS(Config, [{<<"accept">>, <<"application/json">>}]),
    case rest_test_utils:request(Worker, Path, get, Headers, []) of
        {ok, 200, _, Body} ->
            {ok, json_utils:decode(Body)};
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

remove_index_via_rest(Config, Worker, SpaceId, IndexName) ->
    Path = ?INDEX_PATH(SpaceId, IndexName),
    Headers = ?USER_1_AUTH_HEADERS(Config),
    case rest_test_utils:request(Worker, Path, delete, Headers, []) of
        {ok, 204, _, _} ->
            ok;
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

query_index_via_rest(Config, Worker, SpaceId, IndexName, Options) ->
    QueryString = create_query_string(Options),
    Path = <<(?INDEX_PATH(SpaceId, IndexName))/binary, "/query", QueryString/binary>>,
    Headers = ?USER_1_AUTH_HEADERS(Config),
    case rest_test_utils:request(Worker, Path, get, Headers, []) of
        {ok, 200, _, Body} ->
            {ok, json_utils:decode(Body)};
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.

list_indexes_via_rest(Config, Worker, Space, ChunkSize) ->
    Result = list_indexes_via_rest(Config, Worker, Space, ChunkSize, <<"null">>, []),
    % Make sure there are no duplicates
    ?assertEqual(lists:sort(Result), lists:usort(Result)),
    Result.

list_indexes_via_rest(Config, Worker, Space, ChunkSize, StartId, Acc) ->
    {Indexes, NextPageToken} = list_indexes_via_rest(Config, Worker, Space, StartId, ChunkSize),
    case NextPageToken of
        <<"null">> ->
            Acc ++ Indexes;
        _ ->
            ?assertMatch(ChunkSize, length(Indexes)),
            list_indexes_via_rest(Config, Worker, Space, ChunkSize, NextPageToken, Acc ++ Indexes)
    end.

list_indexes_via_rest(Config, Worker, Space, StartId, LimitOrUndef) ->
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
    Url = str_utils:format_bin("spaces/~s/indexes?~s~s", [
        Space, TokenParam, LimitParam
    ]),
    {ok, _, _, Body} = ?assertMatch({ok, 200, _, _}, rest_test_utils:request(
        Worker, Url, get, ?USER_1_AUTH_HEADERS(Config), <<>>
    )),
    ParsedBody = json_utils:decode(Body),
    Indexes = maps:get(<<"indexes">>, ParsedBody),
    NextPageToken = maps:get(<<"nextPageToken">>, ParsedBody, <<"null">>),
    {Indexes, NextPageToken}.

remove_all_indexes(Nodes, SpaceId) ->
    lists:foreach(fun(Node) ->
        case rpc:call(Node, index, list, [SpaceId]) of
            {ok, IndexNames} ->
                lists:foreach(fun(IndexName) ->
                    ok = rpc:call(Node, index, delete, [SpaceId, IndexName])
                end, IndexNames);
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
        {ok, Guid} = cdmi_id:objectid_to_guid(ObjectId),
        Guid
    end, ObjectIds).

query_filter(Config, SpaceId, IndexName) ->
    fun(Node, Options) ->
        case query_index_via_rest(Config, Node, SpaceId, IndexName, Options) of
            {ok, Body} ->
                ObjectIds = lists:map(fun(#{<<"value">> := ObjectId}) ->
                    ObjectId
                end, Body),
                lists:sort(objectids_to_guids(ObjectIds));
            Error ->
                Error
        end
    end.

query_filter2(Config, SpaceId, IndexName) ->
    fun(Node, Options) ->
        case query_index_via_rest(Config, Node, SpaceId, IndexName, Options) of
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
        Path = list_to_binary(filename:join(
            ["/", binary_to_list(SpaceName), Prefix ++ integer_to_list(X)]
        )),
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
