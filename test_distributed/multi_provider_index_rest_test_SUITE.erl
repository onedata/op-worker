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

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    create_geospatial_index/1,
    list_indexes/1,
    create_get_update_delete_index/1,
    query_geospatial_index/1,
    query_file_popularity_index/1,
    spatial_flag_test/1
]).

all() ->
    ?ALL([
        create_geospatial_index,
        list_indexes,
        create_get_update_delete_index,
        query_geospatial_index,
        query_file_popularity_index,
        spatial_flag_test
    ]).

-define(ATTEMPTS, 60).

-define(SPACE_ID, <<"space1">>).
-define(PROVIDER_ID(__Node), rpc:call(__Node, oneprovider, get_id, [])).

-define(USER_1_AUTH_HEADERS(Config), ?USER_1_AUTH_HEADERS(Config, [])).
-define(USER_1_AUTH_HEADERS(Config, OtherHeaders),
    ?USER_AUTH_HEADERS(Config, <<"user1">>, OtherHeaders)).

-define(INDEX_PATH(__SpaceId, __IndexName),
    <<"spaces/", __SpaceId/binary, "/indexes/", __IndexName/binary>>
).

-define(MAP_FUNCTION,
    <<"function (meta) {
        if(meta['onedata_json'] && meta['onedata_json']['meta'] && meta['onedata_json']['meta']['color']) {
            return meta['onedata_json']['meta']['color'];
        }
        return null;
    }">>
).
-define(GEOSPATIAL_MAP_FUNCTION,
    <<"function (meta) {
        if(meta['onedata_json'] && meta['onedata_json']['loc']) {
            return meta['onedata_json']['loc'];
        }
        return null;
    }">>
).

-define(absPath(SpaceId, Path), <<"/", SpaceId/binary, "/", Path/binary>>).

-define(TEST_DATA, <<"test">>).
-define(TEST_DATA_SIZE, 4).
-define(TEST_DATA2, <<"test01234">>).
-define(TEST_DATA_SIZE2, 9).

%%%===================================================================
%%% Test functions
%%%===================================================================

create_get_update_delete_index(Config) ->
    Workers = [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    Provider1 = ?PROVIDER_ID(WorkerP1),
    Provider2 = ?PROVIDER_ID(WorkerP2),

    IndexName = <<"name1">>,
    Options = #{
        update_min_changes => 10000,
        replica_update_min_changes => 100
    },

    % create on one provider
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100)),
    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP1, ?SPACE_ID, IndexName,
        ?MAP_FUNCTION, false, [Provider1, Provider2], Options
    )),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    % get on both
    ExpMapFun = index_utils:escape_js_function(?MAP_FUNCTION),
    lists:foreach(fun(Worker) ->
        ?assertMatch(#{
            <<"indexOptions">> := [], % todo options
            <<"providers">> := [Provider2, Provider1],
            <<"mapFunction">> := ExpMapFun,
            % <<"reduceFunction">> := ReduceFunction, todo
            <<"spatial">> := false
        }, get_index_via_rest(Config, Worker, ?SPACE_ID, IndexName), ?ATTEMPTS)
    end, Workers),

    % delete on other provider
    ?assertMatch(ok, remove_index_via_rest(Config, WorkerP2, ?SPACE_ID, IndexName)),
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS).

list_indexes(Config) ->
    Workers = [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),

    IndexNum = 20,
    Chunk = rand:uniform(IndexNum),

    ?assertMatch([], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, Chunk)),
    ?assertMatch([], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, Chunk)),

    IndexNames = lists:sort(lists:map(fun(Num) ->
        Worker = lists:nth(rand:uniform(length(Workers)), Workers),
        IndexName = <<"index_name_", (integer_to_binary(Num))/binary>>,
        ?assertMatch(ok, create_index_via_rest(
            Config, Worker, ?SPACE_ID, IndexName, ?MAP_FUNCTION
        )),
        IndexName
    end, lists:seq(1, IndexNum))),

    ?assertMatch(IndexNames, list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, Chunk), ?ATTEMPTS),
    ?assertMatch(IndexNames, list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, Chunk), ?ATTEMPTS).

create_geospatial_index(Config) ->
    Workers = [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    IndexName = <<"geospatial_index_1">>,

    create_index_via_rest(Config, WorkerP1, ?SPACE_ID, IndexName, ?GEOSPATIAL_MAP_FUNCTION, true),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP1, ?SPACE_ID, 100), ?ATTEMPTS),
    ?assertMatch([IndexName], list_indexes_via_rest(Config, WorkerP2, ?SPACE_ID, 100), ?ATTEMPTS),

    ExpMapFun = index_utils:escape_js_function(?GEOSPATIAL_MAP_FUNCTION),
    ExpProviders = [?PROVIDER_ID(WorkerP1)],
    lists:foreach(fun(Worker) ->
        ?assertMatch(#{
            <<"indexOptions">> := [],
            <<"providers">> := ExpProviders,
            <<"mapFunction">> := ExpMapFun,
            % <<"reduceFunction">> := null, todo
            <<"spatial">> := true
        }, get_index_via_rest(Config, Worker, ?SPACE_ID, IndexName), ?ATTEMPTS)
    end, Workers).

query_geospatial_index(Config) ->
    Workers = [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    SessionId = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    IndexName = <<"geospatial_index_1">>,

    Path1 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f1"])),
    Path2 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f2"])),
    Path3 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "f3"])),
    {ok, Guid1} = lfm_proxy:create(WorkerP1, SessionId, Path1, 8#777),
    {ok, Guid2} = lfm_proxy:create(WorkerP1, SessionId, Path2, 8#777),
    {ok, Guid3} = lfm_proxy:create(WorkerP1, SessionId, Path3, 8#777),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid1}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [5.1, 10.22]}, [<<"loc">>]),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid2}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [0, 0]}, [<<"loc">>]),
    ok = lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, Guid3}, json, #{<<"type">> => <<"Point">>, <<"coordinates">> => [10, 5]}, [<<"loc">>]),

    ?assertMatch(ok, create_index_via_rest(
        Config, WorkerP1, SpaceId, IndexName,
        ?GEOSPATIAL_MAP_FUNCTION, true,
        [?PROVIDER_ID(WorkerP1), ?PROVIDER_ID(WorkerP2)], []
    )),
    timer:sleep(timer:seconds(5)), % let the data be stored in db todo VFS-3462

    lists:foreach(fun(Worker) ->
        QueryOptions1 = #{
            spatial => true,
            stale => false
        },
        Body1 = query_index_via_rest(Config, Worker, SpaceId, IndexName, QueryOptions1),
        ct:pal("BODY1: ~p", [Body1]),
        Guids1 = objectids_to_guids(Body1),
        ?assertEqual(lists:sort([Guid1, Guid2, Guid3]), lists:sort(Guids1)),

        QueryOptions2 = QueryOptions1#{
            start_range => <<"[0,0]">>,
            end_range => <<"[5.5,10.5]">>
        },
        Body2 = query_index_via_rest(Config, Worker, SpaceId, IndexName, QueryOptions2),
        Guids2 = objectids_to_guids(Body2),
        ?assertEqual(lists:sort([Guid1, Guid2, Guid3]), lists:sort(Guids2))
    end, Workers).


query_file_popularity_index(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    IndexName = <<"file-popularity-", SpaceId/binary>>,
    Options = #{
        spatial => true,
        stale => false
    },
    ?assertMatch(ok, query_index_via_rest(Config, WorkerP1, SpaceId, IndexName, Options)).
%%
%%    ?assertMatch({ok, 200, _, _},
%%        rest_test_utils:request(WorkerP1, <<"query-index/file-popularity-", SpaceId/binary, "?spatial=true&stale=false">>, get, ?USER_1_AUTH_HEADERS(Config), [])).

spatial_flag_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    ?assertMatch({ok, 404, _, _}, rest_test_utils:request(WorkerP1, <<"query-index/file-popularity-", SpaceId/binary>>, get, ?USER_1_AUTH_HEADERS(Config), [])),
    ?assertMatch({ok, 400, _, _}, rest_test_utils:request(WorkerP1, <<"query-index/file-popularity-", SpaceId/binary, "?spatial">>, get, ?USER_1_AUTH_HEADERS(Config), [])),
    ?assertMatch({ok, 200, _, _}, rest_test_utils:request(WorkerP1, <<"query-index/file-popularity-", SpaceId/binary, "?spatial=true">>, get, ?USER_1_AUTH_HEADERS(Config), [])).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        application:start(ssl),
        hackney:start(),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig, "env_desc.json"), NewConfig)
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

init_per_testcase(Case, Config) when
    Case =:= query_file_popularity_index;
    Case =:= spatial_flag_test
    ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    {ok, _} = rpc:call(WorkerP1, space_storage, enable_file_popularity, [?SPACE_ID]),
    {ok, _} = rpc:call(WorkerP2, space_storage, enable_file_popularity, [?SPACE_ID]),
    init_per_testcase(all, Config);

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config).

end_per_testcase(Case, Config) when
    Case =:= query_file_popularity_index;
    Case =:= spatial_flag_test
    ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    rpc:call(WorkerP1, space_storage, disable_file_popularity, [?SPACE_ID]),
    rpc:call(WorkerP2, space_storage, disable_file_popularity, [?SPACE_ID]),
    end_per_testcase(?DEFAULT_CASE(Case), Config);

end_per_testcase(_Case, Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    remove_all_indexes([WorkerP1], ?SPACE_ID),
    lfm_proxy:teardown(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_index_via_rest(Config, Worker, SpaceId, IndexName, MapFunction) ->
    create_index_via_rest(
        Config, Worker, SpaceId, IndexName, MapFunction, false).

create_index_via_rest(Config, Worker, SpaceId, IndexName, MapFunction, Spatial) ->
    create_index_via_rest(
        Config, Worker, SpaceId, IndexName, MapFunction,
        Spatial, [], undefined
    ).

create_index_via_rest(Config, Worker, SpaceId, IndexName, MapFunction, Spatial, Providers, _Options) -> % todo options
    SpatialBin = <<"spatial=", (atom_to_binary(Spatial, utf8))/binary>>,
    ProvidersBin = lists:foldl(fun(ProviderId, Acc) ->
        <<Acc/binary, "&providers[]=", ProviderId/binary>>
    end, <<>>, Providers),
    QueryString = <<"?", SpatialBin/binary, ProvidersBin/binary>>,

    Path = <<(?INDEX_PATH(SpaceId, IndexName))/binary, QueryString/binary>>,

    Headers = ?USER_1_AUTH_HEADERS(Config, [
        {<<"content-type">>, <<"application/javascript">>}
    ]),

    case rest_test_utils:request(Worker, Path, put, Headers, MapFunction) of
        {ok, 200, _, _} ->
            ok;
        Res ->
            Res
    end.

get_index_via_rest(Config, Worker, SpaceId, IndexName) ->
    Path = ?INDEX_PATH(SpaceId, IndexName),
    Headers = ?USER_1_AUTH_HEADERS(Config, [{<<"accept">>, <<"application/json">>}]),
    {ok, 200, _, Body} = ?assertMatch({ok, 200, _, _},
        rest_test_utils:request(Worker, Path, get, Headers, [])
    ),
    json_utils:decode(Body).

remove_index_via_rest(Config, Worker, SpaceId, IndexName) ->
    Path = ?INDEX_PATH(SpaceId, IndexName),
    Headers = ?USER_1_AUTH_HEADERS(Config),
    case rest_test_utils:request(Worker, Path, delete, Headers, []) of
        {ok, 204, _, _} ->
            ok;
        Res ->
            Res
    end.

query_index_via_rest(Config, Worker, SpaceId, IndexName, Options) ->
    QueryString = create_query_string(Options),
    Path = <<(?INDEX_PATH(SpaceId, IndexName))/binary, "/query", QueryString/binary>>,
    ct:pal("~p", [Path]),
    Headers = ?USER_1_AUTH_HEADERS(Config),
    case rest_test_utils:request(Worker, Path, get, Headers, []) of
        {ok, 200, _, Body} ->
            json_utils:decode(Body);
        Res ->
            Res
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
        {ok, IndexNames} = rpc:call(Node, index, list, [SpaceId]),
        lists:foreach(fun(IndexName) ->
            ok = rpc:call(Node, index, delete, [SpaceId, IndexName])
        end, IndexNames)
    end, Nodes).

create_query_string(Options) when is_map(Options) ->
    create_query_string(maps:to_list(Options));
create_query_string(Options) ->
    lists:foldl(fun(Option, AccQuery) ->
        OptionBin = case Option of
            {Key, Val} ->
                KeyBin = atom_to_binary(Key, utf8),
                ValBin = binary_from_term(Val),
                <<KeyBin/binary, "=", ValBin/binary>>;
            _ ->
                atom_to_binary(Option, utf8)
        end,
        <<AccQuery/binary, "&", OptionBin/binary>>
    end, <<>>, Options).

binary_from_term(Val) when is_binary(Val) ->
    Val;
binary_from_term(Val) when is_integer(Val) ->
    integer_to_binary(Val);
binary_from_term(Val) when is_float(Val) ->
    float_to_binary(Val);
binary_from_term(Val) when is_atom(Val) ->
    atom_to_binary(Val, utf8).

objectids_to_guids(ObjectIds) ->
    lists:map(fun(X) ->
        {ok, ObjId} = cdmi_id:objectid_to_guid(X),
        ObjId
    end, ObjectIds).
