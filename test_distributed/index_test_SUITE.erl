%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Basic tests of couchbase indexes
%%% @end
%%%-------------------------------------------------------------------
-module(index_test_SUITE).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    create_and_delete_simple_index_test/1,
    query_simple_empty_index_test/1,
    query_index_using_file_meta/1,
    query_index_using_times/1,
    query_index_using_custom_metadata_when_xattr_is_not_set/1,
    query_index_using_custom_metadata/1,
    query_index_using_file_popularity/1,
    query_index_and_emit_ctx/1,
    wrong_map_function/1
]).


%% macros
-define(SPACE_ID, <<"space_id1">>).
-define(SPACE_NAME, <<"space_name1">>).
-define(TEST_FILE(SpaceName), begin
    FunctionNameBin = str_utils:to_binary(?FUNCTION),
    RandIntBin = str_utils:to_binary(rand:uniform(1000000000)),
    FileName = <<FunctionNameBin/binary, "_", RandIntBin/binary>>,
    filename:join(["/", SpaceName, FileName])
end).

-define(MODE, 8#664).

-define(USER_ID, <<"user1">>).
-define(SESS_ID(Worker),
    ?config({session_id, {?USER_ID, ?GET_DOMAIN(Worker)}}, Config)).

-define(index_name, begin <<"index_", (str_utils:to_binary(?FUNCTION))/binary>> end).
-define(ATTEMPTS, 15).

-define(assertQuery(ExpectedRows, Worker, SpaceId, IndexName, Options),
    ?assertQuery(ExpectedRows, Worker, SpaceId, IndexName, Options, ?ATTEMPTS)).

-define(assertQuery(ExpectedRows, Worker, SpaceId, IndexName, Options, Attempts),
    ?assertMatch(ExpectedRows, begin
        {ok, QueryResult} = query_index(Worker, SpaceId, IndexName, Options),
        query_result_to_map(QueryResult)
    end, Attempts)).


%%%===================================================================
%%% API
%%%===================================================================
all() -> ?ALL([
    create_and_delete_simple_index_test,
    query_simple_empty_index_test,
    query_index_using_file_meta,
    query_index_using_times,
    query_index_using_custom_metadata_when_xattr_is_not_set,
    query_index_using_custom_metadata,
    query_index_using_file_popularity,
    query_index_and_emit_ctx,
    wrong_map_function
]).

%%%===================================================================
%%% Test functions
%%%===================================================================

create_and_delete_simple_index_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    IndexName = ?index_name,
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            return [id, id];
        }
    ">>,
    create_index(Worker, SpaceId, IndexName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertMatch({ok, [IndexName]}, list_indexes(Worker, SpaceId)),
    delete_index(Worker, SpaceId, IndexName),
    ?assertMatch({ok, []}, list_indexes(Worker, SpaceId)).

query_simple_empty_index_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    IndexName = ?index_name,
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            return [id, id];
        }
    ">>,
    create_index(Worker, SpaceId, IndexName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    {ok, Result} = query_index(Worker, SpaceId, IndexName, []),
    ?assertMatch([], query_result_to_map(Result)).

query_index_using_file_meta(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    IndexName = ?index_name,
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    {ok, CdmiId} = cdmi_id:guid_to_objectid(SpaceGuid),
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'file_meta')
                return [id, meta];
        }
    ">>,
    create_index(Worker, SpaceId, IndexName, SimpleMapFunction, undefined, [], false, [ProviderId]),

    ?assertQuery([#{
        <<"id">> := _,
        <<"key">> := CdmiId,
        <<"value">> := #{
            <<"name">> := ?SPACE_ID,
            <<"type">> := <<"DIR">>,
            <<"mode">> := 8#775,
            <<"owner">> := ?ROOT_USER_ID,
            <<"group_owner">> := null,
            <<"version">> := 0,
            <<"provider_id">> := ProviderId,
            <<"shares">> := [],
            <<"deleted">> := false,
            <<"parent_uuid">> := <<"">>
        }}],Worker, SpaceId, IndexName, [{stale, false}]).

query_index_using_times(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    IndexName = ?index_name,
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    {ok, CdmiId} = cdmi_id:guid_to_objectid(SpaceGuid),
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'times')
                return [id, meta];
        }
    ">>,
    create_index(Worker, SpaceId, IndexName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertQuery([#{
        <<"id">> := _,
        <<"key">> := CdmiId,
        <<"value">> := #{
            <<"atime">> := _,
            <<"mtime">> := _,
            <<"ctime">> := _

        }}],Worker, SpaceId, IndexName, [{stale, false}]).

query_index_using_custom_metadata_when_xattr_is_not_set(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    IndexName = ?index_name,
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    SimpleMapFunction = <<"
        function(id, file_meta, times, custom_metadata, file_popularity, ctx) {
            if(type == 'custom_metadata')
                return [id, meta];
        }
    ">>,
    create_index(Worker, SpaceId, IndexName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertQuery([],Worker, SpaceId, IndexName, [{stale, false}]).

query_index_using_custom_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    IndexName = ?index_name,
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    {ok, CdmiId} = cdmi_id:guid_to_objectid(SpaceGuid),
    ProviderId = ?GET_DOMAIN_BIN(Worker),

    XattrName = <<"xattr_name">>,
    XattrValue = <<"xattr_value">>,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    lfm_proxy:set_xattr(Worker, ?SESS_ID(Worker), {guid, SpaceGuid}, Xattr),

    XattrName2 = <<"xattr_name2">>,
    XattrValue2 = <<"xattr_value2">>,
    Xattr2 = #xattr{name = XattrName2, value = XattrValue2},
    lfm_proxy:set_xattr(Worker, ?SESS_ID(Worker), {guid, SpaceGuid}, Xattr2),

    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'custom_metadata')
                return [id, meta];
        }
    ">>,
    create_index(Worker, SpaceId, IndexName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertQuery([#{
        <<"id">> := _,
        <<"key">> := CdmiId,
        <<"value">> := #{
            XattrName := XattrValue,
            XattrName2 := XattrValue2
        }
    }],Worker, SpaceId, IndexName, [{stale, false}]).

query_index_using_file_popularity(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    IndexName = ?index_name,
    SessionId = ?SESS_ID(Worker),
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    TestData = <<"test_data">>,
    TestDataSize = byte_size(TestData),

    {ok, _} = rpc:call(Worker, file_popularity_api, enable, [?SPACE_ID]),
    FilePath = ?TEST_FILE(?SPACE_NAME),
    {ok, Guid} = lfm_proxy:create(Worker, SessionId, FilePath, 8#664),
    Uuid = fslogic_uuid:guid_to_uuid(Guid),
    {ok, H} = lfm_proxy:open(Worker, SessionId, {guid, Guid}, write),
    lfm_proxy:write(Worker, H, 0, TestData),
    lfm_proxy:close(Worker, H),

    {ok, CdmiId} = cdmi_id:guid_to_objectid(Guid),
    ProviderId = ?GET_DOMAIN_BIN(Worker),

    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'file_popularity')
                return [id, meta];
        }
    ">>,
    create_index(Worker, SpaceId, IndexName, SimpleMapFunction, undefined, [], false, [ProviderId]),

    ?assertQuery([#{
        <<"id">> := _,
        <<"key">> := CdmiId,
        <<"value">> := #{
            <<"file_uuid">> := Uuid,
            <<"space_id">> := ?SPACE_ID,
            <<"dy_hist">> :=[1 | _],
            <<"hr_hist">> := [1 | _],
            <<"mth_hist">> := [1 | _],
            <<"dy_mov_avg">> := 1,
            <<"hr_mov_avg">> := 1,
            <<"mth_mov_avg">> := 1,
            <<"last_open">> := _,
            <<"open_count">> := 1,
            <<"size">> := TestDataSize
    }}],Worker, SpaceId, IndexName, [{stale, false}]).

query_index_and_emit_ctx(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    IndexName = ?index_name,
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    {ok, CdmiId} = cdmi_id:guid_to_objectid(SpaceGuid),
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'file_meta')
                return [id, ctx];
        }
    ">>,
    create_index(Worker, SpaceId, IndexName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertQuery([#{
        <<"id">> := _,
        <<"key">> := CdmiId,
        <<"value">> := #{
            <<"providerId">> := ProviderId

        }}],Worker, SpaceId, IndexName, [{stale, false}, {key, CdmiId}]).

wrong_map_function(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SpaceId = <<"space_id1">>,
    IndexName = ?index_name,
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    SpaceGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
    {ok, CdmiId} = cdmi_id:guid_to_objectid(SpaceGuid),
    ProviderId = ?GET_DOMAIN_BIN(Worker),
    SimpleMapFunction = <<"
        function(_, _, _, _) {
            throw 'Test error';
        }
    ">>,
    create_index(Worker, SpaceId, IndexName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertQuery([], Worker, SpaceId, IndexName, [{stale, false}, {key, CdmiId}]).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    lfm_proxy:init(ConfigWithSessionInfo).

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_index(Worker, SpaceId, IndexName, MapFunction, ReduceFunction, Options, Spatial, ProviderIds) ->
    ok = rpc:call(Worker, index, create, [SpaceId, IndexName, MapFunction,
        ReduceFunction, Options, Spatial, ProviderIds]).

delete_index(Worker, SpaceId, IndexName) ->
    ok = rpc:call(Worker, index, delete, [SpaceId, IndexName]).

query_index(Worker, SpaceId, ViewName, Options) ->
    rpc:call(Worker, index, query, [SpaceId, ViewName, Options]).

list_indexes(Worker, SpaceId) ->
    rpc:call(Worker, index, list, [SpaceId]).

query_result_to_map(QueryResult) ->
    {Rows} = QueryResult,
    lists:map(fun(Row) ->
        {<<"value">>, {Value}} = lists:keyfind(<<"value">>, 1, Row),
        Row2 = lists:keyreplace(<<"value">>, 1, Row, {<<"value">>, maps:from_list(Value)}),
        maps:from_list(Row2)
    end, Rows).