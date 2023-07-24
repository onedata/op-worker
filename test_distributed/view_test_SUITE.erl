%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Basic tests of couchbase views.
%%% @end
%%%-------------------------------------------------------------------
-module(view_test_SUITE).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% API
%% export for ct
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_and_delete_simple_view_test/1,
    query_simple_empty_view_test/1,
    query_view_using_file_meta/1,
    query_view_using_times/1,
    query_view_using_custom_metadata_when_xattr_is_not_set/1,
    query_view_using_custom_metadata/1,
    query_view_using_file_popularity/1,
    query_view_and_emit_ctx/1,
    wrong_map_function/1,
    emitting_null_key_in_map_function_should_return_empty_result/1,
    spatial_function_returning_null_in_key_should_return_empty_result/1,
    spatial_function_returning_null_in_array_key_should_return_empty_result/1,
    spatial_function_returning_null_in_range_key_should_return_empty_result/1,
    spatial_function_returning_integer_key_should_return_error/1,
    spatial_function_returning_string_key_should_return_error/1
]).


%% macros
-define(TEST_FILE(SpaceName), begin
    FunctionNameBin = str_utils:to_binary(?FUNCTION),
    RandIntBin = str_utils:to_binary(rand:uniform(1000000000)),
    FileName = <<FunctionNameBin/binary, "_", RandIntBin/binary>>,
    filename:join(["/", SpaceName, FileName])
end).

-define(view_name, begin <<"view_", (str_utils:to_binary(?FUNCTION))/binary>> end).
-define(ATTEMPTS, 15).

-define(assertQuery(ExpectedRows, Worker, SpaceId, ViewName, Options),
    ?assertQuery(ExpectedRows, Worker, SpaceId, ViewName, Options, ?ATTEMPTS)).

-define(assertQuery(ExpectedRows, Worker, SpaceId, ViewName, Options, Attempts),
    ?assertMatch(ExpectedRows, begin
        case query_view(Worker, SpaceId, ViewName, Options) of
            {ok, #{<<"rows">> := Rows}} -> Rows;
            Error -> Error
        end
    end, Attempts)).


%%%===================================================================
%%% API
%%%===================================================================
all() -> [
    create_and_delete_simple_view_test,
    query_simple_empty_view_test,
    query_view_using_file_meta,
    query_view_using_times,
    query_view_using_custom_metadata_when_xattr_is_not_set,
    query_view_using_custom_metadata,
    query_view_using_file_popularity,
    query_view_and_emit_ctx,
    wrong_map_function,
    emitting_null_key_in_map_function_should_return_empty_result,
    spatial_function_returning_null_in_key_should_return_empty_result,
    spatial_function_returning_null_in_array_key_should_return_empty_result,
    spatial_function_returning_null_in_range_key_should_return_empty_result,
    spatial_function_returning_integer_key_should_return_error,
    spatial_function_returning_string_key_should_return_error
].

%%%===================================================================
%%% Test functions
%%%===================================================================

create_and_delete_simple_view_test(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            return [id, id];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertMatch({ok, [ViewName]}, list_views(Worker, SpaceId)),
    delete_view(Worker, SpaceId, ViewName),
    ?assertMatch({ok, []}, list_views(Worker, SpaceId)).

query_simple_empty_view_test(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            return null;
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertQuery([], Worker, SpaceId, ViewName, [{stale, false}]).

query_view_using_file_meta(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SpaceUuid = fslogic_file_id:spaceid_to_space_dir_uuid(SpaceId),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    TrashGuid = fslogic_file_id:spaceid_to_trash_dir_guid(SpaceId),
    TmpGuid = fslogic_file_id:spaceid_to_tmp_dir_guid(SpaceId),
    {ok, CdmiId} = file_id:guid_to_objectid(SpaceGuid),
    {ok, TrashObjectId} = file_id:guid_to_objectid(TrashGuid),
    {ok, TmpObjectId} = file_id:guid_to_objectid(TmpGuid),
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'file_meta')
                return [id, meta];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    SpaceOwnerId = ?SPACE_OWNER_ID(SpaceId),
    ?assertQuery([
        #{
            <<"id">> := _,
            <<"key">> := TmpObjectId,
            <<"value">> := #{
                <<"name">> := ?TMP_DIR_NAME,
                <<"type">> := <<"DIR">>,
                <<"mode">> := ?DEFAULT_DIR_MODE,
                <<"owner">> := SpaceOwnerId,
                <<"provider_id">> := ProviderId,
                <<"shares">> := [],
                <<"deleted">> := false,
                <<"parent_uuid">> := SpaceUuid
            }
        },
        #{
            <<"id">> := _,
            <<"key">> := TrashObjectId,
            <<"value">> := #{
                <<"name">> := ?TRASH_DIR_NAME,
                <<"type">> := <<"DIR">>,
                <<"mode">> := ?DEFAULT_DIR_MODE,
                <<"owner">> := SpaceOwnerId,
                <<"provider_id">> := ProviderId,
                <<"shares">> := [],
                <<"deleted">> := false,
                <<"parent_uuid">> := SpaceUuid
            }
        },
        #{
            <<"id">> := _,
            <<"key">> := CdmiId,
            <<"value">> := #{
                <<"name">> := SpaceId,
                <<"type">> := <<"DIR">>,
                <<"mode">> := ?DEFAULT_DIR_MODE,
                <<"owner">> := SpaceOwnerId,
                <<"provider_id">> := ProviderId,
                <<"shares">> := [],
                <<"deleted">> := false,
                <<"parent_uuid">> := <<"">>
            }
        }
    ],Worker, SpaceId, ViewName, [{stale, false}]).

query_view_using_times(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    {ok, CdmiId} = file_id:guid_to_objectid(SpaceGuid),
    TmpGuid = fslogic_file_id:spaceid_to_tmp_dir_guid(SpaceId),
    {ok, TmpObjectId} = file_id:guid_to_objectid(TmpGuid),
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'times')
                return [id, meta];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertQuery([
        #{
            <<"id">> := _,
            <<"key">> := TmpObjectId,
            <<"value">> := #{
                <<"atime">> := _,
                <<"mtime">> := _,
                <<"ctime">> := _

            }
        },
        #{
            <<"id">> := _,
            <<"key">> := CdmiId,
            <<"value">> := #{
                <<"atime">> := _,
                <<"mtime">> := _,
                <<"ctime">> := _

            }
        }
    ],Worker, SpaceId, ViewName, [{stale, false}]).

query_view_using_custom_metadata_when_xattr_is_not_set(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SimpleMapFunction = <<"
        function(id, file_meta, times, custom_metadata, file_popularity, ctx) {
            if(type == 'custom_metadata')
                return [id, meta];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertQuery([],Worker, SpaceId, ViewName, [{stale, false}]).

query_view_using_custom_metadata(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SessionId = oct_background:get_user_session_id(user1, krakow),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    {ok, CdmiId} = file_id:guid_to_objectid(SpaceGuid),

    XattrName = <<"xattr_name">>,
    XattrValue = <<"xattr_value">>,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    lfm_proxy:set_xattr(Worker, SessionId, ?FILE_REF(SpaceGuid), Xattr),

    XattrName2 = <<"xattr_name2">>,
    XattrValue2 = <<"xattr_value2">>,
    Xattr2 = #xattr{name = XattrName2, value = XattrValue2},
    lfm_proxy:set_xattr(Worker, SessionId, ?FILE_REF(SpaceGuid), Xattr2),

    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'custom_metadata')
                return [id, meta];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertQuery([#{
        <<"id">> := _,
        <<"key">> := CdmiId,
        <<"value">> := #{
            XattrName := XattrValue,
            XattrName2 := XattrValue2
        }
    }],Worker, SpaceId, ViewName, [{stale, false}]).

query_view_using_file_popularity(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    SpaceName = oct_background:get_space_name(space_krk),
    ViewName = ?view_name,
    SessionId = oct_background:get_user_session_id(user1, krakow),
    ProviderId = oct_background:get_provider_id(krakow),
    TestData = <<"test_data">>,
    TestDataSize = byte_size(TestData),

    ok = rpc:call(Worker, file_popularity_api, enable, [SpaceId]),
    FilePath = ?TEST_FILE(SpaceName),
    {ok, Guid} = lfm_proxy:create(Worker, SessionId, FilePath),
    Uuid = file_id:guid_to_uuid(Guid),
    {ok, H} = lfm_proxy:open(Worker, SessionId, ?FILE_REF(Guid), write),
    lfm_proxy:write(Worker, H, 0, TestData),
    lfm_proxy:close(Worker, H),

    {ok, CdmiId} = file_id:guid_to_objectid(Guid),

    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'file_popularity')
                return [id, meta];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SimpleMapFunction, undefined, [], false, [ProviderId]),

    ?assertQuery([#{
        <<"id">> := _,
        <<"key">> := CdmiId,
        <<"value">> := #{
            <<"file_uuid">> := Uuid,
            <<"space_id">> := SpaceId,
            <<"dy_hist">> :=[1 | _],
            <<"hr_hist">> := [1 | _],
            <<"mth_hist">> := [1 | _],
            <<"dy_mov_avg">> := 1/30,
            <<"hr_mov_avg">> := 1/24,
            <<"mth_mov_avg">> := 1/12,
            <<"last_open">> := _,
            <<"open_count">> := 1,
            <<"size">> := TestDataSize
    }}],Worker, SpaceId, ViewName, [{stale, false}]).

query_view_and_emit_ctx(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    {ok, CdmiId} = file_id:guid_to_objectid(SpaceGuid),
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'file_meta')
                return [id, ctx];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertQuery([#{
        <<"id">> := _,
        <<"key">> := CdmiId,
        <<"value">> := #{
            <<"providerId">> := ProviderId

        }}],Worker, SpaceId, ViewName, [{stale, false}, {key, CdmiId}]).

wrong_map_function(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    {ok, CdmiId} = file_id:guid_to_objectid(SpaceGuid),
    SimpleMapFunction = <<"
        function(_, _, _, _) {
            throw 'Test error';
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertQuery([], Worker, SpaceId, ViewName, [{stale, false}, {key, CdmiId}]).

emitting_null_key_in_map_function_should_return_empty_result(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SpaceGuid = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
    {ok, CdmiId} = file_id:guid_to_objectid(SpaceGuid),
    SimpleMapFunction = <<"
        function(_, _, _, _) {
            return [null, null];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SimpleMapFunction, undefined, [], false, [ProviderId]),
    ?assertQuery([], Worker, SpaceId, ViewName, [{stale, false}, {key, CdmiId}]).

spatial_function_returning_null_in_key_should_return_empty_result(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SpatialFunction = <<"
        function(_, _, _, _) {
            return [null, null];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SpatialFunction, undefined, [], true, [ProviderId]),
    ?assertQuery([], Worker, SpaceId, ViewName, [{stale, false}, {spatial, true}]).

spatial_function_returning_null_in_array_key_should_return_empty_result(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SpatialFunction = <<"
        function(_, _, _, _) {
            return [[null, 1], null];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SpatialFunction, undefined, [], true, [ProviderId]),
    ?assertQuery([], Worker, SpaceId, ViewName, [{stale, false}, {spatial, true}]).

spatial_function_returning_null_in_range_key_should_return_empty_result(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SpatialFunction = <<"
        function(_, _, _, _) {
            return [[[null, 1], [5, 7]], null];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SpatialFunction, undefined, [], true, [ProviderId]),
    ?assertQuery([], Worker, SpaceId, ViewName, [{stale, false}, {spatial, true}]).

spatial_function_returning_integer_key_should_return_error(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SpatialFunction = <<"
        function(_, _, _, _) {
            return [1, null];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SpatialFunction, undefined, [], true, [ProviderId]),
    ?assertQuery(?ERROR_VIEW_QUERY_FAILED(_, _),
        Worker, SpaceId, ViewName, [{stale, false}, {spatial, true}]).

spatial_function_returning_string_key_should_return_error(_Config) ->
    Worker = oct_background:get_random_provider_node(krakow),
    SpaceId = oct_background:get_space_id(space_krk),
    ViewName = ?view_name,
    ProviderId = oct_background:get_provider_id(krakow),
    SpatialFunction = <<"
        function(_, _, _, _) {
            return [[\"string\"], null];
        }
    ">>,
    create_view(Worker, SpaceId, ViewName, SpatialFunction, undefined, [], true, [ProviderId]),
    ?assertQuery(?ERROR_VIEW_QUERY_FAILED(_, _),
        Worker, SpaceId, ViewName, [{stale, false}, {spatial, true}]).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "1op"
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 10}),
    lfm_proxy:init(Config),
    Config.


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

create_view(Worker, SpaceId, ViewName, MapFunction, ReduceFunction, Options, Spatial, ProviderIds) ->
    ok = rpc:call(Worker, index, save, [SpaceId, ViewName, MapFunction,
        ReduceFunction, Options, Spatial, ProviderIds]).

delete_view(Worker, SpaceId, ViewName) ->
    ok = rpc:call(Worker, index, delete, [SpaceId, ViewName]).

query_view(Worker, SpaceId, ViewName, Options) ->
    rpc:call(Worker, index, query, [SpaceId, ViewName, Options]).

list_views(Worker, SpaceId) ->
    rpc:call(Worker, index, list, [SpaceId]).