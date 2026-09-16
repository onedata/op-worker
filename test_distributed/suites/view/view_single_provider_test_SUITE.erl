%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Tests of how a view is evaluated - what a map function is handed for each
%%% document type flowing through the space, and how a function that throws or
%%% emits a malformed key is treated. None of that depends on more than one
%%% provider holding the definition, so the suite runs on a single one and
%%% drives the internal view API directly, bypassing REST.
%%%
%%% The API through which views are actually managed - its parameter
%%% sanitization and privilege checks, and the way a definition is propagated to
%%% and told apart on the providers supporting the space - is covered by
%%% view_multi_provider_test_SUITE.
%%%
%%% All the cases share one space and none of them cleans up after itself. The
%%% ones asserting the exact set of documents of a given type therefore need a
%%% space no earlier run has left a file or an xattr in - the suite is written
%%% for the fresh deployment CI gives it and will not survive a rerun against a
%%% reused one. The views themselves never collide, as each is named after the
%%% case that creates it.
%%% @end
%%%-------------------------------------------------------------------
-module(view_single_provider_test_SUITE).
-author("Jakub Kudzia").

-include("modules/dataset/archivisation_tree.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("test_rpc.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").

%% export for CT
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    created_view_is_listed_until_deleted_test/1,
    map_function_emitting_nothing_yields_empty_view_test/1,
    map_function_receives_file_meta_documents_test/1,
    map_function_receives_times_documents_test/1,
    map_function_receives_no_custom_metadata_without_xattrs_test/1,
    map_function_receives_custom_metadata_documents_test/1,
    map_function_receives_file_popularity_documents_test/1,
    emitted_ctx_carries_provider_id_test/1,
    throwing_map_function_yields_empty_view_test/1,
    map_function_emitting_null_key_yields_empty_view_test/1,
    spatial_function_emitting_null_key_yields_empty_view_test/1,
    spatial_function_emitting_null_in_array_key_yields_empty_view_test/1,
    spatial_function_emitting_null_in_range_key_yields_empty_view_test/1,
    spatial_function_emitting_integer_key_fails_the_query_test/1,
    spatial_function_emitting_string_key_fails_the_query_test/1
]).

all() -> [
    created_view_is_listed_until_deleted_test,
    map_function_emitting_nothing_yields_empty_view_test,
    map_function_receives_file_meta_documents_test,
    map_function_receives_times_documents_test,
    map_function_receives_no_custom_metadata_without_xattrs_test,
    map_function_receives_custom_metadata_documents_test,
    map_function_receives_file_popularity_documents_test,
    emitted_ctx_carries_provider_id_test,
    throwing_map_function_yields_empty_view_test,
    map_function_emitting_null_key_yields_empty_view_test,
    spatial_function_emitting_null_key_yields_empty_view_test,
    spatial_function_emitting_null_in_array_key_yields_empty_view_test,
    spatial_function_emitting_null_in_range_key_yields_empty_view_test,
    spatial_function_emitting_integer_key_fails_the_query_test,
    spatial_function_emitting_string_key_fails_the_query_test
].


%% macros
-define(TEST_FILE(SpaceName), begin
    FunctionNameBin = str_utils:to_binary(?FUNCTION),
    RandIntBin = str_utils:to_binary(rand:uniform(1000000000)),
    FileName = <<FunctionNameBin/binary, "_", RandIntBin/binary>>,
    filename:join(["/", SpaceName, FileName])
end).

-define(view_name, begin <<"view_", (str_utils:to_binary(?FUNCTION))/binary>> end).
-define(ATTEMPTS, 15).

-define(assertQuery(ExpectedRows, ViewName, Options),
    ?assertQuery(ExpectedRows, ViewName, Options, ?ATTEMPTS)).

-define(assertQuery(ExpectedRows, ViewName, Options, Attempts),
    ?assertMatch(ExpectedRows, begin
        case query_view(ViewName, Options) of
            {ok, #{<<"rows">> := Rows}} ->
                lists:sort(fun(Row1, Row2) ->
                    Id1 = maps:get(<<"id">>, Row1),
                    Id2 = maps:get(<<"id">>, Row2),
                    Id1 > Id2
                end, Rows);
            Error ->
                Error
        end
    end, Attempts)).

-define(PROVIDER_SELECTOR, krakow).
-define(rpc(Expr), ?rpc(?PROVIDER_SELECTOR, Expr)).


%%%===================================================================
%%% Test cases
%%%===================================================================


created_view_is_listed_until_deleted_test(_Config) ->
    ViewName = ?view_name,
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            return [id, id];
        }
    ">>,

    create_view(ViewName, SimpleMapFunction, undefined, [], false),
    ?assertMatch({ok, [ViewName]}, list_views()),

    delete_view(ViewName),
    ?assertMatch({ok, []}, list_views()).


map_function_emitting_nothing_yields_empty_view_test(_Config) ->
    ViewName = ?view_name,
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            return null;
        }
    ">>,

    create_view(ViewName, SimpleMapFunction, undefined, [], false),
    ?assertQuery([], ViewName, [{stale, false}]).


map_function_receives_file_meta_documents_test(_Config) ->
    ProviderId = oct_background:get_provider_id(krakow),

    SpaceId = oct_background:get_space_id(space_krk),
    SpaceDirUuid = space_dir:uuid(SpaceId),
    SpaceDirGuid = space_dir:guid(SpaceId),
    SpaceOwnerId = ?SPACE_OWNER_ID(SpaceId),
    {ok, SpaceObjectId} = file_id:guid_to_objectid(SpaceDirGuid),

    TmpDirGuid = tmp_dir:guid(SpaceId),
    TmpDirUuid = tmp_dir:uuid(SpaceId),
    {ok, TmpObjectId} = file_id:guid_to_objectid(TmpDirGuid),

    OpenedDeletedGuid = file_id:pack_guid(?OPENED_DELETED_FILES_DIR_UUID(SpaceId), SpaceId),
    {ok, OpenedDeletedObjectId} = file_id:guid_to_objectid(OpenedDeletedGuid),

    TrashGuid = trash_dir:guid(SpaceId),
    {ok, TrashObjectId} = file_id:guid_to_objectid(TrashGuid),
    
    ArchivesRootDirName = ?SPACE_ARCHIVES_DIR_NAME,
    {ok, SpaceArchivesObjectId} = file_id:guid_to_objectid(space_archives_dir:guid(SpaceId)),

    ViewName = ?view_name,
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'file_meta')
                return [id, meta];
        }
    ">>,

    create_view(ViewName, SimpleMapFunction, undefined, [], false),
    ?assertQuery([
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
                <<"parent_uuid">> := SpaceDirUuid
            }
        },
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
                <<"parent_uuid">> := SpaceDirUuid
            }
        },
        #{
            <<"id">> := _,
            <<"key">> := SpaceObjectId,
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
        },
        #{
            <<"id">> := _,
            <<"key">> := SpaceArchivesObjectId,
            <<"value">> := #{
                <<"name">> := ArchivesRootDirName,
                <<"type">> := <<"DIR">>,
                <<"mode">> := ?SPACE_ARCHIVES_DIR_PERMS,
                <<"owner">> := SpaceOwnerId,
                <<"provider_id">> := ProviderId,
                <<"shares">> := [],
                <<"deleted">> := false,
                <<"parent_uuid">> := SpaceDirUuid
            }
        },
        #{
            <<"id">> := _,
            <<"key">> := OpenedDeletedObjectId,
            <<"value">> := #{
                <<"name">> := ?OPENED_DELETED_FILES_DIR_DIR_NAME,
                <<"type">> := <<"DIR">>,
                <<"mode">> := ?DEFAULT_DIR_MODE,
                <<"owner">> := SpaceOwnerId,
                <<"provider_id">> := ProviderId,
                <<"shares">> := [],
                <<"deleted">> := false,
                <<"parent_uuid">> := TmpDirUuid
            }
        }
    ], ViewName, [{stale, false}]).


map_function_receives_times_documents_test(_Config) ->
    SpaceId = oct_background:get_space_id(space_krk),
    SpaceDirGuid = space_dir:guid(SpaceId),
    {ok, SpaceObjectId} = file_id:guid_to_objectid(SpaceDirGuid),

    TrashGuid = trash_dir:guid(SpaceId),
    {ok, TrashObjectId} = file_id:guid_to_objectid(TrashGuid),
    
    TmpDirGuid = tmp_dir:guid(SpaceId),
    {ok, TmpObjectId} = file_id:guid_to_objectid(TmpDirGuid),

    {ok, SpaceArchivesObjectId} = file_id:guid_to_objectid(space_archives_dir:guid(SpaceId)),
    
    OpenedDeletedGuid = file_id:pack_guid(?OPENED_DELETED_FILES_DIR_UUID(SpaceId), SpaceId),
    {ok, OpenedDeletedObjectId} = file_id:guid_to_objectid(OpenedDeletedGuid),

    ViewName = ?view_name,
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'times')
                return [id, meta];
        }
    ">>,

    create_view(ViewName, SimpleMapFunction, undefined, [], false),
    ?assertQuery([
        #{
            <<"id">> := _,
            <<"key">> := TrashObjectId,
            <<"value">> := #{
                <<"atime">> := _,
                <<"mtime">> := _,
                <<"ctime">> := _

            }
        },
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
            <<"key">> := SpaceObjectId,
            <<"value">> := #{
                <<"atime">> := _,
                <<"mtime">> := _,
                <<"ctime">> := _

            }
        },
        #{
            <<"id">> := _,
            <<"key">> := SpaceArchivesObjectId,
            <<"value">> := #{
                <<"atime">> := _,
                <<"mtime">> := _,
                <<"ctime">> := _

            }
        },
        #{
            <<"id">> := _,
            <<"key">> := OpenedDeletedObjectId,
            <<"value">> := #{
                <<"atime">> := _,
                <<"mtime">> := _,
                <<"ctime">> := _

            }
        }
    ], ViewName, [{stale, false}]).


map_function_receives_no_custom_metadata_without_xattrs_test(_Config) ->
    ViewName = ?view_name,
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'custom_metadata')
                return [id, meta];
        }
    ">>,

    create_view(ViewName, SimpleMapFunction, undefined, [], false),
    ?assertQuery([], ViewName, [{stale, false}]).


map_function_receives_custom_metadata_documents_test(_Config) ->
    SpaceId = oct_background:get_space_id(space_krk),
    SessionId = oct_background:get_user_session_id(user1, krakow),
    SpaceDirGuid = space_dir:guid(SpaceId),
    {ok, SpaceObjectId} = file_id:guid_to_objectid(SpaceDirGuid),

    Worker = oct_background:get_random_provider_node(krakow),

    XattrName = <<"xattr_name">>,
    XattrValue = <<"xattr_value">>,
    Xattr = #xattr{name = XattrName, value = XattrValue},
    lfm_proxy:set_xattr(Worker, SessionId, ?FILE_REF(SpaceDirGuid), Xattr),

    XattrName2 = <<"xattr_name2">>,
    XattrValue2 = <<"xattr_value2">>,
    Xattr2 = #xattr{name = XattrName2, value = XattrValue2},
    lfm_proxy:set_xattr(Worker, SessionId, ?FILE_REF(SpaceDirGuid), Xattr2),

    ViewName = ?view_name,
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'custom_metadata')
                return [id, meta];
        }
    ">>,

    create_view(ViewName, SimpleMapFunction, undefined, [], false),
    ?assertQuery([#{
        <<"id">> := _,
        <<"key">> := SpaceObjectId,
        <<"value">> := #{
            XattrName := XattrValue,
            XattrName2 := XattrValue2
        }
    }], ViewName, [{stale, false}]).


map_function_receives_file_popularity_documents_test(_Config) ->
    SpaceId = oct_background:get_space_id(space_krk),
    SpaceName = oct_background:get_space_name(space_krk),
    SessionId = oct_background:get_user_session_id(user1, krakow),

    TestData = <<"test_data">>,
    TestDataSize = byte_size(TestData),

    Worker = oct_background:get_random_provider_node(krakow),

    ok = ?rpc(file_popularity_api:enable(SpaceId)),
    FilePath = ?TEST_FILE(SpaceName),
    {ok, FileGuid} = lfm_proxy:create(Worker, SessionId, FilePath),
    {ok, FileHandle} = lfm_proxy:open(Worker, SessionId, ?FILE_REF(FileGuid), write),
    lfm_proxy:write(Worker, FileHandle, 0, TestData),
    lfm_proxy:close(Worker, FileHandle),
    FileUuid = file_id:guid_to_uuid(FileGuid),

    {ok, SpaceObjectId} = file_id:guid_to_objectid(FileGuid),

    ViewName = ?view_name,
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'file_popularity')
                return [id, meta];
        }
    ">>,

    create_view(ViewName, SimpleMapFunction, undefined, [], false),
    ?assertQuery([#{
        <<"id">> := _,
        <<"key">> := SpaceObjectId,
        <<"value">> := #{
            <<"file_uuid">> := FileUuid,
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
        }
    }], ViewName, [{stale, false}]).


emitted_ctx_carries_provider_id_test(_Config) ->
    ProviderId = oct_background:get_provider_id(krakow),

    SpaceId = oct_background:get_space_id(space_krk),
    SpaceDirGuid = space_dir:guid(SpaceId),
    {ok, SpaceObjectId} = file_id:guid_to_objectid(SpaceDirGuid),

    ViewName = ?view_name,
    SimpleMapFunction = <<"
        function(id, type, meta, ctx) {
            if(type == 'file_meta')
                return [id, ctx];
        }
    ">>,

    create_view(ViewName, SimpleMapFunction, undefined, [], false),
    ?assertQuery([#{
        <<"id">> := _,
        <<"key">> := SpaceObjectId,
        <<"value">> := #{
            <<"providerId">> := ProviderId
        }
    }], ViewName, [{stale, false}, {key, SpaceObjectId}]).


throwing_map_function_yields_empty_view_test(_Config) ->
    SpaceId = oct_background:get_space_id(space_krk),
    SpaceDirGuid = space_dir:guid(SpaceId),
    {ok, SpaceObjectId} = file_id:guid_to_objectid(SpaceDirGuid),

    ViewName = ?view_name,
    SimpleMapFunction = <<"
        function(_, _, _, _) {
            throw 'Test error';
        }
    ">>,

    create_view(ViewName, SimpleMapFunction, undefined, [], false),
    ?assertQuery([], ViewName, [{stale, false}, {key, SpaceObjectId}]).


map_function_emitting_null_key_yields_empty_view_test(_Config) ->
    SpaceId = oct_background:get_space_id(space_krk),
    SpaceDirGuid = space_dir:guid(SpaceId),
    {ok, SpaceObjectId} = file_id:guid_to_objectid(SpaceDirGuid),

    ViewName = ?view_name,
    SimpleMapFunction = <<"
        function(_, _, _, _) {
            return [null, null];
        }
    ">>,

    create_view(ViewName, SimpleMapFunction, undefined, [], false),
    ?assertQuery([], ViewName, [{stale, false}, {key, SpaceObjectId}]).


spatial_function_emitting_null_key_yields_empty_view_test(_Config) ->
    ViewName = ?view_name,
    SpatialFunction = <<"
        function(_, _, _, _) {
            return [null, null];
        }
    ">>,

    create_view(ViewName, SpatialFunction, undefined, [], true),
    ?assertQuery([], ViewName, [{stale, false}, {spatial, true}]).


spatial_function_emitting_null_in_array_key_yields_empty_view_test(_Config) ->
    ViewName = ?view_name,
    SpatialFunction = <<"
        function(_, _, _, _) {
            return [[null, 1], null];
        }
    ">>,

    create_view(ViewName, SpatialFunction, undefined, [], true),
    ?assertQuery([], ViewName, [{stale, false}, {spatial, true}]).


spatial_function_emitting_null_in_range_key_yields_empty_view_test(_Config) ->
    ViewName = ?view_name,
    SpatialFunction = <<"
        function(_, _, _, _) {
            return [[[null, 1], [5, 7]], null];
        }
    ">>,

    create_view(ViewName, SpatialFunction, undefined, [], true),
    ?assertQuery([], ViewName, [{stale, false}, {spatial, true}]).


spatial_function_emitting_integer_key_fails_the_query_test(_Config) ->
    ViewName = ?view_name,
    SpatialFunction = <<"
        function(_, _, _, _) {
            return [1, null];
        }
    ">>,

    create_view(ViewName, SpatialFunction, undefined, [], true),
    ?assertQuery(
        ?ERR_VIEW_QUERY_FAILED(_, _),
        ViewName, [{stale, false}, {spatial, true}]
    ).


spatial_function_emitting_string_key_fails_the_query_test(_Config) ->
    ViewName = ?view_name,
    SpatialFunction = <<"
        function(_, _, _, _) {
            return [[\"string\"], null];
        }
    ">>,

    create_view(ViewName, SpatialFunction, undefined, [], true),
    ?assertQuery(
        ?ERR_VIEW_QUERY_FAILED(_, _),
        ViewName,  [{stale, false}, {spatial, true}]
    ).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    opt:init_per_suite(Config, #onenv_test_config{
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


create_view(ViewName, MapFunction, ReduceFunction, Options, Spatial) ->
    SpaceId = oct_background:get_space_id(space_krk),
    ProviderId = oct_background:get_provider_id(krakow),
    ok = ?rpc(index:save(
        SpaceId, ViewName, MapFunction, ReduceFunction,
        Options, Spatial, [ProviderId]
    )).


delete_view(ViewName) ->
    SpaceId = oct_background:get_space_id(space_krk),
    ok = ?rpc(index:delete(SpaceId, ViewName)).


query_view(ViewName, Options) ->
    SpaceId = oct_background:get_space_id(space_krk),
    ?rpc(index:query(SpaceId, ViewName, Options)).


list_views() ->
    SpaceId = oct_background:get_space_id(space_krk),
    ?rpc(index:list(SpaceId)).