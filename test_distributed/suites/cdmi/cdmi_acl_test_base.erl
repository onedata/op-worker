%%%-------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% CDMI acl tests
%%% @end
%%%-------------------------------------------------------------------
-module(cdmi_acl_test_base).
-author("Katarzyna Such").

-include("cdmi_test.hrl").
-include("http/cdmi.hrl").
-include("modules/fslogic/acl.hrl").
-include("onenv_test_utils.hrl").

-include_lib("ctool/include/test/test_utils.hrl").

%% Tests
-export([
    write_acl_metadata_test/1,
    acl_read_file_test/1,
    acl_write_file_test/1,
    acl_delete_file_test/1,
    acl_read_write_dir_test/1
]).


%%%===================================================================
%%% Test functions
%%%===================================================================

write_acl_metadata_test(Config) ->
    UserName = oct_background:get_user_fullname(user2),
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    UserId = oct_background:get_user_id(user2),
    DirPath = filename:join([RootPath, "metadataTestDir"]) ++ "/",

    RequestHeaders1 = [?CDMI_CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody1 = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value">>}},
    RawRequestBody1 = json_utils:encode(RequestBody1),
    {ok, ?HTTP_201_CREATED, _Headers1, _Response1} = cdmi_test_utils:do_request(
        ?WORKERS(Config), DirPath, put, RequestHeaders1, RawRequestBody1
    ),

    Ace1 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?read_all_object_mask
    }, cdmi),
    Ace2 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?write_all_object_mask
    }, cdmi),
    Ace2Full = #{
        <<"acetype">> => ?allow,
        <<"identifier">> => <<UserName/binary, "#", UserId/binary>>,
        <<"aceflags">> => ?no_flags,
        <<"acemask">> => <<
            ?write_object/binary, ",",
            ?write_metadata/binary, ",",
            ?write_attributes/binary, ",",
            ?delete/binary, ",",
            ?write_acl/binary
        >>
    },
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file.txt">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),

    FilePath = filename:join([RootPath, "acl_test_file.txt"]),
    RequestBody2 = #{<<"metadata">> => #{<<"cdmi_acl">> => [Ace1, Ace2Full]}},
    RawRequestBody2 = json_utils:encode(RequestBody2),
    RequestHeaders2 = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],

    {ok, ?HTTP_204_NO_CONTENT, _Headers2, _Response2} = cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath ++ "?metadata:cdmi_acl", put, RequestHeaders2, RawRequestBody2
    ),

    GetFileMetadataFun = fun() ->
        {ok, ?HTTP_200_OK, _Headers3, Response3} =
            cdmi_test_utils:do_request(
                ?WORKERS(Config), FilePath ++ "?metadata", get, RequestHeaders2, []
        ),
        json_utils:decode(Response3)
    end,

    ?assertEqual(6, maps:size(maps:get(<<"metadata">>,  GetFileMetadataFun())), ?ATTEMPTS),
    ExpResponse = GetFileMetadataFun(),
    ?assertEqual(1, maps:size(ExpResponse)),

    Metadata = maps:get(<<"metadata">>, ExpResponse),
    ?assertMatch(#{<<"cdmi_acl">> := [Ace1, Ace2]}, Metadata),

    {ok, _Code4, _Headers4, Response4} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath, get, [cdmi_test_utils:user_2_token_header()], []
    )),
    ?assertEqual(<<"data">>, Response4),

    %%-- create forbidden by acl ---
    Ace3 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?write_metadata_mask
    }, cdmi),
    RequestBody3 = #{<<"metadata">> => #{<<"cdmi_acl">> => [Ace3]}},
    RawRequestBody3 = json_utils:encode(RequestBody3),
    RequestHeaders3 = [cdmi_test_utils:user_2_token_header(), ?CDMI_CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER],

    ?assertMatch(
        {ok, ?HTTP_204_NO_CONTENT, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS(Config), filename:join([DirPath, "?metadata:cdmi_acl"]), put, RequestHeaders3, RawRequestBody3
        ),
        ?ATTEMPTS
    ),

    Ace4 = ace:to_json(#access_control_entity{
        acetype = ?deny_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?write_object_mask
    }, cdmi),
    RequestBody4 = #{<<"metadata">> => #{<<"cdmi_acl">> => [Ace4]}},
    RawRequestBody4 = json_utils:encode(RequestBody4),

    DirPath2 = filename:join([RootPath, "metadataTestDir2"]) ++ "/",
    {ok, ?HTTP_201_CREATED, _, _} = cdmi_test_utils:do_request(
        ?WORKERS(Config), DirPath2, put, RequestHeaders1, RawRequestBody1
    ),
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        ?WORKERS(Config), filename:join([DirPath2, "?metadata:cdmi_acl"]), put, RequestHeaders3, RawRequestBody4
    ), ?ATTEMPTS),

    Fun2 = fun() ->
        {ok, Code7, _Headers7, Response7} = cdmi_test_utils:do_request(
            ?WORKERS(Config), filename:join(DirPath, "some_file"), put, [cdmi_test_utils:user_2_token_header()], []
        ),
        {Code7, json_utils:decode(Response7)}
    end,
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError, Fun2(), ?ATTEMPTS).


acl_read_file_test(Config) ->
    {RootPath, _, _, MetadataAclWrite, _, MetadataAclReadWriteFull} = acl_file_test_base(Config),
    FilePath = filename:join(RootPath, "acl_test_file1"),

    % create test file with dummy data
    ?assert(not cdmi_test_utils:object_exists(FilePath, Config)),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file1">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),

    EaccesError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),

    RequestHeaders = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath, put, RequestHeaders, MetadataAclWrite
    ),
    {ok, Code1, _, Response1} = ?assertMatch(
        {ok, ?HTTP_400_BAD_REQUEST, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), FilePath, get, RequestHeaders, []),
        ?ATTEMPTS
    ),
    ?assertMatch(EaccesError, {Code1, json_utils:decode(Response1)}),

    {ok, Code2, _, Response2} = ?assertMatch(
        {ok, ?HTTP_400_BAD_REQUEST, _, _},
        cdmi_test_utils:do_request(?WORKERS(Config), FilePath, get, [cdmi_test_utils:user_2_token_header()], []),
        ?ATTEMPTS
    ),
    ?assertMatch(EaccesError, {Code2, json_utils:decode(Response2)}),
    ?assertEqual({error, ?EACCES}, cdmi_test_utils:open_file(
        Config#cdmi_test_config.p2_selector, FilePath, read), ?ATTEMPTS
    ),

    % set acl to 'read&write' and test cdmi/non-cdmi get request (should succeed)
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
            ?WORKERS(Config), FilePath, put, RequestHeaders, MetadataAclReadWriteFull
    ), ?ATTEMPTS),
    ?assertMatch({ok, ?HTTP_200_OK, _, _}, cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath, get, RequestHeaders, []
    ), ?ATTEMPTS),
    ?assertMatch({ok, ?HTTP_200_OK, _, _}, cdmi_test_utils:do_request(
        ?WORKERS(Config), FilePath, get, [cdmi_test_utils:user_2_token_header()], []
    ), ?ATTEMPTS).


acl_write_file_test(Config) ->
    {RootPath, MetadataAclReadFull, _, _, MetadataAclReadWrite, _} = acl_file_test_base(Config),
    [WorkerP1, _WorkerP2] = ?WORKERS(Config),
    FilePath = filename:join([RootPath, "acl_test_file2"]),
    % create test file with dummy data
    ?assert(not cdmi_test_utils:object_exists(FilePath, Config)),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file2">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),
    RequestHeaders = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    EaccesError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),

    % set acl to 'read&write' and test cdmi/non-cdmi put request (should succeed)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, RequestHeaders, MetadataAclReadWrite
    ),
    RequestBody = json_utils:encode(#{<<"value">> => <<"new_data">>}),
    ?assertMatch( {ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, RequestHeaders, RequestBody
    ), ?ATTEMPTS),
    ?assertEqual(<<"new_data">>, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    ?assertMatch({ok, _}, cdmi_test_utils:write_to_file(FilePath, <<"1">>, 8, Config), ?ATTEMPTS),
    ?assertEqual(<<"new_data1">>, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, [cdmi_test_utils:user_2_token_header()], <<"new_data2">>
    ), ?ATTEMPTS),

    ?assertEqual(<<"new_data2">>, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, RequestHeaders, MetadataAclReadFull
    ), ?ATTEMPTS),

    RequestBody6 = json_utils:encode(#{<<"value">> => <<"new_data3">>}),
    {ok, Code3, _, Response3} = ?assertMatch(
        {ok, ?HTTP_400_BAD_REQUEST, _, _},
        cdmi_test_utils:do_request(WorkerP1, FilePath, put, RequestHeaders, RequestBody6),
        ?ATTEMPTS
    ),
    ?assertMatch(EaccesError, {Code3, json_utils:decode(Response3)}),

    {ok, Code4, _, Response4} = cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, [cdmi_test_utils:user_2_token_header()], <<"new_data4">>
    ),
    ?assertMatch(EaccesError, {Code4, json_utils:decode(Response4)}),
    ?assertEqual(<<"new_data2">>, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),
    ?assertEqual(
        {error, ?EACCES},
        cdmi_test_utils:open_file(Config#cdmi_test_config.p2_selector, FilePath, write),
        ?ATTEMPTS
    ),
    ?assertEqual(<<"new_data2">>, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS).


acl_delete_file_test(Config) ->
    {RootPath, MetadataAclReadFull, MetadataAclDelete, _, _, _} = acl_file_test_base(Config),
    [WorkerP1, _WorkerP2] = ?WORKERS(Config),
    FilePath = filename:join([RootPath, "acl_test_file3"]),
    % create test file with dummy data
    ?assert(not cdmi_test_utils:object_exists(FilePath, Config)),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file3">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),
    RequestHeaders = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],

    % set acl to 'read'
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, RequestHeaders, MetadataAclReadFull
    ),

    % fail to delete file because of eacces error
    {ok, ?HTTP_400_BAD_REQUEST, _, _} = cdmi_test_utils:do_request(
        WorkerP1, FilePath, delete, [cdmi_test_utils:user_2_token_header()], []
    ),

    % set acl to 'delete'
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        WorkerP1, FilePath, put, RequestHeaders, MetadataAclDelete
    ),

    % delete file
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        WorkerP1, FilePath, delete, [cdmi_test_utils:user_2_token_header()], []
    ),
    ?assert(not cdmi_test_utils:object_exists(FilePath, Config), ?ATTEMPTS).


acl_read_write_dir_test(Config) ->
    [WorkerP1, WorkerP2] = ?WORKERS(Config),
    RootPath = cdmi_test_utils:get_tests_root_path(Config),

    UserId = oct_background:get_user_id(user2),
    UserName = oct_background:get_user_fullname(user2),
    DirRead = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?read_all_container_mask bor ?traverse_container_mask
    }, cdmi),
    DirWrite = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?write_all_container_mask bor ?traverse_container_mask
    }, cdmi),
    WriteAcl = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?write_acl_mask
    }, cdmi),
    DirMetadataAclReadWrite = json_utils:encode(#{
        <<"metadata">> => #{<<"cdmi_acl">> => [DirWrite, DirRead]}
    }),
    DirMetadataAclRead = json_utils:encode(#{
        <<"metadata">> => #{<<"cdmi_acl">> => [DirRead, WriteAcl]}
    }),
    DirMetadataAclWrite = json_utils:encode(#{
        <<"metadata">> => #{<<"cdmi_acl">> => [DirWrite]}
    }),
    Dirname = filename:join(RootPath, "acl_test_dir1") ++ "/",
    EaccesError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),

    %%--- read write dir test ------
    ?assert(not cdmi_test_utils:object_exists(Dirname, Config)),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = <<"acl_test_dir1">>,
            children = [
                #file_spec{name = <<"3">>}
        ]},
        Config#cdmi_test_config.p1_selector
    ),
    File1 = filename:join(Dirname, "1"),
    File2 = filename:join(Dirname, "2"),
    File3 = filename:join(Dirname, "3"),
    File4 = filename:join(Dirname, "4"),

    RequestHeaders1 = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    % set acl to 'read&write' and test cdmi get request (should succeed)
    RequestHeaders2 = [
        cdmi_test_utils:user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CDMI_CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        WorkerP1, Dirname, put, RequestHeaders2, DirMetadataAclReadWrite
    ),
    ?assertMatch({ok, ?HTTP_200_OK, _, _}, cdmi_test_utils:do_request(
        WorkerP2, Dirname, get, RequestHeaders2, []
    ), ?ATTEMPTS),

    % create files in directory (should succeed)
    {ok, ?HTTP_201_CREATED, _, _} = cdmi_test_utils:do_request(
        WorkerP1, File1, put, [cdmi_test_utils:user_2_token_header()], []
    ),
    ?assert(cdmi_test_utils:object_exists(File1, Config), ?ATTEMPTS),

    {ok, ?HTTP_201_CREATED, _, _} = cdmi_test_utils:do_request(
        WorkerP1, File2, put, RequestHeaders1, <<"{\"value\":\"val\"}">>
    ),
    ?assert(cdmi_test_utils:object_exists(File2, Config), ?ATTEMPTS),
    ?assert(cdmi_test_utils:object_exists(File3, Config), ?ATTEMPTS),

    % delete files (should succeed)
    ?assertMatch({ok, ?HTTP_204_NO_CONTENT, _, _}, cdmi_test_utils:do_request(
        WorkerP1, File1, delete, [cdmi_test_utils:user_2_token_header()], []
    ), ?ATTEMPTS),
    ?assert(not cdmi_test_utils:object_exists(File1, Config)),

    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        WorkerP1, File2, delete, [cdmi_test_utils:user_2_token_header()], []
    ),
    ?assert(not cdmi_test_utils:object_exists(File2, Config)),

    % set acl to 'write' and test cdmi get request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        WorkerP1, Dirname, put, RequestHeaders2, DirMetadataAclWrite
    ),
    {ok, Code5, _, Response5} = ?assertMatch(
        {ok, ?HTTP_400_BAD_REQUEST, _, _},
        cdmi_test_utils:do_request(WorkerP2, Dirname, get, RequestHeaders2, []),
        ?ATTEMPTS
    ),
    ?assertMatch(EaccesError, {Code5, json_utils:decode(Response5)}),

    % set acl to 'read' and test cdmi put request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        WorkerP1, Dirname, put, RequestHeaders2, DirMetadataAclRead
    ),
    {ok, ?HTTP_200_OK, _, _} = cdmi_test_utils:do_request(?WORKERS(Config), Dirname, get, RequestHeaders2, []),
    {ok, Code6, _, Response6} = cdmi_test_utils:do_request(
        WorkerP1, Dirname, put, RequestHeaders2,
        json_utils:encode(#{<<"metadata">> => #{<<"my_meta">> => <<"value">>}})
    ),
    ?assertMatch(EaccesError, {Code6, json_utils:decode(Response6)}),

    % create files (should return 403 forbidden)
    {ok, Code7, _, Response7} = cdmi_test_utils:do_request(
        WorkerP1, File1, put, [cdmi_test_utils:user_2_token_header()], []
    ),
    ?assertMatch(EaccesError, {Code7, json_utils:decode(Response7)}),
    ?assert(not cdmi_test_utils:object_exists(File1, Config)),

    {ok, Code8, _, Response8} = cdmi_test_utils:do_request(
        WorkerP1, File2, put, RequestHeaders1, <<"{\"value\":\"val\"}">>
    ),
    ?assertMatch(EaccesError, {Code8, json_utils:decode(Response8)}),
    ?assert(not cdmi_test_utils:object_exists(File2, Config)),
    ?assertEqual({error, ?EACCES}, cdmi_test_utils:create_new_file(File4, Config)),
    ?assert(not cdmi_test_utils:object_exists(File4, Config)),

    % delete files (should return 403 forbidden)
    {ok, Code9, _, Response9} = cdmi_test_utils:do_request(
        WorkerP1, File3, delete, [cdmi_test_utils:user_2_token_header()], []
    ),
    ?assertMatch(EaccesError, {Code9, json_utils:decode(Response9)}),
    ?assert(cdmi_test_utils:object_exists(File3, Config)).


%% @private
acl_file_test_base(Config) ->
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    UserId = oct_background:get_user_id(user2),
    UserName = oct_background:get_user_fullname(user2),
    Identifier = <<UserName/binary, "#", UserId/binary>>,

    Read = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?read_all_object_mask
    }, cdmi),
    ReadFull = #{
        <<"acetype">> => ?allow,
        <<"identifier">> => Identifier,
        <<"aceflags">> => ?no_flags,
        <<"acemask">> => <<
            ?read_object/binary, ",",
            ?read_metadata/binary, ",",
            ?read_attributes/binary, ",",
            ?read_acl/binary
        >>
    },
    Write = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?write_all_object_mask
    }, cdmi),
    ReadWriteVerbose = #{
        <<"acetype">> => ?allow,
        <<"identifier">> => Identifier,
        <<"aceflags">> => ?no_flags,
        <<"acemask">> => <<
            ?read_object/binary, ",",
            ?read_metadata/binary, ",",
            ?read_attributes/binary, ",",
            ?read_acl/binary, ",",
            ?write_object/binary, ",",
            ?write_metadata/binary, ",",
            ?write_attributes/binary, ",",
            ?delete/binary, ",",
            ?write_acl/binary
        >>
    },
    WriteAcl = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?write_acl_mask
    }, cdmi),
    Delete = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?delete_mask
    }, cdmi),

    MetadataAclReadFull = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [ReadFull, WriteAcl]}}),
    MetadataAclDelete = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [Delete]}}),
    MetadataAclWrite = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [Write]}}),
    MetadataAclReadWrite = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [Write, Read]}}),
    MetadataAclReadWriteFull = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [ReadWriteVerbose]}}),
    {
        RootPath, MetadataAclReadFull, MetadataAclDelete,
        MetadataAclWrite, MetadataAclReadWrite, MetadataAclReadWriteFull
    }.