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

-include("http/cdmi.hrl").
-include("onenv_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include("cdmi_test.hrl").
-include("modules/fslogic/acl.hrl").

%% API
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
    UserName = <<"Unnamed User">>,
    RootPath = cdmi_test_utils:get_tests_root_path(Config),
    UserId = oct_background:get_user_id(user2),
    DirPath = filename:join([RootPath, "metadataTestDir2"]) ++ "/",

    RequestHeaders1 = [?CDMI_CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],
    RequestBody1 = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value">>}},
    RawRequestBody1 = json_utils:encode(RequestBody1),
    {ok, ?HTTP_201_CREATED, _Headers1, _Response1} = cdmi_test_utils:do_request(
        ?WORKERS, DirPath, put, RequestHeaders1, RawRequestBody1
    ),

    FilePath = filename:join([RootPath, "acl_test_file.txt"]),
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

    RequestBody2 = #{<<"metadata">> => #{<<"cdmi_acl">> => [Ace1, Ace2Full]}},
    RawRequestBody2 = json_utils:encode(RequestBody2),
    RequestHeaders2 = [?CDMI_OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, cdmi_test_utils:user_2_token_header()],

    {ok, Code2, _Headers2, Response2} = cdmi_test_utils:do_request(
        ?WORKERS, FilePath ++ "?metadata:cdmi_acl", put, RequestHeaders2, RawRequestBody2
    ),
    ?assertMatch({?HTTP_204_NO_CONTENT, _}, {Code2, Response2}),

    Fun = fun() ->
        {ok, _Code3, _Headers3, Response3} =
            cdmi_test_utils:do_request(
                ?WORKERS, FilePath ++ "?metadata", get, RequestHeaders2, []
        ),
        Response3
    end,

    ?assertEqual(6, maps:size(maps:get(<<"metadata">>, json_utils:decode(Fun()))), ?ATTEMPTS),
    {ok, _Code4, _Headers4, Response4} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS, FilePath ++ "?metadata", get, RequestHeaders2, []
        )
    ),
    CdmiResponse4 = json_utils:decode(Response4),
    Metadata4 = maps:get(<<"metadata">>, CdmiResponse4),

    ?assertEqual(1, maps:size(CdmiResponse4)),
    ?assertMatch(#{<<"cdmi_acl">> := [Ace1, Ace2]}, Metadata4),

    {ok, _Code5, _Headers5, Response5} = ?assertMatch(
        {ok, ?HTTP_200_OK, _, _},
        cdmi_test_utils:do_request(
        ?WORKERS, FilePath, get, [cdmi_test_utils:user_2_token_header()], []
    )),
    ?assertEqual(<<"data">>, Response5),

    %%-- create forbidden by acl ---
    Ace3 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?write_metadata_mask
    }, cdmi),
    Ace4 = ace:to_json(#access_control_entity{
        acetype = ?deny_mask,
        identifier = UserId,
        name = UserName,
        aceflags = ?no_flags_mask,
        acemask = ?write_object_mask
    }, cdmi),
    RequestBody3 = #{<<"metadata">> => #{<<"cdmi_acl">> => [Ace3, Ace4]}},
    RawRequestBody3 = json_utils:encode(RequestBody3),
    RequestHeaders3 = [cdmi_test_utils:user_2_token_header(), ?CDMI_CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER],


    {ok, _Code6, _Headers6, _Response6} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS, filename:join([DirPath, "?metadata:cdmi_acl"]), put, RequestHeaders3, RawRequestBody3
        ),
        ?ATTEMPTS
    ),

    Fun2 = fun() ->
        {ok, Code7, _Headers7, Response7} = cdmi_test_utils:do_request(
            ?WORKERS, filename:join(DirPath, "some_file"), put, [cdmi_test_utils:user_2_token_header()], []
        ),
        {Code7, json_utils:decode(Response7)}
    end,
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError, Fun2(), ?ATTEMPTS).


acl_read_file_test(Config) ->
    {RootPath, _, _, MetadataAclWrite, _, MetadataAclReadWriteFull} = acl_file_test_base(Config),
    Filename = filename:join(RootPath, "acl_test_file1"),

    % create test file with dummy data
    ?assert(not cdmi_test_utils:object_exists(Filename, Config)),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file1">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),

    EaccesError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),

    % set acl to 'write' and test cdmi/non-cdmi get request (should return 403 forbidden)
    RequestHeaders = [cdmi_test_utils:user_2_token_header(), ?CDMI_VERSION_HEADER, ?CDMI_OBJECT_CONTENT_TYPE_HEADER],
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        ?WORKERS, Filename, put, RequestHeaders, MetadataAclWrite
    ),
    {ok, Code1, _, Response1} = ?assertMatch(
        {ok, 400, _, _},
        cdmi_test_utils:do_request(?WORKERS, Filename, get, RequestHeaders, []),
        ?ATTEMPTS
    ),
    ?assertMatch(EaccesError, {Code1, json_utils:decode(Response1)}),

    {ok, Code2, _, Response2} = ?assertMatch(
        {ok, 400, _, _},
        cdmi_test_utils:do_request(?WORKERS, Filename, get, [cdmi_test_utils:user_2_token_header()], []),
        ?ATTEMPTS
    ),
    ?assertMatch(EaccesError, {Code2, json_utils:decode(Response2)}),
    ?assertEqual({error, ?EACCES}, cdmi_test_utils:open_file(
        Config#cdmi_test_config.p2_selector, Filename, read), ?ATTEMPTS
    ),

    % set acl to 'read&write' and test cdmi/non-cdmi get request (should succeed)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            ?WORKERS, Filename, put, RequestHeaders, MetadataAclReadWriteFull
        ),
        ?ATTEMPTS
    ),
    {ok, ?HTTP_200_OK, _, _} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_test_utils:do_request(?WORKERS, Filename, get, RequestHeaders, []),
        ?ATTEMPTS
    ),
    {ok, ?HTTP_200_OK, _, _} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_test_utils:do_request(?WORKERS, Filename, get, [cdmi_test_utils:user_2_token_header()], []),
        ?ATTEMPTS
    ).


acl_write_file_test(Config) ->
    {RootPath, MetadataAclReadFull, _, _, MetadataAclReadWrite, _} = acl_file_test_base(Config),
    [WorkerP1, _WorkerP2] = ?WORKERS,
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
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            WorkerP1, FilePath, put, RequestHeaders, RequestBody
        ),
        ?ATTEMPTS
    ),
    ?assertEqual(<<"new_data">>, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    ?assertMatch({ok, _}, cdmi_test_utils:write_to_file(FilePath, <<"1">>, 8, Config), ?ATTEMPTS),
    ?assertEqual(<<"new_data1">>, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            WorkerP1, FilePath, put, [cdmi_test_utils:user_2_token_header()], <<"new_data2">>
        ),
        ?ATTEMPTS
    ),
    ?assertEqual(<<"new_data2">>, cdmi_test_utils:get_file_content(FilePath, Config), ?ATTEMPTS),

    % set acl to 'read' and test cdmi/non-cdmi put request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            WorkerP1, FilePath, put, RequestHeaders, MetadataAclReadFull
        ),
        ?ATTEMPTS
    ),
    RequestBody6 = json_utils:encode(#{<<"value">> => <<"new_data3">>}),
    {ok, Code3, _, Response3} = ?assertMatch(
        {ok, 400, _, _},
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
    {RootPath, _, MetadataAclDelete, _, _, _} = acl_file_test_base(Config),
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

    % set acl to 'delete'
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        ?WORKERS, FilePath, put, RequestHeaders, MetadataAclDelete
    ),

    % delete file
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        ?WORKERS, FilePath, delete, [cdmi_test_utils:user_2_token_header()], []
    ),
    ?assert(not cdmi_test_utils:object_exists(FilePath, Config), ?ATTEMPTS).


acl_read_write_dir_test(Config) ->
    [WorkerP1, WorkerP2] = ?WORKERS,
    SpaceName = binary_to_list(oct_background:get_space_name(
        Config#cdmi_test_config.space_selector
    )) ++ "/",
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = filename:join([SpaceName, RootName]),

    UserId = oct_background:get_user_id(user2),
    UserName = <<"Unnamed User">>,
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
    {ok, ?HTTP_200_OK, _, _} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_test_utils:do_request(WorkerP2, Dirname, get, RequestHeaders2, []),
        ?ATTEMPTS
    ),

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
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_test_utils:do_request(
            WorkerP1, File1, delete, [cdmi_test_utils:user_2_token_header()], []
        ),
        ?ATTEMPTS
    ),
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
        {ok, 400, _, _},
        cdmi_test_utils:do_request(WorkerP2, Dirname, get, RequestHeaders2, []),
        ?ATTEMPTS
    ),
    ?assertMatch(EaccesError, {Code5, json_utils:decode(Response5)}),

    % set acl to 'read' and test cdmi put request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_test_utils:do_request(
        WorkerP1, Dirname, put, RequestHeaders2, DirMetadataAclRead
    ),
    {ok, ?HTTP_200_OK, _, _} = cdmi_test_utils:do_request(?WORKERS, Dirname, get, RequestHeaders2, []),
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
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = filename:join(SpaceName, RootName),

    UserId = oct_background:get_user_id(user2),
    UserName = <<"Unnamed User">>,
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