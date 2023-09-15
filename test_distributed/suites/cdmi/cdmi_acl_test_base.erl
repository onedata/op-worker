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
    write_acl_metadata/1,
    acl_read_file/1,
    acl_write_file/1,
    acl_delete_file/1,
    acl_read_write_dir/1
]).

user_2_token_header() ->
    rest_test_utils:user_token_header(oct_background:get_user_access_token(user2)).

write_acl_metadata(Config) ->
    UserName2 = <<"Unnamed User">>,
    {Workers, RootPath, UserId2} = cdmi_test_base:metadata_base(Config),
    DirName = filename:join([RootPath, "metadataTestDir2"]) ++ "/",
    RequestHeaders1 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],

    RequestHeaders2 = [?CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],
    RequestBody11 = #{<<"metadata">> => #{<<"my_metadata">> => <<"my_dir_value">>}},
    RawRequestBody11 = json_utils:encode(RequestBody11),
    {ok, ?HTTP_201_CREATED, _Headers11, _Response11} = cdmi_internal:do_request(
        Workers, DirName, put, RequestHeaders2, RawRequestBody11
    ),
    %%------ write acl metadata ----------
    FileName2 = filename:join([RootPath, "acl_test_file.txt"]),
    Ace1 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?read_all_object_mask
    }, cdmi),
    Ace2 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_all_object_mask
    }, cdmi),
    Ace2Full = #{
        <<"acetype">> => ?allow,
        <<"identifier">> => <<UserName2/binary, "#", UserId2/binary>>,
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

    RequestBody15 = #{<<"metadata">> => #{<<"cdmi_acl">> => [Ace1, Ace2Full]}},
    RawRequestBody15 = json_utils:encode(RequestBody15),
    RequestHeaders15 = [?OBJECT_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER, user_2_token_header()],

    {ok, Code15, _Headers15, Response15} = cdmi_internal:do_request(
        Workers, FileName2 ++ "?metadata:cdmi_acl", put, RequestHeaders15, RawRequestBody15
    ),
    ?assertMatch({?HTTP_204_NO_CONTENT, _}, {Code15, Response15}),

    Fun = fun() ->
        {ok, _Code16, _Headers16, Response16} =
            cdmi_internal:do_request(
            Workers, FileName2 ++ "?metadata", get, RequestHeaders1, []
        ),
        Response16
    end,

    ?assertEqual(6, maps:size(maps:get(<<"metadata">>, json_utils:decode(Fun()))), ?ATTEMPTS),
    {ok, Code16, _Headers16, Response16} =
        cdmi_internal:do_request(
            Workers, FileName2 ++ "?metadata", get, RequestHeaders1, []
        ),
    CdmiResponse16 = (json_utils:decode(Response16)),
    Metadata16 = maps:get(<<"metadata">>, CdmiResponse16),
    ?assertEqual(?HTTP_200_OK, Code16),
    ?assertEqual(1, maps:size(CdmiResponse16)),

    ?assertMatch(#{<<"cdmi_acl">> := [Ace1, Ace2]}, Metadata16),

    {ok, Code17, _Headers17, Response17} = cdmi_internal:do_request(
        Workers, FileName2, get, [user_2_token_header()], []
    ),
    ?assertEqual(?HTTP_200_OK, Code17),
    ?assertEqual(<<"data">>, Response17),
    %%------------------------------

    %%-- create forbidden by acl ---
    Ace3 = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_metadata_mask
    }, cdmi),
    Ace4 = ace:to_json(#access_control_entity{
        acetype = ?deny_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_object_mask
    }, cdmi),
    RequestBody18 = #{<<"metadata">> => #{<<"cdmi_acl">> => [Ace3, Ace4]}},
    RawRequestBody18 = json_utils:encode(RequestBody18),
    RequestHeaders18 = [user_2_token_header(), ?CONTAINER_CONTENT_TYPE_HEADER, ?CDMI_VERSION_HEADER],

    {ok, Code18, _Headers18, _Response18} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            Workers, DirName ++ "?metadata:cdmi_acl", put, RequestHeaders18, RawRequestBody18
        ),
        ?ATTEMPTS
    ),
    ?assertEqual(?HTTP_204_NO_CONTENT, Code18),

    Fun2 = fun() ->
        {ok, Code19, _Headers19, Response19} = cdmi_internal:do_request(
            Workers, filename:join(DirName, "some_file"), put, [user_2_token_header()], []
        ),
        {Code19, json_utils:decode(Response19)}
    end,
    ExpRestError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    ?assertMatch(ExpRestError, Fun2(), ?ATTEMPTS).

% tests access control lists
acl_file_base(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    UserId2 = oct_background:get_user_id(user2),
    UserName2 = <<"Unnamed User">>,
    Identifier1 = <<UserName2/binary, "#", UserId2/binary>>,

    Read = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?read_all_object_mask
    }, cdmi),
    ReadFull = #{
        <<"acetype">> => ?allow,
        <<"identifier">> => Identifier1,
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
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_all_object_mask
    }, cdmi),
    ReadWriteVerbose = #{
        <<"acetype">> => ?allow,
        <<"identifier">> => Identifier1,
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
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_acl_mask
    }, cdmi),
    Delete = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?delete_mask
    }, cdmi),

    MetadataAclReadFull = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [ReadFull, WriteAcl]}}),
    MetadataAclDelete = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [Delete]}}),
    MetadataAclWrite = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [Write]}}),
    MetadataAclReadWrite = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [Write, Read]}}),
    MetadataAclReadWriteFull = json_utils:encode(#{<<"metadata">> => #{<<"cdmi_acl">> => [ReadWriteVerbose]}}),
    {
        Workers, RootPath, MetadataAclReadFull, MetadataAclDelete,
        MetadataAclWrite, MetadataAclReadWrite, MetadataAclReadWriteFull
    }.


acl_read_file(Config) ->
    {Workers, RootPath, _, _, MetadataAclWrite, _, MetadataAclReadWriteFull} = acl_file_base(Config),
    Filename1 = filename:join([RootPath, "acl_test_file1"]),

    %%----- read file test ---------
    % create test file with dummy data
    ?assert(not cdmi_internal:object_exists(Filename1, Config)),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file1">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),

    EaccesError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),

    % set acl to 'write' and test cdmi/non-cdmi get request (should return 403 forbidden)
    RequestHeaders1 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Filename1, put, RequestHeaders1, MetadataAclWrite
    ),
    {ok, Code1, _, Response1} = ?assertMatch(
        {ok, 400, _, _},
        cdmi_internal:do_request(Workers, Filename1, get, RequestHeaders1, []),
        ?ATTEMPTS
    ),
    ?assertMatch(EaccesError, {Code1, json_utils:decode(Response1)}),

    {ok, Code2, _, Response2} = ?assertMatch(
        {ok, 400, _, _},
        cdmi_internal:do_request(Workers, Filename1, get, [user_2_token_header()], []),
        ?ATTEMPTS
    ),
    ?assertMatch(EaccesError, {Code2, json_utils:decode(Response2)}),
    ?assertEqual({error, ?EACCES}, cdmi_internal:open_file(
        Config#cdmi_test_config.p2_selector, Filename1, read), ?ATTEMPTS
    ),

    % set acl to 'read&write' and test cdmi/non-cdmi get request (should succeed)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            Workers, Filename1, put, RequestHeaders1, MetadataAclReadWriteFull
        ),
        ?ATTEMPTS
    ),
    {ok, ?HTTP_200_OK, _, _} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_internal:do_request(Workers, Filename1, get, RequestHeaders1, []),
        ?ATTEMPTS
    ),
    {ok, ?HTTP_200_OK, _, _} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_internal:do_request(Workers, Filename1, get, [user_2_token_header()], []),
        ?ATTEMPTS
    ).
%%------------------------------


acl_write_file(Config) ->
    {[WorkerP1, WorkerP2], RootPath, MetadataAclReadFull, _, _, MetadataAclReadWrite, _} = acl_file_base(Config),
%%    [WorkerP1, WorkerP2] = Workers,
    Filename1 = filename:join([RootPath, "acl_test_file2"]),
    % create test file with dummy data
    ?assert(not cdmi_internal:object_exists(Filename1, Config)),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file2">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),
    RequestHeaders1 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    EaccesError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),
    %%------- write file test ------
    % set acl to 'read&write' and test cdmi/non-cdmi put request (should succeed)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        WorkerP1, Filename1, put, RequestHeaders1, MetadataAclReadWrite
    ),
    RequestBody4 = json_utils:encode(#{<<"value">> => <<"new_data">>}),
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            WorkerP1, Filename1, put, RequestHeaders1, RequestBody4
        ),
        ?ATTEMPTS
    ),
    ?assertEqual(<<"new_data">>, cdmi_internal:get_file_content(Filename1, Config), ?ATTEMPTS),

    cdmi_internal:write_to_file(Filename1, <<"1">>, 8, Config),
    timer:sleep(5000),
    ?assertEqual(<<"new_data1">>, cdmi_internal:get_file_content(Filename1, Config), ?ATTEMPTS),

    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            WorkerP1, Filename1, put, [user_2_token_header()], <<"new_data2">>
        ),
        ?ATTEMPTS
    ),
    ?assertEqual(<<"new_data2">>, cdmi_internal:get_file_content(Filename1, Config), ?ATTEMPTS),

    % set acl to 'read' and test cdmi/non-cdmi put request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            WorkerP1, Filename1, put, RequestHeaders1, MetadataAclReadFull
        ),
        ?ATTEMPTS
    ),
    RequestBody6 = json_utils:encode(#{<<"value">> => <<"new_data3">>}),
    {ok, Code3, _, Response3} = ?assertMatch(
        {ok, 400, _, _},
        cdmi_internal:do_request(WorkerP1, Filename1, put, RequestHeaders1, RequestBody6),
        ?ATTEMPTS
    ),
    ?assertMatch(EaccesError, {Code3, json_utils:decode(Response3)}),

    {ok, Code4, _, Response4} = cdmi_internal:do_request(
        WorkerP1, Filename1, put, [user_2_token_header()], <<"new_data4">>
    ),
    ?assertMatch(EaccesError, {Code4, json_utils:decode(Response4)}),
    ?assertEqual(<<"new_data2">>, cdmi_internal:get_file_content(Filename1, Config), ?ATTEMPTS),
    ?assertEqual(
        {error, ?EACCES},
        cdmi_internal:open_file(Config#cdmi_test_config.p2_selector, Filename1, write),
        ?ATTEMPTS
    ),
    ?assertEqual(<<"new_data2">>, cdmi_internal:get_file_content(Filename1, Config), ?ATTEMPTS).
%%------------------------------


acl_delete_file(Config) ->
    {Workers, RootPath, _, MetadataAclDelete, _, _, _} = acl_file_base(Config),
    Filename3 = filename:join([RootPath, "acl_test_file3"]),
    % create test file with dummy data
    ?assert(not cdmi_internal:object_exists(Filename3, Config)),

    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #file_spec{
            name = <<"acl_test_file3">>,
            content = <<"data">>
        }, Config#cdmi_test_config.p1_selector
    ),
    RequestHeaders1 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    %%------ delete file test ------
    % set acl to 'delete'
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Filename3, put, RequestHeaders1, MetadataAclDelete
    ),

    % delete file
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        Workers, Filename3, delete, [user_2_token_header()], []
    ),
    ?assert(not cdmi_internal:object_exists(Filename3, Config), ?ATTEMPTS).
%%------------------------------

%% @private
acl_dir_base(Config) ->
    Workers = [
        oct_background:get_random_provider_node(Config#cdmi_test_config.p1_selector),
        oct_background:get_random_provider_node(Config#cdmi_test_config.p2_selector)
    ],
    SpaceName = binary_to_list(oct_background:get_space_name(Config#cdmi_test_config.space_selector)),
    RootName = node_cache:get(root_dir_name) ++ "/",
    RootPath = SpaceName ++ "/" ++ RootName,

    UserId2 = oct_background:get_user_id(user2),
    UserName2 = <<"Unnamed User">>,
    DirRead = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?read_all_container_mask bor ?traverse_container_mask
    }, cdmi),
    DirWrite = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
        aceflags = ?no_flags_mask,
        acemask = ?write_all_container_mask bor ?traverse_container_mask
    }, cdmi),
    WriteAcl = ace:to_json(#access_control_entity{
        acetype = ?allow_mask,
        identifier = UserId2,
        name = UserName2,
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
    {Workers, RootPath, DirMetadataAclReadWrite, DirMetadataAclRead, DirMetadataAclWrite}.


acl_read_write_dir(Config) ->
    {Workers, RootPath, DirMetadataAclReadWrite, DirMetadataAclRead, DirMetadataAclWrite} = acl_dir_base(Config),
    [WorkerP1, WorkerP2] = Workers,
    Dirname1 = filename:join([RootPath, "acl_test_dir1"]) ++ "/",
    EaccesError = rest_test_utils:get_rest_error(?ERROR_POSIX(?EACCES)),

    %%--- read write dir test ------
    ?assert(not cdmi_internal:object_exists(Dirname1, Config)),
    onenv_file_test_utils:create_and_sync_file_tree(user2, node_cache:get(root_dir_guid),
        #dir_spec{
            name = <<"acl_test_dir1">>,
            children = [
                #file_spec{
                    name = <<"3">>
                }
            ]
        }, Config#cdmi_test_config.p1_selector
    ),
    File1 = filename:join(Dirname1, "1"),
    File2 = filename:join(Dirname1, "2"),
    File3 = filename:join(Dirname1, "3"),
    File4 = filename:join(Dirname1, "4"),

    RequestHeaders1 = [user_2_token_header(), ?CDMI_VERSION_HEADER, ?OBJECT_CONTENT_TYPE_HEADER],
    % set acl to 'read&write' and test cdmi get request (should succeed)
    RequestHeaders2 = [
        user_2_token_header(),
        ?CDMI_VERSION_HEADER,
        ?CONTAINER_CONTENT_TYPE_HEADER
    ],
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        WorkerP1, Dirname1, put, RequestHeaders2, DirMetadataAclReadWrite
    ),
    {ok, ?HTTP_200_OK, _, _} = ?assertMatch(
        {ok, 200, _, _},
        cdmi_internal:do_request(WorkerP2, Dirname1, get, RequestHeaders2, []),
        ?ATTEMPTS
    ),

    % create files in directory (should succeed)
    {ok, ?HTTP_201_CREATED, _, _} = cdmi_internal:do_request(
        WorkerP1, File1, put, [user_2_token_header()], []
    ),
    ?assert(cdmi_internal:object_exists(File1, Config), ?ATTEMPTS),

    {ok, ?HTTP_201_CREATED, _, _} = cdmi_internal:do_request(
        WorkerP1, File2, put, RequestHeaders1, <<"{\"value\":\"val\"}">>
    ),
    ?assert(cdmi_internal:object_exists(File2, Config), ?ATTEMPTS),
    ?assert(cdmi_internal:object_exists(File3, Config), ?ATTEMPTS),

    % delete files (should succeed)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = ?assertMatch(
        {ok, 204, _, _},
        cdmi_internal:do_request(
            WorkerP1, File1, delete, [user_2_token_header()], []
        ),
        ?ATTEMPTS
    ),
    ?assert(not cdmi_internal:object_exists(File1, Config)),

    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        WorkerP1, File2, delete, [user_2_token_header()], []
    ),
    ?assert(not cdmi_internal:object_exists(File2, Config)),

    % set acl to 'write' and test cdmi get request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        WorkerP1, Dirname1, put, RequestHeaders2, DirMetadataAclWrite
    ),
    {ok, Code5, _, Response5} = ?assertMatch(
        {ok, 400, _, _},
        cdmi_internal:do_request(WorkerP2, Dirname1, get, RequestHeaders2, []),
        ?ATTEMPTS
    ),
    ?assertMatch(EaccesError, {Code5, json_utils:decode(Response5)}),

    % set acl to 'read' and test cdmi put request (should return 403 forbidden)
    {ok, ?HTTP_204_NO_CONTENT, _, _} = cdmi_internal:do_request(
        WorkerP1, Dirname1, put, RequestHeaders2, DirMetadataAclRead
    ),
    {ok, ?HTTP_200_OK, _, _} = cdmi_internal:do_request(Workers, Dirname1, get, RequestHeaders2, []),
    {ok, Code6, _, Response6} = cdmi_internal:do_request(
        WorkerP1, Dirname1, put, RequestHeaders2,
        json_utils:encode(#{<<"metadata">> => #{<<"my_meta">> => <<"value">>}})
    ),
    ?assertMatch(EaccesError, {Code6, json_utils:decode(Response6)}),

    % create files (should return 403 forbidden)
    {ok, Code7, _, Response7} = cdmi_internal:do_request(
        WorkerP1, File1, put, [user_2_token_header()], []
    ),
    ?assertMatch(EaccesError, {Code7, json_utils:decode(Response7)}),
    ?assert(not cdmi_internal:object_exists(File1, Config)),

    {ok, Code8, _, Response8} = cdmi_internal:do_request(
        WorkerP1, File2, put, RequestHeaders1, <<"{\"value\":\"val\"}">>
    ),
    ?assertMatch(EaccesError, {Code8, json_utils:decode(Response8)}),
    ?assert(not cdmi_internal:object_exists(File2, Config)),
    ?assertEqual({error, ?EACCES}, cdmi_internal:create_new_file(File4, Config)),
    ?assert(not cdmi_internal:object_exists(File4, Config)),

    % delete files (should return 403 forbidden)
    {ok, Code9, _, Response9} = cdmi_internal:do_request(
        WorkerP1, File3, delete, [user_2_token_header()], []
    ),
    ?assertMatch(EaccesError, {Code9, json_utils:decode(Response9)}),
    ?assert(cdmi_internal:object_exists(File3, Config)).
