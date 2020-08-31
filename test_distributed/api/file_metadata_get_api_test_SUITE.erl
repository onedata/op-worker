%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file metadata get basic API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_metadata_get_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("file_metadata_api_test_utils.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/graph_sync/gri.hrl").
-include_lib("ctool/include/http/codes.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    % Get rdf metadata test cases
    get_file_rdf_metadata_with_rdf_set_test/1,
    get_file_rdf_metadata_without_rdf_set_test/1,
    get_shared_file_rdf_metadata_with_rdf_set_test/1,
    get_shared_file_rdf_metadata_without_rdf_set_test/1,
    get_file_rdf_metadata_on_provider_not_supporting_space_test/1,

    % Get json metadata test cases
    get_file_json_metadata_with_json_set_test/1,
    get_file_json_metadata_without_json_set_test/1,
    get_shared_file_json_metadata_with_json_set_test/1,
    get_shared_file_json_metadata_without_json_set_test/1,
    get_file_json_metadata_on_provider_not_supporting_space_test/1,

    % Get xattrs test cases
    get_file_xattrs_with_xattrs_set_test/1,
    get_file_xattrs_without_xattrs_set_test/1,
    get_shared_file_xattrs_with_xattrs_set_test/1,
    get_shared_file_xattrs_without_xattrs_set_test/1,
    get_file_xattrs_on_provider_not_supporting_space_test/1
]).

all() ->
    ?ALL([
        get_file_rdf_metadata_with_rdf_set_test,
        get_file_rdf_metadata_without_rdf_set_test,
        get_shared_file_rdf_metadata_with_rdf_set_test,
        get_shared_file_rdf_metadata_without_rdf_set_test,
        get_file_rdf_metadata_on_provider_not_supporting_space_test,

        get_file_json_metadata_with_json_set_test,
        get_file_json_metadata_without_json_set_test,
        get_shared_file_json_metadata_with_json_set_test,
        get_shared_file_json_metadata_without_json_set_test,
        get_file_json_metadata_on_provider_not_supporting_space_test,

        get_file_xattrs_with_xattrs_set_test,
        get_file_xattrs_without_xattrs_set_test,
        get_shared_file_xattrs_with_xattrs_set_test,
        get_shared_file_xattrs_without_xattrs_set_test,
        get_file_xattrs_on_provider_not_supporting_space_test
    ]).


-define(ATTEMPTS, 30).


%%%===================================================================
%%% Get Rdf metadata test functions
%%%===================================================================


get_file_rdf_metadata_with_rdf_set_test(Config) ->
    get_rdf_metadata_test_base(set_rdf, normal_mode, Config).


get_file_rdf_metadata_without_rdf_set_test(Config) ->
    get_rdf_metadata_test_base(do_not_set_rdf, normal_mode, Config).


get_shared_file_rdf_metadata_with_rdf_set_test(Config) ->
    get_rdf_metadata_test_base(set_rdf, share_mode, Config).


get_shared_file_rdf_metadata_without_rdf_set_test(Config) ->
    get_rdf_metadata_test_base(do_not_set_rdf, share_mode, Config).


%% @private
get_rdf_metadata_test_base(SetRdfPolicy, TestMode, Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),

    SpaceOwnerSessIdP1 = api_test_env:get_user_session_id(user2, p1, Config),
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    UserSessIdP2 = api_test_env:get_user_session_id(user3, p2, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1Node, UserSessIdP1, FilePath, 8#707),

    GetExpCallResultFun = case SetRdfPolicy of
        set_rdf ->
            lfm_proxy:set_metadata(P1Node, UserSessIdP1, {guid, FileGuid}, rdf, ?RDF_METADATA_1, []),
            fun(_TestCtx) -> {ok, ?RDF_METADATA_1} end;
        do_not_set_rdf ->
            fun(_TestCtx) -> ?ERROR_POSIX(?ENODATA) end
    end,

    {ShareId, ClientSpec} = case TestMode of
        share_mode ->
            {ok, ShId} = lfm_proxy:create_share(
                P1Node, SpaceOwnerSessIdP1, {guid, FileGuid}, <<"share">>
            ),
            {
                ShId,
                #client_spec{
                    correct = [nobody, user1, user2, user3, user4],
                    unauthorized = [],
                    forbidden_not_in_space = []
                }
            };
        normal_mode ->
            {
                undefined,
                #client_spec{
                    correct = [
                        user2, % space owner - doesn't need any perms
                        user3  % files owner
                    ],
                    unauthorized = [nobody],
                    forbidden_not_in_space = [user1],
                    forbidden_in_space = [{user4, ?ERROR_POSIX(?EACCES)}]
                }
            }
    end,

    api_test_utils:wait_for_file_sync(P2Node, UserSessIdP2, FileGuid),

    DataSpec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
        FileGuid, ShareId, undefined
    ),

    get_metadata_test_base(
        <<"rdf">>,
        FileType, FilePath, FileGuid, ShareId,
        build_get_metadata_validate_rest_call_fun(GetExpCallResultFun),
        build_get_metadata_validate_gs_call_fun(GetExpCallResultFun),
        _Providers = ?config(op_worker_nodes, Config),
        ClientSpec,
        DataSpec,
        _QsParams = [],
        _RandomlySelectScenario = false,
        Config
    ).


get_file_rdf_metadata_on_provider_not_supporting_space_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),

    SessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_1, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1Node, SessIdP1, FilePath),
    lfm_proxy:set_metadata(P1Node, SessIdP1, {guid, FileGuid}, rdf, ?RDF_METADATA_1, []),

    GetExpCallResultFun = fun(_TestCtx) -> {ok, ?RDF_METADATA_1} end,

    ClientSpec = #client_spec{
        correct = [user1, user3, user4],
        unauthorized = [nobody],
        forbidden_not_in_space = [user2]
    },

    get_metadata_test_base(
        <<"rdf">>,
        FileType, FilePath, FileGuid, undefined,
        build_get_metadata_validate_rest_call_fun(GetExpCallResultFun, P2Node),
        build_get_metadata_validate_gs_call_fun(GetExpCallResultFun, P2Node),
        [P1Node, P2Node],
        ClientSpec,
        _DataSpec = undefined,
        _QsParams = [],
        _RandomlySelectScenario = false,
        Config
    ).


%%%===================================================================
%%% Get Json metadata test functions
%%%===================================================================


get_file_json_metadata_with_json_set_test(Config) ->
    get_json_metadata_test_base(set_direct_json, normal_mode, Config).


get_file_json_metadata_without_json_set_test(Config) ->
    get_json_metadata_test_base(do_not_set_direct_json, normal_mode, Config).


get_shared_file_json_metadata_with_json_set_test(Config) ->
    get_json_metadata_test_base(set_direct_json, share_mode, Config).


get_shared_file_json_metadata_without_json_set_test(Config) ->
    get_json_metadata_test_base(do_not_set_direct_json, share_mode, Config).


%% @private
get_json_metadata_test_base(SetDirectJsonPolicy, TestMode, Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    {FileLayer5Path, FileLayer5Guid, ShareId} = create_get_json_metadata_tests_env(
        FileType, SetDirectJsonPolicy, TestMode, Config
    ),

    GetExpCallResultFun = create_get_json_call_exp_result_fun(
        ShareId, SetDirectJsonPolicy, Config
    ),

    ClientSpec = case TestMode of
        share_mode ->
            #client_spec{
                correct = [nobody, user1, user2, user3, user4],
                unauthorized = [],
                forbidden_not_in_space = []
            };
        normal_mode ->
            #client_spec{
                correct = [
                    user2, % space owner - doesn't need any perms
                    user3, % files owner
                    user4  % space member
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
            }
    end,

    DataSpec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
        FileLayer5Guid, ShareId, #data_spec{
            optional = QsParams = [<<"inherited">>, <<"filter_type">>, <<"filter">>],
            correct_values = #{
                <<"inherited">> => [true, false],
                <<"filter_type">> => [<<"keypath">>],
                <<"filter">> => [
                    <<"attr3.attr32">>, <<"attr3.[10]">>,
                    <<"attr2.attr22.[2]">>, <<"attr2.attr22.[10]">>
                ]
            },
            bad_values = [
                {<<"inherited">>, -100, ?ERROR_BAD_VALUE_BOOLEAN(<<"inherited">>)},
                {<<"inherited">>, <<"dummy">>, ?ERROR_BAD_VALUE_BOOLEAN(<<"inherited">>)},
                {<<"filter_type">>, <<"dummy">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])},

                % Below differences between error returned by rest and gs are results of sending
                % parameters via qs in REST, so they lost their original type and are cast to binary
                {<<"filter_type">>, 100, {rest_with_file_path, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])}},
                {<<"filter_type">>, 100, {rest, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])}},
                {<<"filter_type">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"filter_type">>)}},
                {<<"filter">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"filter">>)}}
            ]
        }
    ),

    get_metadata_test_base(
        <<"json">>,
        FileType, FileLayer5Path, FileLayer5Guid, ShareId,
        build_get_metadata_validate_rest_call_fun(GetExpCallResultFun),
        build_get_metadata_validate_gs_call_fun(GetExpCallResultFun),
        _Providers = ?config(op_worker_nodes, Config),
        ClientSpec,
        DataSpec,
        QsParams,
        _RandomlySelectScenario = true,
        Config
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates following directory structure:
%%
%%   TopDir (posix: 777) - ?JSON_METADATA_1
%%     |
%%     |-- DirLayer2 (posix: 717) - ?JSON_METADATA_2
%%            |
%%            |-- DirLayer3 (posix: 777) - ?JSON_METADATA_3
%%                   |
%%                   |-- DirLayer4 (posix: 777, maybe shared) - ?JSON_METADATA_4
%%                          |
%%                          |-- FileLayer5 (posix: 777) - ?JSON_METADATA_5
%% @end
%%--------------------------------------------------------------------
create_get_json_metadata_tests_env(FileType, SetJsonPolicy, TestMode, Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),

    SpaceOwnerSessIdP1 = api_test_env:get_user_session_id(user2, p1, Config),
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    UserSessIdP2 = api_test_env:get_user_session_id(user3, p2, Config),

    TopDirPath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, TopDirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, TopDirPath, 8#777),
    lfm_proxy:set_metadata(P1Node, UserSessIdP1, {guid, TopDirGuid}, json, ?JSON_METADATA_1, []),

    DirLayer2Path = filename:join([TopDirPath, <<"dir_layer_2">>]),
    {ok, DirLayer2Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, DirLayer2Path, 8#717),
    lfm_proxy:set_metadata(P1Node, UserSessIdP1, {guid, DirLayer2Guid}, json, ?JSON_METADATA_2, []),

    DirLayer3Path = filename:join([DirLayer2Path, <<"dir_layer_3">>]),
    {ok, DirLayer3Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, DirLayer3Path, 8#777),
    lfm_proxy:set_metadata(P1Node, UserSessIdP1, {guid, DirLayer3Guid}, json, ?JSON_METADATA_3, []),

    DirLayer4Path = filename:join([DirLayer3Path, <<"dir_layer_4">>]),
    {ok, DirLayer4Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, DirLayer4Path, 8#777),
    lfm_proxy:set_metadata(P1Node, UserSessIdP1, {guid, DirLayer4Guid}, json, ?JSON_METADATA_4, []),
    ShareId = case TestMode of
        share_mode ->
            {ok, Id} = lfm_proxy:create_share(
                P1Node, SpaceOwnerSessIdP1, {guid, DirLayer4Guid}, <<"share">>
            ),
            Id;
        normal_mode ->
            undefined
    end,

    FileLayer5Path = filename:join([DirLayer4Path, ?RANDOM_FILE_NAME()]),
    {ok, FileLayer5Guid} = api_test_utils:create_file(FileType, P1Node, UserSessIdP1, FileLayer5Path),
    case SetJsonPolicy of
        set_direct_json ->
            lfm_proxy:set_metadata(
                P1Node, UserSessIdP1, {guid, FileLayer5Guid}, json, ?JSON_METADATA_5, []
            );
        do_not_set_direct_json ->
            ok
    end,

    api_test_utils:wait_for_file_sync(P2Node, UserSessIdP2, FileLayer5Guid),

    {FileLayer5Path, FileLayer5Guid, ShareId}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates function returning expected result from get json metadata
%% rest/gs call taking into account env created by
%% create_get_json_metadata_tests_env/4.
%% @end
%%--------------------------------------------------------------------
create_get_json_call_exp_result_fun(ShareId, SetDirectJsonPolicy, Config) ->
    User4Auth = ?USER(api_test_env:get_user_id(user4, Config)),

    fun(#api_test_ctx{client = Client, data = Data}) ->
        try
            IncludeInherited = maps:get(<<"inherited">>, Data, false),
            FilterType = maps:get(<<"filter_type">>, Data, undefined),
            Filter = maps:get(<<"filter">>, Data, undefined),

            FilterList = case {FilterType, Filter} of
                {undefined, _} ->
                    [];
                {<<"keypath">>, undefined} ->
                    throw(?ERROR_MISSING_REQUIRED_VALUE(<<"filter">>));
                {<<"keypath">>, _} ->
                    binary:split(Filter, <<".">>, [global])
            end,

            ExpJsonMetadata = case {SetDirectJsonPolicy, IncludeInherited} of
                {set_direct_json, true} ->
                    case ShareId of
                        undefined when Client == User4Auth ->
                            % User belonging to the same space as owner of files
                            % shouldn't be able to get inherited metadata due to
                            % insufficient perms on DirLayer2 (exception would be
                            % space owner)
                            throw(?ERROR_POSIX(?EACCES));
                        undefined ->
                            json_utils:merge([
                                ?JSON_METADATA_1,
                                ?JSON_METADATA_2,
                                ?JSON_METADATA_3,
                                ?JSON_METADATA_4,
                                ?JSON_METADATA_5
                            ]);
                        _ ->
                            json_utils:merge([
                                ?JSON_METADATA_4,
                                ?JSON_METADATA_5
                            ])
                    end;
                {set_direct_json, false} ->
                    ?JSON_METADATA_5;
                {do_not_set_direct_json, true} ->
                    case ShareId of
                        undefined when Client == User4Auth ->
                            % User belonging to the same space as owner of files
                            % shouldn't be able to get inherited metadata due to
                            % insufficient perms on DirLayer2 (exception would be
                            % space owner)
                            throw(?ERROR_POSIX(?EACCES));
                        undefined ->
                            json_utils:merge([
                                ?JSON_METADATA_1,
                                ?JSON_METADATA_2,
                                ?JSON_METADATA_3,
                                ?JSON_METADATA_4
                            ]);
                        _ ->
                            ?JSON_METADATA_4
                    end;
                {do_not_set_direct_json, false} ->
                    throw(?ERROR_POSIX(?ENODATA))
            end,

            case json_utils:query(ExpJsonMetadata, FilterList) of
                {ok, _} = Result ->
                    Result;
                error ->
                    ?ERROR_POSIX(?ENODATA)
            end
        catch throw:Error ->
            Error
        end
    end.


get_file_json_metadata_on_provider_not_supporting_space_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),

    SessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_1, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1Node, SessIdP1, FilePath),
    lfm_proxy:set_metadata(P1Node, SessIdP1, {guid, FileGuid}, json, ?JSON_METADATA_2, []),

    GetExpCallResultFun = fun(_TestCtx) -> {ok, ?JSON_METADATA_2} end,

    ClientSpec = #client_spec{
        correct = [user1, user3, user4],
        unauthorized = [nobody],
        forbidden_not_in_space = [user2]
    },

    get_metadata_test_base(
        <<"json">>,
        FileType, FilePath, FileGuid, undefined,
        build_get_metadata_validate_rest_call_fun(GetExpCallResultFun, P2Node),
        build_get_metadata_validate_gs_call_fun(GetExpCallResultFun, P2Node),
        [P1Node, P2Node],
        ClientSpec,
        _DataSpec = undefined,
        _QsParams = [],
        _RandomlySelectScenario = false,
        Config
    ).


%%%===================================================================
%%% Get xattrs functions
%%%===================================================================



get_file_xattrs_with_xattrs_set_test(Config) ->
    get_xattrs_test_base(set_direct_xattr, normal_mode, Config).


get_file_xattrs_without_xattrs_set_test(Config) ->
    get_xattrs_test_base(do_not_set_direct_xattr, normal_mode, Config).


get_shared_file_xattrs_with_xattrs_set_test(Config) ->
    get_xattrs_test_base(set_direct_xattr, share_mode, Config).


get_shared_file_xattrs_without_xattrs_set_test(Config) ->
    get_xattrs_test_base(do_not_set_direct_xattr, share_mode, Config).


%% @private
get_xattrs_test_base(SetDirectXattrsPolicy, TestMode, Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    {FileLayer3Path, FileLayer3Guid, ShareId} = create_get_xattrs_tests_env(
        FileType, SetDirectXattrsPolicy, TestMode, Config
    ),
    NotSetXattrKey = <<"not_set_xattr">>,
    GetExpCallResultFun = create_get_xattrs_call_exp_result_fun(
        ShareId, SetDirectXattrsPolicy, NotSetXattrKey, Config
    ),

    ClientSpec = case TestMode of
        share_mode ->
            #client_spec{
                correct = [nobody, user1, user2, user3, user4],
                unauthorized = [],
                forbidden_not_in_space = []
            };
        normal_mode ->
            #client_spec{
                correct = [
                    user2, % space owner - doesn't need any perms
                    user3, % files owner
                    user4  % space member
                ],
                unauthorized = [nobody],
                forbidden_not_in_space = [user1]
            }
    end,
    DataSpec = api_test_utils:add_file_id_errors_for_operations_available_in_share_mode(
        FileLayer3Guid, ShareId, #data_spec{
            optional = QsParams = [<<"attribute">>, <<"inherited">>, <<"show_internal">>],
            correct_values = #{
                <<"attribute">> => [
                    NotSetXattrKey,
                    % Xattr name with prefixes 'cdmi_' and 'onedata_' should be forbidden
                    % with exception of those listed in ?ALL_XATTRS_KEYS. Nonetheless that is
                    % checked not in middleware but in lfm and depends on whether request will
                    % arrive there. That is why, depending where request was rejected, different
                    % error than ?EPERM may be returned
                    <<"cdmi_attr">>, <<"onedata_attr">>
                    | ?ALL_XATTRS_KEYS
                ],
                <<"inherited">> => [true, false],
                <<"show_internal">> => [true, false]
            },
            bad_values = [
                {<<"attribute">>, <<>>, ?ERROR_BAD_VALUE_EMPTY(<<"attribute">>)},
                {<<"inherited">>, -100, ?ERROR_BAD_VALUE_BOOLEAN(<<"inherited">>)},
                {<<"inherited">>, <<"dummy">>, ?ERROR_BAD_VALUE_BOOLEAN(<<"inherited">>)},
                {<<"show_internal">>, -100, ?ERROR_BAD_VALUE_BOOLEAN(<<"show_internal">>)},
                {<<"show_internal">>, <<"dummy">>, ?ERROR_BAD_VALUE_BOOLEAN(<<"show_internal">>)}
            ]
        }
    ),

    get_metadata_test_base(
        <<"xattrs">>,
        FileType, FileLayer3Path, FileLayer3Guid, ShareId,
        build_get_metadata_validate_rest_call_fun(GetExpCallResultFun),
        build_get_metadata_validate_gs_call_fun(GetExpCallResultFun),
        _Providers = ?config(op_worker_nodes, Config),
        ClientSpec,
        DataSpec,
        QsParams,
        _RandomlySelectScenario = true,
        Config
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates following directory structure:
%%
%%   TopDir (posix: 777) - ?ALL_METADATA_SET_1
%%     |
%%     |-- DirLayer2 (posix: 717, maybe shared) - no xattrs
%%            |
%%            |-- FileLayer3 (posix: 777) - ?ALL_METADATA_SET_2
%% @end
%%--------------------------------------------------------------------
create_get_xattrs_tests_env(FileType, SetXattrsPolicy, TestMode, Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),

    SpaceOwnerSessIdP1 = api_test_env:get_user_session_id(user2, p1, Config),
    UserSessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),
    UserSessIdP2 = api_test_env:get_user_session_id(user3, p2, Config),

    TopDirPath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, TopDirGuid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, TopDirPath, 8#777),
    set_all_metadata_types(P1Node, UserSessIdP1, TopDirGuid, set_1),

    DirLayer2Path = filename:join([TopDirPath, <<"dir_layer_2">>]),
    {ok, DirLayer2Guid} = lfm_proxy:mkdir(P1Node, UserSessIdP1, DirLayer2Path, 8#717),
    ShareId = case TestMode of
        share_mode ->
            {ok, Id} = lfm_proxy:create_share(
                P1Node, SpaceOwnerSessIdP1, {guid, DirLayer2Guid}, <<"share">>
            ),
            Id;
        normal_mode ->
            undefined
    end,

    FileLayer3Path = filename:join([DirLayer2Path, ?RANDOM_FILE_NAME()]),
    {ok, FileLayer3Guid} = api_test_utils:create_file(
        FileType, P1Node, UserSessIdP1, FileLayer3Path
    ),
    case SetXattrsPolicy of
        set_direct_xattr ->
            set_all_metadata_types(P1Node, UserSessIdP1, FileLayer3Guid, set_2);
        do_not_set_direct_xattr ->
            ok
    end,

    api_test_utils:wait_for_file_sync(P2Node, UserSessIdP2, FileLayer3Guid),

    {FileLayer3Path, FileLayer3Guid, ShareId}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates function returning expected result from get xattrs
%% rest/gs call taking into account env created by
%% create_get_xattrs_tests_env/4.
%% @end
%%--------------------------------------------------------------------
create_get_xattrs_call_exp_result_fun(ShareId, DirectMetadataSetPolicy, NotSetXattrKey, Config) ->
    User4Auth = ?USER(api_test_env:get_user_id(user4, Config)),

    fun(#api_test_ctx{client = Client, data = Data}) ->
        try
            Attribute = maps:get(<<"attribute">>, Data, undefined),
            IncludeInherited = maps:get(<<"inherited">>, Data, false),
            ShowInternal = maps:get(<<"show_internal">>, Data, false),

            XattrsToGet = case Attribute of
                undefined ->
                    case {DirectMetadataSetPolicy, IncludeInherited, ShareId, ShowInternal} of
                        {set_direct_xattr, true, undefined, true} ->
                            ?ALL_XATTRS_KEYS;
                        {set_direct_xattr, true, undefined, false} ->
                            % Only custom xattrs are shown
                            [?XATTR_1_KEY, ?XATTR_2_KEY];
                        {set_direct_xattr, true, _ShareId, true} ->
                            % Xattr1 cannot be fetched as it is above share root
                            ?ALL_XATTRS_KEYS -- [?XATTR_1_KEY];
                        {set_direct_xattr, true, _ShareId, false} ->
                            [?XATTR_2_KEY];
                        {set_direct_xattr, false, _, true} ->
                            ?ALL_XATTRS_KEYS -- [?XATTR_1_KEY];
                        {set_direct_xattr, false, _, false} ->
                            [?XATTR_2_KEY];
                        {do_not_set_direct_xattr, true, undefined, true} ->
                            % Exclude cdmi attrs as those are not inherited and xattr2 as it is not set
                            (?ALL_XATTRS_KEYS -- [?XATTR_2_KEY]) -- ?CDMI_XATTRS_KEY;
                        {do_not_set_direct_xattr, true, undefined, false} ->
                            [?XATTR_1_KEY];
                        {do_not_set_direct_xattr, _, _, _} ->
                            % No xattr could be inherited due to either not specified
                            % 'inherited' flag or share root blocking further traverse
                            []
                    end;
                _ ->
                    IsInternal = lists:any(fun(InternalPrefix) ->
                        str_utils:binary_starts_with(Attribute, InternalPrefix)
                    end, ?METADATA_INTERNAL_PREFIXES),

                    case IsInternal of
                        true ->
                            case lists:member(Attribute, ?ALL_XATTRS_KEYS) of
                                true ->
                                    [Attribute];
                                false ->
                                    % It is not possible for user to get internal
                                    % key other than allowed ones
                                    throw(?ERROR_POSIX(?EPERM))
                            end;
                        false ->
                            [Attribute]
                    end
            end,

            AllDirectMetadata = #{
                ?ACL_KEY => ?ACL_2,
                ?MIMETYPE_KEY => ?MIMETYPE_2,
                ?TRANSFER_ENCODING_KEY => ?TRANSFER_ENCODING_2,
                ?CDMI_COMPLETION_STATUS_KEY => ?CDMI_COMPLETION_STATUS_2,
                ?JSON_METADATA_KEY => ?JSON_METADATA_5,
                ?RDF_METADATA_KEY => ?RDF_METADATA_2,
                ?XATTR_2_KEY => ?XATTR_2_VALUE
            },

            AvailableXattrsMap = case {DirectMetadataSetPolicy, IncludeInherited} of
                {set_direct_xattr, false} ->
                    AllDirectMetadata;
                {set_direct_xattr, true} ->
                    case ShareId of
                        undefined when Client == User4Auth ->
                            % User belonging to the same space as owner of files
                            % shouldn't be able to get inherited json metadata, not set xattr
                            % or metadata set only on ancestor directories
                            % due to insufficient perms on Dir1 (exception would be space owner).
                            % But can get all other xattrs as the first found value is returned
                            % and ancestors aren't traversed further (json metadata is exceptional
                            % since it collects all ancestors jsons and merges them)
                            ForbiddenKeysForUserInSpace2 = [?JSON_METADATA_KEY, ?XATTR_1_KEY, NotSetXattrKey],
                            case lists:any(fun(Key) -> lists:member(Key, XattrsToGet) end, ForbiddenKeysForUserInSpace2) of
                                true ->
                                    throw(?ERROR_POSIX(?EACCES));
                                false ->
                                    AllDirectMetadata
                            end;
                        undefined ->
                            % When 'inherited' flag is set all ancestors json metadata
                            % are merged but for rest the first value found (which in
                            % this case is value directly set on file) is returned
                            AllDirectMetadata#{
                                ?JSON_METADATA_KEY => json_utils:merge([
                                    ?JSON_METADATA_4,
                                    ?JSON_METADATA_5
                                ]),
                                ?XATTR_1_KEY => ?XATTR_1_VALUE
                            };
                        _ ->
                            % In share mode only metadata directly set on file is available
                            AllDirectMetadata
                    end;
                {do_not_set_direct_xattr, false} ->
                    #{};
                {do_not_set_direct_xattr, true} ->
                    case ShareId of
                        undefined when Client == User4Auth ->
                            case lists:any(fun(Xattr) -> lists:member(Xattr, ?CDMI_XATTRS_KEY) end, XattrsToGet) of
                                true ->
                                    % Cdmi attrs cannot be inherited, so trying to get them when
                                    % they are not directly set result in ?ENODATA no matter the
                                    % value of 'inherited' flag (exception would be space owner).
                                    throw(?ERROR_POSIX(?ENODATA));
                                false ->
                                    % User belonging to the same space as owner of files
                                    % shouldn't be able to get any inherited metadata due to
                                    % insufficient perms on Dir1 (exception would be space owner).
                                    throw(?ERROR_POSIX(?EACCES))
                            end;
                        undefined ->
                            % User should fetch all metadata set on ancestor dirs
                            #{
                                ?JSON_METADATA_KEY => ?JSON_METADATA_4,
                                ?RDF_METADATA_KEY => ?RDF_METADATA_1,
                                ?XATTR_1_KEY => ?XATTR_1_VALUE
                            };
                        _ ->
                            #{}
                    end
            end,

            case XattrsToGet -- maps:keys(AvailableXattrsMap) of
                [] ->
                    {ok, maps:with(XattrsToGet, AvailableXattrsMap)};
                _ ->
                    ?ERROR_POSIX(?ENODATA)
            end
        catch throw:Error ->
            Error
        end
    end.


get_file_xattrs_on_provider_not_supporting_space_test(Config) ->
    [P1Node] = api_test_env:get_provider_nodes(p1, Config),
    [P2Node] = api_test_env:get_provider_nodes(p2, Config),

    SessIdP1 = api_test_env:get_user_session_id(user3, p1, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_1, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1Node, SessIdP1, FilePath),
    ?assertMatch(ok, lfm_proxy:set_xattr(P1Node, SessIdP1, {guid, FileGuid}, ?XATTR_1)),

    GetExpCallResultFun = fun(_TestCtx) -> {ok, #{?XATTR_1_KEY => ?XATTR_1_VALUE}} end,

    ClientSpec = #client_spec{
        correct = [user1, user3, user4],
        unauthorized = [nobody],
        forbidden_not_in_space = [user2]
    },

    get_metadata_test_base(
        <<"xattrs">>,
        FileType, FilePath, FileGuid, undefined,
        build_get_metadata_validate_rest_call_fun(GetExpCallResultFun, P2Node),
        build_get_metadata_validate_gs_call_fun(GetExpCallResultFun, P2Node),
        [P1Node, P2Node],
        ClientSpec,
        _DataSpec = undefined,
        _QsParams = [],
        _RandomlySelectScenario = false,
        Config
    ).


%%%===================================================================
%%% Get metadata generic functions
%%%===================================================================


%% @private
build_get_metadata_validate_rest_call_fun(GetExpResultFun) ->
    build_get_metadata_validate_rest_call_fun(GetExpResultFun, undefined).


%% @private
build_get_metadata_validate_rest_call_fun(GetExpResultFun, ProviderNotSupportingSpace) ->
    fun
        (#api_test_ctx{node = TestNode}, {ok, RespCode, _RespHeaders, RespBody}) when TestNode == ProviderNotSupportingSpace ->
            ProvId = op_test_rpc:get_provider_id(TestNode),
            ExpError = ?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(ProvId)),
            ?assertEqual({?HTTP_400_BAD_REQUEST, ExpError}, {RespCode, RespBody});
        (TestCtx, {ok, RespCode, _RespHeaders, RespBody}) ->
            case GetExpResultFun(TestCtx) of
                {ok, ExpMetadata} ->
                    ?assertEqual({?HTTP_200_OK, ExpMetadata}, {RespCode, RespBody});
                {error, _} = Error ->
                    ExpRestError = {errors:to_http_code(Error), ?REST_ERROR(Error)},
                    ?assertEqual(ExpRestError, {RespCode, RespBody})
            end
    end.


%% @private
build_get_metadata_validate_gs_call_fun(GetExpResultFun) ->
    build_get_metadata_validate_gs_call_fun(GetExpResultFun, undefined).


%% @private
build_get_metadata_validate_gs_call_fun(GetExpResultFun, ProviderNotSupportingSpace) ->
    fun
        (#api_test_ctx{node = TestNode}, Result) when TestNode == ProviderNotSupportingSpace ->
            ProvId = op_test_rpc:get_provider_id(TestNode),
            ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(ProvId), Result);
        (TestCtx, Result) ->
            case GetExpResultFun(TestCtx) of
                {ok, ExpMetadata} ->
                    ?assertEqual({ok, #{<<"metadata">> => ExpMetadata}}, Result);
                {error, _} = ExpError ->
                    ?assertEqual(ExpError, Result)
            end
    end.


%% @private
-spec get_metadata_test_base(
    MetadataType :: binary(),  %% <<"json">> | <<"rdf">> | <<"xattrs">>
    FileType :: binary(),      %% <<"dir">> | <<"file">>
    FilePath :: file_meta:path(),
    FileGuid :: file_id:file_guid(),
    ShareId :: undefined | od_share:id(),
    ValidateRestCallResultFun :: fun((api_test_ctx(), {ok, RespCode :: pos_integer(), RespBody :: term()}) -> ok),
    ValidateGsCallResultFun :: fun((api_test_ctx(), Result :: term()) -> ok),
    Providers :: [node()],
    client_spec(),
    data_spec(),
    QsParameters :: [binary()],
    RandomlySelectScenario :: boolean(),
    Config :: proplists:proplist()
) ->
    ok.
get_metadata_test_base(
    MetadataType, FileType, FilePath, FileGuid, _ShareId = undefined,
    ValidateRestCallResultFun, ValidateGsCallResultFun,
    Providers, ClientSpec, DataSpec, QsParameters, RandomlySelectScenario, Config
) ->
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ClientSpec,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_metadata_create_prepare_new_id_rest_args_fun(MetadataType, FileObjectId, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " using deprecated path rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = build_get_metadata_prepare_deprecated_path_rest_args_fun(MetadataType, FilePath, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " using deprecated id rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_metadata_prepare_deprecated_id_rest_args_fun(MetadataType, FileObjectId, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = build_get_metadata_prepare_gs_args_fun(MetadataType, FileGuid, private),
                    validate_result_fun = ValidateGsCallResultFun
                }
            ],
            randomly_select_scenarios = RandomlySelectScenario,
            data_spec = DataSpec
        }
    ]));
get_metadata_test_base(
    MetadataType, FileType, _FilePath, FileGuid, ShareId,
    ValidateRestCallResultFun, ValidateGsCallResultFun,
    Providers, ClientSpec, DataSpec, QsParameters, RandomlySelectScenario, Config
) ->
    FileShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, FileShareObjectId} = file_id:guid_to_objectid(FileShareGuid),

    ?assert(onenv_api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ClientSpec,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from shared ", FileType/binary, " using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_metadata_create_prepare_new_id_rest_args_fun(MetadataType, FileShareObjectId, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from shared ", FileType/binary, " using deprecated id rest endpoint">>,
                    type = rest,
                    prepare_args_fun = build_get_metadata_prepare_deprecated_id_rest_args_fun(MetadataType, FileShareObjectId, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from shared ", FileType/binary, " using gs public api">>,
                    type = gs,
                    prepare_args_fun = build_get_metadata_prepare_gs_args_fun(MetadataType, FileShareGuid, public),
                    validate_result_fun = ValidateGsCallResultFun
                },
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from shared ", FileType/binary, " using gs private api">>,
                    type = gs_with_shared_guid_and_aspect_private,
                    prepare_args_fun = build_get_metadata_prepare_gs_args_fun(MetadataType, FileShareGuid, private),
                    validate_result_fun = fun(_, Result) ->
                        ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                    end
                }
            ],
            randomly_select_scenarios = RandomlySelectScenario,
            data_spec = DataSpec
        }
    ])).


%% @private
build_get_metadata_create_prepare_new_id_rest_args_fun(MetadataType, FileObjectId, QsParams) ->
    build_get_metadata_prepare_rest_args_fun(new_id, MetadataType, FileObjectId, QsParams).


%% @private
build_get_metadata_prepare_deprecated_path_rest_args_fun(MetadataType, FilePath, QsParams) ->
    build_get_metadata_prepare_rest_args_fun(deprecated_path, MetadataType, FilePath, QsParams).


%% @private
build_get_metadata_prepare_deprecated_id_rest_args_fun(MetadataType, FileObjectId, QsParams) ->
    build_get_metadata_prepare_rest_args_fun(deprecated_id, MetadataType, FileObjectId, QsParams).


%% @private
build_get_metadata_prepare_rest_args_fun(Endpoint, MetadataType, ValidId, QsParams) ->
    fun(#api_test_ctx{data = Data0}) ->
        Data1 = api_test_utils:ensure_defined(Data0, #{}),
        {Id, Data2} = api_test_utils:maybe_substitute_bad_id(ValidId, Data1),

        Id = case maps:find(bad_id, Data1) of
            {ok, BadId} -> BadId;
            error -> ValidId
        end,
        Path = case Endpoint of
            new_id -> ?NEW_ID_METADATA_REST_PATH(Id, MetadataType);
            deprecated_path -> ?DEPRECATED_PATH_METADATA_REST_PATH(Id, MetadataType);
            deprecated_id -> ?DEPRECATED_ID_METADATA_REST_PATH(Id, MetadataType)
        end,
        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(Path, maps:with(QsParams, Data2))
        }
    end.


%% @private
build_get_metadata_prepare_gs_args_fun(MetadataType, FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data0}) ->
        {GriId, Data1} = api_test_utils:maybe_substitute_bad_id(FileGuid, Data0),

        Aspect = case MetadataType of
            <<"json">> -> json_metadata;
            <<"rdf">> -> rdf_metadata;
            <<"xattrs">> -> xattrs
        end,
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = GriId, aspect = Aspect, scope = Scope},
            data = Data1
        }
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        application:start(ssl),
        hackney:start(),
        api_test_env:init(onenv_test_utils:prepare_base_test_config(NewConfig))
    end,
    test_config:set_many(Config, [
        {add_envs, [op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]]},
        {set_onenv_scenario, ["api_tests"]}, % name of yaml file in test_distributed/onenv_scenarios
        {set_posthook, Posthook}
    ]).


end_per_suite(_Config) ->
    hackney:stop(),
    application:stop(ssl).


init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 30}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
set_all_metadata_types(Node, SessionId, Guid, set_1) ->
    ?assertMatch(ok, lfm_proxy:set_metadata(Node, SessionId, {guid, Guid}, json, ?JSON_METADATA_4, [])),
    ?assertMatch(ok, lfm_proxy:set_metadata(Node, SessionId, {guid, Guid}, rdf, ?RDF_METADATA_1, [])),
    ?assertMatch(ok, lfm_proxy:set_mimetype(Node, SessionId, {guid, Guid}, ?MIMETYPE_1)),
    ?assertMatch(ok, lfm_proxy:set_transfer_encoding(Node, SessionId, {guid, Guid}, ?TRANSFER_ENCODING_1)),
    ?assertMatch(ok, lfm_proxy:set_cdmi_completion_status(Node, SessionId, {guid, Guid}, ?CDMI_COMPLETION_STATUS_1)),
    ?assertMatch(ok, lfm_proxy:set_xattr(Node, SessionId, {guid, Guid}, ?XATTR_1)),
    ?assertMatch(ok, lfm_proxy:set_acl(Node, SessionId, {guid, Guid}, acl:from_json(?ACL_1, cdmi)));
set_all_metadata_types(Node, SessionId, Guid, set_2) ->
    ?assertMatch(ok, lfm_proxy:set_metadata(Node, SessionId, {guid, Guid}, json, ?JSON_METADATA_5, [])),
    ?assertMatch(ok, lfm_proxy:set_metadata(Node, SessionId, {guid, Guid}, rdf, ?RDF_METADATA_2, [])),
    ?assertMatch(ok, lfm_proxy:set_mimetype(Node, SessionId, {guid, Guid}, ?MIMETYPE_2)),
    ?assertMatch(ok, lfm_proxy:set_transfer_encoding(Node, SessionId, {guid, Guid}, ?TRANSFER_ENCODING_2)),
    ?assertMatch(ok, lfm_proxy:set_cdmi_completion_status(Node, SessionId, {guid, Guid}, ?CDMI_COMPLETION_STATUS_2)),
    ?assertMatch(ok, lfm_proxy:set_xattr(Node, SessionId, {guid, Guid}, ?XATTR_2)),
    ?assertMatch(ok, lfm_proxy:set_acl(Node, SessionId, {guid, Guid}, acl:from_json(?ACL_2, cdmi))).
