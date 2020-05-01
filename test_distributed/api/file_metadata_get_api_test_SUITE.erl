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


%%%===================================================================
%%% Get Rdf metadata test functions
%%%===================================================================


get_file_rdf_metadata_with_rdf_set_test(Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    get_rdf_metadata_test_base(FileType, set_rdf, normal_mode, Config).


get_file_rdf_metadata_without_rdf_set_test(Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    get_rdf_metadata_test_base(FileType, do_not_set_rdf, normal_mode, Config).


get_shared_file_rdf_metadata_with_rdf_set_test(Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    get_rdf_metadata_test_base(FileType, set_rdf, share_mode, Config).


get_shared_file_rdf_metadata_without_rdf_set_test(Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    get_rdf_metadata_test_base(FileType, do_not_set_rdf, share_mode, Config).


%% @private
get_rdf_metadata_test_base(FileType, SetRdfPolicy, TestMode, Config) ->
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    FilePath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath),

    GetExpCallResultFun = case SetRdfPolicy of
        set_rdf ->
            lfm_proxy:set_metadata(P1, SessIdP1, {guid, FileGuid}, rdf, ?RDF_METADATA_1, []),
            fun(_TestCtx) -> {ok, ?RDF_METADATA_1} end;
        do_not_set_rdf ->
            fun(_TestCtx) -> ?ERROR_POSIX(?ENODATA) end
    end,
    {ShareId, ClientSpec} = case TestMode of
        share_mode ->
            {ok, Id} = lfm_proxy:create_share(P1, SessIdP1, {guid, FileGuid}, <<"share">>),
            {Id, ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config)};
        normal_mode ->
            {undefined, ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config)}
    end,

    api_test_utils:wait_for_file_sync(P2, SessIdP2, FileGuid),

    get_metadata_test_base(
        <<"rdf">>,
        FileType, FilePath, FileGuid, ShareId,
        create_validate_get_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_get_metadata_gs_call_fun(GetExpCallResultFun),
        _Providers = ?config(op_worker_nodes, Config),
        ClientSpec,
        _DataSpec = undefined,
        _QsParams = [],
        Config
    ).


get_file_rdf_metadata_on_provider_not_supporting_space_test(Config) ->
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_1, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath),
    lfm_proxy:set_metadata(P1, SessIdP1, {guid, FileGuid}, rdf, ?RDF_METADATA_1, []),

    GetExpCallResultFun = fun(_TestCtx) -> {ok, ?RDF_METADATA_1} end,

    get_metadata_test_base(
        <<"rdf">>,
        FileType, FilePath, FileGuid, undefined,
        create_validate_get_metadata_rest_call_fun(GetExpCallResultFun, P2),
        create_validate_get_metadata_gs_call_fun(GetExpCallResultFun, P2),
        Providers,
        ?CLIENT_SPEC_FOR_SPACE_1_SCENARIOS(Config),
        _DataSpec = undefined,
        _QsParams = [],
        Config
    ).


%%%===================================================================
%%% Get Json metadata test functions
%%%===================================================================


get_file_json_metadata_with_json_set_test(Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    get_json_metadata_test_base(FileType, set_direct_json, normal_mode, Config).


get_file_json_metadata_without_json_set_test(Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    get_json_metadata_test_base(FileType, do_not_set_direct_json, normal_mode, Config).


get_shared_file_json_metadata_with_json_set_test(Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    get_json_metadata_test_base(FileType, set_direct_json, share_mode, Config).


get_shared_file_json_metadata_without_json_set_test(Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    get_json_metadata_test_base(FileType, do_not_set_direct_json, share_mode, Config).


%% @private
get_json_metadata_test_base(FileType, SetDirectJsonPolicy, TestMode, Config) ->
    {FileLayer5Path, FileLayer5Guid, ShareId} = create_get_json_metadata_tests_env(
        FileType, SetDirectJsonPolicy, TestMode, Config
    ),
    GetExpCallResultFun = create_get_json_call_exp_result_fun(ShareId, SetDirectJsonPolicy),

    ClientSpec = case TestMode of
        share_mode -> ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config);
        normal_mode -> ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config)
    end,
    DataSpec = #data_spec{
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
            {<<"filter_type">>, 100, {rest, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])}},
            {<<"filter_type">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"filter_type">>)}},
            {<<"filter">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"filter">>)}}
        ]
    },

    get_metadata_test_base(
        <<"json">>,
        FileType, FileLayer5Path, FileLayer5Guid, ShareId,
        create_validate_get_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_get_metadata_gs_call_fun(GetExpCallResultFun),
        _Providers = ?config(op_worker_nodes, Config),
        ClientSpec,
        DataSpec,
        QsParams,
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
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    TopDirPath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, TopDirGuid} = lfm_proxy:mkdir(P1, SessIdP1, TopDirPath, 8#777),
    lfm_proxy:set_metadata(P1, SessIdP1, {guid, TopDirGuid}, json, ?JSON_METADATA_1, []),

    DirLayer2Path = filename:join([TopDirPath, <<"dir_layer_2">>]),
    {ok, DirLayer2Guid} = lfm_proxy:mkdir(P1, SessIdP1, DirLayer2Path, 8#717),
    lfm_proxy:set_metadata(P1, SessIdP1, {guid, DirLayer2Guid}, json, ?JSON_METADATA_2, []),

    DirLayer3Path = filename:join([DirLayer2Path, <<"dir_layer_3">>]),
    {ok, DirLayer3Guid} = lfm_proxy:mkdir(P1, SessIdP1, DirLayer3Path, 8#777),
    lfm_proxy:set_metadata(P1, SessIdP1, {guid, DirLayer3Guid}, json, ?JSON_METADATA_3, []),

    DirLayer4Path = filename:join([DirLayer3Path, <<"dir_layer_4">>]),
    {ok, DirLayer4Guid} = lfm_proxy:mkdir(P1, SessIdP1, DirLayer4Path, 8#777),
    lfm_proxy:set_metadata(P1, SessIdP1, {guid, DirLayer4Guid}, json, ?JSON_METADATA_4, []),
    ShareId = case TestMode of
        share_mode ->
            {ok, Id} = lfm_proxy:create_share(P1, SessIdP1, {guid, DirLayer4Guid}, <<"share">>),
            Id;
        normal_mode ->
            undefined
    end,

    FileLayer5Path = filename:join([DirLayer4Path, ?RANDOM_FILE_NAME()]),
    {ok, FileLayer5Guid} = api_test_utils:create_file(FileType, P1, SessIdP1, FileLayer5Path),
    case SetJsonPolicy of
        set_direct_json ->
            lfm_proxy:set_metadata(P1, SessIdP1, {guid, FileLayer5Guid}, json, ?JSON_METADATA_5, []);
        do_not_set_direct_json ->
            ok
    end,

    api_test_utils:wait_for_file_sync(P2, SessIdP2, FileLayer5Guid),

    {FileLayer5Path, FileLayer5Guid, ShareId}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates function returning expected result from get json metadata
%% rest/gs call taking into account env created by
%% create_get_json_metadata_tests_env/4.
%% @end
%%--------------------------------------------------------------------
create_get_json_call_exp_result_fun(ShareId, SetDirectJsonPolicy) ->
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
                        undefined when Client == ?USER_IN_SPACE_2_AUTH ->
                            % User belonging to the same space as owner of files
                            % shouldn't be able to get inherited metadata due to
                            % insufficient perms on DirLayer2
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
                        undefined when Client == ?USER_IN_SPACE_2_AUTH ->
                            % User belonging to the same space as owner of files
                            % shouldn't be able to get inherited metadata due to
                            % insufficient perms on DirLayer2
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
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_1, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath),
    lfm_proxy:set_metadata(P1, SessIdP1, {guid, FileGuid}, json, ?JSON_METADATA_2, []),

    GetExpCallResultFun = fun(_TestCtx) -> {ok, ?JSON_METADATA_2} end,

    get_metadata_test_base(
        <<"json">>,
        FileType, FilePath, FileGuid, undefined,
        create_validate_get_metadata_rest_call_fun(GetExpCallResultFun, P2),
        create_validate_get_metadata_gs_call_fun(GetExpCallResultFun, P2),
        Providers,
        ?CLIENT_SPEC_FOR_SPACE_1_SCENARIOS(Config),
        _DataSpec = undefined,
        _QsParams = [],
        Config
    ).


%%%===================================================================
%%% Get xattrs functions
%%%===================================================================



get_file_xattrs_with_xattrs_set_test(Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    get_xattrs_test_base(FileType, set_direct_xattr, normal_mode, Config).


get_file_xattrs_without_xattrs_set_test(Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    get_xattrs_test_base(FileType, do_not_set_direct_xattr, normal_mode, Config).


get_shared_file_xattrs_with_xattrs_set_test(Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    get_xattrs_test_base(FileType, set_direct_xattr, share_mode, Config).


get_shared_file_xattrs_without_xattrs_set_test(Config) ->
    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    get_xattrs_test_base(FileType, do_not_set_direct_xattr, share_mode, Config).


%% @private
get_xattrs_test_base(FileType, SetDirectXattrsPolicy, TestMode, Config) ->
    {FileLayer3Path, FileLayer3Guid, ShareId} = create_get_xattrs_tests_env(
        FileType, SetDirectXattrsPolicy, TestMode, Config
    ),
    NotSetXattrKey = <<"not_set_xattr">>,
    GetExpCallResultFun = create_get_xattrs_call_exp_result_fun(ShareId, SetDirectXattrsPolicy, NotSetXattrKey),

    ClientSpec = case TestMode of
        share_mode -> ?CLIENT_SPEC_FOR_SHARE_SCENARIOS(Config);
        normal_mode -> ?CLIENT_SPEC_FOR_SPACE_2_SCENARIOS(Config)
    end,
    DataSpec = #data_spec{
        optional = QsParams = [<<"attribute">>, <<"inherited">>, <<"show_internal">>],
        correct_values = #{
            <<"attribute">> => [
                % Xattr name with prefixes 'cdmi_' and 'onedata_' should be forbidden
                % with exception of those listed in ?ALL_XATTRS_KEYS. Nonetheless that is
                % checked not in middleware but in lfm and depends on whether request will
                % arrive there. That is why, depending where request was rejected, different
                % error than ?EPERM may be returned
                <<"cdmi_attr">>, <<"onedata_attr">>,
                NotSetXattrKey
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
    },

    get_metadata_test_base(
        <<"xattrs">>,
        FileType, FileLayer3Path, FileLayer3Guid, ShareId,
        create_validate_get_metadata_rest_call_fun(GetExpCallResultFun),
        create_validate_get_metadata_gs_call_fun(GetExpCallResultFun),
        _Providers = ?config(op_worker_nodes, Config),
        ClientSpec,
        DataSpec,
        QsParams,
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
    [P2, P1] = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),
    SessIdP2 = ?USER_IN_BOTH_SPACES_SESS_ID(P2, Config),

    TopDirPath = filename:join(["/", ?SPACE_2, ?RANDOM_FILE_NAME()]),
    {ok, TopDirGuid} = lfm_proxy:mkdir(P1, SessIdP1, TopDirPath, 8#777),
    set_all_metadata_types(P1, SessIdP1, TopDirGuid, set_1),

    DirLayer2Path = filename:join([TopDirPath, <<"dir_layer_2">>]),
    {ok, DirLayer2Guid} = lfm_proxy:mkdir(P1, SessIdP1, DirLayer2Path, 8#717),
    ShareId = case TestMode of
        share_mode ->
            {ok, Id} = lfm_proxy:create_share(P1, SessIdP1, {guid, DirLayer2Guid}, <<"share">>),
            Id;
        normal_mode ->
            undefined
    end,

    FileLayer3Path = filename:join([DirLayer2Path, ?RANDOM_FILE_NAME()]),
    {ok, FileLayer3Guid} = api_test_utils:create_file(FileType, P1, SessIdP1, FileLayer3Path),
    case SetXattrsPolicy of
        set_direct_xattr ->
            set_all_metadata_types(P1, SessIdP1, FileLayer3Guid, set_2);
        do_not_set_direct_xattr ->
            ok
    end,

    api_test_utils:wait_for_file_sync(P2, SessIdP2, FileLayer3Guid),

    {FileLayer3Path, FileLayer3Guid, ShareId}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates function returning expected result from get xattrs
%% rest/gs call taking into account env created by
%% create_get_xattrs_tests_env/4.
%% @end
%%--------------------------------------------------------------------
create_get_xattrs_call_exp_result_fun(ShareId, DirectMetadataSetPolicy, NotSetXattrKey) ->
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
                        undefined when Client == ?USER_IN_SPACE_2_AUTH ->
                            % User belonging to the same space as owner of files
                            % shouldn't be able to get inherited json metadata, not set xattr
                            % or metadata set only on ancestor directories
                            % due to insufficient perms on Dir1. But can get all other xattrs
                            % as the first found value is returned and ancestors aren't
                            % traversed further (json metadata is exceptional since it
                            % collects all ancestors jsons and merges them)
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
                        undefined when Client == ?USER_IN_SPACE_2_AUTH ->
                            case lists:any(fun(Xattr) -> lists:member(Xattr, ?CDMI_XATTRS_KEY) end, XattrsToGet) of
                                true ->
                                    % Cdmi attrs cannot be inherited, so trying to get them when
                                    % they are not directly set result in ?ENODATA no matter the
                                    % value of 'inherited' flag
                                    throw(?ERROR_POSIX(?ENODATA));
                                false ->
                                    % User belonging to the same space as owner of files
                                    % shouldn't be able to get any inherited metadata due to
                                    % insufficient perms on Dir1
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
    [P2, P1] = Providers = ?config(op_worker_nodes, Config),
    SessIdP1 = ?USER_IN_BOTH_SPACES_SESS_ID(P1, Config),

    FileType = api_test_utils:randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_1, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = api_test_utils:create_file(FileType, P1, SessIdP1, FilePath),
    ?assertMatch(ok, lfm_proxy:set_xattr(P1, SessIdP1, {guid, FileGuid}, ?XATTR_1)),

    GetExpCallResultFun = fun(_TestCtx) -> {ok, #{?XATTR_1_KEY => ?XATTR_1_VALUE}} end,

    get_metadata_test_base(
        <<"xattrs">>,
        FileType, FilePath, FileGuid, undefined,
        create_validate_get_metadata_rest_call_fun(GetExpCallResultFun, P2),
        create_validate_get_metadata_gs_call_fun(GetExpCallResultFun, P2),
        Providers,
        ?CLIENT_SPEC_FOR_SPACE_1_SCENARIOS(Config),
        _DataSpec = undefined,
        _QsParams = [],
        Config
    ).


%%%===================================================================
%%% Get metadata generic functions
%%%===================================================================


%% @private
create_validate_get_metadata_rest_call_fun(GetExpResultFun) ->
    create_validate_get_metadata_rest_call_fun(GetExpResultFun, undefined).


%% @private
create_validate_get_metadata_rest_call_fun(GetExpResultFun, ProviderNotSupportingSpace) ->
    fun
        (#api_test_ctx{node = TestNode}, {ok, RespCode, RespBody}) when TestNode == ProviderNotSupportingSpace ->
            ProviderDomain = ?GET_DOMAIN_BIN(ProviderNotSupportingSpace),
            ExpError = ?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(ProviderDomain)),
            ?assertEqual({?HTTP_400_BAD_REQUEST, ExpError}, {RespCode, RespBody});
        (TestCtx, {ok, RespCode, RespBody}) ->
            case GetExpResultFun(TestCtx) of
                {ok, ExpMetadata} ->
                    ?assertEqual({?HTTP_200_OK, ExpMetadata}, {RespCode, RespBody});
                {error, _} = Error ->
                    ExpRestError = {errors:to_http_code(Error), ?REST_ERROR(Error)},
                    ?assertEqual(ExpRestError, {RespCode, RespBody})
            end
    end.


%% @private
create_validate_get_metadata_gs_call_fun(GetExpResultFun) ->
    create_validate_get_metadata_gs_call_fun(GetExpResultFun, undefined).


%% @private
create_validate_get_metadata_gs_call_fun(GetExpResultFun, ProviderNotSupportingSpace) ->
    fun
        (#api_test_ctx{node = TestNode}, Result) when TestNode == ProviderNotSupportingSpace ->
            ProviderDomain = ?GET_DOMAIN_BIN(ProviderNotSupportingSpace),
            ExpError = ?ERROR_SPACE_NOT_SUPPORTED_BY(ProviderDomain),
            ?assertEqual(ExpError, Result);
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
    Config :: proplists:proplist()
) ->
    ok.
get_metadata_test_base(
    MetadataType, FileType, FilePath, FileGuid, _ShareId = undefined,
    ValidateRestCallResultFun, ValidateGsCallResultFun,
    Providers, ClientSpec, DataSpec, QsParameters, Config
) ->
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ClientSpec,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_get_metadata_rest_args_fun(MetadataType, FileObjectId, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " using deprecated path rest endpoint">>,
                    type = rest_with_file_path,
                    prepare_args_fun = create_prepare_deprecated_path_get_metadata_rest_args_fun(MetadataType, FilePath, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " using deprecated id rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_deprecated_id_get_metadata_rest_args_fun(MetadataType, FileObjectId, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " using gs api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_get_metadata_gs_args_fun(MetadataType, FileGuid, private),
                    validate_result_fun = ValidateGsCallResultFun
                }
            ],
            data_spec = DataSpec
        }
    ]));
get_metadata_test_base(
    MetadataType, FileType, _FilePath, FileGuid, ShareId,
    ValidateRestCallResultFun, ValidateGsCallResultFun,
    Providers, ClientSpec, DataSpec, QsParameters, Config
) ->
    FileShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, FileShareObjectId} = file_id:guid_to_objectid(FileShareGuid),

    ?assert(api_test_runner:run_tests(Config, [
        #suite_spec{
            target_nodes = Providers,
            client_spec = ClientSpec,
            scenario_templates = [
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from shared ", FileType/binary, " using rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_new_id_get_metadata_rest_args_fun(MetadataType, FileShareObjectId, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from shared ", FileType/binary, " using deprecated id rest endpoint">>,
                    type = rest,
                    prepare_args_fun = create_prepare_deprecated_id_get_metadata_rest_args_fun(MetadataType, FileShareObjectId, QsParameters),
                    validate_result_fun = ValidateRestCallResultFun
                },
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from shared ", FileType/binary, " using gs public api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_get_metadata_gs_args_fun(MetadataType, FileShareGuid, public),
                    validate_result_fun = ValidateGsCallResultFun
                },
                #scenario_template{
                    name = <<"Get ", MetadataType/binary, " metadata from shared ", FileType/binary, " using gs private api">>,
                    type = gs,
                    prepare_args_fun = create_prepare_get_metadata_gs_args_fun(MetadataType, FileShareGuid, private),
                    validate_result_fun = fun(_, Result) ->
                        ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                    end
                }
            ],
            data_spec = DataSpec
        }
    ])).


%% @private
create_prepare_new_id_get_metadata_rest_args_fun(MetadataType, FileObjectId, QsParams) ->
    create_prepare_get_metadata_rest_args_fun(
        ?NEW_ID_METADATA_REST_PATH(FileObjectId, MetadataType),
        QsParams
    ).


%% @private
create_prepare_deprecated_path_get_metadata_rest_args_fun(MetadataType, FilePath, QsParams) ->
    create_prepare_get_metadata_rest_args_fun(
        ?DEPRECATED_PATH_METADATA_REST_PATH(FilePath, MetadataType),
        QsParams
    ).


%% @private
create_prepare_deprecated_id_get_metadata_rest_args_fun(MetadataType, FileObjectId, QsParams) ->
    create_prepare_get_metadata_rest_args_fun(
        ?DEPRECATED_ID_METADATA_REST_PATH(FileObjectId, MetadataType),
        QsParams
    ).


%% @private
create_prepare_get_metadata_rest_args_fun(RestPath, QsParams) ->
    fun(#api_test_ctx{data = Data}) ->
        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                RestPath,
                maps:with(QsParams, utils:ensure_defined(Data, undefined, #{}))
            )
        }
    end.


%% @private
create_prepare_get_metadata_gs_args_fun(MetadataType, FileGuid, Scope) ->
    fun(#api_test_ctx{data = Data}) ->
        Aspect = case MetadataType of
            <<"json">> -> json_metadata;
            <<"rdf">> -> rdf_metadata;
            <<"xattrs">> -> xattrs
        end,
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = FileGuid, aspect = Aspect, scope = Scope},
            data = Data
        }
    end.


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        ),
        initializer:mock_auth_manager(NewConfig3, _CheckIfUserIsSupported = true),
        lists:foreach(fun(Worker) ->
            % TODO VFS-6251
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, 20),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, 20),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, 20),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, 20),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, 20), % TODO - change to 2 seconds
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off),
            test_utils:set_env(Worker, ?APP_NAME, public_block_size_treshold, 0),
            test_utils:set_env(Worker, ?APP_NAME, public_block_percent_treshold, 0)
        end, ?config(op_worker_nodes, NewConfig3)),
        application:start(ssl),
        hackney:start(),
        NewConfig3
    end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].


end_per_suite(Config) ->
    hackney:stop(),
    application:stop(ssl),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:teardown_storage(Config).


init_per_testcase(_Case, Config) ->
    initializer:mock_share_logic(Config),
    ct:timetrap({minutes, 30}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
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
