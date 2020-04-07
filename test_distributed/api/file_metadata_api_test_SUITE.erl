%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests concerning file metadata basic API (REST + gs).
%%% @end
%%%-------------------------------------------------------------------
-module(file_metadata_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("api_test_utils.hrl").
-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/aai/aai.hrl").
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
    get_rdf_metadata_test/1,
    get_json_metadata_test/1,
    get_xattr_metadata_test/1,
    set_rdf_metadata_test/1,
    set_json_metadata_test/1
]).

all() ->
    ?ALL([
        get_rdf_metadata_test,
        get_json_metadata_test,
        get_xattr_metadata_test,
        set_rdf_metadata_test,
        set_json_metadata_test
    ]).


-define(ATTEMPTS, 30).
-define(SCENARIO_NAME, atom_to_binary(?FUNCTION_NAME, utf8)).


-define(SPACE_1, <<"space1">>).
-define(SPACE_2, <<"space2">>).

-define(USER_IN_SPACE_1, <<"user1">>).
-define(USER_IN_SPACE_1_AUTH, ?USER(?USER_IN_SPACE_1)).

-define(USER_IN_SPACE_2, <<"user3">>).
-define(USER_IN_SPACE_2_AUTH, ?USER(?USER_IN_SPACE_2)).

-define(USER_IN_BOTH_SPACES, <<"user2">>).
-define(USER_IN_BOTH_SPACES_AUTH, ?USER(?USER_IN_BOTH_SPACES)).

-define(JSON_METADATA_1, #{
    <<"attr1">> => 1,
    <<"attr2">> => #{
        <<"attr21">> => <<"val21">>,
        <<"attr22">> => <<"val22">>
    }
}).
-define(JSON_METADATA_2, <<"metadata">>).
-define(JSON_METADATA_3, #{
    <<"attr2">> => #{
        <<"attr22">> => [1, 2, 3],
        <<"attr23">> => <<"val23">>
    },
    <<"attr3">> => <<"val3">>
}).
-define(JSON_METADATA_4, #{
    <<"attr3">> => #{
        <<"attr31">> => null
    }
}).
-define(JSON_METADATA_5, #{
    <<"attr3">> => #{
        <<"attr32">> => [<<"a">>, <<"b">>, <<"c">>]
    }
}).

-define(RDF_METADATA_1, <<"<rdf>metadata_1</rdf>">>).
-define(RDF_METADATA_2, <<"<rdf>metadata_2</rdf>">>).

-define(MIMETYPE_1, <<"text/plain">>).
-define(MIMETYPE_2, <<"text/javascript">>).

-define(TRANSFER_ENCODING_1, <<"utf-8">>).
-define(TRANSFER_ENCODING_2, <<"base64">>).

-define(CDMI_COMPLETION_STATUS_1, <<"Completed">>).
-define(CDMI_COMPLETION_STATUS_2, <<"Processing">>).

-define(XATTR_1_KEY, <<"custom_xattr1">>).
-define(XATTR_1_VALUE, <<"value1">>).
-define(XATTR_1, #xattr{name = ?XATTR_1_KEY, value = ?XATTR_1_VALUE}).
-define(XATTR_2_KEY, <<"custom_xattr2">>).
-define(XATTR_2_VALUE, <<"value2">>).
-define(XATTR_2, #xattr{name = ?XATTR_2_KEY, value = ?XATTR_2_VALUE}).

-define(ACL_1, [#{
    <<"acetype">> => <<"0x", (integer_to_binary(?allow_mask, 16))/binary>>,
    <<"identifier">> => ?everyone,
    <<"aceflags">> => <<"0x", (integer_to_binary(?no_flags_mask, 16))/binary>>,
    <<"acemask">> => <<"0x", (integer_to_binary(?all_container_perms_mask, 16))/binary>>
}]).
-define(ACL_2, [#{
    <<"acetype">> => <<"0x", (integer_to_binary(?allow_mask, 16))/binary>>,
    <<"identifier">> => ?everyone,
    <<"aceflags">> => <<"0x", (integer_to_binary(?no_flags_mask, 16))/binary>>,
    <<"acemask">> => <<"0x", (integer_to_binary(
        ?read_attributes_mask bor ?read_metadata_mask bor ?read_acl_mask,
        16
    ))/binary>>
}]).

-define(CDMI_XATTRS_KEY, [
    ?ACL_KEY,
    ?MIMETYPE_KEY,
    ?TRANSFER_ENCODING_KEY,
    ?CDMI_COMPLETION_STATUS_KEY
]).
-define(ONEDATA_XATTRS_KEY, [
    ?JSON_METADATA_KEY,
    ?RDF_METADATA_KEY
]).
-define(ALL_XATTRS_KEYS, [
    ?ACL_KEY,
    ?MIMETYPE_KEY,
    ?TRANSFER_ENCODING_KEY,
    ?CDMI_COMPLETION_STATUS_KEY,
    ?JSON_METADATA_KEY,
    ?RDF_METADATA_KEY,
    ?XATTR_1_KEY,
    ?XATTR_2_KEY
]).

-define(ALL_METADATA_SET_1, #{
    ?ACL_KEY => ?ACL_1,
    ?MIMETYPE_KEY => ?MIMETYPE_1,
    ?TRANSFER_ENCODING_KEY => ?TRANSFER_ENCODING_1,
    ?CDMI_COMPLETION_STATUS_KEY => ?CDMI_COMPLETION_STATUS_1,
    ?JSON_METADATA_KEY => ?JSON_METADATA_4,
    ?RDF_METADATA_KEY => ?RDF_METADATA_1,
    ?XATTR_1_KEY => ?XATTR_1_VALUE
}).
-define(ALL_METADATA_SET_2, #{
    ?ACL_KEY => ?ACL_2,
    ?MIMETYPE_KEY => ?MIMETYPE_2,
    ?TRANSFER_ENCODING_KEY => ?TRANSFER_ENCODING_2,
    ?CDMI_COMPLETION_STATUS_KEY => ?CDMI_COMPLETION_STATUS_2,
    ?JSON_METADATA_KEY => ?JSON_METADATA_5,
    ?RDF_METADATA_KEY => ?RDF_METADATA_2,
    ?XATTR_2_KEY => ?XATTR_2_VALUE
}).


%%%===================================================================
%%% Test functions
%%%===================================================================


get_rdf_metadata_test(Config) ->
    [Provider2, Provider1] = ?config(op_worker_nodes, Config),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    UserSessId = GetSessionFun(Provider2),

    RootDirPath = filename:join(["/", ?SPACE_2, ?SCENARIO_NAME]),
    {ok, RootDirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, RootDirPath, 8#777),
    {ok, ShareId} = lfm_proxy:create_share(Provider2, UserSessId, {guid, RootDirGuid}, <<"share">>),

    DirWithoutRdfMetadataPath = filename:join([RootDirPath, <<"dir_without_rdf_metadata">>]),
    {ok, DirWithoutRdfMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithoutRdfMetadataPath, 8#777),

    DirWithRdfMetadataPath = filename:join([RootDirPath, <<"dir_with_rdf_metadata">>]),
    {ok, DirWithRdfMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithRdfMetadataPath, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, DirWithRdfMetadataGuid}, rdf, ?RDF_METADATA_1, []),

    RegularFileWithoutRdfMetadataPath = filename:join([RootDirPath, <<"file_without_rdf_metadata">>]),
    {ok, RegularFileWithoutRdfMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithoutRdfMetadataPath, 8#777),

    RegularFileWithRdfMetadataPath = filename:join([RootDirPath, <<"file_with_rdf_metadata">>]),
    {ok, RegularFileWithRdfMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithRdfMetadataPath, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, RegularFileWithRdfMetadataGuid}, rdf, ?RDF_METADATA_1, []),

    % Wait for metadata sync between providers
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(Provider1, GetSessionFun(Provider1), {guid, RegularFileWithRdfMetadataGuid}),
        ?ATTEMPTS
    ),

    GetExpectedResultFun = fun(_TestCtx, _ShareId, DirectMetadataSet) ->
        case DirectMetadataSet of
            true ->
                {ok, ?RDF_METADATA_1};
            false ->
                ?ERROR_POSIX(?ENODATA)
        end
    end,

    get_metadata_test_base(
        Config,
        <<"rdf">>,
        GetExpectedResultFun,
        undefined,
        ShareId,
        [
            {
                <<"dir">>,
                DirWithoutRdfMetadataPath, DirWithoutRdfMetadataGuid,
                DirWithRdfMetadataPath, DirWithRdfMetadataGuid
            },
            {
                <<"file">>,
                RegularFileWithoutRdfMetadataPath, RegularFileWithoutRdfMetadataGuid,
                RegularFileWithRdfMetadataPath, RegularFileWithRdfMetadataGuid
            }
        ]
    ).


get_json_metadata_test(Config) ->
    [Provider2, Provider1] = ?config(op_worker_nodes, Config),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    UserSessId = GetSessionFun(Provider2),

    RootDirPath = filename:join(["/", ?SPACE_2, ?SCENARIO_NAME]),
    {ok, RootDirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, RootDirPath, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, RootDirGuid}, json, ?JSON_METADATA_1, []),

    DirLayer2Path = filename:join([RootDirPath, <<"dir_layer_2">>]),
    {ok, DirLayer2Guid} = lfm_proxy:mkdir(Provider2, UserSessId, DirLayer2Path, 8#717),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, DirLayer2Guid}, json, ?JSON_METADATA_2, []),

    DirLayer3Path = filename:join([DirLayer2Path, <<"dir_layer_3">>]),
    {ok, DirLayer3Guid} = lfm_proxy:mkdir(Provider2, UserSessId, DirLayer3Path, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, DirLayer3Guid}, json, ?JSON_METADATA_3, []),

    DirLayer4Path = filename:join([DirLayer3Path, <<"dir_layer_4">>]),
    {ok, DirLayer4Guid} = lfm_proxy:mkdir(Provider2, UserSessId, DirLayer4Path, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, DirLayer4Guid}, json, ?JSON_METADATA_4, []),
    {ok, ShareId} = lfm_proxy:create_share(Provider2, UserSessId, {guid, DirLayer4Guid}, <<"share">>),

    DirWithoutJsonMetadataPath = filename:join([DirLayer4Path, <<"dir_without_json_metadata">>]),
    {ok, DirWithoutJsonMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithoutJsonMetadataPath, 8#777),

    DirWithJsonMetadataPath = filename:join([DirLayer4Path, <<"dir_with_json_metadata">>]),
    {ok, DirWithJsonMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithJsonMetadataPath, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, DirWithJsonMetadataGuid}, json, ?JSON_METADATA_5, []),

    RegularFileWithoutJsonMetadataPath = filename:join([DirLayer4Path, <<"file_without_json_metadata">>]),
    {ok, RegularFileWithoutJsonMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithoutJsonMetadataPath, 8#777),

    RegularFileWithJsonMetadataPath = filename:join([DirLayer4Path, <<"file_with_json_metadata">>]),
    {ok, RegularFileWithJsonMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithJsonMetadataPath, 8#777),
    lfm_proxy:set_metadata(Provider2, UserSessId, {guid, RegularFileWithJsonMetadataGuid}, json, ?JSON_METADATA_5, []),

    % Wait for metadata sync between providers
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(Provider1, GetSessionFun(Provider1), {guid, RegularFileWithJsonMetadataGuid}),
        ?ATTEMPTS
    ),

    DataSpec = #data_spec{
        optional = [<<"inherited">>, <<"filter_type">>, <<"filter">>],
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

    GetExpectedResultFun = fun(#api_test_ctx{client = Client, data = Data}, ShareId, DirectMetadataSet) ->
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

            ExpJsonMetadata = case {DirectMetadataSet, IncludeInherited} of
                {true, true} ->
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
                {true, false} ->
                    ?JSON_METADATA_5;
                {false, true} ->
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
                {false, false} ->
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
    end,

    get_metadata_test_base(
        Config,
        <<"json">>,
        GetExpectedResultFun,
        DataSpec,
        ShareId,
        [
            {
                <<"dir">>,
                DirWithoutJsonMetadataPath, DirWithoutJsonMetadataGuid,
                DirWithJsonMetadataPath, DirWithJsonMetadataGuid
            },
            {
                <<"file">>,
                RegularFileWithoutJsonMetadataPath, RegularFileWithoutJsonMetadataGuid,
                RegularFileWithJsonMetadataPath, RegularFileWithJsonMetadataGuid
            }
        ]
    ).


get_xattr_metadata_test(Config) ->
    [Provider2, Provider1] = ?config(op_worker_nodes, Config),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    UserSessId = GetSessionFun(Provider2),

    Dir1Path = filename:join(["/", ?SPACE_2, ?SCENARIO_NAME]),
    {ok, Dir1Guid} = lfm_proxy:mkdir(Provider2, UserSessId, Dir1Path, 8#777),
    set_all_metadata_types(Provider2, UserSessId, Dir1Guid, set_1),

    Dir2Path = filename:join([Dir1Path, <<"dir_layer_4">>]),
    {ok, Dir2Guid} = lfm_proxy:mkdir(Provider2, UserSessId, Dir2Path, 8#717),
    {ok, ShareId} = lfm_proxy:create_share(Provider2, UserSessId, {guid, Dir2Guid}, <<"share">>),

    DirWithoutMetadataPath = filename:join([Dir2Path, <<"dir_without_metadata">>]),
    {ok, DirWithoutMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithoutMetadataPath, 8#777),

    DirWithMetadataPath = filename:join([Dir2Path, <<"dir_with_metadata">>]),
    {ok, DirWithMetadataGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirWithMetadataPath, 8#777),
    set_all_metadata_types(Provider2, UserSessId, DirWithMetadataGuid, set_2),

    RegularFileWithoutMetadataPath = filename:join([Dir2Path, <<"file_without_metadata">>]),
    {ok, RegularFileWithoutMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithoutMetadataPath, 8#777),

    RegularFileWithMetadataPath = filename:join([Dir2Path, <<"file_with_metadata">>]),
    {ok, RegularFileWithMetadataGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFileWithMetadataPath, 8#777),
    set_all_metadata_types(Provider2, UserSessId, RegularFileWithMetadataGuid, set_2),

    % Wait for metadata sync between providers
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(Provider1, GetSessionFun(Provider1), {guid, RegularFileWithMetadataGuid}),
        ?ATTEMPTS
    ),

    NotSetXattrKey = <<"not_set_xattr">>,

    DataSpec = #data_spec{
        optional = [<<"attribute">>, <<"inherited">>, <<"show_internal">>],
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

    GetExpectedResultFun = fun(#api_test_ctx{client = Client, data = Data}, ShareId, DirectMetadataSet) ->
        try
            Attribute = maps:get(<<"attribute">>, Data, undefined),
            IncludeInherited = maps:get(<<"inherited">>, Data, false),
            ShowInternal = maps:get(<<"show_internal">>, Data, false),

            XattrsToGet = case Attribute of
                undefined ->
                    case {DirectMetadataSet, IncludeInherited, ShareId, ShowInternal} of
                        {true, true, undefined, true} ->
                            ?ALL_XATTRS_KEYS;
                        {true, true, undefined, false} ->
                            % Only custom xattrs are shown
                            [?XATTR_1_KEY, ?XATTR_2_KEY];
                        {true, true, _ShareId, true} ->
                            % Xattr1 cannot be fetched as it is above share root
                            ?ALL_XATTRS_KEYS -- [?XATTR_1_KEY];
                        {true, true, _ShareId, false} ->
                            [?XATTR_2_KEY];
                        {true, false, _, true} ->
                            ?ALL_XATTRS_KEYS -- [?XATTR_1_KEY];
                        {true, false, _, false} ->
                            [?XATTR_2_KEY];
                        {false, true, undefined, true} ->
                            % Exclude cdmi attrs as those are not inherited and xattr2 as it is not set
                            (?ALL_XATTRS_KEYS -- [?XATTR_2_KEY]) -- ?CDMI_XATTRS_KEY;
                        {false, true, undefined, false} ->
                            [?XATTR_1_KEY];
                        {false, _, _, _} ->
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

            AvailableXattrsMap = case {DirectMetadataSet, IncludeInherited} of
                {true, false} ->
                    ?ALL_METADATA_SET_2;
                {true, true} ->
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
                                    ?ALL_METADATA_SET_2
                            end;
                        undefined ->
                            % When 'inherited' flag is set all ancestors json metadata
                            % are merged but for rest the first value found (which in
                            % this case is value directly set on file) is returned
                            ?ALL_METADATA_SET_2#{
                                ?JSON_METADATA_KEY => json_utils:merge([
                                    ?JSON_METADATA_4,
                                    ?JSON_METADATA_5
                                ]),
                                ?XATTR_1_KEY => ?XATTR_1_VALUE
                            };
                        _ ->
                            % In share mode only metadata directly set on file is available
                            ?ALL_METADATA_SET_2
                    end;
                {false, false} ->
                    #{};
                {false, true} ->
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
    end,

    get_metadata_test_base(
        Config,
        <<"xattrs">>,
        GetExpectedResultFun,
        DataSpec,
        ShareId,
        [
            {
                <<"dir">>,
                DirWithoutMetadataPath, DirWithoutMetadataGuid,
                DirWithMetadataPath, DirWithMetadataGuid
            },
            {
                <<"file">>,
                RegularFileWithoutMetadataPath, RegularFileWithoutMetadataGuid,
                RegularFileWithMetadataPath, RegularFileWithMetadataGuid
            }
        ]
    ).


-spec get_metadata_test_base(
    Config :: proplists:proplist(),
    MetadataType :: binary(),  %% <<"json">> | <<"rdf">> | <<"xattrs">>

    GetExpectedResultFun :: fun((api_test_ctx(), undefined | od_share:id(), DirectMetadataSet :: boolean()) -> ExpResult :: term()),

    DataSpec :: data_spec(),
    ShareId :: od_share:id(),
    FilesInSpace2List :: [{
        FileType :: binary(), % <<"file">> | <<"dir">>
        FileWithoutMetadataPath :: binary(),
        FileWithoutMetadataGuid :: file_id:file_guid(),
        FileWithMetadataPath :: binary(),
        FileWithMetadataGuid :: file_id:file_guid()
    }]
) ->
    ok | no_return().
get_metadata_test_base(
    Config,
    MetadataType,

    GetExpectedResultFun,

    DataSpec,
    ShareId,
    FilesInSpace2List
) ->
    [Provider2, Provider1] = Providers = ?config(op_worker_nodes, Config),

    SupportedClientsPerNode = #{
        Provider1 => [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        Provider2 => [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH]
    },

    ClientSpecForGetJsonInSpace2Scenarios = #client_spec{
        correct = [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [?NOBODY],
        forbidden = [?USER_IN_SPACE_1_AUTH],
        supported_clients_per_node = SupportedClientsPerNode
    },

    % Special case -> any user can make requests for shares but if request is
    % being made using credentials by user not supported on specific provider
    % ?ERROR_USER_NOT_SUPPORTED will be returned
    ClientSpecForShareScenarios = #client_spec{
        correct = [?NOBODY, ?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [],
        forbidden = [],
        supported_clients_per_node = SupportedClientsPerNode
    },

    QsParameters = case DataSpec of
        undefined ->
            [];
        #data_spec{optional = OptionalParams} ->
            OptionalParams
    end,

    ConstructPrepareRestArgsFun = fun(FileId) -> fun(#api_test_ctx{data = Data}) ->
        #rest_args{
            method = get,
            path = http_utils:append_url_parameters(
                <<"data/", FileId/binary, "/metadata/", MetadataType/binary>>,
                maps:with(QsParameters, utils:ensure_defined(Data, undefined, #{}))
            )
        }
    end end,
    ConstructPrepareDeprecatedFilePathRestArgsFun = fun(FilePath) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = get,
                path = http_utils:append_url_parameters(
                    <<"metadata/", MetadataType/binary, FilePath/binary>>,
                    maps:with(QsParameters, utils:ensure_defined(Data, undefined, #{}))
                )
            }
        end
    end,
    ConstructPrepareDeprecatedFileIdRestArgsFun = fun(Fileid) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = get,
                path = http_utils:append_url_parameters(
                    <<"metadata-id/", MetadataType/binary, "/", Fileid/binary>>,
                    maps:with(QsParameters, utils:ensure_defined(Data, undefined, #{}))
                )
            }
        end
    end,
    ConstructPrepareGsArgsFun = fun(FileId, Scope) -> fun(#api_test_ctx{data = Data}) ->
        Aspect = case MetadataType of
            <<"json">> -> json_metadata;
            <<"rdf">> -> rdf_metadata;
            <<"xattrs">> -> xattrs
        end,
        #gs_args{
            operation = get,
            gri = #gri{type = op_file, id = FileId, aspect = Aspect, scope = Scope},
            data = Data
        }
    end end,

    ConstructValidateSuccessfulRestResultFun = fun(ShareId, DirectMetadataSet) ->
        fun(TestCtx, {ok, RespCode, RespBody}) ->
            {ExpCode, ExpBody} = case GetExpectedResultFun(TestCtx, ShareId, DirectMetadataSet) of
                {ok, ExpResult} ->
                    {?HTTP_200_OK, ExpResult};
                {error, _} = ExpError ->
                    {errors:to_http_code(ExpError), ?REST_ERROR(ExpError)}
            end,
            ?assertEqual({ExpCode, ExpBody}, {RespCode, RespBody})
        end
    end,
    ConstructValidateSuccessfulGsResultFun = fun(ShareId, DirectMetadataSet) ->
        fun(TestCtx, Result) ->
            case GetExpectedResultFun(TestCtx, ShareId, DirectMetadataSet) of
                {ok, ExpResult} ->
                    ?assertEqual({ok, #{<<"metadata">> => ExpResult}}, Result);
                {error, _} = ExpError ->
                    ?assertEqual(ExpError, Result)
            end
        end
    end,

    lists:foreach(fun({
        FileType,
        FileWithoutJsonMetadataPath, FileWithoutJsonMetadataGuid,
        FileWithJsonMetadataPath, FileWithJsonMetadataGuid
    }) ->
        {ok, FileWithoutJsonMetadataObjectId} = file_id:guid_to_objectid(FileWithoutJsonMetadataGuid),
        {ok, FileWithJsonMetadataObjectId} = file_id:guid_to_objectid(FileWithJsonMetadataGuid),

        ShareGuid = file_id:guid_to_share_guid(FileWithJsonMetadataGuid, ShareId),
        {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

        ?assert(api_test_utils:run_scenarios(Config, [

            %% TEST METADATA FOR FILE WITH METADATA SET

            #scenario_spec{
                name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " with ", MetadataType/binary, " set using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(FileWithJsonMetadataObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " with ", MetadataType/binary, " set using /files/ rest endpoint">>,
                type = rest_with_file_path,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FileWithJsonMetadataPath),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " with ", MetadataType/binary, " set using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileWithJsonMetadataObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " with ", MetadataType/binary, " set using gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(FileWithJsonMetadataGuid, private),
                validate_result_fun = ConstructValidateSuccessfulGsResultFun(undefined, true),
                data_spec = DataSpec
            },

            %% TEST GET METADATA FOR SHARED FILE WITH METADATA SET

            #scenario_spec{
                name = <<"Get ", MetadataType/binary, " metadata from shared ", FileType/binary, " with ", MetadataType/binary, " set using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(ShareObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(ShareId, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get ", MetadataType/binary, " metadata from shared ", FileType/binary, " with ", MetadataType/binary, " set using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(ShareObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(ShareId, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get ", MetadataType/binary, " metadata from shared ", FileType/binary, " with ", MetadataType/binary, " set using gs public api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, public),
                validate_result_fun = ConstructValidateSuccessfulGsResultFun(ShareId, true),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get ", MetadataType/binary, " metadata from shared ", FileType/binary, " with ", MetadataType/binary, " set using private gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, private),
                validate_result_fun = fun(_, Result) ->
                    ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                end,
                data_spec = DataSpec
            },

            %% TEST GET METADATA FOR FILE WITHOUT METADATA SET

            #scenario_spec{
                name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " without ", MetadataType/binary, " set using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(FileWithoutJsonMetadataObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, false),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " without ", MetadataType/binary, " set using /files/ rest endpoint">>,
                type = rest_with_file_path,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FileWithoutJsonMetadataPath),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, false),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " without ", MetadataType/binary, " set using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileWithoutJsonMetadataObjectId),
                validate_result_fun = ConstructValidateSuccessfulRestResultFun(undefined, false),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Get ", MetadataType/binary, " metadata from ", FileType/binary, " without ", MetadataType/binary, " set using gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(FileWithoutJsonMetadataGuid, private),
                validate_result_fun = ConstructValidateSuccessfulGsResultFun(undefined, false),
                data_spec = DataSpec
            }
        ]))
    end, FilesInSpace2List),

    %% TEST GET METADATA FOR FILE ON PROVIDER NOT SUPPORTING USER

    Provider2DomainBin = ?GET_DOMAIN_BIN(Provider2),

    ClientSpecForGetMetadataInSpace1Scenarios = #client_spec{
        correct = [?USER_IN_SPACE_1_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [?NOBODY],
        forbidden = [?USER_IN_SPACE_2_AUTH],
        supported_clients_per_node = SupportedClientsPerNode
    },

    ExpSpace1Metadata = case MetadataType of
        <<"json">> -> ?JSON_METADATA_4;
        <<"rdf">> -> ?RDF_METADATA_1;
        <<"xattrs">> -> #{?XATTR_1_KEY => ?XATTR_1_VALUE}
    end,
    ValidateRestGetMetadataOnProvidersNotSupportingUserFun = fun
        (#api_test_ctx{node = Node, client = Client}, {ok, ?HTTP_400_BAD_REQUEST, Response}) when
            Node == Provider2,
            Client == ?USER_IN_BOTH_SPACES_AUTH
        ->
            ?assertEqual(?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin)), Response);
        (_TestCaseCtx, {ok, ?HTTP_200_OK, Response}) ->
            ?assertEqual(ExpSpace1Metadata, Response)
    end,
    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_1),
    set_all_metadata_types(Provider1, ?ROOT_SESS_ID, Space1Guid, set_1),
    {ok, Space1ObjectId} = file_id:guid_to_objectid(Space1Guid),

    ?assert(api_test_utils:run_scenarios(Config, [
        #scenario_spec{
            name = <<"Get ", MetadataType/binary, " metadata from ", ?SPACE_1/binary, " with ", MetadataType/binary, " set on provider not supporting user using /data/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForGetMetadataInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestGetMetadataOnProvidersNotSupportingUserFun
        },
        #scenario_spec{
            name = <<"Get ", MetadataType/binary, " metadata from ", ?SPACE_1/binary, " with ", MetadataType/binary, " set on provider not supporting user using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = ClientSpecForGetMetadataInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(<<"/", ?SPACE_1/binary>>),
            validate_result_fun = ValidateRestGetMetadataOnProvidersNotSupportingUserFun
        },
        #scenario_spec{
            name = <<"Get ", MetadataType/binary, " metadata from ", ?SPACE_1/binary, " with ", MetadataType/binary, " set on provider not supporting user using /files-id/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForGetMetadataInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestGetMetadataOnProvidersNotSupportingUserFun
        },
        #scenario_spec{
            name = <<"Get ", MetadataType/binary, " metadata from ", ?SPACE_1/binary, " with ", MetadataType/binary, " set on provider not supporting user using gs api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpecForGetMetadataInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareGsArgsFun(Space1Guid, private),
            validate_result_fun = fun
                (#api_test_ctx{node = Node, client = Client}, Result) when
                    Node == Provider2,
                    Client == ?USER_IN_BOTH_SPACES_AUTH
                ->
                    ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin), Result);
                (_TestCaseCtx, {ok, Result}) ->
                    ?assertEqual(#{<<"metadata">> => ExpSpace1Metadata}, Result)
            end
        }
    ])).


set_rdf_metadata_test(Config) ->
    [Provider2, Provider1] = Providers = ?config(op_worker_nodes, Config),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    UserSessId = GetSessionFun(Provider2),

    RootDirPath = filename:join(["/", ?SPACE_2, ?SCENARIO_NAME]),
    {ok, RootDirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, RootDirPath, 8#777),
    {ok, ShareId} = lfm_proxy:create_share(Provider2, UserSessId, {guid, RootDirGuid}, <<"share">>),

    DirPath = filename:join([RootDirPath, <<"dir">>]),
    {ok, DirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirPath, 8#777),

    RegularFilePath = filename:join([RootDirPath, <<"file">>]),
    {ok, RegularFileGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFilePath, 8#777),

    % Wait for metadata sync between providers
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(Provider1, GetSessionFun(Provider1), {guid, RegularFileGuid}),
        ?ATTEMPTS
    ),

    GetMetadataFun = fun(Node, FileGuid) ->
        SessId = GetSessionFun(Node),
        lfm_proxy:get_metadata(Node, SessId, {guid, FileGuid}, rdf, [], false)
    end,
    RemoveMetadataFun = fun(Node, FileGuid) ->
        SessId = GetSessionFun(Node),
        lfm_proxy:remove_metadata(Node, SessId, {guid, FileGuid}, rdf)
    end,

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        correct_values = #{
            <<"metadata">> => [?RDF_METADATA_1, ?RDF_METADATA_2]
        }
    },

    ConstructVerifyEnvForSuccessfulCallsFun = fun(FileGuid) -> fun
        (false, #api_test_ctx{node = TestNode}) ->
            ?assertMatch({error, ?ENODATA}, GetMetadataFun(TestNode, FileGuid), ?ATTEMPTS),
            true;
        (true, #api_test_ctx{node = TestNode, data = #{<<"metadata">> := Metadata}}) ->
            lists:foreach(fun(Node) ->
                ?assertMatch({ok, Metadata}, GetMetadataFun(Node, FileGuid), ?ATTEMPTS)
            end, Providers),

            case Metadata of
                ?RDF_METADATA_1 ->
                    % Do nothing after setting ?RDF_METADATA_1 to test
                    % overriding it by ?RDF_METADATA_2
                    ok;
                ?RDF_METADATA_2 ->
                    % Remove ?RDF_METADATA_2 to test setting ?RDF_METADATA_1 on clean state
                    ?assertMatch(ok, RemoveMetadataFun(TestNode, FileGuid)),
                    % Wait for removal to be synced between providers.
                    lists:foreach(fun(Node) ->
                        ?assertMatch({error, ?ENODATA}, GetMetadataFun(Node, FileGuid), ?ATTEMPTS)
                    end, Providers)
            end,
            true
    end end,

    set_metadata_test_base(
        Config,
        <<"rdf">>,

        GetMetadataFun,
        RemoveMetadataFun,
        % Setting rdf metadata with correct values should always succeed
        fun(_) -> ok end,
        ConstructVerifyEnvForSuccessfulCallsFun,

        DataSpec,
        ShareId,
        [
            {<<"dir">>, DirPath, DirGuid},
            {<<"file">>, RegularFilePath, RegularFileGuid}
        ]
    ).


set_json_metadata_test(Config) ->
    [Provider2, Provider1] = Providers = ?config(op_worker_nodes, Config),

    GetSessionFun = fun(Node) ->
        ?config({session_id, {?USER_IN_BOTH_SPACES, ?GET_DOMAIN(Node)}}, Config)
    end,

    UserSessId = GetSessionFun(Provider2),

    RootDirPath = filename:join(["/", ?SPACE_2, ?SCENARIO_NAME]),
    {ok, RootDirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, RootDirPath, 8#777),
    {ok, ShareId} = lfm_proxy:create_share(Provider2, UserSessId, {guid, RootDirGuid}, <<"share">>),

    DirPath = filename:join([RootDirPath, <<"dir">>]),
    {ok, DirGuid} = lfm_proxy:mkdir(Provider2, UserSessId, DirPath, 8#777),

    RegularFilePath = filename:join([RootDirPath, <<"file">>]),
    {ok, RegularFileGuid} = lfm_proxy:create(Provider2, UserSessId, RegularFilePath, 8#777),

    % Wait for metadata sync between providers
    ?assertMatch(
        {ok, #file_attr{}},
        lfm_proxy:stat(Provider1, GetSessionFun(Provider1), {guid, RegularFileGuid}),
        ?ATTEMPTS
    ),

    GetMetadataFun = fun(Node, FileGuid) ->
        SessId = GetSessionFun(Node),
        lfm_proxy:get_metadata(Node, SessId, {guid, FileGuid}, json, [], false)
    end,
    RemoveMetadataFun = fun(Node, FileGuid) ->
        SessId = GetSessionFun(Node),
        lfm_proxy:remove_metadata(Node, SessId, {guid, FileGuid}, json)
    end,

    ExampleJson = #{<<"attr1">> => [0, 1, <<"val">>]},

    DataSpec = #data_spec{
        required = [<<"metadata">>],
        optional = [<<"filter_type">>, <<"filter">>],
        correct_values = #{
            <<"metadata">> => [ExampleJson],
            <<"filter_type">> => [<<"keypath">>],
            <<"filter">> => [
                <<"attr1.[1]">>,        % Test setting attr in existing array
                <<"attr1.[2].attr22">>, % Test error when trying to set subjson to binary (<<"val">> in ExampleJson)
                <<"attr1.[5]">>,        % Test setting attr beyond existing array
                <<"attr2.[2]">>         % Test setting attr in nonexistent array
            ]
        },
        bad_values = [
            % invalid json error can be returned only for rest (invalid json is send as
            % body without modification) and not gs (#{<<"metadata">> => some_binary} is send,
            % so no matter what that some_binary is it will be treated as string)
            {<<"metadata">>, <<"aaa">>, {rest_handler, ?ERROR_BAD_VALUE_JSON(<<"metadata">>)}},
            {<<"metadata">>, <<"{">>, {rest_handler, ?ERROR_BAD_VALUE_JSON(<<"metadata">>)}},
            {<<"metadata">>, <<"{\"aaa\": aaa}">>, {rest_handler, ?ERROR_BAD_VALUE_JSON(<<"metadata">>)}},

            {<<"filter_type">>, <<"dummy">>, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])},

            % Below differences between error returned by rest and gs are results of sending
            % parameters via qs in REST, so they lost their original type and are cast to binary
            {<<"filter_type">>, 100, {rest, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"filter_type">>, [<<"keypath">>])}},
            {<<"filter_type">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"filter_type">>)}},
            {<<"filter">>, 100, {gs, ?ERROR_BAD_VALUE_BINARY(<<"filter">>)}}
        ]
    },

    GetExpectedResultAndFiltersFun = fun(#api_test_ctx{data = Data}) ->
        FilterType = maps:get(<<"filter_type">>, Data, undefined),
        Filter = maps:get(<<"filter">>, Data, undefined),

        case {FilterType, Filter} of
            {undefined, _} ->
                {ok, []};
            {<<"keypath">>, undefined} ->
                ?ERROR_MISSING_REQUIRED_VALUE(<<"filter">>);
            {<<"keypath">>, _} ->
                case binary:split(Filter, <<".">>, [global]) of
                    [<<"attr1">>, <<"[2]">>, <<"attr22">>] ->
                        ?ERROR_POSIX(?ENODATA);
                    ExistingPath ->
                        {ok, ExistingPath}
                end
        end
    end,
    ConstructVerifyEnvForSuccessfulCallsFun = fun(FileGuid) -> fun
        (false, #api_test_ctx{node = Node}) ->
            ?assertMatch({error, ?ENODATA}, GetMetadataFun(Node, FileGuid), ?ATTEMPTS),
            true;
        (true, #api_test_ctx{node = TestNode} = TestCtx) ->
            ExpResult = GetExpectedResultAndFiltersFun(TestCtx),
            lists:foreach(fun(Node) ->
                % Below expected metadata depends on the tested parameters combination order.
                % First only required params will be tested, then with only one optional params,
                % next with 2 and so on. If optional param has multiple values then those later
                % will be also tested later.
                ExpJson = case ExpResult of
                    {ok, []} ->
                        ExampleJson;
                    ?ERROR_MISSING_REQUIRED_VALUE(_) ->
                        % Test failed to override previously set json because of specifying
                        % filter_type without specifying filter
                        ExampleJson;
                    {ok, [<<"attr1">>, <<"[1]">>]} ->
                        #{<<"attr1">> => [0, ExampleJson, <<"val">>]};
                    ?ERROR_POSIX(?ENODATA) ->
                        % Operation failed and nothing should be changed -
                        % it should match the same json as above
                        #{<<"attr1">> => [0, ExampleJson, <<"val">>]};
                    {ok, [<<"attr1">>, <<"[5]">>]} ->
                        #{<<"attr1">> => [0, ExampleJson, <<"val">>, null, null, ExampleJson]};
                    {ok, [<<"attr2">>, <<"[2]">>]} ->
                        #{
                            <<"attr1">> => [0, ExampleJson, <<"val">>, null, null, ExampleJson],
                            <<"attr2">> => [null, null, ExampleJson]
                        }
                end,
                ?assertMatch({ok, ExpJson}, GetMetadataFun(Node, FileGuid), ?ATTEMPTS)
            end, Providers),

            case ExpResult of
                {ok, [<<"attr2">>, <<"[2]">>]} ->
                    % Remove metadata after last successful parameters combination tested so that
                    % next tests can start from setting rather then updating metadata
                    ?assertMatch(ok, RemoveMetadataFun(TestNode, FileGuid)),
                    % Wait for changes to be synced between providers. Otherwise it can possible
                    % interfere with tests on other node (e.g. information about deletion that
                    % comes after setting ExampleJson and before setting using filter results in
                    % json metadata removal. In such case next test using 'filter' parameter should expect
                    % ExpMetadata = #{<<"attr1">> => [null, null, null, null, null, ExampleJson]}
                    % rather than above one as that will be the result of setting ExampleJson
                    % with attr1.[5] filter and no prior json set)
                    lists:foreach(fun(Node) ->
                        ?assertMatch({error, ?ENODATA}, GetMetadataFun(Node, FileGuid), ?ATTEMPTS)
                    end, Providers);
                _ ->
                    ok
            end,
            true
    end end,

    set_metadata_test_base(
        Config,
        <<"json">>,

        GetMetadataFun,
        RemoveMetadataFun,
        fun(TestCtx) ->
            case GetExpectedResultAndFiltersFun(TestCtx) of
                {ok, _} ->
                    % Filters are not needed in 'set_metadata_test_base' - what
                    % is needed is only information whether call should succeed
                    % or fail with specific error
                    ok;
                {error, _} = Error ->
                    Error
            end
        end,
        ConstructVerifyEnvForSuccessfulCallsFun,

        DataSpec,
        ShareId,
        [
            {<<"dir">>, DirPath, DirGuid},
            {<<"file">>, RegularFilePath, RegularFileGuid}
        ]
    ),

    % Test setting primitive json values

    SetPrimitiveJsonDataSpec = #data_spec{
        required = [<<"metadata">>],
        correct_values = #{<<"metadata">> => [
            <<"{}">>, <<"[]">>, <<"true">>, <<"0">>, <<"0.1">>,
            <<"null">>, <<"\"string\"">>
        ]}
    },

    ConstructSetPrimitiveJsonVerifyEnvForSuccessfulCallsFun = fun(FileGuid) -> fun
        (false, #api_test_ctx{node = TestNode}) ->
            ?assertMatch({error, ?ENODATA}, GetMetadataFun(TestNode, FileGuid), ?ATTEMPTS),
            true;
        (true, #api_test_ctx{node = TestNode, data = #{<<"metadata">> := Metadata}}) ->
            % Primitive jsons are set without optional params so should match exactly to itself
            ExpMetadata = json_utils:decode(Metadata),
            lists:foreach(fun(Node) ->
                ?assertMatch({ok, ExpMetadata}, GetMetadataFun(Node, FileGuid), ?ATTEMPTS)
            end, Providers),

            case Metadata of
                <<"\"string\"">> ->
                    % Remove metadata after last successful parameters combination tested so that
                    % next tests can start from setting rather then updating metadata
                    ?assertMatch(ok, RemoveMetadataFun(TestNode, FileGuid)),
                    lists:foreach(fun(Node) ->
                        ?assertMatch({error, ?ENODATA}, GetMetadataFun(Node, FileGuid), ?ATTEMPTS)
                    end, Providers);
                _ ->
                    ok
            end,
            true
    end end,

    set_metadata_test_base(
        Config,
        <<"json">>,

        GetMetadataFun,
        RemoveMetadataFun,
        fun(_) -> ok end,
        ConstructSetPrimitiveJsonVerifyEnvForSuccessfulCallsFun,

        SetPrimitiveJsonDataSpec,
        ShareId,
        [
            {<<"dir">>, DirPath, DirGuid},
            {<<"file">>, RegularFilePath, RegularFileGuid}
        ]
    ).


-spec set_metadata_test_base(
    Config :: proplists:proplist(),
    MetadataType :: binary(),  %% <<"json">> | <<"rdf">> | <<"xattrs">>

    GetMetadataFun :: fun((node(), file_id:file_guid()) -> FileMetadata :: term()),
    RemoveMetadataFun :: fun((node(), file_id:file_guid()) -> ok),
    GetExpectedResultFun :: fun((api_test_ctx()) -> ok | {error, term()}),
    ConstructVerifyEnvForSuccessfulCallsFun :: fun((file_id:file_guid()) ->
        fun((ShouldSucceed :: boolean(), api_test_env()) -> boolean())
    ),

    DataSpec :: data_spec(),
    ShareId :: od_share:id(),
    FilesInSpace2List :: [{
        FileType :: binary(), % <<"file">> | <<"dir">>
        FilePath :: binary(),
        FileGuid :: file_id:file_guid()
    }]
) ->
    ok | no_return().
set_metadata_test_base(
    Config,
    MetadataType,

    GetMetadataFun,
    RemoveMetadataFun,
    GetExpectedResultFun,
    ConstructVerifyEnvForSuccessfulCallsFun,

    DataSpec,
    ShareId,
    FilesInSpace2List
) ->
    [Provider2, Provider1] = Providers = ?config(op_worker_nodes, Config),

    SupportedClientsPerNode = #{
        Provider1 => [?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        Provider2 => [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH]
    },

    ClientSpecForGetJsonInSpace2Scenarios = #client_spec{
        correct = [?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [?NOBODY],
        forbidden = [?USER_IN_SPACE_1_AUTH],
        supported_clients_per_node = SupportedClientsPerNode
    },

    % Special case -> any user can make requests for shares but if request is
    % being made using credentials by user not supported on specific provider
    % ?ERROR_USER_NOT_SUPPORTED will be returned
    ClientSpecForShareScenarios = #client_spec{
        correct = [?NOBODY, ?USER_IN_SPACE_1_AUTH, ?USER_IN_SPACE_2_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [],
        forbidden = [],
        supported_clients_per_node = SupportedClientsPerNode
    },

    QsParameters = case DataSpec of
        undefined ->
            [];
        #data_spec{optional = OptionalParams} ->
            OptionalParams
    end,

    ConstructPrepareRestArgsFun = fun(FileId) -> fun(#api_test_ctx{data = Data}) ->
        #rest_args{
            method = put,
            headers = case MetadataType of
                <<"rdf">> -> #{<<"content-type">> => <<"application/rdf+xml">>};
                _ -> #{<<"content-type">> => <<"application/json">>}
            end,
            path = http_utils:append_url_parameters(
                <<"data/", FileId/binary, "/metadata/", MetadataType/binary>>,
                maps:with(QsParameters, utils:ensure_defined(Data, undefined, #{}))
            ),
            body = encode_metadata(maps:get(<<"metadata">>, Data))
        }
    end end,
    ConstructPrepareDeprecatedFilePathRestArgsFun = fun(FilePath) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = put,
                headers = case MetadataType of
                    <<"rdf">> -> #{<<"content-type">> => <<"application/rdf+xml">>};
                    _ -> #{<<"content-type">> => <<"application/json">>}
                end,
                path = http_utils:append_url_parameters(
                    <<"metadata/", MetadataType/binary, FilePath/binary>>,
                    maps:with(QsParameters, utils:ensure_defined(Data, undefined, #{}))
                ),
                body = encode_metadata(maps:get(<<"metadata">>, Data))
            }
        end
    end,
    ConstructPrepareDeprecatedFileIdRestArgsFun = fun(Fileid) ->
        fun(#api_test_ctx{data = Data}) ->
            #rest_args{
                method = put,
                headers = case MetadataType of
                    <<"rdf">> -> #{<<"content-type">> => <<"application/rdf+xml">>};
                    _ -> #{<<"content-type">> => <<"application/json">>}
                end,
                path = http_utils:append_url_parameters(
                    <<"metadata-id/", MetadataType/binary, "/", Fileid/binary>>,
                    maps:with(QsParameters, utils:ensure_defined(Data, undefined, #{}))
                ),
                body = encode_metadata(maps:get(<<"metadata">>, Data))
            }
        end
    end,
    ConstructPrepareGsArgsFun = fun(FileId, Scope) -> fun(#api_test_ctx{data = Data0}) ->
        {Aspect, Data1} = case MetadataType of
            <<"json">> ->
                % Primitive metadata were specified as binaries to be send via REST,
                % but gs needs them decoded first to be able to send them properly
                Meta = maps:get(<<"metadata">>, Data0),
                {json_metadata, Data0#{<<"metadata">> => maybe_decode_json(Meta)}};
            <<"rdf">> ->
                {rdf_metadata, Data0};
            <<"xattrs">> ->
                {xattrs, Data0}
        end,
        #gs_args{
            operation = create,
            gri = #gri{type = op_file, id = FileId, aspect = Aspect, scope = Scope},
            data = Data1
        }
    end end,

    ValidateRestSuccessfulCallFun =  fun(TestCtx, {ok, RespCode, RespBody}) ->
        {ExpCode, ExpBody} = case GetExpectedResultFun(TestCtx) of
            ok ->
                {?HTTP_204_NO_CONTENT, #{}};
            {error, _} = ExpError ->
                {errors:to_http_code(ExpError), ?REST_ERROR(ExpError)}
        end,
        ?assertEqual({ExpCode, ExpBody}, {RespCode, RespBody})
    end,
    ValidateRestOperationNotSupportedFun = fun(_, {ok, ?HTTP_400_BAD_REQUEST, Response}) ->
        ?assertEqual(?REST_ERROR(?ERROR_NOT_SUPPORTED), Response)
    end,

    lists:foreach(fun({FileType, FilePath, FileGuid}) ->
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

        ShareGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
        {ok, ShareObjectId} = file_id:guid_to_objectid(ShareGuid),

        %% TEST SETTING METADATA FOR FILE IN NORMAL MODE

        ?assert(api_test_utils:run_scenarios(Config, [

            #scenario_spec{
                name = <<"Set ", MetadataType/binary, " metadata for ", FileType/binary, " using /data/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(FileObjectId),
                validate_result_fun = ValidateRestSuccessfulCallFun,
                verify_fun = ConstructVerifyEnvForSuccessfulCallsFun(FileGuid),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Set ", MetadataType/binary, " metadata for ", FileType/binary, " using /files/ rest endpoint">>,
                type = rest_with_file_path,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(FilePath),
                validate_result_fun = ValidateRestSuccessfulCallFun,
                verify_fun = ConstructVerifyEnvForSuccessfulCallsFun(FileGuid),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Set ", MetadataType/binary, " metadata for ", FileType/binary, " using /files-id/ rest endpoint">>,
                type = rest,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(FileObjectId),
                validate_result_fun = ValidateRestSuccessfulCallFun,
                verify_fun = ConstructVerifyEnvForSuccessfulCallsFun(FileGuid),
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Set ", MetadataType/binary, " metadata for ", FileType/binary, " using gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForGetJsonInSpace2Scenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(FileGuid, private),
                validate_result_fun = fun(TestCtx, Result) ->
                    case GetExpectedResultFun(TestCtx) of
                        ok ->
                            ?assertEqual({ok, undefined}, Result);
                        {error, _} = ExpError ->
                            ?assertEqual(ExpError, Result)
                    end
                end,
                verify_fun = ConstructVerifyEnvForSuccessfulCallsFun(FileGuid),
                data_spec = DataSpec
            }
        ])),

        %% TEST SETTING METADATA FOR SHARED FILE SHOULD BE FORBIDDEN

        % Remove metadata and assert that no below call sets it again
        lists:foreach(fun(Node) ->
            ?assertMatch(ok, RemoveMetadataFun(Node, FileGuid))
        end, Providers),

        VerifyEnvForShareCallsFun = fun(_, #api_test_ctx{node = Node}) ->
            ?assertMatch({error, ?ENODATA}, GetMetadataFun(Node, FileGuid)),
            true
        end,

        ?assert(api_test_utils:run_scenarios(Config, [

            #scenario_spec{
                name = <<"Set ", MetadataType/binary, " metadata for shared ", FileType/binary, " using /data/ rest endpoint">>,
                type = rest_not_supported,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareRestArgsFun(ShareObjectId),
                validate_result_fun = ValidateRestOperationNotSupportedFun,
                verify_fun = VerifyEnvForShareCallsFun,
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Set ", MetadataType/binary, " metadata for shared ", FileType/binary, " using /files-id/ rest endpoint">>,
                type = rest_not_supported,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(ShareObjectId),
                validate_result_fun = ValidateRestOperationNotSupportedFun,
                verify_fun = VerifyEnvForShareCallsFun,
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Set ", MetadataType/binary, " metadata for shared ", FileType/binary, " using gs public api">>,
                type = gs_not_supported,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, public),
                validate_result_fun = fun(_TestCaseCtx, Result) ->
                    ?assertEqual(?ERROR_NOT_SUPPORTED, Result)
                end,
                verify_fun = VerifyEnvForShareCallsFun,
                data_spec = DataSpec
            },
            #scenario_spec{
                name = <<"Set ", MetadataType/binary, " metadata for shared ", FileType/binary, " using private gs api">>,
                type = gs,
                target_nodes = Providers,
                client_spec = ClientSpecForShareScenarios,
                prepare_args_fun = ConstructPrepareGsArgsFun(ShareGuid, private),
                validate_result_fun = fun(_, Result) ->
                    ?assertEqual(?ERROR_UNAUTHORIZED, Result)
                end,
                verify_fun = VerifyEnvForShareCallsFun,
                data_spec = DataSpec
            }
        ]))
    end, FilesInSpace2List),

    %% TEST SET METADATA FOR FILE ON PROVIDER NOT SUPPORTING USER

    Space1Guid = fslogic_uuid:spaceid_to_space_dir_guid(?SPACE_1),
    {ok, Space1ObjectId} = file_id:guid_to_objectid(Space1Guid),

    % Remove metadata on space1 that may have been set by previous tests
    lists:foreach(fun(Node) -> ?assertMatch(ok, RemoveMetadataFun(Node, Space1Guid)) end, Providers),

    Provider2DomainBin = ?GET_DOMAIN_BIN(Provider2),

    % For testing setting metadata on provider not supporting user (expected error)
    % only bare minimum to make call is needed. That is why optional parameters
    % are erased (they were tested already in above scenarios)
    DataSpecForSetMetadataInSpace1Scenarios = DataSpec#data_spec{optional = []},

    ClientSpecForSetMetadataInSpace1Scenarios = #client_spec{
        correct = [?USER_IN_SPACE_1_AUTH, ?USER_IN_BOTH_SPACES_AUTH],
        unauthorized = [?NOBODY],
        forbidden = [?USER_IN_SPACE_2_AUTH],
        supported_clients_per_node = SupportedClientsPerNode
    },

    ValidateRestSetMetadataOnProvidersNotSupportingUserFun = fun
        (#api_test_ctx{node = Node, client = Client}, {ok, ?HTTP_400_BAD_REQUEST, Response}) when
            Node == Provider2,
            Client == ?USER_IN_BOTH_SPACES_AUTH
        ->
            ?assertEqual(?REST_ERROR(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin)), Response);
        (_TestCaseCtx, {ok, ?HTTP_204_NO_CONTENT, Response}) ->
            ?assertEqual(#{}, Response)
    end,
    ConstructVerifyEnvFunForSetMetadataInSpace1Scenarios = fun(FileGuid) -> fun
        (false, #api_test_ctx{node = Node}) ->
            ?assertMatch({error, ?ENODATA}, GetMetadataFun(Node, FileGuid), ?ATTEMPTS),
            true;
        (true, #api_test_ctx{node = Node, client = Client, data = #{<<"metadata">> := Metadata}}) ->
            case {Node, Client} of
                {Provider2, ?USER_IN_BOTH_SPACES_AUTH} ->
                    % Request from user not supported by provider should be rejected
                    ?assertMatch({error, ?ENODATA}, GetMetadataFun(Node, FileGuid), ?ATTEMPTS);
                _ ->
                    ExpMetadata = case MetadataType of
                        <<"json">> -> maybe_decode_json(Metadata);
                        _ -> Metadata
                    end,
                    ?assertMatch({ok, ExpMetadata}, GetMetadataFun(Node, FileGuid)),
                    ?assertMatch(ok, RemoveMetadataFun(Node, FileGuid))
            end,
            true
    end end,

    ?assert(api_test_utils:run_scenarios(Config, [
        #scenario_spec{
            name = <<"Set ", MetadataType/binary, " metadata for ", ?SPACE_1/binary, " on provider not supporting user using /data/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSetMetadataInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestSetMetadataOnProvidersNotSupportingUserFun,
            verify_fun = ConstructVerifyEnvFunForSetMetadataInSpace1Scenarios(Space1Guid),
            data_spec = DataSpecForSetMetadataInSpace1Scenarios
        },
        #scenario_spec{
            name = <<"Set ", MetadataType/binary, " metadata for ", ?SPACE_1/binary, " on provider not supporting user using /files/ rest endpoint">>,
            type = rest_with_file_path,
            target_nodes = Providers,
            client_spec = ClientSpecForSetMetadataInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareDeprecatedFilePathRestArgsFun(<<"/", ?SPACE_1/binary>>),
            validate_result_fun = ValidateRestSetMetadataOnProvidersNotSupportingUserFun,
            verify_fun = ConstructVerifyEnvFunForSetMetadataInSpace1Scenarios(Space1Guid),
            data_spec = DataSpecForSetMetadataInSpace1Scenarios
        },
        #scenario_spec{
            name = <<"Set ", MetadataType/binary, " metadata for ", ?SPACE_1/binary, " on provider not supporting user using /files-id/ rest endpoint">>,
            type = rest,
            target_nodes = Providers,
            client_spec = ClientSpecForSetMetadataInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareDeprecatedFileIdRestArgsFun(Space1ObjectId),
            validate_result_fun = ValidateRestSetMetadataOnProvidersNotSupportingUserFun,
            verify_fun = ConstructVerifyEnvFunForSetMetadataInSpace1Scenarios(Space1Guid),
            data_spec = DataSpecForSetMetadataInSpace1Scenarios
        },
        #scenario_spec{
            name = <<"Set ", MetadataType/binary, " metadata for ", ?SPACE_1/binary, " on provider not supporting user using gs api">>,
            type = gs,
            target_nodes = Providers,
            client_spec = ClientSpecForSetMetadataInSpace1Scenarios,
            prepare_args_fun = ConstructPrepareGsArgsFun(Space1Guid, private),
            validate_result_fun = fun
                (#api_test_ctx{node = Node, client = Client}, Result) when
                    Node == Provider2,
                    Client == ?USER_IN_BOTH_SPACES_AUTH
                ->
                    ?assertEqual(?ERROR_SPACE_NOT_SUPPORTED_BY(Provider2DomainBin), Result);
                (_TestCaseCtx, Result) ->
                    ?assertEqual({ok, undefined}, Result)
            end,
            verify_fun = ConstructVerifyEnvFunForSetMetadataInSpace1Scenarios(Space1Guid),
            data_spec = DataSpecForSetMetadataInSpace1Scenarios
        }
    ])).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
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
        end, ?config(op_worker_nodes, NewConfig2)),
        application:start(ssl),
        hackney:start(),
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        ),
        initializer:mock_auth_manager(NewConfig3, _CheckIfUserIsSupported = true),
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
    ct:timetrap({minutes, 40}),
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


encode_metadata(Metadata) when is_binary(Metadata) ->
    Metadata;
encode_metadata(Metadata) when is_map(Metadata) ->
    json_utils:encode(Metadata).


maybe_decode_json(MaybeEncodedJson) ->
    try
        json_utils:decode(MaybeEncodedJson)
    catch _:_ ->
        MaybeEncodedJson
    end.


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
