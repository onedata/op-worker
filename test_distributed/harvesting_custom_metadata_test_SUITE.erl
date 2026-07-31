%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module tests mechanism of custom harvesting metadata in
%%% multi-provider environment.
%%% NOTE !!!
%%% Test cases in this suite are not independent as it is impossible to
%%% reset couchbase_changes_stream.
%%% `harvesting model` is not cleaned between test cases.
%%% Because of that, harvesting in next testcase starts from the sequence
%%% on which harvesting in previous test case stopped.
%%% @end
%%%-------------------------------------------------------------------
-module(harvesting_custom_metadata_test_SUITE).
-author("Jakub Kudzia").

-include("harvesting/harvesting_test.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/data_access_control.hrl").
-include("modules/harvesting/harvesting.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_attr.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/http/codes.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    set_json_metadata/1,
    modify_json_metadata/1,
    delete_json_metadata/1,
    delete_file_with_json_metadata/1,
    modify_json_many_times/1,
    set_rdf_metadata/1,
    modify_rdf_metadata/1,
    delete_rdf_metadata/1,
    delete_file_with_rdf_metadata/1,
    modify_rdf_many_times/1,
    set_xattr_metadata/1,
    cdmi_xattr_should_not_be_harvested/1,
    modify_xattr_metadata/1,
    delete_xattr_metadata/1,
    delete_file_with_xattr_metadata/1,
    modify_xattr_many_times/1,
    modify_metadata_and_rename_file/1
]).

all() ->
    ?ALL([
        set_json_metadata,
        modify_json_metadata,
        delete_json_metadata,
        delete_file_with_json_metadata,
        modify_json_many_times,
        set_rdf_metadata,
        modify_rdf_metadata,
        delete_rdf_metadata,
        delete_file_with_rdf_metadata,
        modify_rdf_many_times,
        set_xattr_metadata,
        cdmi_xattr_should_not_be_harvested,
        modify_xattr_metadata,
        delete_xattr_metadata,
        delete_file_with_xattr_metadata,
        modify_xattr_many_times,
        modify_metadata_and_rename_file
    ]).

%%%====================================================================
%%% Test function
%%%====================================================================

set_json_metadata(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON
        }
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON
        }
    }], ProviderId2).

modify_json_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON
        }
    }], ProviderId),

    JSON2 = #{<<"color">> => <<"blue">>, <<"size">> => <<"big">>},
    ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON2, []),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON2
        }
    }], ProviderId).

delete_json_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON
        }
    }], ProviderId),

    ok = opt_file_metadata:remove_custom_metadata(Worker, SessId, ?FILE_REF(Guid), json),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId).

delete_file_with_json_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON
        }
    }], ProviderId),

    ok = lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Guid)),
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"delete">>
    }], ProviderId).

modify_json_many_times(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    Modifications = 10000,
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ExpectedFinalJSON = lists:foldl(fun(I, _) ->
        Key = <<"key_", (integer_to_binary(I))/binary>>,
        Value = <<"value_", (integer_to_binary(I))/binary>>,
        JSON = #{Key => Value},
        ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON, []),
        JSON
    end, undefined, lists:seq(1, Modifications)),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => ExpectedFinalJSON
        }
    }], ProviderId).

set_rdf_metadata(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    RDF = ?DUMMY_RDF,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, RDF, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"rdf">> => RDF
        }
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => RDF
        }
    }], ProviderId2).

modify_rdf_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    RDF = ?DUMMY_RDF,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, RDF, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"rdf">> => RDF
        }
    }], ProviderId),

    RDF2 = ?DUMMY_RDF(2),
    ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, RDF2, []),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"rdf">> => RDF2
        }
    }], ProviderId).

delete_rdf_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    RDF = ?DUMMY_RDF,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, RDF, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"rdf">> => RDF
        }
    }], ProviderId),

    ok = opt_file_metadata:remove_custom_metadata(Worker, SessId, ?FILE_REF(Guid), rdf),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId).

delete_file_with_rdf_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    RDF = ?DUMMY_RDF,

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, RDF, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"rdf">> => RDF
        }
    }], ProviderId),

    ok = lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Guid)),
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"delete">>
    }], ProviderId).

modify_rdf_many_times(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    Modifications = 10000,
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ExpectedFinalRDF = lists:foldl(fun(I, _) ->
        RDF = ?DUMMY_RDF(I),
        ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), rdf, RDF, []),
        RDF
    end, undefined, lists:seq(1, Modifications)),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"rdf">> => ExpectedFinalRDF
        }
    }], ProviderId).

set_xattr_metadata(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    XattrName = <<"name">>,
    XattrValue = <<"value">>,
    Xattr = #xattr{name = XattrName, value = XattrValue},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"xattrs">> => #{
                XattrName => XattrValue
            }
        }
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"xattrs">> => #{
                XattrName => XattrValue
            }
        }
    }], ProviderId2).

cdmi_xattr_should_not_be_harvested(Config) ->
    [Worker, Worker2 | _] = ?config(op_worker_nodes, Config),
    FileName = ?FILE_NAME,
    FileContent = <<"file content">>,

    ObjectContentTypeHeader = {?HDR_CONTENT_TYPE, <<"application/cdmi-object">>},
    CDMIVersionHeader = {<<"X-CDMI-Specification-Version">>, <<"1.1.1">>},
    UserTokenHeader = rest_test_utils:user_token_header(?config({access_token, ?USER_ID}, Config)),

    RequestHeaders1 = [ObjectContentTypeHeader, CDMIVersionHeader, UserTokenHeader],
    RequestBody1 = #{<<"value">> => FileContent},
    RawRequestBody1 = json_utils:encode((RequestBody1)),
    {ok, _, _, Response1} = ?assertMatch({ok, ?HTTP_201_CREATED, _, _},
        cdmi_test_utils:do_request(Worker, binary_to_list(filename:join(?SPACE_NAME1, FileName)), put, RequestHeaders1, RawRequestBody1)),
    CdmiResponse1 = json_utils:decode(Response1),
    FileId = maps:get(<<"objectID">>, CdmiResponse1),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),
    ProviderId2 = ?PROVIDER_ID(Worker2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId),

    % Worker2 does not support SPACE1 so it shouldn't submit metadata entry
    ?assertNotReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId2).


modify_xattr_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    XattrName = <<"name">>,
    XattrValue = <<"value">>,
    Xattr = #xattr{name = XattrName, value = XattrValue},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"xattrs">> => #{
                XattrName => XattrValue
            }
        }
    }], ProviderId),

    XattrName2 = <<"name2">>,
    XattrValue2 = <<"value2">>,
    Xattr2 = #xattr{name = XattrName2, value = XattrValue2},

    ok = lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr2),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"xattrs">> => #{
                XattrName => XattrValue,
                XattrName2 => XattrValue2
            }
        }
    }], ProviderId).

delete_xattr_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    XattrName = <<"name">>,
    XattrValue = <<"value">>,
    Xattr = #xattr{name = XattrName, value = XattrValue},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"xattrs">> => #{
                XattrName => XattrValue
            }
        }
    }], ProviderId),

    ok = lfm_proxy:remove_xattr(Worker, SessId, ?FILE_REF(Guid), XattrName),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{}
    }], ProviderId).

delete_file_with_xattr_metadata(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,
    XattrName = <<"name">>,
    XattrValue = <<"value">>,
    Xattr = #xattr{name = XattrName, value = XattrValue},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"submit">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"payload">> => #{
            <<"xattrs">> => #{
                XattrName => XattrValue
            }
        }
    }], ProviderId),

    ok = lfm_proxy:unlink(Worker, SessId, ?FILE_REF(Guid)),
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"operation">> => <<"delete">>
    }], ProviderId).

modify_xattr_many_times(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    Modifications = 10000,
    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    ExpectedFinalXattrs = lists:foldl(fun(I, XattrsIn) ->
        XattrName = <<"name", (integer_to_binary(I))/binary>>,
        XattrValue = <<"value", (integer_to_binary(I))/binary>>,
        Xattr = #xattr{name = XattrName, value = XattrValue},
        ok = lfm_proxy:set_xattr(Worker, SessId, ?FILE_REF(Guid), Xattr),
        XattrsIn#{XattrName => XattrValue}
    end, #{}, lists:seq(1, Modifications)),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"xattrs">> => ExpectedFinalXattrs
        }
    }], ProviderId).

modify_metadata_and_rename_file(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    FileName2 = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1)),
    ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON, []),
    {ok, FileId} = file_id:guid_to_objectid(Guid),

    Destination = #{?HARVESTER1 => [?INDEX11]},
    ProviderId = ?PROVIDER_ID(Worker),

    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON
        }
    }], ProviderId),

    JSON2 = #{<<"color">> => <<"blue">>, <<"size">> => <<"big">>},
    {ok, Guid} = lfm_proxy:mv(Worker, SessId, ?FILE_REF(Guid), ?PATH(FileName2, ?SPACE_ID1)),
    ok = opt_file_metadata:set_custom_metadata(Worker, SessId, ?FILE_REF(Guid), json, JSON2, []),

    % both updates should be aggregated and sent in one batch
    ?assertReceivedHarvestMetadata(?SPACE_ID1, Destination, [#{
        <<"fileId">> => FileId,
        <<"spaceId">> => ?SPACE_ID1,
        <<"fileName">> => FileName2,
        <<"fileType">> => str_utils:to_binary(?REGULAR_FILE_TYPE),
        <<"operation">> => <<"submit">>,
        <<"payload">> => #{
            <<"json">> => JSON2
        }
    }], ProviderId).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    harvesting_test_utils:init_per_suite(Config).

end_per_suite(Config) ->
    harvesting_test_utils:end_per_suite(Config).

init_per_testcase(Case, Config) ->
    harvesting_test_utils:init_per_testcase(Case, Config).

end_per_testcase(Case, Config) ->
    harvesting_test_utils:end_per_testcase(Case, Config).