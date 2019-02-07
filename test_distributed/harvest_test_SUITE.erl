%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains tests of lfm_attrs API.
%%% @end
%%%-------------------------------------------------------------------
-module(harvest_test_SUITE).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

%% tests
-export([
    set_xattr_test/1, modify_xattr_test/1,
    delete_xattr_test/1, delete_file_with_xattr_test/1,
    set_json_metadata_test/1, modify_json_metadata_test/1,
    delete_json_metadata_test/1, delete_file_with_json_metadata_test/1,
    set_rdf_metadata_test/1, modify_rdf_metadata_test/1,
    delete_rdf_metadata_test/1, delete_file_with_rdf_metadata_test/1,
    set_mixed_metadata_test/1, modify_mixed_metadata_test/1,
    delete_mixed_metadata_test/1, delete_file_with_mixed_metadata_test/1,
    set_many_xattrs_test/1,
    changes_should_be_submitted_to_all_harvesters_subscribed_for_the_space/1,
    changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester/1,
    each_provider_should_submit_only_local_changes_to_the_harvester/1]).

all() ->
    ?ALL([
        set_xattr_test,
        modify_xattr_test,
        delete_xattr_test,
        delete_file_with_xattr_test,
        set_json_metadata_test,
        modify_json_metadata_test,
        delete_json_metadata_test,
        delete_file_with_json_metadata_test,
        set_rdf_metadata_test,
        modify_rdf_metadata_test,
        delete_rdf_metadata_test,
        delete_file_with_rdf_metadata_test,
        set_mixed_metadata_test,
        modify_mixed_metadata_test,
        delete_mixed_metadata_test,
        delete_file_with_mixed_metadata_test,
        set_many_xattrs_test,
        changes_should_be_submitted_to_all_harvesters_subscribed_for_the_space,
        changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester,
        each_provider_should_submit_only_local_changes_to_the_harvester
    ]).

-define(SPACE_ID1, <<"space_id1">>).
-define(SPACE_ID2, <<"space_id2">>).
-define(SPACE_ID3, <<"space_id3">>).
-define(SPACE_ID4, <<"space_id4">>).
-define(SPACE_ID5, <<"space_id5">>).

-define(SPACE_NAME1, <<"space1">>).
-define(SPACE_NAME2, <<"space2">>).
-define(SPACE_NAME3, <<"space3">>).
-define(SPACE_NAME4, <<"space4">>).
-define(SPACE_NAME5, <<"space5">>).

-define(SPACE_NAMES, #{
    ?SPACE_ID1 => ?SPACE_NAME1,
    ?SPACE_ID2 => ?SPACE_NAME2,
    ?SPACE_ID3 => ?SPACE_NAME3,
    ?SPACE_ID4 => ?SPACE_NAME4,
    ?SPACE_ID5 => ?SPACE_NAME5
}).

-define(SPACE_NAME(__SpaceId), maps:get(__SpaceId, ?SPACE_NAMES)).

-define(PATH(FileName, SpaceId), filename:join(["/", ?SPACE_NAME(SpaceId), FileName])).

-define(HARVESTER1, <<"harvester1">>).
-define(HARVESTER2, <<"harvester2">>).
-define(HARVESTER3, <<"harvester3">>).

-define(USER_ID, <<"user1">>).
-define(SESS_ID(Worker),
    ?config({session_id, {?USER_ID, ?GET_DOMAIN(Worker)}}, Config)).

-define(XATTR_NAME, <<"xattr_name_", (?RAND_NAME)/binary>>).
-define(XATTR_VAL, <<"xattr_val_", (?RAND_NAME)/binary>>).

-define(FILE_NAME, <<"file_", (?RAND_NAME)/binary>>).

-define(RAND_NAME,
    <<(str_utils:to_binary(?FUNCTION))/binary, "_", (integer_to_binary(rand:uniform(?RAND_RANGE)))/binary>>).

-define(RAND_RANGE, 1000000000).
-define(TIMEOUT, 10000).

% todo test of passing only local changes !!!!! 2 providers supporting space

%% Test config:
%% space_id1:
%%  * supported by: p1
%%  * harvesters: harvester1
%% space_id2:
%%  * supported by: p1
%%  * harvesters: harvester1, harvester2
%% space_id3:
%%  * supported by: p1
%%  * harvesters: harvester1
%% space_id4:
%%  * supported by: p1
%%  * harvesters: harvester1
%% space_id5:
%%  * supported by: p1, p2
%%  * harvesters: harvester3


%%%====================================================================
%%% Test function
%%%====================================================================

set_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    Name = ?XATTR_NAME,
    Value1 = ?XATTR_VAL,
    Xattr1 = #xattr{name = Name, value = Value1},
    Name2 = ?XATTR_NAME,
    Value2 = ?XATTR_VAL,
    Xattr2 = #xattr{name = Name2, value = Value2},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr2),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        Name => Value1,
        Name2 => Value2
    }},?TIMEOUT).

modify_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    Name = ?XATTR_NAME,
    Value1 = ?XATTR_VAL,
    Xattr1 = #xattr{name = Name, value = Value1},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        Name => Value1
    }},?TIMEOUT),

    Value2 = ?XATTR_VAL,
    Xattr2 = #xattr{name = Name, value = Value2},
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr2),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        Name => Value2
    }},?TIMEOUT).

delete_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    Name = ?XATTR_NAME,
    Value1 = ?XATTR_VAL,
    Xattr1 = #xattr{name = Name, value = Value1},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        Name => Value1
    }},?TIMEOUT),

    ok = lfm_proxy:remove_xattr(Worker, SessId, {guid, Guid}, Name),
    
    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1
    }},?TIMEOUT).

delete_file_with_xattr_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    Name = ?XATTR_NAME,
    Value1 = ?XATTR_VAL,
    Xattr1 = #xattr{name = Name, value = Value1},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        Name => Value1
    }},?TIMEOUT),

    ok = lfm_proxy:unlink(Worker, SessId, {guid, Guid}),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"true">>,
        <<"spaceId">> => ?SPACE_ID1,
        Name => Value1
    }},?TIMEOUT).

set_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},
    EncodedJSON = json_utils:encode(JSON),
    
    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"json">> => EncodedJSON
    }},?TIMEOUT).

modify_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},
    EncodedMetaJSON = json_utils:encode(JSON),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"json">> => EncodedMetaJSON
    }},?TIMEOUT),
    
    JSON2 = #{<<"color">> => <<"blue">>, <<"size">> => <<"big">>},
    EncodedJSON2 = json_utils:encode(JSON2),

    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON2, []),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"json">> => EncodedJSON2
    }},?TIMEOUT).

delete_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},
    EncodedJSON = json_utils:encode(JSON),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"json">> => EncodedJSON
    }},?TIMEOUT),
    
    ok = lfm_proxy:remove_metadata(Worker, SessId, {guid, Guid}, json),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1
    }},?TIMEOUT).

delete_file_with_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    MetaBlue = #{<<"color">> => <<"blue">>},
    EncodedMetaBlue = json_utils:encode(MetaBlue),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, MetaBlue, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"json">> => EncodedMetaBlue
    }},?TIMEOUT),

    ok = lfm_proxy:unlink(Worker, SessId, {guid, Guid}),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"true">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"json">> => EncodedMetaBlue
    }},?TIMEOUT).

set_rdf_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    RDF = <<"dummy rdf">>,
    EncodedRDF = json_utils:encode(RDF),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, rdf, RDF, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"rdf">> => EncodedRDF
    }},?TIMEOUT).

modify_rdf_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    RDF = <<"dummy rdf">>,
    EncodedRDF = json_utils:encode(RDF),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, rdf, RDF, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"rdf">> => EncodedRDF
    }},?TIMEOUT),

    RDF2 = <<"dummy rdf 2">>,
    EncodedRDF2 = json_utils:encode(RDF2),

    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, rdf, RDF2, []),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"rdf">> => EncodedRDF2
    }},?TIMEOUT).

delete_rdf_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    RDF = <<"dummy rdf">>,
    EncodedRDF = json_utils:encode(RDF),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, rdf, RDF, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"rdf">> => EncodedRDF
    }},?TIMEOUT),

    ok = lfm_proxy:remove_metadata(Worker, SessId, {guid, Guid}, rdf),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1
    }},?TIMEOUT).

delete_file_with_rdf_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    RDF = <<"dummy rdf">>,
    EncodedRDF = json_utils:encode(RDF),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, rdf, RDF, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"rdf">> => EncodedRDF
    }},?TIMEOUT),

    ok = lfm_proxy:unlink(Worker, SessId, {guid, Guid}),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"true">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"rdf">> => EncodedRDF
    }},?TIMEOUT).

set_mixed_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    Name = ?XATTR_NAME,
    Value1 = ?XATTR_VAL,
    Xattr1 = #xattr{name = Name, value = Value1},
    JSON = #{<<"color">> => <<"blue">>},
    EncodedJSON = json_utils:encode(JSON),
    RDF = <<"dummy rdf">>,
    EncodedRDF = json_utils:encode(RDF),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, rdf, RDF, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        Name => Value1,
        <<"json">> => EncodedJSON,
        <<"rdf">> => EncodedRDF
    }},?TIMEOUT).

modify_mixed_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    Name = ?XATTR_NAME,
    Value1 = ?XATTR_VAL,
    Xattr1 = #xattr{name = Name, value = Value1},
    JSON = #{<<"color">> => <<"blue">>},
    EncodedJSON = json_utils:encode(JSON),
    RDF = <<"dummy rdf">>,
    EncodedRDF = json_utils:encode(RDF),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, rdf, RDF, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        Name => Value1,
        <<"json">> => EncodedJSON,
        <<"rdf">> => EncodedRDF
    }},?TIMEOUT),

    Value2 = ?XATTR_VAL,
    Xattr2 = #xattr{name = Name, value = Value2},
    Name3 = ?XATTR_NAME,
    Value3 = ?XATTR_VAL,
    Xattr3 = #xattr{name = Name3, value = Value3},
    JSON2 = #{<<"size">> => <<"big">>, <<"color">> => <<"blue">>},
    EncodedJSON2 = json_utils:encode(JSON2),
    RDF2 = <<"dummy rdf 2">>,
    EncodedRDF2 = json_utils:encode(RDF2),

    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr2),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr3),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON2, []),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, rdf, RDF2, []),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        Name => Value2,
        Name3 => Value3,
        <<"json">> => EncodedJSON2,
        <<"rdf">> => EncodedRDF2
    }},?TIMEOUT).

delete_mixed_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    Name = ?XATTR_NAME,
    Value1 = ?XATTR_VAL,
    Xattr1 = #xattr{name = Name, value = Value1},
    JSON = #{<<"color">> => <<"blue">>},
    EncodedJSON = json_utils:encode(JSON),
    RDF = <<"dummy rdf">>,
    EncodedRDF = json_utils:encode(RDF),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, rdf, RDF, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        Name => Value1,
        <<"json">> => EncodedJSON,
        <<"rdf">> => EncodedRDF
    }},?TIMEOUT),
    
    % delete xattr and rdf metadata
    ok = lfm_proxy:remove_xattr(Worker, SessId, {guid, Guid}, Name),
    ok = lfm_proxy:remove_metadata(Worker, SessId, {guid, Guid}, rdf),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        <<"json">> => EncodedJSON
    }},?TIMEOUT).

delete_file_with_mixed_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    Name = ?XATTR_NAME,
    Value1 = ?XATTR_VAL,
    Xattr1 = #xattr{name = Name, value = Value1},
    JSON = #{<<"color">> => <<"blue">>},
    EncodedJSON = json_utils:encode(JSON),
    RDF = <<"dummy rdf">>,
    EncodedRDF = json_utils:encode(RDF),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, rdf, RDF, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1,
        Name => Value1,
        <<"json">> => EncodedJSON,
        <<"rdf">> => EncodedRDF
    }},?TIMEOUT),
    
    ok = lfm_proxy:unlink(Worker, SessId, {guid, Guid}),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"true">>,
        <<"spaceId">> => ?SPACE_ID1,
        Name => Value1,
        <<"json">> => EncodedJSON,
        <<"rdf">> => EncodedRDF
    }},?TIMEOUT).

set_many_xattrs_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    XattrsToSetNum = 10000,

    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ExpectedDoc0 = #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID1
    },

    ExpectedDoc = lists:foldl(fun(_, AccIn) ->
        Name = ?XATTR_NAME,
        Value = ?XATTR_VAL,
        Xattr = #xattr{name = Name, value = Value},
        ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr),
        AccIn#{Name => Value}
    end, ExpectedDoc0, lists:seq(1, XattrsToSetNum)),

    ?assertReceivedEqual({?HARVESTER1, ExpectedDoc},?TIMEOUT).

changes_should_be_submitted_to_all_harvesters_subscribed_for_the_space(Config) ->
    % ?HARVESTER1 and ?HARVESTER2 are subscribed for ?SPACE_ID2
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    Name = ?XATTR_NAME,
    Value1 = ?XATTR_VAL,
    Xattr1 = #xattr{name = Name, value = Value1},
    Name2 = ?XATTR_NAME,
    Value2 = ?XATTR_VAL,
    Xattr2 = #xattr{name = Name2, value = Value2},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID2), 8#600),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr2),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID2,
        Name => Value1,
        Name2 => Value2
    }},?TIMEOUT),

    ?assertReceivedEqual({?HARVESTER2, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID2,
        Name => Value1,
        Name2 => Value2
    }},?TIMEOUT).

changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester(Config) ->
    % ?HARVESTER1 is subscribed for ?SPACE_ID3 and ?SPACE_ID4
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    Name = ?XATTR_NAME,
    Value1 = ?XATTR_VAL,
    Xattr1 = #xattr{name = Name, value = Value1},
    FileName2 = ?FILE_NAME,
    Name2 = ?XATTR_NAME,
    Value2 = ?XATTR_VAL,
    Xattr2 = #xattr{name = Name2, value = Value2},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID3), 8#600),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid}, Xattr1),
    {ok, Guid2} = lfm_proxy:create(Worker, SessId, ?PATH(FileName2, ?SPACE_ID4), 8#600),
    {ok, FileId2} = cdmi_id:guid_to_objectid(Guid2),
    ok = lfm_proxy:set_xattr(Worker, SessId, {guid, Guid2}, Xattr2),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID3,
        Name => Value1
    }},?TIMEOUT),

    ?assertReceivedEqual({?HARVESTER1, #{
        <<"id">> => FileId2,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID4,
        Name2 => Value2
    }},?TIMEOUT).

each_provider_should_submit_only_local_changes_to_the_harvester(Config) ->
    % ?HARVESTER3 is subscribed for ?SPACE_ID5 which is supported by both providers
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(WorkerP1),
    SessId2 = ?SESS_ID(WorkerP2),

    FileName = ?FILE_NAME,
    Name = ?XATTR_NAME,
    Value1 = ?XATTR_VAL,
    Xattr1 = #xattr{name = Name, value = Value1},
    FileName2 = ?FILE_NAME,
    Name2 = ?XATTR_NAME,
    Value2 = ?XATTR_VAL,
    Xattr2 = #xattr{name = Name2, value = Value2},

    {ok, Guid} = lfm_proxy:create(WorkerP1, SessId, ?PATH(FileName, ?SPACE_ID5), 8#600),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_xattr(WorkerP1, SessId, {guid, Guid}, Xattr1),


    {ok, Guid2} = lfm_proxy:create(WorkerP2, SessId2, ?PATH(FileName2, ?SPACE_ID5), 8#600),
    {ok, FileId2} = cdmi_id:guid_to_objectid(Guid2),
    ok = lfm_proxy:set_xattr(WorkerP2, SessId2, {guid, Guid2}, Xattr2),

    ?assertReceivedEqual({?HARVESTER3, #{
        <<"id">> => FileId,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID5,
        Name => Value1
    }},?TIMEOUT),

    ?assertReceivedEqual({?HARVESTER3, #{
        <<"id">> => FileId2,
        <<"deleted">> => <<"false">>,
        <<"spaceId">> => ?SPACE_ID5,
        Name2 => Value2
    }},?TIMEOUT),

    ?assertNotReceivedMatch({?HARVESTER3, #{<<"id">> := FileId}}, ?TIMEOUT),
    ?assertNotReceivedMatch({?HARVESTER3, #{<<"id">> := FileId2}}, ?TIMEOUT).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(_Case, Config) ->
    Config2 = sort_workers(Config),
    Workers = ?config(op_worker_nodes, Config2),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config2),
    Config3 = lfm_proxy:init(ConfigWithSessionInfo),
    mock_harvest_logic_submit(Workers),
    Config3.

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    unmock_harvest_logic_submit(Workers),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

mock_harvest_logic_submit(Node) ->
    Self = self(),
    ok = test_utils:mock_new(Node, harvester_logic),
    ok = test_utils:mock_expect(Node, harvester_logic, submit, fun(_SessionId, HarvesterId, Data) ->
        Self ! {HarvesterId, Data}
    end).

unmock_harvest_logic_submit(Node) ->
    ok = test_utils:mock_unload(Node, harvester_logic).

sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).
