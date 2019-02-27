%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module tests mechanism for harvesting metadata
%%% @end
%%%-------------------------------------------------------------------
-module(harvest_test_SUITE).
-author("Jakub Kudzia").

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
    set_json_metadata_test/1,
    set_json_metadata_custom_accepted_type_field_test/1,
    set_json_metadata_custom_not_accepted_type_field_test/1,
    modify_json_metadata_test/1,
    modify_json_metadata_custom_accepted_type_field_test/1,
    modify_json_metadata_custom_not_accepted_type_field_test/1,
    delete_json_metadata_test/1,
    delete_json_metadata_custom_accepted_type_field_test/1,
    delete_file_with_json_metadata_test/1,
    delete_file_with_json_metadata_custom_accepted_type_field_test/1,
    modify_json_many_times/1,
    changes_should_be_submitted_to_all_harvesters_subscribed_for_the_space/1,
    changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester/1,
    each_provider_should_submit_only_local_changes_to_the_harvester/1,
    each_provider_should_submit_only_local_changes_to_the_harvester_deletion_test/1,
    restart_harvest_stream_on_submit_entry_failure_test/1,
    restart_harvest_stream_on_delete_entry_failure_test/1
]).

all() ->
    ?ALL([
        set_json_metadata_test,
        set_json_metadata_custom_accepted_type_field_test,
        set_json_metadata_custom_not_accepted_type_field_test,
        modify_json_metadata_test,
        modify_json_metadata_custom_accepted_type_field_test,
        modify_json_metadata_custom_not_accepted_type_field_test,
        delete_json_metadata_test,
        delete_json_metadata_custom_accepted_type_field_test,
        delete_file_with_json_metadata_test,
        delete_file_with_json_metadata_custom_accepted_type_field_test,
        modify_json_many_times,
        changes_should_be_submitted_to_all_harvesters_subscribed_for_the_space,
        changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester,
        each_provider_should_submit_only_local_changes_to_the_harvester,
        each_provider_should_submit_only_local_changes_to_the_harvester_deletion_test,
        restart_harvest_stream_on_submit_entry_failure_test,
        restart_harvest_stream_on_delete_entry_failure_test
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
-define(ATTEMPTS, 10).
-define(TIMEOUT, timer:seconds(?ATTEMPTS)).

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

-define(SUBMIT_ENTRY, submit_entry).
-define(SUBMIT_ENTRY(FileId, Harvester, Payload), {?SUBMIT_ENTRY, FileId, Harvester, Payload}).

-define(assertReceivedSubmitEntry(ExpFileId, ExpHarvester, ExpPayload),
    ?assertReceivedSubmitEntry(ExpFileId, ExpHarvester, ExpPayload, ?TIMEOUT)).
-define(assertReceivedSubmitEntry(ExpFileId, ExpHarvester, ExpPayload, Timeout),
    ?assertReceivedEqual(?SUBMIT_ENTRY(ExpFileId, ExpHarvester,
        #{<<"payload">> => json_utils:encode(ExpPayload)}
    ), Timeout)).

-define(assertNotReceivedSubmitEntry(ExpFileId, ExpHarvester, ExpPayload),
    ?assertNotReceivedSubmitEntry(ExpFileId, ExpHarvester, ExpPayload, ?TIMEOUT)).
-define(assertNotReceivedSubmitEntry(ExpFileId, ExpHarvester, ExpPayload, Timeout),
    ?assertNotReceivedMatch(?SUBMIT_ENTRY(ExpFileId, ExpHarvester, ExpPayload), Timeout)).


-define(DELETE_ENTRY, delete_entry).
-define(DELETE_ENTRY(FileId, Harvester), {?DELETE_ENTRY, FileId, Harvester}).

-define(assertReceivedDeleteEntry(ExpFileId, ExpHarvester),
    ?assertReceivedDeleteEntry(ExpFileId, ExpHarvester, ?TIMEOUT)).
-define(assertReceivedDeleteEntry(ExpFileId, ExpHarvester, Timeout),
    ?assertReceivedEqual(?DELETE_ENTRY(ExpFileId, ExpHarvester), Timeout)).

-define(assertNotReceivedDeleteEntry(ExpFileId, ExpHarvester),
    ?assertNotReceivedDeleteEntry(ExpFileId, ExpHarvester, ?TIMEOUT)).
-define(assertNotReceivedDeleteEntry(ExpFileId, ExpHarvester, Timeout),
    ?assertNotReceivedEqual(?DELETE_ENTRY(ExpFileId, ExpHarvester), Timeout)).

-define(TYPE_PAYLOAD(Type), <<(Type)/binary, "_payload">>).

-define(CUSTOM_ENTRY_TYPE_FIELD, <<"CUSTOM_METADATA_TYPE_FIELD">>).

-define(CUSTOM_ACCEPTED_ENTRY_TYPE, <<"CUSTOM_ACCEPTED_METADATA_TYPE">>).
-define(CUSTOM_NOT_ACCEPTED_ENTRY_TYPE, <<"CUSTOM_NOT_ACCEPTED_METADATA_TYPE">>).

-define(DEFAULT_ENTRY_TYPE, <<"DEFAULT_METADATA_TYPE">>).

-define(ACCEPTED_ENTRY_TYPES, [?CUSTOM_ACCEPTED_ENTRY_TYPE, ?DEFAULT_ENTRY_TYPE]).

%%%====================================================================
%%% Test function
%%%====================================================================

set_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON
    }).

set_json_metadata_custom_accepted_type_field_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{
        <<"color">> => <<"blue">>,
        ?CUSTOM_ENTRY_TYPE_FIELD => ?CUSTOM_ACCEPTED_ENTRY_TYPE
    },

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?CUSTOM_ACCEPTED_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?CUSTOM_ACCEPTED_ENTRY_TYPE) => JSON
    }).

set_json_metadata_custom_not_accepted_type_field_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{
        <<"color">> => <<"blue">>,
        ?CUSTOM_ENTRY_TYPE_FIELD => ?CUSTOM_NOT_ACCEPTED_ENTRY_TYPE
    },

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertNotReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> := ?CUSTOM_NOT_ACCEPTED_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?CUSTOM_NOT_ACCEPTED_ENTRY_TYPE) := JSON
    }).

modify_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON
    }),

    JSON2 = #{<<"color">> => <<"blue">>, <<"size">> => <<"big">>},
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON2, []),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON2
    }).

modify_json_metadata_custom_accepted_type_field_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{
        <<"color">> => <<"blue">>,
        ?CUSTOM_ENTRY_TYPE_FIELD => ?CUSTOM_ACCEPTED_ENTRY_TYPE
    },

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?CUSTOM_ACCEPTED_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?CUSTOM_ACCEPTED_ENTRY_TYPE) => JSON
    }),

    JSON2 = #{
        <<"color">> => <<"blue">>,
        <<"size">> => <<"big">>,
        ?CUSTOM_ENTRY_TYPE_FIELD => ?CUSTOM_ACCEPTED_ENTRY_TYPE
    },
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON2, []),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?CUSTOM_ACCEPTED_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?CUSTOM_ACCEPTED_ENTRY_TYPE) => JSON2
    }).

modify_json_metadata_custom_not_accepted_type_field_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{
        <<"color">> => <<"blue">>,
        ?CUSTOM_ENTRY_TYPE_FIELD => ?CUSTOM_ACCEPTED_ENTRY_TYPE
    },

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?CUSTOM_ACCEPTED_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?CUSTOM_ACCEPTED_ENTRY_TYPE) => JSON
    }),

    JSON2 = #{
        <<"color">> => <<"blue">>,
        <<"size">> => <<"big">>,
        ?CUSTOM_ENTRY_TYPE_FIELD => ?CUSTOM_NOT_ACCEPTED_ENTRY_TYPE
    },
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON2, []),

    ?assertNotReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> := ?CUSTOM_NOT_ACCEPTED_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?CUSTOM_NOT_ACCEPTED_ENTRY_TYPE) := JSON2
    }).


delete_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON
    }),

    ok = lfm_proxy:remove_metadata(Worker, SessId, {guid, Guid}, json),
    ?assertReceivedDeleteEntry(FileId, ?HARVESTER1).

delete_json_metadata_custom_accepted_type_field_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #
        {<<"color">> => <<"blue">>,
        ?CUSTOM_ENTRY_TYPE_FIELD => ?CUSTOM_ACCEPTED_ENTRY_TYPE
    },

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?CUSTOM_ACCEPTED_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?CUSTOM_ACCEPTED_ENTRY_TYPE) => JSON
    }),

    ok = lfm_proxy:remove_metadata(Worker, SessId, {guid, Guid}, json),
    ?assertReceivedDeleteEntry(FileId, ?HARVESTER1).


delete_file_with_json_metadata_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON
    }),

    ok = lfm_proxy:unlink(Worker, SessId, {guid, Guid}),
    ?assertReceivedDeleteEntry(FileId, ?HARVESTER1).

delete_file_with_json_metadata_custom_accepted_type_field_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON = #{
        <<"color">> => <<"blue">>,
        ?CUSTOM_ENTRY_TYPE_FIELD => ?CUSTOM_ACCEPTED_ENTRY_TYPE
    },

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?CUSTOM_ACCEPTED_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?CUSTOM_ACCEPTED_ENTRY_TYPE) => JSON
    }),

    ok = lfm_proxy:unlink(Worker, SessId, {guid, Guid}),
    ?assertReceivedDeleteEntry(FileId, ?HARVESTER1).

modify_json_many_times(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    Modifications = 10000,

    FileName = ?FILE_NAME,
    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ExpectedFinalJSON = lists:foldl(fun(I, _) ->
        Key = <<"key_", (integer_to_binary(I))/binary>>,
        Value = <<"value_", (integer_to_binary(I))/binary>>,
        JSON = #{Key => Value},
        ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON, []),
        JSON
    end, undefined, lists:seq(1, Modifications)),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => ExpectedFinalJSON
    }).

changes_should_be_submitted_to_all_harvesters_subscribed_for_the_space(Config) ->
    % ?HARVESTER1 and ?HARVESTER2 are subscribed for ?SPACE_ID2
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),
    FileName = ?FILE_NAME,

    JSON1 = #{<<"color">> => <<"blue">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID2), 8#600),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON1
    }),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER2, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON1
    }).

changes_from_all_subscribed_spaces_should_be_submitted_to_the_harvester(Config) ->
    % ?HARVESTER1 is subscribed for ?SPACE_ID3 and ?SPACE_ID4
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    FileName2 = ?FILE_NAME,
    JSON2 = #{<<"color">> => <<"red">>},

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID3), 8#600),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),

    {ok, Guid2} = lfm_proxy:create(Worker, SessId, ?PATH(FileName2, ?SPACE_ID4), 8#600),
    {ok, FileId2} = cdmi_id:guid_to_objectid(Guid2),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid2}, json, JSON2, []),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON1
    }),

    ?assertReceivedSubmitEntry(FileId2, ?HARVESTER1, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON2
    }).

each_provider_should_submit_only_local_changes_to_the_harvester(Config) ->
    % ?HARVESTER3 is subscribed for ?SPACE_ID5 which is supported by both providers
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(WorkerP1),
    SessId2 = ?SESS_ID(WorkerP2),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    FileName2 = ?FILE_NAME,
    JSON2 = #{<<"color">> => <<"red">>},

    {ok, Guid} = lfm_proxy:create(WorkerP1, SessId, ?PATH(FileName, ?SPACE_ID5), 8#600),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(WorkerP1, SessId, {guid, Guid}, json, JSON1, []),

    {ok, Guid2} = lfm_proxy:create(WorkerP2, SessId2, ?PATH(FileName2, ?SPACE_ID5), 8#600),
    {ok, FileId2} = cdmi_id:guid_to_objectid(Guid2),
    ok = lfm_proxy:set_metadata(WorkerP2, SessId2, {guid, Guid2}, json, JSON2, []),


    ?assertReceivedSubmitEntry(FileId, ?HARVESTER3, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON1
    }),

    ?assertReceivedSubmitEntry(FileId2, ?HARVESTER3, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON2
    }),

    % calls to harvester_logic:create entry should not be duplicated
    ?assertNotReceivedSubmitEntry(FileId, ?HARVESTER3, _),
    ?assertNotReceivedSubmitEntry(FileId2, ?HARVESTER3, _).

each_provider_should_submit_only_local_changes_to_the_harvester_deletion_test(Config) ->
    % ?HARVESTER3 is subscribed for ?SPACE_ID5 which is supported by both providers
    [WorkerP1, WorkerP2 | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(WorkerP1),
    SessId2 = ?SESS_ID(WorkerP2),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    FileName2 = ?FILE_NAME,
    JSON2 = #{<<"color">> => <<"red">>},

    {ok, Guid} = lfm_proxy:create(WorkerP1, SessId, ?PATH(FileName, ?SPACE_ID5), 8#600),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(WorkerP1, SessId, {guid, Guid}, json, JSON1, []),


    {ok, Guid2} = lfm_proxy:create(WorkerP2, SessId2, ?PATH(FileName2, ?SPACE_ID5), 8#600),
    {ok, FileId2} = cdmi_id:guid_to_objectid(Guid2),
    ok = lfm_proxy:set_metadata(WorkerP2, SessId2, {guid, Guid2}, json, JSON2, []),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER3, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON1
    }),

    ?assertReceivedSubmitEntry(FileId2, ?HARVESTER3, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON2
    }),

    % calls to harvester_logic:create entry should not be duplicated
    ?assertNotReceivedSubmitEntry(FileId, ?HARVESTER3, _),
    ?assertNotReceivedSubmitEntry(FileId2, ?HARVESTER3, _),

    ok = lfm_proxy:unlink(WorkerP1, SessId, {guid, Guid2}),
    ok = lfm_proxy:unlink(WorkerP2, SessId2, {guid, Guid}),

    ?assertReceivedDeleteEntry(FileId, ?HARVESTER3),
    ?assertReceivedDeleteEntry(FileId2, ?HARVESTER3),

    % calls to harvester_logic:delete entry should not be duplicated
    ?assertNotReceivedDeleteEntry(FileId, ?HARVESTER3),
    ?assertNotReceivedDeleteEntry(FileId2, ?HARVESTER3).

restart_harvest_stream_on_submit_entry_failure_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    HSPid1 = get_harvest_stream_pid(Worker, ?HARVESTER1, ?SPACE_ID1),

    mock_harvester_logic_submit_entry(Worker),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),


    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON1
    }),

    mock_harvester_logic_submit_entry_failure(Worker),

    JSON2 = #{<<"color">> => <<"red">>},
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON2, []),

    % change should not be submitted as connection to onezone failed
    ?assertNotReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> := ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) := JSON2
    }),

    mock_harvester_logic_submit_entry(Worker),

    % harvest_stream should have been restarted
    ?assertNotEqual(HSPid1,
        get_harvest_stream_pid(Worker, ?HARVESTER1, ?SPACE_ID1)),

    % previously sent change should not be submitted
    ?assertNotReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> := ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) := JSON1
    }),

    % missing change should be submitted
    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON2
    }).

restart_harvest_stream_on_delete_entry_failure_test(Config) ->
    [Worker | _] = ?config(op_worker_nodes, Config),
    SessId = ?SESS_ID(Worker),

    FileName = ?FILE_NAME,
    JSON1 = #{<<"color">> => <<"blue">>},

    HSPid1 = get_harvest_stream_pid(Worker, ?HARVESTER1, ?SPACE_ID1),

    mock_harvester_logic_submit_entry(Worker),

    {ok, Guid} = lfm_proxy:create(Worker, SessId, ?PATH(FileName, ?SPACE_ID1), 8#600),
    {ok, FileId} = cdmi_id:guid_to_objectid(Guid),
    ok = lfm_proxy:set_metadata(Worker, SessId, {guid, Guid}, json, JSON1, []),

    ?assertReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> => ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) => JSON1
    }),

    mock_harvester_logic_delete_entry_failure(Worker),

    ok = lfm_proxy:unlink(Worker, SessId, {guid, Guid}),

    % change should not be submitted as connection to onezone failed
    ?assertNotReceivedDeleteEntry(FileId, ?HARVESTER1),

    mock_harvester_logic_delete_entry(Worker),

    % harvest_stream should have been restarted
    ?assertNotEqual(HSPid1,
        get_harvest_stream_pid(Worker, ?HARVESTER1, ?SPACE_ID1)),

    % previously sent change should not be submitted
    ?assertNotReceivedSubmitEntry(FileId, ?HARVESTER1, #{
        <<"type">> := ?DEFAULT_ENTRY_TYPE,
        ?TYPE_PAYLOAD(?DEFAULT_ENTRY_TYPE) := JSON1
    }),

    % missing change should be submitted
    ?assertReceivedDeleteEntry(FileId, ?HARVESTER1).

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    Posthook = fun(NewConfig) -> initializer:setup_storage(NewConfig) end,
    [{?ENV_UP_POSTHOOK, Posthook}, {?LOAD_MODULES, [initializer]} | Config].

end_per_suite(Config) ->
    initializer:teardown_storage(Config).

init_per_testcase(Case, Config) when
    Case =:= restart_harvest_stream_on_submit_entry_failure_test;
    Case =:= restart_harvest_stream_on_delete_entry_failure_test
    ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, harvester_logic),
    mock_harvester_logic_get(Workers),
    init_per_testcase(default, Config);

init_per_testcase(default, Config) ->
    Config2 = sort_workers(Config),
    Workers = ?config(op_worker_nodes, Config2),
    initializer:communicator_mock(Workers),
    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config2),
    lfm_proxy:init(ConfigWithSessionInfo);

init_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_new(Workers, harvester_logic),
    mock_harvester_logic_submit_entry(Workers),
    mock_harvester_logic_delete_entry(Workers),
    mock_harvester_logic_get(Workers),
    init_per_testcase(default, Config).

end_per_testcase(_Case, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    ok = test_utils:mock_unload(Workers, harvester_logic),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    test_utils:mock_validate_and_unload(Workers, [communicator]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

mock_harvester_logic_get(Node) ->
    ok = test_utils:mock_expect(Node, harvester_logic, get,
        fun(_) ->
            {ok, #document{value = #od_harvester{
                entry_type_field = ?CUSTOM_ENTRY_TYPE_FIELD,
                default_entry_type = ?DEFAULT_ENTRY_TYPE,
                accepted_entry_types = ?ACCEPTED_ENTRY_TYPES
            }}}
        end
    ).

mock_harvester_logic_submit_entry(Node) ->
    Self = self(),
    ok = test_utils:mock_expect(Node, harvester_logic, submit_entry_internal,
        fun(HarvesterId, FileId, Payload) ->
            Self ! ?SUBMIT_ENTRY(FileId, HarvesterId, Payload),
            ok
        end
    ).

mock_harvester_logic_delete_entry(Node) ->
    Self = self(),
    ok = test_utils:mock_expect(Node, harvester_logic, delete_entry,
        fun(HarvesterId, FileId) ->
            Self ! ?DELETE_ENTRY(FileId, HarvesterId),
            ok
        end
    ).

mock_harvester_logic_submit_entry_failure(Node) ->
    ok = test_utils:mock_expect(Node, harvester_logic, submit_entry_internal,
        fun(_HarvesterId, _FileId, _Payload) -> {error, test_error} end).

mock_harvester_logic_delete_entry_failure(Node) ->
    ok = test_utils:mock_expect(Node, harvester_logic, delete_entry,
        fun(_HarvesterId, _FileId) -> {error, test_error} end).

sort_workers(Config) ->
    Workers = ?config(op_worker_nodes, Config),
    lists:keyreplace(op_worker_nodes, 1, Config, {op_worker_nodes, lists:sort(Workers)}).

which_children(Node, SupRef) ->
    rpc:call(Node, supervisor, which_children, [SupRef]).

get_harvest_stream_pid(Node, HarvesterId, SpaceId) ->
    {_, HSPid, _, _} = ?assertMatch({_, _, _, _},
        lists:keyfind({HarvesterId, SpaceId}, 1, which_children(Node, harvest_stream_sup)),
        ?ATTEMPTS),
    HSPid.