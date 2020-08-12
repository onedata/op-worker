%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Multi provider rest tests for metadata changes mechanism.
%%% @end
%%%-------------------------------------------------------------------
-module(multi_provider_changes_rest_test_SUITE).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("proto/common/handshake_messages.hrl").
-include("rest_test_utils.hrl").
-include_lib("cluster_worker/include/graph_sync/graph_sync.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/http/headers.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/privileges.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    token_auth_test/1,
    invalid_request_should_fail/1,
    unauthorized_request_should_fail/1,
    changes_stream_file_meta_create_test/1,
    changes_stream_file_meta_change_test/1,
    changes_stream_xattr_test/1,
    changes_stream_json_metadata_test/1,
    changes_stream_times_test/1,
    changes_stream_file_location_test/1,
    changes_triggers_test/1,
    changes_stream_request_several_records_test/1,
    changes_stream_on_multi_provider_test/1,
    changes_stream_closed_on_disconnection/1
]).

all() ->
    ?ALL([
        token_auth_test,
        invalid_request_should_fail,
        unauthorized_request_should_fail,
        changes_stream_file_meta_create_test,
        changes_stream_file_meta_change_test,
        changes_stream_xattr_test,
        changes_stream_json_metadata_test,
        changes_stream_times_test,
        changes_stream_file_location_test,
        changes_triggers_test,
        changes_stream_request_several_records_test,
        changes_stream_on_multi_provider_test,
        changes_stream_closed_on_disconnection
    ]).

-define(USER_1, <<"user1">>).
-define(USER_1_AUTH_HEADERS(Config), ?USER_1_AUTH_HEADERS(Config, [])).
-define(USER_1_AUTH_HEADERS(Config, OtherHeaders),
    ?USER_AUTH_HEADERS(Config, ?USER_1, OtherHeaders)
).

-define(ATTEMPTS, 5).

%%%===================================================================
%%% Test functions
%%%===================================================================


token_auth_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Json = #{<<"fileMeta">> => #{
        <<"fields">> => [<<"mode">>, <<"owner">>, <<"name">>]
    }},
    Token = ?config({access_token, ?USER_1}, Config),

    % Request containing data caveats should be rejected
    DataCaveat = #cv_data_path{whitelist = [<<"/", SpaceId/binary>>]},
    TokenWithDataCaveat = tokens:confine(Token, DataCaveat),
    ExpRestError1 = rest_test_utils:get_rest_error(
        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED(DataCaveat))
    ),
    ?assertMatch(ExpRestError1, get_changes(
        [{{access_token, ?USER_1}, TokenWithDataCaveat} | Config],
        WorkerP1, SpaceId, Json
    )),

    % Request containing invalid api caveat should be rejected
    GRIPattern = #gri_pattern{type = op_metrics, id = SpaceId, aspect = changes},
    InvalidApiCaveat = #cv_api{whitelist = [{all, all, GRIPattern#gri_pattern{id = <<"ASD">>}}]},
    TokenWithInvalidApiCaveat = tokens:confine(Token, InvalidApiCaveat),
    ExpRestError2 = rest_test_utils:get_rest_error(
        ?ERROR_UNAUTHORIZED(?ERROR_TOKEN_CAVEAT_UNVERIFIED(InvalidApiCaveat))
    ),
    ?assertMatch(ExpRestError2, get_changes(
        [{{access_token, ?USER_1}, TokenWithInvalidApiCaveat} | Config],
        WorkerP1, SpaceId, Json
    )),

    % Request containing valid api caveat should succeed
    ValidApiCaveat = #cv_api{whitelist = [{all, all, GRIPattern}]},
    TokenWithValidApiCaveat = tokens:confine(Token, ValidApiCaveat),
    ?assertMatch({ok, _}, get_changes(
        [{{access_token, ?USER_1}, TokenWithValidApiCaveat} | Config],
        WorkerP1, SpaceId, Json
    )).


invalid_request_should_fail(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    lists:foreach(fun({Json, ExpError}) ->
        ExpRestError = rest_test_utils:get_rest_error(ExpError),
        ?assertMatch(ExpRestError, get_changes(Config, WorkerP1, SpaceId, Json))
    end, [
        {<<"ASD">>, ?ERROR_BAD_VALUE_JSON(<<"changesSpecification">>)},
        {#{}, ?ERROR_BAD_VALUE_EMPTY(<<"changesSpecification">>)},
        {#{<<"triggers">> => <<"ASD">>}, ?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<"triggers">>)},
        {#{<<"triggers">> => [<<"ASD">>]}, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"triggers">>, [<<"fileMeta">>, <<"fileLocation">>, <<"times">>, <<"customMetadata">>])},
        {#{<<"fielMeta">> => #{<<"fields">> => [<<"owner">>]}}, ?ERROR_BAD_DATA(<<"fielMeta">>)},
        {#{<<"fileMeta">> => #{<<"fields">> => <<"owner">>}}, ?ERROR_BAD_VALUE_LIST_OF_BINARIES(<<"fileMeta.fields">>)},
        {#{<<"fileMeta">> => #{<<"fields">> => [<<"HEH">>]}}, ?ERROR_BAD_VALUE_NOT_ALLOWED(<<"fileMeta.fields">>, [
            <<"name">>, <<"type">>, <<"mode">>, <<"owner">>,
            <<"provider_id">>, <<"shares">>, <<"deleted">>
        ])},
        {#{<<"fileMeta">> => #{<<"always">> => <<"true">>}}, ?ERROR_BAD_VALUE_BOOLEAN(<<"fileMeta.always">>)}
    ]).


unauthorized_request_should_fail(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),
    Json = #{<<"fileMeta">> => #{
        <<"fields">> => [<<"mode">>, <<"owner">>, <<"name">>]
    }},

    ExpRestError = rest_test_utils:get_rest_error(?ERROR_FORBIDDEN),
    ?assertMatch(ExpRestError, get_changes(Config, WorkerP1, SpaceId, Json)).


changes_stream_file_meta_create_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    FileName = <<"file2_csfmt">>,
    FilePath = filename:join(["/", SpaceName, FileName]),
    FileMode = 8#700,

    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:create(WorkerP1, SessionId, FilePath, FileMode)
    end),

    ChangesSpec = #{<<"fileMeta">> => #{
        <<"fields">> => [<<"mode">>, <<"owner">>, <<"name">>]
    }},

    % On file creation a lot of docs are created (file_meta, times, etc.),
    % but since only file_meta doc is observed only its change should
    % trigger event
    ?assertMatch(
        [
            #{<<"fileMeta">> := #{
                <<"changed">> := true,
                <<"fields">> := #{
                    <<"name">> := FileName,
                    <<"owner">> := UserId,
                    <<"mode">> := FileMode
                }
            }}
        ],
        get_changes_for_file(Config, WorkerP1, SpaceId, ChangesSpec, {path, FilePath}),
        ?ATTEMPTS
    ).


changes_stream_file_meta_change_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    FileName = <<"file1_csfmt">>,
    FilePath = filename:join(["/", SpaceName, FileName]),
    Mode1 = 8#700,
    Mode2 = 8#777,

    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, FilePath, Mode1),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:set_perms(WorkerP1, SessionId, {guid, FileGuid}, Mode2)
    end),

    ChangesSpec = #{<<"fileMeta">> => #{
        <<"fields">> => [<<"mode">>, <<"owner">>, <<"name">>]
    }},

    % Both file_meta and times docs are changed, but since only
    % file_meta doc is observed only its change should trigger event
    ?assertMatch(
        [
            #{<<"fileMeta">> := #{
                <<"changed">> := true,
                <<"fields">> := #{
                    <<"name">> := FileName,
                    <<"owner">> := UserId,
                    <<"mode">> := Mode2
                }
            }}
        ],
        get_changes_for_file(Config, WorkerP1, SpaceId, ChangesSpec, {object_id, FileObjectId}),
        ?ATTEMPTS
    ).


changes_stream_xattr_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    File = filename:join(["/", SpaceName, "file3_csxt"]),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid},
            #xattr{name = <<"name">>, value = <<"value">>}
        )
    end),

    ChangesSpec = #{<<"customMetadata">> => #{
        <<"fields">> => [<<"name">>],
        <<"exists">> => [<<"onedata_keyvalue">>, <<"k1">>]
    }},

    % Both custom_metadata and times docs are changed, but since only
    % custom_metadata doc is observed only its change should trigger event
    ?assertMatch(
        [
            #{<<"customMetadata">> := #{
                <<"changed">> := true,
                <<"fields">> := #{
                    <<"name">> := <<"value">>
                },
                <<"exists">> := #{
                    <<"onedata_keyvalue">> := true,
                    <<"k1">> := false
                }
            }}
        ],
        get_changes_for_file(Config, WorkerP1, SpaceId, ChangesSpec, {object_id, FileObjectId}),
        ?ATTEMPTS
    ).


changes_stream_json_metadata_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    File = filename:join(["/", SpaceName, "file4_csjmt"]),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    OnedataJson = #{
        <<"k1">> => <<"v1">>,
        <<"k2">> => [<<"v2">>, <<"v3">>],
        <<"k3">> => #{<<"k31">> => <<"v31">>}
    },

    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:set_metadata(WorkerP1, SessionId, {guid, FileGuid}, json, OnedataJson, [])
    end),

    ChangesSpec = #{<<"customMetadata">> => #{
        <<"fields">> => [<<"onedata_json">>]
    }},

    % Both custom_metadata and times docs are changed, but since only
    % custom_metadata doc is observed only its change should trigger event
    ?assertMatch(
        [
            #{<<"customMetadata">> := #{
                <<"changed">> := true,
                <<"fields">> := #{
                    <<"onedata_json">> := OnedataJson
                }
            }}
        ],
        get_changes_for_file(Config, WorkerP1, SpaceId, ChangesSpec, {object_id, FileObjectId}),
        ?ATTEMPTS
    ).


changes_stream_times_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    File = filename:join(["/", SpaceName, "file5_cstt"]),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    Time = 1000,

    % when
    spawn(fun() ->
        timer:sleep(1000),
        lfm_proxy:update_times(WorkerP1, SessionId, {guid, FileGuid}, Time, Time, Time)
    end),

    ChangesSpec = #{<<"times">> => #{
        <<"fields">> => [<<"atime">>, <<"mtime">>, <<"ctime">>]
    }},

    % times docs are created on file creation and later updated using
    % 'lfm_proxy:update_times'. Changes stream aggregates events but with some
    % delay. Therefore it is possible for both events to be returned in changes
    % stream rather than the last one.
    % That is why fetching changes stream is retried ?ATTEMPTS number of times
    % to ensure events aggregation did happen and only one time event is
    % returned.
    ?assertMatch(
        [
            #{<<"times">> := #{
                <<"changed">> := true,
                <<"fields">> := #{
                    <<"atime">> := Time,
                    <<"mtime">> := Time,
                    <<"ctime">> := Time
                }
            }}
        ],
        get_changes_for_file(Config, WorkerP1, SpaceId, ChangesSpec, {object_id, FileObjectId}),
        ?ATTEMPTS
    ).


changes_stream_file_location_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    File = filename:join(["/", SpaceName, "file6_csflt"]),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    % when
    spawn(fun() ->
        timer:sleep(500),
        {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
        {ok, 5} = lfm_proxy:write(WorkerP1, Handle, 0, <<"01234">>)
    end),

    ChangesSpec = #{<<"fileLocation">> => #{<<"fields">> => [<<"size">>]}},

    % file_location docs are created on file creation and later updated on write
    % Changes stream aggregates events but with some delay. Therefore it is
    % possible for both events to be returned in changes stream rather than
    % the last one.
    % That is why fetching changes stream is retried ?ATTEMPTS number of times
    % to ensure events aggregation did happen and only one fileLocation event is
    % returned.
    ?assertMatch(
        [
            #{<<"fileLocation">> := #{
                <<"changed">> := true,
                <<"fields">> := #{
                    <<"size">> := 5
                }
            }}
        ],
        get_changes_for_file(Config, WorkerP1, SpaceId, ChangesSpec, {object_id, FileObjectId}),
        ?ATTEMPTS
    ).


changes_triggers_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    Mode = 8#700,
    FileName = <<"file_triggers_cstm">>,
    FilePath = filename:join(["/", SpaceName, FileName]),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, FilePath, Mode),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    Time = 1000,

    % when
    spawn(fun() ->
        timer:sleep(500),
        {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
        {ok, 5} = lfm_proxy:write(WorkerP1, Handle, 0, <<"01234">>),

        timer:sleep(500),
        lfm_proxy:update_times(WorkerP1, SessionId, {guid, FileGuid}, Time, Time, Time)
    end),

    ChangesSpec = #{
        <<"triggers">> => [<<"times">>],
        <<"fileMeta">> => #{
            <<"always">> => true,
            <<"fields">> => [<<"mode">>, <<"owner">>, <<"name">>]
        }
    },

    % Both file_location and times docs are changed, but since only times
    % doc is observed only its change should trigger event. It is still
    % possible to receive several events if changes stream aggregation hasn't
    % occurred (time docs are created on file creation and later updated).
    % That is why fetching changes stream is retried ?ATTEMPTS number of times
    % to ensure events aggregation did happen and only one time event is observed
    ?assertMatch(
        [
            #{<<"fileMeta">> := #{
                <<"changed">> := false,
                <<"fields">> := #{
                    <<"name">> := FileName,
                    <<"owner">> := UserId,
                    <<"mode">> := Mode
                }
            }}
        ],
        get_changes_for_file(Config, WorkerP1, SpaceId, ChangesSpec, {object_id, FileObjectId}),
        ?ATTEMPTS
    ).


changes_stream_request_several_records_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    Name = <<"file7_csflt">>,
    File = filename:join(["/", SpaceName, Name]),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    Time = 1000,
    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid},
            #xattr{name = <<"name">>, value = <<"value">>}
        ),
        timer:sleep(500),
        lfm_proxy:update_times(WorkerP1, SessionId, {guid, FileGuid}, Time, Time, Time)
    end),

    ChangesSpec = #{
        <<"triggers">> => [<<"times">>, <<"customMetadata">>],
        <<"fileMeta">> => #{
            <<"fields">> => [<<"name">>],
            <<"always">> => true
        },
        <<"times">> => #{
            <<"fields">> => [<<"atime">>, <<"mtime">>, <<"ctime">>]
        },
        <<"customMetadata">> => #{
            <<"fields">> => [<<"name">>]
        }
    },

    ?assertMatch(
        [
            % Update times event
            #{
                <<"fileMeta">> := #{
                    <<"changed">> := false,
                    <<"fields">> := #{
                        <<"name">> := Name
                    }
                },
                <<"times">> := #{
                    <<"changed">> := true,
                    <<"fields">> := #{
                        <<"atime">> := Time,
                        <<"mtime">> := Time,
                        <<"ctime">> := Time
                    }
                }
            },
            % Set metadata event
            #{
                <<"fileMeta">> := #{
                    <<"changed">> := false,
                    <<"fields">> := #{
                        <<"name">> := Name
                    }
                },
                <<"customMetadata">> := #{
                    <<"changed">> := true,
                    <<"fields">> := #{
                        <<"name">> := <<"value">>
                    }
                }
            }
        ],
        get_changes_for_file(Config, WorkerP1, SpaceId, ChangesSpec, {object_id, FileObjectId}),
        ?ATTEMPTS
    ).


changes_stream_on_multi_provider_test(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionIdP2 = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP2)}}, Config),
    [_, {SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    File = filename:join(["/", SpaceName, "file8_csompt"]),
    {ok, FileGuid} = lfm_proxy:create(WorkerP2, SessionIdP2, File, 8#700),
    {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),

    % when
    spawn(fun() ->
        timer:sleep(500),
        {ok, Handle} = lfm_proxy:open(WorkerP2, SessionIdP2, {guid, FileGuid}, write),
        lfm_proxy:write(WorkerP2, Handle, 0, <<"data">>)
    end),

    ChangesSpec = #{<<"fileLocation">> => #{
        <<"fields">> => [<<"size">>, <<"provider_id">>]
    }},
    DomainP2 = domain(WorkerP2),

    ?assertMatch(
        [
            #{
                <<"fileLocation">> := #{
                    <<"changed">> := true,
                    <<"mutators">> := [DomainP2],
                    <<"fields">> := #{
                        <<"size">> := 4,
                        <<"provider_id">> := DomainP2
                    }
                }
            }
        ],
        get_changes_for_file(Config, WorkerP1, SpaceId, ChangesSpec, {object_id, FileObjectId}),
        ?ATTEMPTS
    ).


changes_stream_closed_on_disconnection(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    [{SpaceId, _SpaceName} | _] = ?config({spaces, UserId}, Config),

    Json = #{<<"fileLocation">> => #{<<"fields">> => [<<"size">>]}},

    spawn(fun() ->
        get_changes(Config, WorkerP1, SpaceId, Json, <<"infinity">>, [{recv_timeout, 40000}])
    end),

    timer:sleep(timer:seconds(1)),
    receive
        {stream_pid, StreamPid} ->
            ?assertEqual(true, rpc:call(WorkerP1, erlang, is_process_alive, [StreamPid]))
    end,

    get_changes(Config, WorkerP1, SpaceId, Json, 100, [{recv_timeout, 400}]),
    receive
        {stream_pid, StreamPid1} ->
            ?assertEqual(false, rpc:call(WorkerP1, erlang, is_process_alive, [StreamPid1]))
    end,

    lists_utils:pforeach(fun(_) ->
        Pid = spawn(fun() ->
            get_changes(Config, WorkerP1, SpaceId, Json, <<"infinity">>, [{recv_timeout, 4000}])
        end),
        timer:sleep(timer:seconds(1)),
        exit(Pid, kill)
    end, lists:seq(0,10)),

    timer:sleep(timer:seconds(1)),
    lists:foreach(fun(_) ->
        receive
            {stream_pid, StreamPid2} ->
                ?assertEqual(false, rpc:call(WorkerP1, erlang, is_process_alive, [StreamPid2]))
        end
    end, lists:seq(0,10)).


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    Posthook = fun(NewConfig) ->
        NewConfig1 = [{space_storage_mock, false} | NewConfig],
        NewConfig2 = initializer:setup_storage(NewConfig1),
        lists:foreach(fun(Worker) ->
            test_utils:set_env(Worker, ?APP_NAME, dbsync_changes_broadcast_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, couchbase_changes_stream_update_interval, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_delay_ms, timer:seconds(1)),
            test_utils:set_env(Worker, ?CLUSTER_WORKER_APP_NAME, cache_to_disk_force_delay_ms, timer:seconds(1)), % TODO - change to 2 seconds
            test_utils:set_env(Worker, ?APP_NAME, prefetching, off),
            test_utils:set_env(Worker, ?APP_NAME, public_block_size_treshold, 0),
            test_utils:set_env(Worker, ?APP_NAME, public_block_percent_treshold, 0)
        end, ?config(op_worker_nodes, NewConfig2)),

        application:start(ssl),
        hackney:start(),
        initializer:create_test_users_and_spaces(?TEST_FILE(NewConfig2, "env_desc.json"), NewConfig2)
    end,
    {ok, _} = application:ensure_all_started(worker_pool),
    [
        {?ENV_UP_POSTHOOK, Posthook},
        {?LOAD_MODULES, [initializer, multi_provider_changes_rest_test_SUITE]}
        | Config
    ].


end_per_suite(Config) ->
    %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
    initializer:clean_test_users_and_spaces_no_validate(Config),
    hackney:stop(),
    application:stop(ssl),
    initializer:teardown_storage(Config).


init_per_testcase(token_auth_test, Config) ->
    initializer:mock_auth_manager(Config),
    init_per_testcase(all, Config);

init_per_testcase(changes_stream_closed_on_disconnection, Config) ->
    ct:timetrap(timer:minutes(3)),
    Workers = ?config(op_worker_nodes, Config),
    Pid = self(),
    ok = test_utils:mock_new(Workers, changes_stream_handler),
    ok = test_utils:mock_expect(Workers, changes_stream_handler, init_stream,
        fun(State) ->
            State1 = meck:passthrough([State]),
            StreamPid = maps:get(changes_stream, State1, undefined),
            Pid ! {stream_pid, StreamPid},
            State1
        end),
    init_per_testcase(all, Config);

init_per_testcase(unauthorized_request_should_fail, Config) ->
    Workers = [_, Worker] = ?config(op_worker_nodes, Config),

    UserId = <<"user1">>,
    [{SpaceId, _SpaceName} | _] = ?config({spaces, UserId}, Config),

    OldPrivs = rpc:call(Worker, initializer, node_get_mocked_space_user_privileges, [SpaceId, UserId]),
    NewPrivs = OldPrivs -- [?SPACE_VIEW_CHANGES_STREAM],

    initializer:testmaster_mock_space_user_privileges(Workers, SpaceId, UserId, NewPrivs),

    init_per_testcase(all, [{old_privs, OldPrivs} | Config]);

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    lfm_proxy:init(Config).


end_per_testcase(token_auth_test, Config) ->
    initializer:unmock_auth_manager(Config),
    end_per_testcase(all, Config);

end_per_testcase(changes_stream_closed_on_disconnection, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, changes_stream_handler),
    end_per_testcase(all, Config);

end_per_testcase(unauthorized_request_should_fail, Config) ->
    UserId = <<"user1">>,
    [{SpaceId, _SpaceName} | _] = ?config({spaces, UserId}, Config),
    Workers = ?config(op_worker_nodes, Config),
    OldPrivs = ?config(old_privs, Config),
    initializer:testmaster_mock_space_user_privileges(Workers, SpaceId, <<"user1">>, OldPrivs),
    end_per_testcase(all, Config);

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


domain(Node) ->
    atom_to_binary(?GET_DOMAIN(Node), utf8).


get_changes_for_file(Config, Worker, SpaceId, Json, FileKey) ->
    {ok, Changes} = get_changes(Config, Worker, SpaceId, Json),

    lists:filter(fun(#{<<"fileId">> := FileId, <<"filePath">> := FilePath}) ->
        case FileKey of
            {path, Path} -> Path == FilePath;
            {object_id, ObjectId} -> ObjectId == FileId
        end
    end, Changes).


get_changes(Config, Worker, SpaceId, Json) ->
    get_changes(Config, Worker, SpaceId, Json, 50000).


get_changes(Config, Worker, SpaceId, Json, Timeout) ->
    case Timeout of
        <<"infinity">> ->
            get_changes(Config, Worker, SpaceId, Json, Timeout, [{recv_timeout, 40000}]);
        _ ->
            Opts = [{recv_timeout, max(60000, Timeout + 10000)}],
            get_changes(Config, Worker, SpaceId, Json, Timeout, Opts)
    end.


get_changes(Config, Worker, SpaceId, Json, Timeout, Opts) ->
    Qs = case Timeout of
        undefined ->
            <<"?last_seq=0">>;
        <<"infinity">> ->
            <<"?last_seq=0&timeout=infinity">>;
        _ ->
            <<"?last_seq=0&timeout=", (integer_to_binary(Timeout))/binary>>
    end,
    Path = <<"changes/metadata/", SpaceId/binary, Qs/binary>>,
    Payload = json_utils:encode(Json),
    Headers = ?USER_1_AUTH_HEADERS(Config, [
        {?HDR_CONTENT_TYPE, <<"application/json">>}
    ]),

    case rest_test_utils:request(Worker, Path, post, Headers, Payload, Opts) of
        {ok, 200, _, Body} ->
            Changes = lists:map(fun(Change) ->
                json_utils:decode(Change)
            end, binary:split(Body, <<"\r\n">>, [global])),
            [_EmptyMap | RealChanges] = lists:reverse(Changes),
            {ok, RealChanges};
        {ok, Code, _, Body} ->
            {Code, json_utils:decode(Body)}
    end.
