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
-include("rest_test_utils.hrl").
-include("http/rest/rest_api/rest_errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    invalid_request_should_fail/1,
    changes_stream_file_meta_test/1,
    changes_stream_xattr_test/1,
    changes_stream_json_metadata_test/1,
    changes_stream_times_test/1,
    changes_stream_file_location_test/1,
    changes_stream_request_several_records_test/1,
    changes_stream_on_multi_provider_test/1,
    changes_stream_closed_on_disconnection/1
]).

all() ->
    ?ALL([
        invalid_request_should_fail,
        changes_stream_file_meta_test,
        changes_stream_xattr_test,
        changes_stream_json_metadata_test,
        changes_stream_times_test,
        changes_stream_file_location_test,
        changes_stream_request_several_records_test,
        changes_stream_on_multi_provider_test,
        changes_stream_closed_on_disconnection
    ]).

-define(USER_1_AUTH_HEADERS(Config), ?USER_1_AUTH_HEADERS(Config, [])).
-define(USER_1_AUTH_HEADERS(Config, OtherHeaders),
    ?USER_AUTH_HEADERS(Config, <<"user1">>, OtherHeaders)).


%%%===================================================================
%%% Test functions
%%%===================================================================


invalid_request_should_fail(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    [{SpaceId, _SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    lists:foreach(fun({Json, ExpError}) ->
        ?assertMatch(ExpError, get_changes(Config, WorkerP1, SpaceId, Json))
    end, [
        {<<"ASD">>, ?ERROR_INVALID_CHANGES_REQ},
        {#{}, ?ERROR_INVALID_CHANGES_REQ},
        {#{<<"fielMeta">> => #{<<"fields">> => [<<"owner">>]}}, ?ERROR_INVALID_FORMAT(<<"fielMeta">>)},
        {#{<<"fileMeta">> => #{<<"fields">> => <<"owner">>}}, ?ERROR_INVALID_FORMAT(<<"fileMeta">>)},
        {#{<<"fileMeta">> => #{<<"fields">> => [<<"HEH">>]}}, ?ERROR_INVALID_FIELD(<<"fileMeta">>, <<"HEH">>)}
    ]).


changes_stream_file_meta_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    Name1 = <<"file1_csfmt">>,
    File1 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), binary_to_list(Name1)])),
    Name2 = <<"file2_csfmt">>,
    File2 = list_to_binary(filename:join(["/", binary_to_list(SpaceName), binary_to_list(Name2)])),

    Mode1 = 8#700,
    Mode2 = 8#777,

    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File1, Mode1),

    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:set_perms(WorkerP1, SessionId, {guid, FileGuid}, 8#777),
        lfm_proxy:create(WorkerP1, SessionId, File2, Mode1)
    end),

    Json = #{<<"fileMeta">> => #{
        <<"fields">> => [<<"mode">>, <<"owner">>, <<"name">>]
    }},
    {ok, Changes} = get_changes(Config, WorkerP1, SpaceId, Json),
    ?assert(length(Changes) >= 2),

    [LastChange, PreLastChange | _] = Changes,
    ?assertMatch(#{<<"fileMeta">> := #{
        <<"changed">> := true,
        <<"fields">> := #{
            <<"name">> := Name2,
            <<"owner">> := UserId,
            <<"mode">> := Mode1
        }
    }}, LastChange),

    ?assertMatch(#{<<"fileMeta">> := #{
        <<"changed">> := true,
        <<"fields">> := #{
            <<"name">> := Name1,
            <<"owner">> := UserId,
            <<"mode">> := Mode2
        }
    }}, PreLastChange),

    ?assert(maps:get(<<"seq">>, LastChange) > maps:get(<<"seq">>, PreLastChange)).


changes_stream_xattr_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file3_csxt"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),

    % when
    spawn(fun() ->
        timer:sleep(500),
        lfm_proxy:set_xattr(WorkerP1, SessionId, {guid, FileGuid},
            #xattr{name = <<"name">>, value = <<"value">>}
        )
    end),

    Json = #{<<"customMetadata">> => #{
        <<"fields">> => [<<"name">>],
        <<"exists">> => [<<"onedata_keyvalue">>, <<"k1">>]
    }},
    {ok, Changes} = get_changes(Config, WorkerP1, SpaceId, Json),
    ?assert(length(Changes) >= 1),

    [LastChange | _] = Changes,
    ?assertMatch(#{<<"customMetadata">> := #{
        <<"changed">> := true,
        <<"fields">> := #{
            <<"name">> := <<"value">>
        },
        <<"exists">> := #{
            <<"onedata_keyvalue">> := true,
            <<"k1">> := false
        }
    }}, LastChange).


changes_stream_json_metadata_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, <<"user1">>}, Config),

    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file4_csjmt"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),

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

    Json = #{<<"customMetadata">> => #{
        <<"fields">> => [<<"onedata_json">>]
    }},
    {ok, Changes} = get_changes(Config, WorkerP1, SpaceId, Json),
    ?assert(length(Changes) >= 1),

    [LastChange | _] = Changes,
    ?assertMatch(#{<<"customMetadata">> := #{
        <<"changed">> := true,
        <<"fields">> := #{
            <<"onedata_json">> := OnedataJson
        }
    }}, LastChange).


changes_stream_times_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file5_cstt"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),

    Time = 1000,

    % when
    spawn(fun() ->
        timer:sleep(1000),
        lfm_proxy:update_times(WorkerP1, SessionId, {guid, FileGuid}, Time, Time, Time)
    end),

    Json = #{<<"times">> => #{
        <<"fields">> => [<<"atime">>, <<"mtime">>, <<"ctime">>]
    }},
    {ok, Changes} = get_changes(Config, WorkerP1, SpaceId, Json),
    ?assert(length(Changes) >= 1),

    [LastChange | _] = Changes,
    ?assertMatch(#{<<"times">> := #{
        <<"changed">> := true,
        <<"fields">> := #{
            <<"atime">> := Time,
            <<"mtime">> := Time,
            <<"ctime">> := Time
        }
    }}, LastChange).


changes_stream_file_location_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file6_csflt"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),

    % when
    spawn(fun() ->
        timer:sleep(500),
        {ok, Handle} = lfm_proxy:open(WorkerP1, SessionId, {guid, FileGuid}, write),
        {ok, 5} = lfm_proxy:write(WorkerP1, Handle, 0, <<"01234">>)
    end),

    Json = #{<<"fileLocation">> => #{<<"fields">> => [<<"size">>]}},
    {ok, Changes} = get_changes(Config, WorkerP1, SpaceId, Json),
    ?assert(length(Changes) >= 1),

    [LastChange | _] = Changes,
    ?assertMatch(#{<<"fileLocation">> := #{
        <<"changed">> := true,
        <<"fields">> := #{
            <<"size">> := 5
        }
    }}, LastChange).


changes_stream_request_several_records_test(Config) ->
    [_WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionId = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP1)}}, Config),
    [{SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    Name = <<"file7_csflt">>,
    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), binary_to_list(Name)])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP1, SessionId, File, 8#700),

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

    Json = #{
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
    {ok, AllChanges} = get_changes(Config, WorkerP1, SpaceId, Json),
    RelevantChanges = [Change || Change <- AllChanges, map_size(Change) > 4],
    ?assert(length(RelevantChanges) >= 2),

    [LastChange, PreLastChange | _] = RelevantChanges,

    ?assertMatch(#{
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
    }, PreLastChange),
    ?assert(not maps:is_key(<<"times">>, PreLastChange)),

    ?assertMatch(#{
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
    }, LastChange),
    ?assert(not maps:is_key(<<"customMetadata">>, LastChange)).


changes_stream_on_multi_provider_test(Config) ->
    [WorkerP2, WorkerP1] = ?config(op_worker_nodes, Config),
    UserId = <<"user1">>,
    SessionIdP2 = ?config({session_id, {UserId, ?GET_DOMAIN(WorkerP2)}}, Config),
    [_, {SpaceId, SpaceName} | _] = ?config({spaces, UserId}, Config),

    File = list_to_binary(filename:join(["/", binary_to_list(SpaceName), "file8_csompt"])),
    {ok, FileGuid} = lfm_proxy:create(WorkerP2, SessionIdP2, File, 8#700),

    % when
    spawn(fun() ->
        timer:sleep(500),
        {ok, Handle} = lfm_proxy:open(WorkerP2, SessionIdP2, {guid, FileGuid}, write),
        lfm_proxy:write(WorkerP2, Handle, 0, <<"data">>)
    end),

    Json = #{<<"fileLocation">> => #{
        <<"fields">> => [<<"size">>, <<"provider_id">>]
    }},

    {ok, Changes} = get_changes(Config, WorkerP1, SpaceId, Json),
    ?assert(length(Changes) >= 1),

    [LastChange | _] = Changes,

    DomainP2 = domain(WorkerP2),
    ?assertMatch(#{
        <<"fileLocation">> := #{
            <<"changed">> := true,
            <<"mutators">> := [DomainP2],
            <<"fields">> := #{
                <<"size">> := 4,
                <<"provider_id">> := DomainP2
            }
        }
    }, LastChange).


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

    utils:pforeach(fun(_) ->
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


init_per_testcase(changes_stream_closed_on_disconnection, Config) ->
    ct:timetrap(timer:minutes(3)),
    Workers = ?config(op_worker_nodes, Config),
    Pid = self(),
    ok = test_utils:mock_new(Workers, changes),
    ok = test_utils:mock_expect(Workers, changes, init_stream,
        fun(State) ->
            State1 = meck:passthrough([State]),
            StreamPid = maps:get(changes_stream, State1, undefined),
            Pid ! {stream_pid, StreamPid},
            State1
        end),
    init_per_testcase(all, Config);

init_per_testcase(_Case, Config) ->
    ct:timetrap({minutes, 5}),
    lfm_proxy:init(Config).


end_per_testcase(changes_stream_closed_on_disconnection, Config) ->
    Workers = ?config(op_worker_nodes, Config),
    test_utils:mock_unload(Workers, changes),
    end_per_testcase(all, Config);

end_per_testcase(_Case, Config) ->
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================


domain(Node) ->
    atom_to_binary(?GET_DOMAIN(Node), utf8).


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
            <<>>;
        <<"infinity">> ->
            <<"?timeout=infinity">>;
        _ ->
            <<"?timeout=", (integer_to_binary(Timeout))/binary>>
    end,
    Path = <<"changes/metadata/", SpaceId/binary, Qs/binary>>,
    Payload = json_utils:encode(Json),
    Headers = ?USER_1_AUTH_HEADERS(Config, [
        {<<"content-type">>, <<"application/json">>}
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
