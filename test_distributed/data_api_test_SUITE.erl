%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(data_api_test_SUITE).
-author("Bartosz Walkowicz").

-include("global_definitions.hrl").
-include("api_test_utils.hrl").
-include("lfm_permissions_test.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/handshake_messages.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/aai/caveats.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

-export([
    list_dir_test/1
]).

all() ->
    ?ALL([
        list_dir_test
    ]).


-define(SCENARIO_NAME, atom_to_binary(?FUNCTION_NAME, utf8)).


%%%===================================================================
%%% Test functions
%%%===================================================================



list_dir_test(Config) ->
    [W | _] = ?config(op_worker_nodes, Config),

    UserId = <<"user3">>,
    UserSessId = ?config({session_id, {UserId, ?GET_DOMAIN(W)}}, Config),

    SpaceId = <<"space2">>,
    SpaceRootDirGuid = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),

    RootDirName = ?SCENARIO_NAME,
    {ok, RootDirGuid} = lfm_proxy:mkdir(W, UserSessId, SpaceRootDirGuid, RootDirName, 8#777),
    {ok, RootDirObjectId} = file_id:guid_to_objectid(RootDirGuid),

    lists:map(fun(Num) ->
        {ok, FileGuid} = lfm_proxy:create(W, UserSessId, RootDirGuid, <<"file", Num>>, 8#777),
        {ok, FileObjectId} = file_id:guid_to_objectid(FileGuid),
        FileObjectId
    end, [$0, $1, $2, $3, $4]),

    ParamsSpec = #params_spec{
        optional = [<<"limit">>, <<"offset">>],
        correct_values = #{
            <<"limit">> => [1, 100],
            <<"offset">> => [0, 2]
        },
        bad_values = [
            {<<"limit">>, true, ?ERROR_BAD_VALUE_INTEGER(<<"limit">>)},
            {<<"limit">>, -100, ?ERROR_BAD_VALUE_TOO_LOW(<<"limit">>, 1)},
            {<<"limit">>, 0, ?ERROR_BAD_VALUE_TOO_LOW(<<"limit2">>, 1)},
            {<<"offset">>, <<"abc">>, ?ERROR_BAD_VALUE_INTEGER(<<"offset">>)}
        ]
    },

    ?assert(api_test_utils:run_scenarios(Config, [
        #scenario_spec{
            type = rest,
            target_node = W,
            client_spec = #client_spec{
                correct = [{user, UserId}],
                unauthorized = [{user, UserId}],
                forbidden = [{user, UserId}]
            },

            prepare_args_fun = fun(_Env, Data) ->
                Qs = qwe(Data),
                #rest_args{
                    method = get,
                    path = <<"files-id/", RootDirObjectId/binary, Qs/binary>>
            }
            end,
            validate_result_fun = fun(Result) ->
                ct:pal("~p", [Result])
            end,

            params_spec = ParamsSpec
        }
    ])).


qwe(Data) ->
    QsParams = [QsParam || QsParam <- [
        asd(<<"limit">>, Data),
        asd(<<"offset">>, Data)
    ], QsParam /= <<>>],

    case str_utils:join_binary(QsParams, <<"&">>) of
        <<>> ->
            <<>>;
        Qs ->
            <<"?", Qs/binary>>
    end.


asd(ParamName, Data) ->
    case maps:get(ParamName, Data, undefined) of
        undefined ->
            <<>>;
        Value ->
            <<ParamName/binary, "=", (str_utils:to_binary(Value))/binary>>
    end.


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
        NewConfig3 = initializer:create_test_users_and_spaces(
            ?TEST_FILE(NewConfig2, "env_desc.json"),
            NewConfig2
        ),
        initializer:mock_auth_manager(NewConfig3),
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
    lfm_proxy:init(Config).


end_per_testcase(_Case, Config) ->
    initializer:unmock_share_logic(Config),
    lfm_proxy:teardown(Config).


%%%===================================================================
%%% Internal functions
%%%===================================================================
