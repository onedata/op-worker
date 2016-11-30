%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This test suite verifies correct behaviour of rename
%%% @end
%%%-------------------------------------------------------------------
-module(file_redirection_test_SUITE).
-author("Mateusz Paciorek").

-include("global_definitions.hrl").
-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-define(TIMEOUT, timer:seconds(15)).
-define(PHANTOM_LIFESPAN_SECONDS, 5).

%%%-------------------------------------------------------------------
%%% API
%%%-------------------------------------------------------------------

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2]).

-export([
    redirect_fuse_request_test/1,
    redirect_event_test/1,
    phantom_file_deletion_test/1]).

all() ->
    ?ALL([
        redirect_fuse_request_test,
        redirect_event_test,
        phantom_file_deletion_test
    ]).

%%%===================================================================
%%% Test functions
%%%===================================================================

redirect_fuse_request_test(Config) ->
    [W1, W2] = sorted_workers(Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W2)}}, Config),

    {TargetGuid, RedirectionGuid} = create_file_with_redirection(W1, W2, Config),

    {_, TargetStat} = ?assertMatch({ok, _}, lfm_proxy:stat(W1, SessId1, {guid, TargetGuid})),
    {_, RedirectedStat} = ?assertMatch({ok, _}, lfm_proxy:stat(W2, SessId2, {guid, RedirectionGuid})),
    ?assertEqual(TargetStat, RedirectedStat),
    ok.

redirect_event_test(Config) ->
    [W1, W2] = sorted_workers(Config),

    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W1)}}, Config),
    SessId2 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(W2)}}, Config),

    {TargetGuid, RedirectionGuid} = create_file_with_redirection(W1, W2, Config),

    BaseEvent = #file_written_event{size = 1, file_size = 1,
        blocks = [#file_block{offset = 0, size = 1}]},

    ?assertEqual(ok, rpc:call(W1, event, emit, [BaseEvent#file_written_event{
        file_uuid = TargetGuid
    }, SessId1])),
    {_, [TargetEvent]} = ?assertReceivedMatch({events, [#file_written_event{
        file_uuid = TargetGuid
    }]}, ?TIMEOUT),

    ?assertEqual(ok, rpc:call(W2, event, emit, [BaseEvent#file_written_event{
        file_uuid = RedirectionGuid
    }, SessId2])),
    {_, [RedirectedEvent]} = ?assertReceivedMatch({events, [#file_written_event{
        file_uuid = TargetGuid
    }]}, ?TIMEOUT),

    ?assertEqual(TargetEvent, RedirectedEvent),
    ok.

phantom_file_deletion_test(Config) ->
    [W, _] = sorted_workers(Config),

    ?assertEqual(ok, rpc:call(W, application, set_env,
        [op_worker, phantom_lifespan_seconds, ?PHANTOM_LIFESPAN_SECONDS])),

    TargetGuid = <<"some_guid">>,
    OldScope = <<"some_scope">>,
    {ok, PhantomUuid} = ?assertMatch({ok, _}, rpc:call(W, file_meta,
        create_phantom_file, [<<"some_uuid">>, OldScope, TargetGuid])),

    ?assertMatch({ok, _}, rpc:call(W, file_meta, get, [PhantomUuid])),

    %% wait for phantom file deletion
    timer:sleep(timer:seconds(?PHANTOM_LIFESPAN_SECONDS + 1)),

    ?assertEqual({error, {not_found, file_meta}}, rpc:call(W, file_meta, get, [PhantomUuid])),

    ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
    ConfigWithNodes = ?TEST_INIT(Config, ?TEST_FILE(Config, "env_desc.json"), [initializer]),
    initializer:setup_storage(ConfigWithNodes).

end_per_suite(Config) ->
    initializer:teardown_storage(Config),
    ?TEST_STOP(Config).

init_per_testcase(CaseName, Config) ->
    ?CASE_START(CaseName),
    initializer:enable_grpca_based_communication(Config),

    Workers = ?config(op_worker_nodes, Config),
    Self = self(),
    Stm = event_stream_factory:create(#file_written_subscription{time_threshold = 1000}),
    rpc:multicall(Workers, subscription, save, [#document{
        key = ?FILE_WRITTEN_SUB_ID,
        value = #subscription{
            id = ?FILE_WRITTEN_SUB_ID,
            type = #file_written_subscription{},
            stream = Stm#event_stream{
                event_handler = fun(Events, _) -> Self ! {events, Events} end
            }
        }
    }]),

    ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
    NewConfig = lfm_proxy:init(ConfigWithSessionInfo),
    initializer:disable_quota_limit(NewConfig),
    CaseNameBinary = list_to_binary(atom_to_list(CaseName)),
    [{test_dir, <<CaseNameBinary/binary, "_dir">>} | NewConfig].

end_per_testcase(Case, Config) ->
    ?CASE_STOP(Case),
    initializer:unload_quota_mocks(Config),
    lfm_proxy:teardown(Config),
    initializer:clean_test_users_and_spaces_no_validate(Config),
    initializer:disable_grpca_based_communication(Config).

%%%===================================================================
%%% Internal functions
%%%===================================================================
create_file_with_redirection(FileWorker, RedirectionWorker, Config) ->
    TestDir = ?config(test_dir, Config),
    SessId1 = ?config({session_id, {<<"user1">>, ?GET_DOMAIN(FileWorker)}}, Config),

    ?assertMatch({ok, _}, lfm_proxy:mkdir(FileWorker, SessId1, filename(1, TestDir, ""))),
    {_, TargetGuid} = ?assertMatch({ok, _}, lfm_proxy:create(FileWorker, SessId1, filename(1, TestDir, "/target_file"), 8#770)),

    RedirectionUuid = <<"redirection_uuid">>,
    RedirectionGuid = fslogic_uuid:uuid_to_guid(RedirectionUuid, <<"space_id2">>),
    ?assertMatch({ok, _}, rpc:call(RedirectionWorker, file_meta, create_phantom_file, [RedirectionUuid, <<"space_id2">>, TargetGuid])),
    {TargetGuid, RedirectionGuid}.

filename(SpaceNo, TestDir, Suffix) ->
    SpaceNoBinary = integer_to_binary(SpaceNo),
    SuffixBinary = list_to_binary(Suffix),
    <<"/space_name", SpaceNoBinary/binary, "/", TestDir/binary, SuffixBinary/binary>>.

sorted_workers(Config) ->
    [W1, W2] = ?config(op_worker_nodes, Config),
    [P1Domain | _] = [V || {K, V} <- ?config(domain_mappings, Config), K =:= p1],
    case ?GET_DOMAIN(W1) of
        P1Domain ->
            [W1, W1];
        _ ->
            [W2, W1]
    end.