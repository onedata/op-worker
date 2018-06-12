%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This SUITE contains stress test for 2 provider environment with null storage.
%%% @end
%%%--------------------------------------------------------------------
-module(stress_2_provider_null_test_SUITE).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/performance.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").

%% export for ct
-export([all/0, init_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([stress_test/1, stress_test_base/1]).

-export([
  random_read_test/1, random_read_test_base/1
]).

-define(STRESS_CASES, []).
-define(STRESS_NO_CLEARING_CASES, [
  random_read_test
]).

all() ->
  ?STRESS_ALL(?STRESS_CASES, ?STRESS_NO_CLEARING_CASES).

%%%===================================================================
%%% Test functions
%%%===================================================================

stress_test(Config) ->
  ?STRESS(Config,[
    {description, "Main stress test function. Links together all cases to be done multiple times as one continous test."},
    {success_rate, 90},
    {config, [{name, stress}, {description, "Basic config for stress test"}]}
  ]
  ).
stress_test_base(Config) ->
  ?STRESS_TEST_BASE(Config).

%%%===================================================================

random_read_test(Config) ->
  ?PERFORMANCE(Config, [
    {parameters, [
      [{name, file_size_gb}, {value, 1}, {description, "File size in GB"}],
      [{name, block_size}, {value, 1}, {description, "Block size in bytes"}],
      [{name, block_per_repeat}, {value, 1000},
        {description, "Number of blocks read in each cycle"}]
    ]},
    {description, "Performs multiple file operations on space 1."}
  ]).
random_read_test_base(Config) ->
  random_read_test_base(Config, true),
  random_read_test_base(Config, false).

random_read_test_base(Config0, SeparateBlocks) ->
  Config = multi_provider_file_ops_test_base:extend_config(Config0,
    <<"user1">>, {2,0,0, 1}, 1),
  FileSize = ?config(file_size_gb, Config),
  BlockSize = ?config(block_size, Config),
  BlocksCount = ?config(block_per_repeat, Config),

  [Worker1, Worker2] = ?config(op_worker_nodes, Config),
  SessId = ?config(session, Config),
  SpaceName = ?config(space_name, Config),

  RepNum = ?config(rep_num, Config),
  FilePath = case RepNum of
    1 ->
      File = <<"/", SpaceName/binary, "/",  (generator:gen_name())/binary>>,

      ?assertMatch({ok, _}, lfm_proxy:create(Worker2, SessId(Worker2), File, 8#755)),
      OpenAns = lfm_proxy:open(Worker2, SessId(Worker2), {path, File}, rdwr),
      ?assertMatch({ok, _}, OpenAns),
      {ok, Handle} = OpenAns,

      ChunkSize = 10240,
      ChunksNum = 1024,
      PartNum = 100 * FileSize,
      FileSizeBytes = ChunkSize * ChunksNum * PartNum,

      BytesChunk = crypto:strong_rand_bytes(ChunkSize),
      Bytes = lists:foldl(fun(_, Acc) ->
        <<Acc/binary, BytesChunk/binary>>
      end, <<>>, lists:seq(1, ChunksNum)),

      PartSize = ChunksNum * ChunkSize,
      lists:foreach(fun(Num) ->
        ?assertEqual({ok, PartSize}, lfm_proxy:write(Worker2, Handle, Num * PartSize, Bytes))
      end, lists:seq(0, PartNum - 1)),

      ?assertEqual(ok, lfm_proxy:close(Worker2, Handle)),

      ?assertMatch({ok, #file_attr{size = FileSizeBytes}},
        lfm_proxy:stat(Worker1, SessId(Worker1), {path, File}), 60),

      put({file, SeparateBlocks}, File),
      File;
    _ ->
      get({file, SeparateBlocks})
  end,

  Start1 = os:timestamp(),
  OpenAns2 = lfm_proxy:open(Worker1, SessId(Worker1), {path, FilePath}, rdwr),
  OpenTime = timer:now_diff(os:timestamp(), Start1),
  ?assertMatch({ok, _}, OpenAns2),
  {ok, Handle2} = OpenAns2,
  ReadTime1 = read_blocks(Worker1, Handle2, BlockSize, BlocksCount, RepNum, SeparateBlocks),
  ReadTime2 = read_blocks(Worker1, Handle2, BlockSize, BlocksCount, RepNum, SeparateBlocks),
  Start2 = os:timestamp(),
  ?assertEqual(ok, lfm_proxy:close(Worker1, Handle2)),
  CloseTime = timer:now_diff(os:timestamp(), Start2),

  ct:print("Repeat: ~p, many blocks: ~p, read remote: ~p, read local: ~p, open: ~p, close: ~p", [
    RepNum, SeparateBlocks, ReadTime1 / BlocksCount, ReadTime2 / BlocksCount, OpenTime, CloseTime]),

  ok.

%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================

init_per_suite(Config) ->
  [{?LOAD_MODULES, [initializer, multi_provider_file_ops_test_base]} | Config].


init_per_testcase(stress_test, Config) ->
  lists:foreach(fun(Worker) ->
    test_utils:set_env(Worker, ?APP_NAME, minimal_sync_request, 1)
  end, ?config(op_worker_nodes, Config)),

  ssl:start(),
  hackney:start(),
  initializer:disable_quota_limit(Config),
  ConfigWithSessionInfo = initializer:create_test_users_and_spaces(?TEST_FILE(Config, "env_desc.json"), Config),
  lfm_proxy:init(ConfigWithSessionInfo);

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(stress_test, Config) ->
  lfm_proxy:teardown(Config),
  %% TODO change for initializer:clean_test_users_and_spaces after resolving VFS-1811
  initializer:clean_test_users_and_spaces_no_validate(Config),
  initializer:unload_quota_mocks(Config),
  hackney:stop(),
  ssl:stop();

end_per_testcase(_Case, Config) ->
  Config.

%%%===================================================================
%%% Internal functions
%%%===================================================================

read_blocks(Worker, Handle, BlockSize, BlocksCount, RepNum, SeparateBlocks) ->
  Blocks = case SeparateBlocks of
    true ->
      lists:map(fun(Num) ->
        2 * Num - 1 + 2 * BlocksCount * (RepNum - 1)
      end, lists:seq(1, BlocksCount));
    _ ->
      lists:map(fun(Num) ->
        Num + BlocksCount * (RepNum - 1)
      end, lists:seq(1, BlocksCount))
  end,

  lists:foldl(fun(BlockNum, Acc) ->
    Start = os:timestamp(),
    ReadAns = lfm_proxy:silent_read(Worker, Handle, BlockNum, BlockSize),
    Time = timer:now_diff(os:timestamp(), Start),
    ?assertMatch({ok, _}, ReadAns),
    Acc + Time
  end, 0, Blocks).