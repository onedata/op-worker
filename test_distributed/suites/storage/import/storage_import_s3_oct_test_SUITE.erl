%%%--------------------------------------------------------------------
%%% @author Katarzyna Such
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc This module tests storage import.
%%% @end
%%%-------------------------------------------------------------------
-module(storage_import_s3_oct_test_SUITE).
-author("Katarzyna Such").

-include_lib("onenv_ct/include/oct_background.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("space_setup_utils.hrl").

-export([
    all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_testcase/2, end_per_testcase/2
]).

%% tests
-export([
    create_storage_test/1,
    set_up_space_test/1
]).

-define(TEST_CASES, [
%%    create_storage_test,
    set_up_space_test
]).

-define(RANDOM_PROVIDER(), ?RAND_ELEMENT([krakow, paris])).

all() -> ?ALL(?TEST_CASES).

%%%==================================================================
%%% Test functions
%%%===================================================================

create_storage_test(_Config) ->
    Data =  #s3_storage_params{hostname = <<"dev-volume-s3-paris.default:9000">>,
        bucket_name = <<"test">>, block_size = 0, storage_path_type = <<"canonical">>,
        imported_storage = true},
    space_setup_utils:create_storage(paris, Data).


set_up_space_test(_Config) ->
    Data = #space_spec{name = space_test, owner = user1, users = [user2],
%%        supports = [
%%            #support_spec{
%%                provider = krakow,
%%                storage_spec = #s3_storage_params{hostname = <<"dev-volume-s3-krakow.default:9000">>,
%%                    bucket_name = <<"test">>},
%%                size = 1000000
%%            }
%%        ]},
        supports = [
            #support_spec{
                provider = ?RANDOM_PROVIDER(),
                storage_spec = #posix_storage_params{mount_point = <<"/tmp/a/b/c">>},
                size = 1000000
            }
        ]},
    space_setup_utils:set_up_space(Data).

%===================================================================
% SetUp and TearDown functions
%===================================================================

init_per_suite(Config) ->
    oct_background:init_per_suite(Config, #onenv_test_config{
%%        onenv_scenario = "2op_s3",
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [
            {fuse_session_grace_period_seconds, 24 * 60 * 60}
        ]}]
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case, _Config) ->
    ok.