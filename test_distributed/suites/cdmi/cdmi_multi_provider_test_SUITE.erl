%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% CDMI tests
%%% @end
%%%-------------------------------------------------------------------
-module(cdmi_multi_provider_test_SUITE).
-author("Tomasz Lichon").

-include_lib("ctool/include/http/headers.hrl").
-include_lib("onenv_ct/include/oct_background.hrl").
-include("onenv_test_utils.hrl").
-include("cdmi_test.hrl").

%% API
-export([
    groups/0, all/0,
    init_per_suite/1, end_per_suite/1,
    init_per_group/2, end_per_group/2,
    init_per_testcase/2, end_per_testcase/1
]).

-export([
    list_dir/1,
    move/1,
    copy/1,
    objectid/1,
    errors/1,

    get_file/1,
    metadata/1,
    delete_file/1,
    delete_dir/1,
    create_file/1,
    create_dir/1,
    update_file/1,
    capabilities/1,
    use_supported_cdmi_version/1,
    use_unsupported_cdmi_version/1,
    moved_permanently/1,
    move_copy_conflict/1,
    request_format_check/1,
    mimetype_and_encoding/1,
    out_of_range/1,
    partial_upload/1,
    acl/1,
    accept_header/1
]).

groups() -> [
    {sequential_tests, [sequential], [
        %% list_dir needs to start first as it lists the main directory
        list_dir,
        objectid,
        errors
    ]},
    {parallel_tests, [parallel], [
        get_file,
        metadata,
        delete_file,
        delete_dir,
        create_file,
        create_dir,
        update_file,
        capabilities,
        use_supported_cdmi_version,
        use_unsupported_cdmi_version,
        copy,
        move,
        moved_permanently,
        move_copy_conflict,
        request_format_check,
        mimetype_and_encoding,
        out_of_range,
        partial_upload,
        acl,
        accept_header
    ]}

].

all() -> [
    {group, sequential_tests},
    {group, parallel_tests}
].

-define(RUN_TEST(),
    try
        cdmi_test_base:?FUNCTION_NAME(#cdmi_test_config{
            p1_selector = krakow,
            p2_selector = paris,
            space_selector = space_krk_par_p})
    catch __TYPE:__REASON:__STACKTRACE ->
        ct:pal("Test failed due to ~p:~p.~nStacktrace: ~p", [__TYPE, __REASON, __STACKTRACE]),
        error(test_failed)
    end
).

%%%===================================================================
%%% Test functions
%%%===================================================================

%%%===================================================================
%%% Sequential tests
%%%===================================================================

list_dir(_Config) ->
    ?RUN_TEST().

objectid(_Config) ->
    ?RUN_TEST().

errors(_Config) ->
    ?RUN_TEST().

get_file(_Config) ->
    ?RUN_TEST().

%%%===================================================================
%%% Parallel tests
%%%===================================================================

metadata(_Config) ->
    ?RUN_TEST().


delete_file(_Config) ->
    ?RUN_TEST().


delete_dir(_Config) ->
    ?RUN_TEST().


create_file(_Config) ->
    ?RUN_TEST().

create_dir(_Config) ->
    ?RUN_TEST().

update_file(_Config) ->
    ?RUN_TEST().

capabilities(_Config) ->
    ?RUN_TEST().

use_supported_cdmi_version(_Config) ->
    ?RUN_TEST().

use_unsupported_cdmi_version(_Config) ->
    ?RUN_TEST().

copy(_Config) ->
    ?RUN_TEST().

move(_Config) ->
    ?RUN_TEST().

moved_permanently(_Config) ->
    ?RUN_TEST().

move_copy_conflict(_Config) ->
    ?RUN_TEST().

request_format_check(_Config) ->
    ?RUN_TEST().

mimetype_and_encoding(_Config) ->
    ?RUN_TEST().

out_of_range(_Config) ->
    ?RUN_TEST().

partial_upload(_Config) ->
    ?RUN_TEST().

acl(_Config) ->
    ?RUN_TEST().

accept_header(_Config) ->
    ?RUN_TEST().


%%%===================================================================
%%% SetUp and TearDown functions
%%%===================================================================


init_per_suite(Config) ->
    {{YY, MM, DD}, {Hour, Min, Sec}} = time:seconds_to_datetime(global_clock:timestamp_seconds()),
    DateString = str_utils:format_bin(
        "~4..0w-~2..0w-~2..0w_~2..0w~2..0w~2..0w",
        [YY, MM, DD, Hour, Min, Sec]
    ),
    oct_background:init_per_suite(Config, #onenv_test_config{
        onenv_scenario = "2op",
        envs = [{op_worker, op_worker, [{fuse_session_grace_period_seconds, 24 * 60 * 60}]}],
        posthook = fun(NewConfig) ->
            #object{guid = DirGuid} = onenv_file_test_utils:create_and_sync_file_tree(user2, space_krk_par_p,
                #dir_spec{
                    name = DateString
                }, krakow
            ),
            node_cache:put(root_dir_guid, DirGuid),
            node_cache:put(root_dir_name, binary:bin_to_list(DateString)),
            NewConfig
        end
    }).


end_per_suite(_Config) ->
    oct_background:end_per_suite().


init_per_group(_Group, Config) ->
    lfm_proxy:init(Config, false).


end_per_group(_Group, Config) ->
    lfm_proxy:teardown(Config).


init_per_testcase(_Case, Config) ->
    Config.


end_per_testcase(_Case) ->
    ok.

