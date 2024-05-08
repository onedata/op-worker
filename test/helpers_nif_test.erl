%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Test for basic context management in helpers_nif module
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_nif_test).
-author("Rafal Slota").

-ifdef(TEST).

-include("global_definitions.hrl").
-include("modules/storage/helpers/helpers.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test functions
%%%===================================================================

helpers_test_() ->
    {foreach,
        fun start/0,
        fun stop/1,
        [
            fun get_handle/0,
            fun readdir/0,
            fun blocksize_posix/0,
            fun blocksize_cephrados/0,
            fun refresh_params/0
        ]}.

get_handle() ->
    ?assertMatch({ok, _}, helpers_nif:get_handle(?POSIX_HELPER_NAME, #{
        <<"mountPoint">> => <<"/tmp">>
    })).

readdir() ->
    {ok, Handle} = helpers_nif:get_handle(?POSIX_HELPER_NAME, #{
        <<"mountPoint">> => <<"/tmp">>
    }),
    {ok, Result} = file:list_dir(<<"/tmp">>),
    BinaryResult = lists:map(fun list_to_binary/1, Result),
    {ok, Guard} = helpers_nif:readdir(Handle, <<"">>, 0, 100),
    NifResult =
        receive
            {Guard, Res} ->
                Res
        after 2500 ->
            {error, nif_timeout}
        end,

    ?assertEqual({ok, BinaryResult}, NifResult).

blocksize_posix() ->
    {ok, HelperHandle} = helpers_nif:get_handle(?POSIX_HELPER_NAME, #{
        <<"mountPoint">> => <<"/tmp">>,
        <<"skipStorageDetection">> => <<"true">>
    }),
    {ok, Guard} = helpers_nif:blocksize_for_path(HelperHandle, <<"">>),
    NifResult =
        receive
            {Guard, Res} ->
                Res
        after 2500 ->
            {error, nif_timeout}
        end,
    ?assertEqual({ok, 0}, NifResult).

blocksize_cephrados() ->
    {ok, HelperHandle} = helpers_nif:get_handle(?CEPHRADOS_HELPER_NAME, #{
        <<"clusterName">> => <<"test">>,
        <<"monitorHostname">> => <<"localhost">>,
        <<"poolName">> => <<"test">>,
        <<"username">> => <<"test">>,
        <<"key">> => <<"test">>
    }),
    {ok, Guard} = helpers_nif:blocksize_for_path(HelperHandle, <<"">>),
    NifResult =
        receive
            {Guard, Res} ->
                Res
        after 2500 ->
            {error, nif_timeout}
        end,
    ?assertEqual({ok, 10*1024*1024}, NifResult).

refresh_params() ->
    %%% List contents of /tmp for comparison with helper output
    {ok, Result} = file:list_dir(<<"/tmp">>),
    BinaryResult = lists:map(fun list_to_binary/1, Result),

    %%% First try to list contents of invalid mountpoint
    {ok, Handle} = helpers_nif:get_handle(?POSIX_HELPER_NAME, #{
        <<"type">> => <<"posix">>,
        <<"mountPoint">> => <<"/tmpInvalid">>
    }),
    {ok, Guard} = helpers_nif:readdir(Handle, <<"">>, 0, 100),
    _ =
        receive
            {Guard, Res} ->
                Res
        after 2500 ->
            {error, nif_timeout}
        end,

    %%% Now update the params with correct mountpoint and list again
    {ok, Guard2} = helpers_nif:refresh_params(Handle, #{
        <<"type">> => <<"posix">>,
        <<"mountPoint">> => <<"/tmp">>
    }),
    _ =
        receive
            {Guard2, Res2} ->
                Res2
        after 2500 ->
            {error, nif_timeout}
        end,

    {ok, Guard3} = helpers_nif:readdir(Handle, <<"">>, 0, 100),
    NifResult =
        receive
            {Guard3, Res3} ->
                Res3
        after 2500 ->
            {error, nif_timeout}
        end,
    ?assertEqual({ok, BinaryResult}, NifResult).

start() ->
    prepare_environment(),
    ok = helpers_nif:init().

stop(_) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets required environment variables.
%% @end
%%--------------------------------------------------------------------
-spec prepare_environment() -> ok.
prepare_environment() ->
    op_worker:set_env(ceph_helper_threads_number, 1),
    op_worker:set_env(cephrados_helper_threads_number, 1),
    op_worker:set_env(posix_helper_threads_number, 8),
    op_worker:set_env(s3_helper_threads_number, 1),
    op_worker:set_env(swift_helper_threads_number, 1),
    op_worker:set_env(glusterfs_helper_threads_number, 1),
    op_worker:set_env(webdav_helper_threads_number, 25),
    op_worker:set_env(xrootd_helper_threads_number, 25),
    op_worker:set_env(nfs_helper_threads_number, 25),
    op_worker:set_env(buffer_helpers, false),
    op_worker:set_env(buffer_scheduler_threads_number, 1),
    op_worker:set_env(read_buffer_min_size, 1024),
    op_worker:set_env(read_buffer_max_size, 1024),
    op_worker:set_env(read_buffer_prefetch_duration, 1),
    op_worker:set_env(write_buffer_min_size, 1024),
    op_worker:set_env(write_buffer_max_size, 1024),
    op_worker:set_env(write_buffer_flush_delay, 1).

-endif.
