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
-include("modules/storage_file_manager/helpers/helpers.hrl").
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
        after 1000 ->
            {error, nif_timeout}
        end,

    ?assertEqual({ok, BinaryResult}, NifResult).

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
        after 1000 ->
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
        after 1000 ->
            {error, nif_timeout}
        end,

    {ok, Guard3} = helpers_nif:readdir(Handle, <<"">>, 0, 100),
    NifResult =
        receive
            {Guard3, Res3} ->
                Res3
        after 1000 ->
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
    application:set_env(?APP_NAME, ceph_helper_threads_number, 1),
    application:set_env(?APP_NAME, cephrados_helper_threads_number, 1),
    application:set_env(?APP_NAME, posix_helper_threads_number, 8),
    application:set_env(?APP_NAME, s3_helper_threads_number, 1),
    application:set_env(?APP_NAME, swift_helper_threads_number, 1),
    application:set_env(?APP_NAME, glusterfs_helper_threads_number, 1),
    application:set_env(?APP_NAME, webdav_helper_threads_number, 25),
    application:set_env(?APP_NAME, buffer_helpers, false),
    application:set_env(?APP_NAME, buffer_scheduler_threads_number, 1),
    application:set_env(?APP_NAME, read_buffer_min_size, 1024),
    application:set_env(?APP_NAME, read_buffer_max_size, 1024),
    application:set_env(?APP_NAME, read_buffer_prefetch_duration, 1),
    application:set_env(?APP_NAME, write_buffer_min_size, 1024),
    application:set_env(?APP_NAME, write_buffer_max_size, 1024),
    application:set_env(?APP_NAME, write_buffer_flush_delay, 1).

-endif.
