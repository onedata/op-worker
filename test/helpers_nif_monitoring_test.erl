%%%-------------------------------------------------------------------
%%% @author Bartek Kryza
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Test for helpers monitoring functionality in
%%%      helpers_nif module
%%% @end
%%%-------------------------------------------------------------------
-module(helpers_nif_monitoring_test).
-author("Bartek Kryza").

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
            fun graphite_server/0
        ]}.

graphite_server() ->
    ?assertMatch(ok, start_graphite_listener()).

start() ->
    prepare_environment(),
    ok = helpers_nif:init(),
    helpers_nif:start_monitoring().

stop(_) ->
    helpers_nif:stop_monitoring().

%%%===================================================================
%%% Internal functions
%%%===================================================================

-define(TCP_OPTIONS, [binary, {ip, any}, {packet, 0}, {active, true},
                      {reuseaddr, true}]).
-define(TCP_PORT, 20030).
-define(TCP_ACCEPT_TIMEOUT, 2000).
-define(GRAPHITE_TEST_METRIC_NAME,
        <<"eunit.comp.oneprovider.mod.options.monitoring_reporting_period">>).

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
    op_worker:set_env(posix_helper_threads_number, 1),
    op_worker:set_env(s3_helper_threads_number, 1),
    op_worker:set_env(swift_helper_threads_number, 1),
    op_worker:set_env(glusterfs_helper_threads_number, 1),
    op_worker:set_env(webdav_helper_threads_number, 25),
    op_worker:set_env(xrootd_helper_threads_number, 25),
    op_worker:set_env(nulldevice_helper_threads_number, 1),
    op_worker:set_env(buffer_helpers, false),
    op_worker:set_env(buffer_scheduler_threads_number, 1),
    op_worker:set_env(read_buffer_min_size, 1024),
    op_worker:set_env(read_buffer_max_size, 1024),
    op_worker:set_env(read_buffer_prefetch_duration, 1),
    op_worker:set_env(write_buffer_min_size, 1024),
    op_worker:set_env(write_buffer_max_size, 1024),
    op_worker:set_env(write_buffer_flush_delay, 1),
    op_worker:set_env(helpers_performance_monitoring_enabled, true),
    op_worker:set_env(helpers_performance_monitoring_type,
                        <<"graphite">>),
    op_worker:set_env(helpers_performance_monitoring_level,
                        <<"full">>),
    op_worker:set_env(helpers_performance_monitoring_period, 1),
    application:set_env(?CLUSTER_WORKER_APP_NAME, graphite_host,
                        <<"127.0.0.1">>),
    application:set_env(?CLUSTER_WORKER_APP_NAME, graphite_port,
                        ?TCP_PORT),
    application:set_env(?CLUSTER_WORKER_APP_NAME, graphite_prefix,
                        <<"eunit">>).
-endif.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Start mock Graphite TCP listener and wait for connections from
%% metrics collector in helpers.
%% @end
%%--------------------------------------------------------------------
-spec start_graphite_listener() -> ok.
start_graphite_listener() ->
    {ok, Listen} = gen_tcp:listen(?TCP_PORT, ?TCP_OPTIONS),
    graphite_accept(Listen, 20).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Accept a connection from a Graphite reporter.
%% @end
%%--------------------------------------------------------------------
graphite_accept(Listen, Retry) ->
    {ok, Socket} = gen_tcp:accept(Listen, ?TCP_ACCEPT_TIMEOUT),
    Result = graphite_handler(Socket),
    case Result of
        error when Retry > 0 ->
            graphite_accept(Listen, Retry-1);
        error ->
            gen_tcp:close(Listen),
            error;
        ok -> ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Accept a connection from a Graphite reporter.
%% @end
%%--------------------------------------------------------------------
graphite_handler(Socket) ->
    receive
        {tcp, Socket, Bin} ->
            case binary:match(Bin, ?GRAPHITE_TEST_METRIC_NAME) of
                nomatch -> graphite_handler(Socket);
                _ -> ok
            end;
        {tcp_closed, Socket} ->
            error
    after 2000 ->
        error
    end.
