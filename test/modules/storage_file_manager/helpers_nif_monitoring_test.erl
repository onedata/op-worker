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
    application:set_env(?APP_NAME, ceph_helper_threads_number, 1),
    application:set_env(?APP_NAME, posix_helper_threads_number, 1),
    application:set_env(?APP_NAME, s3_helper_threads_number, 1),
    application:set_env(?APP_NAME, swift_helper_threads_number, 1),
    application:set_env(?APP_NAME, glusterfs_helper_threads_number, 1),
    application:set_env(?APP_NAME, buffer_helpers, false),
    application:set_env(?APP_NAME, buffer_scheduler_threads_number, 1),
    application:set_env(?APP_NAME, read_buffer_min_size, 1024),
    application:set_env(?APP_NAME, read_buffer_max_size, 1024),
    application:set_env(?APP_NAME, read_buffer_prefetch_duration, 1),
    application:set_env(?APP_NAME, write_buffer_min_size, 1024),
    application:set_env(?APP_NAME, write_buffer_max_size, 1024),
    application:set_env(?APP_NAME, write_buffer_flush_delay, 1),
    application:set_env(?APP_NAME, helpers_performance_monitoring_enabled, true),
    application:set_env(?APP_NAME, helpers_performance_monitoring_type,
                        <<"graphite">>),
    application:set_env(?APP_NAME, helpers_performance_monitoring_level,
                        <<"full">>),
    application:set_env(?APP_NAME, helpers_performance_monitoring_namespace_prefix,
                        <<"eunit">>),
    application:set_env(?APP_NAME, helpers_performance_monitoring_graphite_url,
                        <<"tcp://127.0.0.1:20030">>),
    application:set_env(?APP_NAME, helpers_performance_monitoring_period, 1).
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
