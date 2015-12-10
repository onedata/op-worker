%%%--------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc It is the main module of application. It lunches
%%% supervisor which then initializes appropriate components of node.
%%% @end
%%%--------------------------------------------------------------------
-module(op_worker_app).
-author("Michal Wrzeszcz").

-behaviour(application).

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts application by supervisor initialization.
%% @end
%%--------------------------------------------------------------------
-spec start(_StartType :: application:start_type(), _StartArgs :: term()) ->
    {ok, Pid :: pid()} | {ok, Pid :: pid(), State :: term()} |
    {error, Reason ::term()}.
start(_StartType, _StartArgs) ->
    test_node_starter:maybe_start_cover(),

    % Make sure CA bundle is present in the system.
    % It is not required if verify_gr_cert env is false.
    case application:get_env(?APP_NAME, verify_gr_cert) of
        {ok, true} ->
            case file:read_file(web_client_utils:ca_bundle_location()) of
                {ok, _} ->
                    ?info("CA bundle found in: ~s",
                        [web_client_utils:ca_bundle_location()]);
                _ ->
                    ?alert("~n~n~n"
                    "=====> FATAL ERROR <=====~n"
                    "Cannot find trusted CA bundle.~n"
                    "Please make sure it is present under path: ~s~n"
                    "Application will now terminate.~n"
                    "=========================~n"
                    "~n~n", [web_client_utils:ca_bundle_location()]),
                    throw(no_ca_bundle)
            end,
            application:set_env(ctool, verify_gr_cert, true);
        {ok, false} ->
            application:set_env(ctool, verify_gr_cert, false)
    end,
    ok = application:start(cluster_worker, permanent),
    op_worker_sup:start_link().

%%--------------------------------------------------------------------
%% @doc
%% Stops application.
%% @end
%%--------------------------------------------------------------------
-spec stop(State :: term()) -> ok.
stop(_State) ->
    ok = application:stop(cluster_worker),
    test_node_starter:maybe_stop_cover(),
    ok.
