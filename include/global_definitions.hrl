%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains global definitions of component names, macros and types
%%% used accross the application.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(GLOBAL_DEFINITIONS_HRL).
-define(GLOBAL_DEFINITIONS_HRL, 1).

-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/global_definitions.hrl").

%%%===================================================================
%%% Global names
%%%===================================================================

%% Name of the application.
-define(APP_NAME, op_worker).

%% Local name (name and node is used to identify it) of supervisor that
%% coordinates application at each node (one supervisor per node).
-define(APPLICATION_SUPERVISOR_NAME, op_worker_sup).

%% Local name (name and node is used to identify it) of session manager worker.
-define(SESSION_MANAGER_WORKER, session_manager_worker).

%% Local name (name and node is used to identify it) of session manager worker
%% supervisor.
-define(SESSION_MANAGER_WORKER_SUP, ?WORKER_HOST_SUPERVISOR_NAME(?SESSION_MANAGER_WORKER)).

%% Local name (name and node is used to identify it) of rrdtool supervisor.
-define(RRDTOOL_SUPERVISOR, rrdtool_supervisor).

%% ID of provider that is not currently registered in Onezone
-define(UNREGISTERED_PROVIDER_ID, <<"unregistered_provider">>).

%%%===================================================================
%%% Global identities
%%%===================================================================

-define(OZ_IDENTITY, #user_identity{user_id = <<"ONE-ZONE">>}).

-endif.
