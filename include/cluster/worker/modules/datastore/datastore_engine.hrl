%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Defines common macros and records used by datastore engine.
%%%      This header shall not be used outside of core datastore engine (drivers, API and worker).
%%% @end
%%%-------------------------------------------------------------------
-ifndef(DATASTORE_ENGINE_HRL).
-define(DATASTORE_ENGINE_HRL, 1).

-include("cluster/worker/modules/datastore/datastore_common_internal.hrl").
-include("global_definitions.hrl").

%% Drivers' names
-define(PERSISTENCE_DRIVER, persistence_driver_module).
-define(LOCAL_CACHE_DRIVER, ets_cache_driver).
-define(DISTRIBUTED_CACHE_DRIVER, mnesia_cache_driver).


-endif.