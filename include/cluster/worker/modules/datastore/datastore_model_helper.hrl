%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Contains common definitions of types and helper macros.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(DATASTORE_MODEL_HELPER_HRL).
-define(DATASTORE_MODEL_HELPER_HRL, 1).

-include("cluster/worker/modules/datastore/datastore.hrl").
-include("cluster/worker/modules/datastore/datastore_common_internal.hrl").

%% Name of current model
-define(MODEL_NAME, ?MODULE).

-compile({no_auto_import, [get/1]}).

%% Common types
-type model_record() :: #?MODEL_NAME{}.
-type model_name() :: ?MODEL_NAME.
-export_type([model_record/0, model_name/0]).


%% Get default store_level() for given model
-define(STORE_LEVEL(M), (M:model_init())#model_config.store_level).

%% Get default link's store_level() for given model
-define(LINK_STORE_LEVEL(M), (M:model_init())#model_config.link_store_level).

%% Get default store_level() for current model
-define(STORE_LEVEL, ?STORE_LEVEL(?MODEL_NAME)).

%% Get default link's store_level() for current model
-define(LINK_STORE_LEVEL, ?LINK_STORE_LEVEL(?MODEL_NAME)).

-endif.
