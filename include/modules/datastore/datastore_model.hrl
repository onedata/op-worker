%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Contains common definitions of types and helper macros.
%%%      This header must be included by model definition files and shall not be included
%%%      anywhere else.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(DATASTORE_MODEL_HRL).
-define(DATASTORE_MODEL_HRL, 1).

-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common_internal.hrl").

%% Name of current model
-define(MODEL_NAME, ?MODULE).

-compile({no_auto_import, [get/1]}).

%% Common types
-type model_record() :: #?MODEL_NAME{}.
-type model_name() :: ?MODEL_NAME.
-export_type([model_record/0, model_name/0]).


-define(STORE_LEVEL(M), (M:model_init())#model_config.store_level).
-define(LINK_STORE_LEVEL(M), (M:model_init())#model_config.link_store_level).

-define(STORE_LEVEL, ?STORE_LEVEL(?MODEL_NAME)).
-define(LINK_STORE_LEVEL, ?LINK_STORE_LEVEL(?MODEL_NAME)).

-endif.
