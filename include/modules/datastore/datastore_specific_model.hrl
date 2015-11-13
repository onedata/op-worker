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

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/cluster/worker/modules/datastore/datastore_models_def.hrl").
-include_lib("cluster_worker/include/cluster/worker/modules/datastore/datastore_model_helper.hrl").

-endif.
