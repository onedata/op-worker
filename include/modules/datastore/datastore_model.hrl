%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-ifndef(DATASTORE_INTERNAL_HRL).
-define(DATASTORE_INTERNAL_HRL, 1).

-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common_internal.hrl").

-define(MODEL_NAME, ?MODULE).

-compile({no_auto_import, [get/1]}).

-type model_name() :: #?MODEL_NAME{}.
-export_type([model_name/0]).

-endif.
