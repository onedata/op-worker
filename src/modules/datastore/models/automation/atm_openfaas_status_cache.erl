%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model for caching information about OpenFaaS service status.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_openfaas_status_cache).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([save/2, get/1]).


-type id() :: binary().
-type record() :: #atm_openfaas_status_cache{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0, doc/0]).


-define(CTX, #{
    model => ?MODULE,
    memory_copies => all,
    disc_driver => undefined
}).


%%%===================================================================
%%% API
%%%===================================================================


-spec save(id(), atm_openfaas_monitor:status()) -> {ok, doc()} | {error, term()}.
save(Id, Status) ->
    datastore_model:save(?CTX, #document{key = Id, value = #atm_openfaas_status_cache{
        status = Status
    }}).


-spec get(id()) -> {ok, doc()} | {error, term()}.
get(Id) ->
    datastore_model:get(?CTX, Id).
