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
-export([save/1, get/0]).

%% datastore_model callbacks
-export([get_record_struct/1, get_record_version/0]).


-type id() :: binary().
-type record() :: #atm_openfaas_status_cache{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0, doc/0]).


-define(CTX, #{
    model => ?MODULE,
    memory_copies => all,
    disc_driver => undefined
}).
-define(ID, <<"AtmOpenFaaSServiceStatus">>).


%%%===================================================================
%%% API
%%%===================================================================


-spec save(atm_openfaas_monitor:status()) -> {ok, doc()} | {error, term()}.
save(Status) ->
    datastore_model:save(?CTX, #document{key = ?ID, value = #atm_openfaas_status_cache{
        status = Status
    }}).


-spec get() -> {ok, doc()} | {error, term()}.
get() ->
    datastore_model:get(?CTX, ?ID).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {status, atom}
    ]}.
