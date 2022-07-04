%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model storing information about space support state.
%%% @end
%%%-------------------------------------------------------------------
-module(space_support_state).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([create/1, get/1, update/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1]).


-type accounting_status() :: enabled | disabled.

-type record() :: #space_support_state{}.
-type diff() :: datastore_doc:diff(record()).
-type doc() :: datastore_doc:doc(record()).
-type ctx() :: datastore:ctx().

-export_type([accounting_status/0]).
-export_type([record/0, diff/0, doc/0]).


-define(CTX, #{
    model => ?MODULE,
    secure_fold_enabled => true,
    memory_copies => all
}).


%%%===================================================================
%%% API functions
%%%===================================================================


-spec create(doc()) -> {ok, doc()} | {error, term()}.
create(SpaceSupportStateDoc) ->
    datastore_model:create(?CTX, SpaceSupportStateDoc).


-spec get(od_space:id()) -> {ok, doc()} | {error, term()}.
get(SpaceId) ->
    datastore_model:get(?CTX, SpaceId).


-spec update(od_space:id(), diff()) -> {ok, doc()} | {error, term()}.
update(SpaceId, Diff) ->
    datastore_model:update(?CTX, SpaceId, Diff).


-spec delete(od_space:id()) -> ok | {error, term()}.
delete(SpaceId) ->
    datastore_model:delete(?CTX, SpaceId).


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.


-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


-spec get_record_struct(datastore_model:record_version()) -> datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {accounting_status, atom},
        {dir_stats_service_state, {custom, string, {
            persistent_record, encode, decode, dir_stats_service_state
        }}}
    ]}.
