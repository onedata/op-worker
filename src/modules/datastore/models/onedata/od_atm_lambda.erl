%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model serves as cache for od_atm_lambda records
%%% synchronized via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(od_atm_lambda).
-author("Michal Stanisz").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/automation/automation.hrl").

-type id() :: binary().
-type record() :: #od_atm_lambda{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).

-export_type([id/0, record/0, doc/0, diff/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true,
    memory_copies => all,
    disc_driver => undefined
}).

%% API
-export([update_cache/3, get_from_cache/1, invalidate_cache/1, list/0]).
-export([get_latest_revision/1]).

%% datastore_model callbacks
-export([get_ctx/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec update_cache(id(), diff(), doc()) -> {ok, doc()} | {error, term()}.
update_cache(Id, Diff, Default) ->
    datastore_model:update(?CTX, Id, Diff, Default).


-spec get_from_cache(id()) -> {ok, doc()} | {error, term()}.
get_from_cache(Key) ->
    datastore_model:get(?CTX, Key).


-spec invalidate_cache(id()) -> ok | {error, term()}.
invalidate_cache(Key) ->
    datastore_model:delete(?CTX, Key).


-spec list() -> {ok, [id()]} | {error, term()}.
list() ->
    datastore_model:fold_keys(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).


% @TODO VFS-8349 rework when Oneprovider understands workflow schema and lambda versioning
-spec get_latest_revision(record()) -> atm_lambda_revision:record().
get_latest_revision(#od_atm_lambda{revision_registry = RevisionRegistry}) ->
    LatestRevisionNumber = atm_lambda_revision_registry:get_latest_revision_number(RevisionRegistry),
    case LatestRevisionNumber of
        undefined ->
            % return a dummy revision
            #atm_lambda_revision{
                name = <<"unknown">>,
                summary = <<"unknown">>,
                description = <<"unknown">>,
                operation_spec = #atm_openfaas_operation_spec{
                    docker_image = <<"unknown">>,
                    docker_execution_options = #atm_docker_execution_options{}
                },
                argument_specs = [],
                result_specs = [],
                resource_spec = #atm_resource_spec{
                    cpu_requested = 2.0, cpu_limit = 4.0,
                    memory_requested = 1000000000, memory_limit = 5000000000,
                    ephemeral_storage_requested = 1000000000, ephemeral_storage_limit = 5000000000
                },
                checksum = <<>>,
                state = draft
            };
        _ ->
            atm_lambda_revision_registry:get_revision(LatestRevisionNumber, RevisionRegistry)
    end.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.
