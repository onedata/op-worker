%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model serves as cache for od_atm_workflow_schema records
%%% synchronized via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(od_atm_workflow_schema).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/automation/automation.hrl").

-type id() :: binary().
-type record() :: #od_atm_workflow_schema{}.
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
-spec get_latest_revision(record()) -> atm_workflow_schema_revision:record().
get_latest_revision(#od_atm_workflow_schema{revision_registry = RevisionRegistry}) ->
    case atm_workflow_schema_revision_registry:get_all_revision_numbers(RevisionRegistry) of
        [] ->
            % return dummy revision with empty stores and lanes
            #atm_workflow_schema_revision{};
        AllRevisionNumbers ->
            atm_workflow_schema_revision_registry:get_revision(lists:max(AllRevisionNumbers), RevisionRegistry)
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
