%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This model serves as cache for od_harvester records
%%% synchronized via Graph Sync.
%%% @end
%%%-------------------------------------------------------------------
-module(od_harvester).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include("proto/common/credentials.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

-type id() :: binary().
-type record() :: #od_harvester{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type index() :: binary().


-export_type([id/0, record/0, doc/0, diff/0]).
-export_type([index/0]).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true,
    memory_copies => all,
    disc_driver => undefined
}).

%% API
-export([update_cache/3, get_from_cache/1, invalidate_cache/1, list/0]).

%% datastore_model callbacks
-export([get_ctx/0, get_posthooks/0]).

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

%%--------------------------------------------------------------------
%% @doc
%% Harvester update posthook.
%% @end
%%--------------------------------------------------------------------
-spec run_after(atom(), list(), term()) -> term().
run_after(create, _, {ok, Doc}) ->
    run_after(Doc);
run_after(update, _, {ok, Doc}) ->
    run_after(Doc);
run_after(_Function, _Args, Result) ->
    Result.

-spec run_after(doc()) -> {ok, doc()}.
run_after(Doc = #document{key = HrvId, value = #od_harvester{spaces = Spaces, indices = Indices}}) ->
    lists:foreach(fun(SpaceId) ->
        case provider_logic:supports_space(SpaceId) of
            true ->
                spawn(fun() ->
                    main_harvesting_stream:revise_harvester(SpaceId, HrvId, Indices)
                end);
            false ->
                ok
        end
    end, Spaces),
    {ok, Doc}.

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

%%--------------------------------------------------------------------
%% @doc
%% Returns list of callbacks which will be called after each operation
%% on datastore model.
%% @end
%%--------------------------------------------------------------------
-spec get_posthooks() -> [datastore_hooks:posthook()].
get_posthooks() ->
    [fun run_after/3].