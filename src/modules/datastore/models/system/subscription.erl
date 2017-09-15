%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model of events subscriptions.
%%% @end
%%%-------------------------------------------------------------------
-module(subscription).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([generate_id/0, generate_id/1]).
-export([create/1, save/1, get/1, delete/1, list/0]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: binary().
-type base() :: #subscription{}.
-type type() :: #file_attr_changed_subscription{} |
                #file_location_changed_subscription{} |
                #file_read_subscription{} | #file_written_subscription{} |
                #file_perm_changed_subscription{} |
                #file_removed_subscription{} | #file_renamed_subscription{} |
                #quota_exceeded_subscription{} | #monitoring_subscription{}.
-type cancellation() :: #subscription_cancellation{}.
-type doc() :: datastore_doc:doc(base()).

-export_type([id/0, base/0, type/0, cancellation/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined,
    fold_enabled => true
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns increasing subscription IDs based on the monotonic time.
%% Should be used only for temporary subscriptions.
%% @end
%%--------------------------------------------------------------------
-spec generate_id() -> SubId :: id().
generate_id() ->
    integer_to_binary(erlang:unique_integer([monotonic, positive])).

%%--------------------------------------------------------------------
%% @doc
%% Generates subscription ID using binary seed.
%% @end
%%--------------------------------------------------------------------
-spec generate_id(Seed :: binary()) -> SubId :: id().
generate_id(Seed) ->
    Rand = binary:decode_unsigned(crypto:hash(md5, Seed)),
    integer_to_binary(Rand rem 16#FFFFFFFFFFFF).

%%--------------------------------------------------------------------
%% @doc
%% Creates subscription.
%% @end
%%--------------------------------------------------------------------
-spec create(doc() | base() | type()) -> {ok, id()} | {error, term()}.
create(Doc = #document{}) ->
    ?extract_key(datastore_model:create(?CTX, Doc));
create(Sub = #subscription{id = undefined}) ->
    create(Sub#subscription{id = generate_id()});
create(Sub = #subscription{id = Id}) ->
    create(#document{key = Id, value = Sub}).

%%--------------------------------------------------------------------
%% @doc
%% Saves subscription.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, id()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Returns subscription.
%% @end
%%--------------------------------------------------------------------
-spec get(id()) -> {ok, doc()} | {error, term()}.
get(Id) ->
    datastore_model:get(?CTX, Id).

%%--------------------------------------------------------------------
%% @doc
%% Deletes subscription.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Id) ->
    datastore_model:delete(?CTX, Id).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [doc()]} | {error, term()}.
list() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

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