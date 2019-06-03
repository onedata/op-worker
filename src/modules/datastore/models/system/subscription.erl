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
-export([create_durable_subscription/1, list_durable_subscriptions/0]).
%% Model Test API
-export([save/1, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: binary().
-type base() :: #subscription{}.
-type type() :: #file_attr_changed_subscription{} |
                #file_location_changed_subscription{} |
                #file_read_subscription{} | #file_written_subscription{} |
                #file_perm_changed_subscription{} |
                #file_removed_subscription{} | #file_renamed_subscription{} |
                #quota_exceeded_subscription{} | #monitoring_subscription{} |
                #helper_params_changed_subscription{}.
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
%% Creates durable subscription (active for all clients).
%% @end
%%--------------------------------------------------------------------
-spec create_durable_subscription(doc() | base()) -> {ok, id()} | {error, term()}.
create_durable_subscription(Doc = #document{}) ->
    ?extract_key(datastore_model:create(?CTX, Doc));
create_durable_subscription(Sub = #subscription{id = undefined}) ->
    create_durable_subscription(Sub#subscription{id = generate_id()});
create_durable_subscription(Sub = #subscription{id = Id}) ->
    create_durable_subscription(#document{key = Id, value = Sub}).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of durable subscriptions (subscriptions that are always active,
%% created for each client when it connects to op_worker).
%% @end
%%--------------------------------------------------------------------
-spec list_durable_subscriptions() -> {ok, [doc()]} | {error, term()}.
list_durable_subscriptions() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%%===================================================================
%%% Model Test API
%%%===================================================================

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
%% Deletes subscription.
%% @end
%%--------------------------------------------------------------------
-spec delete(id()) -> ok | {error, term()}.
delete(Id) ->
    datastore_model:delete(?CTX, Id).

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