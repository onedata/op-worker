%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Helper model counting subscriptions used to optimize events
%%% management when no oneclients are subscribed (e.g., only REST usage).
%%% NOTE: It is temporary solution to be deleted after refactoring of
%%% events subsystem.
%%% @end
%%%-------------------------------------------------------------------
-module(file_subscription_counter).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").

%% API
-export([subscription_added/1, subscription_deleted/1, has_subscriptions/1]).

%% datastore_model callbacks
-export([get_ctx/0]).

-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec subscription_added(binary()) -> ok | {error, term()}.
subscription_added(Key) ->
    case event_type:get_reference_based_prefix(Key) of
        {ok, Prefix} ->
            Default = #file_subscription_counter{subscriptions_count = 1},
            ?extract_ok(datastore_model:update(?CTX, Prefix,
                fun(Record = #file_subscription_counter{subscriptions_count = Count}) ->
                    {ok, Record#file_subscription_counter{subscriptions_count = Count + 1}}
                end, Default));
        {error, not_reference_based} ->
            ok
    end.

-spec subscription_deleted(binary()) -> ok | {error, term()}.
subscription_deleted(Key) ->
    case event_type:get_reference_based_prefix(Key) of
        {ok, Prefix} ->
            Default = #file_subscription_counter{subscriptions_count = 0},
            ?extract_ok(datastore_model:update(?CTX, Prefix,
                fun(Record = #file_subscription_counter{subscriptions_count = Count}) ->
                    {ok, Record#file_subscription_counter{subscriptions_count = max(Count - 1, 0)}}
                end, Default));
        {error, not_reference_based} ->
            ok
    end.

-spec has_subscriptions(event:base() | event:type() | binary()) -> boolean() | {error, term()}.
has_subscriptions(Evt) ->
    case event_type:get_reference_based_prefix(Evt) of
        {ok, Prefix} ->
            case datastore_model:get(?CTX, Prefix) of
                {ok, #document{value = #file_subscription_counter{subscriptions_count = Count}}} ->
                    Count > 0;
                _ ->
                    false
            end;
        {error, not_reference_based} = Error ->
            Error
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