%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an access to the subscription specific data.
%%% @end
%%%-------------------------------------------------------------------
-module(subscription_type).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").

%% API
-export([get_routing_key/1, get_stream_key/1, get_stream/1, is_remote/1]).
-export([get_context/1, update_context/2]).

-type ctx() :: undefined | {file, fslogic_worker:file_guid()}.

-export_type([ctx/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a routing key that will be used to update globally cached event routing
%% table with a subscriber session ID.
%% @end
%%--------------------------------------------------------------------
-spec get_routing_key(Sub :: subscription:base() | subscription:type()) ->
    {ok, Key :: event_router:key()} | {error, session_only}.
get_routing_key(#subscription{type = Type}) ->
    get_routing_key(Type);
get_routing_key(#file_attr_changed_subscription{file_uuid = FileUuid}) ->
    {ok, <<"file_attr_changed.", FileUuid/binary>>};
get_routing_key(#file_location_changed_subscription{file_uuid = FileUuid}) ->
    {ok, <<"file_location_changed.", FileUuid/binary>>};
get_routing_key(#file_perm_changed_subscription{file_uuid = FileUuid}) ->
    {ok, <<"file_perm_changed.", FileUuid/binary>>};
get_routing_key(#file_removed_subscription{file_uuid = FileUuid}) ->
    {ok, <<"file_removed.", FileUuid/binary>>};
get_routing_key(#file_renamed_subscription{file_uuid = FileUuid}) ->
    {ok, <<"file_renamed.", FileUuid/binary>>};
get_routing_key(#quota_exceeded_subscription{}) ->
    {ok, <<"quota_exceeded">>};
get_routing_key(_) ->
    {error, session_only}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a key of a stream responsible for processing events associated with
%% a subscription..
%% @end
%%--------------------------------------------------------------------
-spec get_stream_key(Sub :: subscription:base() | subscription:type()) ->
    Key :: event_stream:key().
get_stream_key(#subscription{type = Type}) -> get_stream_key(Type);
get_stream_key(#file_read_subscription{}) -> file_read;
get_stream_key(#file_written_subscription{}) -> file_written;
get_stream_key(#file_attr_changed_subscription{}) -> file_attr_changed;
get_stream_key(#file_location_changed_subscription{}) -> file_location_changed;
get_stream_key(#file_perm_changed_subscription{}) -> file_perm_changed;
get_stream_key(#file_removed_subscription{}) -> file_removed;
get_stream_key(#file_renamed_subscription{}) -> file_renamed;
get_stream_key(#quota_exceeded_subscription{}) -> quota_exceeded;
get_stream_key(#monitoring_subscription{}) -> monitoring.

%%--------------------------------------------------------------------
%% @doc
%% Returns an event stream definition or if missing creates default one based on
%% the subscription type.
%% @end
%%--------------------------------------------------------------------
-spec get_stream(Sub :: subscription:base() | subscription:type()) ->
    Stm :: event:stream().
get_stream(#subscription{stream = undefined} = Sub) ->
    event_stream_factory:create(Sub);
get_stream(#subscription{stream = Stm}) ->
    Stm;
get_stream(Sub) ->
    event_stream_factory:create(Sub).

%%--------------------------------------------------------------------
%% @doc
%% Returns 'true' for subscriptions that should be serialized and forwarded to
%% a remote producer.
%% @end
%%--------------------------------------------------------------------
-spec is_remote(Sub :: subscription:base() | subscription:type()) ->
    Remote :: boolean().
is_remote(#subscription{type = Type}) -> is_remote(Type);
is_remote(#file_read_subscription{}) -> true;
is_remote(#file_written_subscription{}) -> true;
is_remote(_) -> false.

%%--------------------------------------------------------------------
%% @doc
%% Returns a subscription context.
%% @end
%%--------------------------------------------------------------------
-spec get_context(Sub :: subscription:base() | subscription:type()) -> Ctx :: ctx().
get_context(#subscription{type = Type}) ->
    get_context(Type);
get_context(#file_attr_changed_subscription{file_uuid = FileUuid}) ->
    {file, FileUuid};
get_context(#file_location_changed_subscription{file_uuid = FileUuid}) ->
    {file, FileUuid};
get_context(#file_perm_changed_subscription{file_uuid = FileUuid}) ->
    {file, FileUuid};
get_context(#file_removed_subscription{file_uuid = FileUuid}) ->
    {file, FileUuid};
get_context(#file_renamed_subscription{file_uuid = FileUuid}) ->
    {file, FileUuid};
get_context(_) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% Updates the subscription context.
%% @end
%%--------------------------------------------------------------------
-spec update_context(Sub :: subscription:base() | subscription:type(), Ctx :: ctx()) ->
    NewSub :: subscription:base() | subscription:type().
update_context(#subscription{type = Type} = Sub, Ctx) ->
    Sub#subscription{type = update_context(Type, Ctx)};
update_context(#file_attr_changed_subscription{} = Object, {file, FileUuid}) ->
    Object#file_attr_changed_subscription{file_uuid = FileUuid};
update_context(#file_location_changed_subscription{} = Object, {file, FileUuid}) ->
    Object#file_location_changed_subscription{file_uuid = FileUuid};
update_context(#file_perm_changed_subscription{} = Object, {file, FileUuid}) ->
    Object#file_perm_changed_subscription{file_uuid = FileUuid};
update_context(#file_removed_subscription{} = Object, {file, FileUuid}) ->
    Object#file_removed_subscription{file_uuid = FileUuid};
update_context(#file_renamed_subscription{} = Object, {file, FileUuid}) ->
    Object#file_renamed_subscription{file_uuid = FileUuid};
update_context(Object, _Ctx) ->
    Object.

