%%%--------------------------------------------------------------------
%%% @author Michal Żmuda
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for maintaining cross-node state related to
%%% subscriptions.
%%% @end
%%%--------------------------------------------------------------------
-module(subscription_monitor).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

-define(STATE_KEY, <<"current_state">>).

-export([account_updates/1, get_missing/0, get_users/0, put_user/1,
    reevaluate_users/0, ensure_initialised/0, get_refreshing_node/0]).

-type(seq() :: pos_integer()).

%%--------------------------------------------------------------------
%% @doc
%% Updates state with already received sequence numbers.
%% @end
%%--------------------------------------------------------------------
-spec account_updates(SequenceNumbers :: ordsets:ordset(seq())) -> no_return().
account_updates(SequenceNumbers) ->
    subscriptions_state:update(?STATE_KEY, fun(State) ->
        #subscriptions_state{missing = Missing, largest = Largest} = State,
        NewMissing = ordsets:subtract(Missing, SequenceNumbers),
        NewLargest = max(Largest, lists:last(SequenceNumbers)),
        {ok, State#subscriptions_state{missing = NewMissing, largest = NewLargest}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Returns gaps in received sequence numbers sequence and largest known
%% sequence number.
%% @end
%%--------------------------------------------------------------------
-spec get_missing() -> {Missing :: ordsets:ordset(seq()), Largest :: seq()}.
get_missing() ->
    {ok, #document{value = #subscriptions_state{missing = Missing,
        largest = Largest}}} = subscriptions_state:get(?STATE_KEY),
    {Missing, Largest}.

%%--------------------------------------------------------------------
%% @doc
%% Returns users currently qualified as working with that provider.
%% @end
%%--------------------------------------------------------------------
-spec get_users() -> [UserID :: binary()].
get_users() ->
    {ok, #document{value = #subscriptions_state{users = UserIDs}}}
        = subscriptions_state:get(?STATE_KEY),
    lists:usort(UserIDs).

%%--------------------------------------------------------------------
%% @doc
%% Ensures given user is qualified as working with this provider.
%% Should be invoked when first session for user is added.
%% @end
%%--------------------------------------------------------------------
-spec put_user(UserID :: binary()) -> no_return().
put_user(UserID) ->
    subscriptions_state:update(?STATE_KEY, fun(State) ->
        Users = [UserID | State#subscriptions_state.users],
        {ok, State#subscriptions_state{users = Users}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Ensures users qualified as working with this provider are synced with
%% the datatstore. Performance may be poor as it examines sessions of users.
%% @end
%%--------------------------------------------------------------------
-spec reevaluate_users() -> no_return().
reevaluate_users() ->
    subscriptions_state:update(?STATE_KEY, fun(State) ->
        {ok, State#subscriptions_state{users = get_users_with_session()}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Ensures that state is initialised.
%% @end
%%--------------------------------------------------------------------
-spec ensure_initialised() -> no_return().
ensure_initialised() ->
    case subscriptions_state:exists(?STATE_KEY) of
        true -> ok;
        false ->
            % as retrieving users is costly, first we've checked for existence
            Users = get_users_with_session(),
            subscriptions_state:create_or_update(#document{
                key = ?STATE_KEY,
                value = #subscriptions_state{
                    largest = 1,
                    missing = [],
                    users = Users,
                    refreshing_node = node()
                }
            }, fun(State) -> {ok, State} end)
    end.

get_refreshing_node() ->
    {ok, #document{value = #subscriptions_state{refreshing_node = Node}}} =
        subscriptions_state:get(?STATE_KEY),
    {ok, Nodes} = request_dispatcher:get_worker_nodes(?MODULE),
    case lists:member(Node, Nodes) of
        true -> {ok, Node};
        false ->
            [NewNode | _] = lists:append(Nodes, [node()]),
            subscriptions_state:update(?STATE_KEY, fun(State) ->
                State#subscriptions_state{refreshing_node = NewNode}
            end),
            {ok, NewNode}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_users_with_session() -> [binary()].
get_users_with_session() ->
    {ok, Docs} = session:all_with_user(),
    UserIDs = lists:filtermap(fun
        (#document{value = #session{identity = #identity{user_id = UserID}}}) ->
            {true, UserID}
    end, Docs),
    UserIDs.