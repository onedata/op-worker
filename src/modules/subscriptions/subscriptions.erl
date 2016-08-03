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
-module(subscriptions).
-author("Michal Zmuda").

-include("global_definitions.hrl").
-include("modules/subscriptions/subscriptions.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").

-export([account_updates/1, get_missing/0, get_users/0, put_user/1,
    reevaluate_users/0, ensure_initialised/0, get_refreshing_node/0]).

-type(seq() :: non_neg_integer()).
-type(rev() :: term()).
-type(model() :: onedata_group | onedata_user | space_info | provider_info).
-type(record() :: #space_info{} | #onedata_user{} | #onedata_group{} | #provider_info{}).
-export_type([seq/0, rev/0, model/0, record/0]).

%%--------------------------------------------------------------------
%% @doc
%% Updates state with already received sequence numbers.
%% @end
%%--------------------------------------------------------------------
-spec account_updates(SequenceNumbers :: ordsets:ordset(seq())) -> ok.
account_updates(SequenceNumbers) ->
    ensure_initialised(),
    subscriptions_state:update(?SUBSCRIPTIONS_STATE_KEY, fun(State) ->
        #subscriptions_state{missing = Missing, largest = Largest} = State,
        NewLargest = max(Largest, lists:last(SequenceNumbers)),
        AddedMissing = case NewLargest =:= Largest of
            true -> [];
            false -> lists:seq(Largest + 1, NewLargest)
        end,
        NewMissing = ordsets:subtract(Missing ++ AddedMissing, SequenceNumbers),
        {ok, State#subscriptions_state{missing = NewMissing, largest = NewLargest}}
    end),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns gaps in received sequence numbers sequence and largest known
%% sequence number.
%% @end
%%--------------------------------------------------------------------
-spec get_missing() -> {Missing :: ordsets:ordset(seq()), Largest :: seq()}.
get_missing() ->
    ensure_initialised(),
    {ok, #document{value = #subscriptions_state{missing = Missing,
        largest = Largest}}} = subscriptions_state:get(?SUBSCRIPTIONS_STATE_KEY),
    {Missing, Largest}.

%%--------------------------------------------------------------------
%% @doc
%% Returns users currently qualified as working with that provider.
%% @end
%%--------------------------------------------------------------------
-spec get_users() -> [UserID :: onedata_user:id()].
get_users() ->
    ensure_initialised(),
    {ok, #document{value = #subscriptions_state{users = UserIDs}}}
        = subscriptions_state:get(?SUBSCRIPTIONS_STATE_KEY),
    lists:usort(sets:to_list(UserIDs)).

%%--------------------------------------------------------------------
%% @doc
%% Ensures given user is qualified as working with this provider.
%% Should be invoked when first session for user is added.
%% @end
%%--------------------------------------------------------------------
-spec put_user(UserID :: onedata_user:id()) -> ok.
put_user(UserID) ->
    ensure_initialised(),
    subscriptions_state:update(?SUBSCRIPTIONS_STATE_KEY, fun(State) ->
        Users = sets:add_element(UserID, State#subscriptions_state.users),
        {ok, State#subscriptions_state{users = Users}}
    end),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Ensures users qualified as working with this provider are synced with
%% the datatstore. Performance may be poor as it examines sessions of users.
%% @end
%%--------------------------------------------------------------------
-spec reevaluate_users() -> ok.
reevaluate_users() ->
    ensure_initialised(),
    subscriptions_state:update(?SUBSCRIPTIONS_STATE_KEY, fun(State) ->
        Users = get_users_with_session(),
        {ok, State#subscriptions_state{users = sets:from_list(Users)}}
    end),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Ensures that state is initialised.
%% @end
%%--------------------------------------------------------------------
-spec ensure_initialised() -> ok.
ensure_initialised() ->
    case subscriptions_state:exists(?SUBSCRIPTIONS_STATE_KEY) of
        true -> ok;
        false ->
            % as retrieving users is costly, first we've checked for existence
            Users = get_users_with_session(),
            %todo add create_or_update operation to all levels of datastore and use it here
            {ok, _} = subscriptions_state:save(#document{
                key = ?SUBSCRIPTIONS_STATE_KEY,
                value = #subscriptions_state{
                    largest = 0,
                    missing = [],
                    users = sets:from_list(Users),
                    refreshing_node = node()
                }
            }),
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns current node dedicated to perform refreshes. If node changes, the
%% selection is persisted.
%% @end
%%--------------------------------------------------------------------
-spec get_refreshing_node() -> {ok, node()}.
get_refreshing_node() ->
    ensure_initialised(),
    {ok, #document{value = #subscriptions_state{refreshing_node = Node}}} =
        subscriptions_state:get(?SUBSCRIPTIONS_STATE_KEY),
    {ok, Nodes} = request_dispatcher:get_worker_nodes(?MODULE),
    case lists:member(Node, Nodes) of
        true -> {ok, Node};
        false ->
            [NewNode | _] = lists:append(Nodes, [node()]),
            subscriptions_state:update(?SUBSCRIPTIONS_STATE_KEY, fun(State) ->
                State#subscriptions_state{refreshing_node = NewNode}
            end),
            {ok, NewNode}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @private
%% Returns users with sessions in the datastore.
%% @end
%%--------------------------------------------------------------------
-spec get_users_with_session() -> [onedata_user:id()].
get_users_with_session() ->
    {ok, Docs} = session:all_with_user(),
    lists:filtermap(fun
        (#document{value = #session{identity = #user_identity{user_id = UserID}}}) ->
            {true, UserID};
        (_) -> false
    end, Docs).