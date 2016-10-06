%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module includes utility functions used in gui modules.
%%% @end
%%%-------------------------------------------------------------------
-module(op_gui_utils).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_user_auth/0]).
-export([ids_to_association/2, association_to_ids/1]).

% @todo temporary solution, fix when subscriptions work better
-export([find_all_spaces/2, find_all_groups/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a tuple that can be used directly in REST operations on behalf of
%% current user.
%% @end
%%--------------------------------------------------------------------
-spec get_user_auth() -> #token_auth{}.
get_user_auth() ->
    {ok, Auth} = session:get_auth(g_session:get_session_id()),
    Auth.


%%--------------------------------------------------------------------
%% @doc
%% Creates an associative ID from two IDs which can be easily decoupled later.
%% @end
%%--------------------------------------------------------------------
-spec ids_to_association(FirstId :: binary(), SecondId :: binary()) -> binary().
ids_to_association(FirstId, SecondId) ->
    <<FirstId/binary, ".", SecondId/binary>>.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Decouples an associative ID into two separate IDs.
%% @end
%%--------------------------------------------------------------------
-spec association_to_ids(AssocId :: binary()) -> {binary(), binary()}.
association_to_ids(AssocId) ->
    [FirstId, SecondId] = binary:split(AssocId, <<".">>, [global]),
    {FirstId, SecondId}.


%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all space ids for given user. Blocks until the spaces
%% are synchronized (by repetitive polling).
% @todo temporary solution, fix when subscriptions work better
%% @end
%%--------------------------------------------------------------------
-spec find_all_spaces(UserAuth :: oz_endpoint:auth(), UserId :: binary()) ->
    [SpaceId :: binary()].
find_all_spaces(UserAuth, UserId) ->
    find_all_spaces(UserAuth, UserId, 100).


%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all space ids for given user. Blocks until the spaces
%% are synchronized (by repetitive polling).
%% Retries up to given amount of times. Retries every 500 milliseconds.
% @todo temporary solution, fix when subscriptions work better
%% @end
%%--------------------------------------------------------------------
-spec find_all_spaces(UserAuth :: oz_endpoint:auth(), UserId :: binary(), MaxRetries :: integer()) ->
    [SpaceId :: binary()].
find_all_spaces(_, _, 0) ->
    [];

find_all_spaces(UserAuth, UserId, MaxRetries) ->
    {ok, Spaces} = user_logic:get_spaces(UserAuth, UserId),
    {SpaceIds, _} = lists:unzip(Spaces),
    case SpaceIds of
        [] ->
            timer:sleep(500),
            find_all_spaces(UserAuth, UserId, MaxRetries - 1);
        _ ->
            SpaceIds
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all group ids for given user. Blocks until the groups
%% are synchronized (by repetitive polling).
% @todo temporary solution, fix when subscriptions work better
%% @end
%%--------------------------------------------------------------------
-spec find_all_groups(UserAuth :: oz_endpoint:auth(), UserId :: binary()) ->
    [SpaceId :: binary()].
find_all_groups(UserAuth, UserId) ->
    find_all_groups(UserAuth, UserId, 100).


%%--------------------------------------------------------------------
%% @doc
%% Returns a list of all group ids for given user. Blocks until the groups
%% are synchronized (by repetitive polling).
%% Retries up to given amount of times. Retries every 500 milliseconds.
% @todo temporary solution, fix when subscriptions work better
%% @end
%%--------------------------------------------------------------------
-spec find_all_groups(UserAuth :: oz_endpoint:auth(), UserId :: binary(),
    MaxRetries :: integer()) -> [SpaceId :: binary()].
find_all_groups(_, _, 0) ->
    [];

find_all_groups(UserAuth, UserId, MaxRetries) ->
    {ok, GroupIds} = user_logic:get_groups(UserAuth, UserId),
    % Make sure that effective groups are synchronized - there should be at
    % least as many as direct groups.
    case GroupIds of
        [] ->
            timer:sleep(500),
            find_all_groups(UserAuth, UserId, MaxRetries - 1);
        _ ->
            GroupIds
    end.



