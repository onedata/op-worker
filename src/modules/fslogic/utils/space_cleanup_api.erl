%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Cleanup of unpopular files.
%%% @end
%%%--------------------------------------------------------------------
-module(space_cleanup_api).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").

% file which was not opened for this period of hours is invalidated,
% regardless of other factors
-define(HOURS_SINCE_LAST_OPEN_HARD_LIMIT,
    application:get_env(?APP_NAME, hours_since_last_open_hard_limit, 7*24)).

% Factors for file cleanup (all of them must be satisfied in order to schedule
% file for cleanup):

% what is the lower limit of size for a file so it should be considered
-define(SIZE_LOWER_LIMIT,
    application:get_env(?APP_NAME, size_lower_limit, 1)).
% how many hours since last open must have passed so the file should be considered unpopular
-define(HOURS_SINCE_LAST_OPEN_LIMIT,
    application:get_env(?APP_NAME, hours_since_last_open_limit, 24)).
% how much opens at maximum has been made on file in total so the file should be considered unpopular
-define(TOTAL_OPEN_LIMIT,
    application:get_env(?APP_NAME, total_open_limit, null)).
% what is the maximal hourly average open count over past 24 hours so the file should be considered unpopular
-define(HOUR_AVERAGE_LIMIT,
    application:get_env(?APP_NAME, hour_average_limit, null)).
% what is the maximal daily average open count over past 30 days so the file should be considered unpopular
-define(DAY_AVERAGE_LIMIT,
    application:get_env(?APP_NAME, day_average_limit, 3)).
% what is the maximal monthly average open count over past 12 months so the file should be considered unpopular
-define(MONTH_AVERAGE_LIMIT,
    application:get_env(?APP_NAME, month_average_limit, null)).

%% API
-export([initialize/1, periodic_cleanup/0, enable_cleanup/1, disable_cleanup/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes space cleanup if its enabled. The function creates popularity view
%% which is later used to fetch and clean replicas of unpopular files.
%% @end
%%--------------------------------------------------------------------
-spec initialize(od_space:id()) -> ok.
initialize(SpaceId) ->
    case space_storage:is_cleanup_enabled(SpaceId) of
        true ->
            file_popularity_view:create(SpaceId);
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Periodic check of unpopular files and cleanup
%% @end
%%--------------------------------------------------------------------
-spec periodic_cleanup() -> ok.
periodic_cleanup() ->
    {ok, Spaces} = provider_logic:get_spaces(),
    SpacesToCleanup = lists:filter(fun(SpaceId) ->
        space_storage:is_cleanup_enabled(SpaceId)
    end, Spaces),
    lists:foreach(fun cleanup_space/1, SpacesToCleanup).

%%--------------------------------------------------------------------
%% @doc
%% Enable file popularity and space cleanup
%% @end
%%--------------------------------------------------------------------
-spec enable_cleanup(od_space:id()) -> {ok, od_space:id()} | {error, term()}.
enable_cleanup(SpaceId) ->
    space_storage:update(SpaceId, fun(SpaceStorage = #space_storage{}) ->
        {ok, SpaceStorage#space_storage{cleanup_enabled = true}}
    end).

%%--------------------------------------------------------------------
%% @doc
%% Disable file popularity and space cleanup
%% @end
%%--------------------------------------------------------------------
-spec disable_cleanup(od_space:id()) -> {ok, od_space:id()} | {error, term()}.
disable_cleanup(SpaceId) ->
    space_storage:update(SpaceId, fun(SpaceStorage = #space_storage{}) ->
        {ok, SpaceStorage#space_storage{cleanup_enabled = false}}
    end).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Cleanups unpopular files from space
%% @end
%%--------------------------------------------------------------------
-spec cleanup_space(od_space:id()) -> ok.
cleanup_space(SpaceId) ->
    FilesToCleanSoftCheck = file_popularity_view:get_unpopular_files(
        SpaceId, ?SIZE_LOWER_LIMIT, ?HOURS_SINCE_LAST_OPEN_LIMIT, ?TOTAL_OPEN_LIMIT,
        ?HOUR_AVERAGE_LIMIT, ?DAY_AVERAGE_LIMIT, ?MONTH_AVERAGE_LIMIT
    ),
    FilesToCleanHardCheck = file_popularity_view:get_unpopular_files(
        SpaceId, ?SIZE_LOWER_LIMIT, ?HOURS_SINCE_LAST_OPEN_HARD_LIMIT, null,
        null, null, null
    ),
    FilesToClean = lists:usort(FilesToCleanSoftCheck ++ FilesToCleanHardCheck),
    lists:foreach(fun cleanup_replica/1, FilesToClean).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Invalidates local replica of given file
%% @end
%%--------------------------------------------------------------------
-spec cleanup_replica(file_ctx:ctx()) -> ok.
cleanup_replica(FileCtx) ->
    RootUserCtx = user_ctx:new(session:root_session_id()),
    sync_req:invalidate_file_replica(RootUserCtx, FileCtx, undefined, undefined).