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
-module(space_cleanup).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").

-define(HOURS_SINCE_LAST_OPEN_LIMIT,
    application:get_env(?APP_NAME, hours_since_last_open_limit, 24)).
-define(TOTAL_OPEN_LIMIT,
    application:get_env(?APP_NAME, total_open_limit, null)).
-define(HOUR_AVERAGE_LIMIT,
    application:get_env(?APP_NAME, hour_average_limit, null)).
-define(DAY_AVERAGE_LIMIT,
    application:get_env(?APP_NAME, day_average_limit, 3)).
-define(MONTH_AVERAGE_LIMIT,
    application:get_env(?APP_NAME, month_average_limit, null)).

%% API
-export([initialize/1, periodic_cleanup/0]).

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
    {ok, #document{value = #od_provider{spaces = Spaces}}} =
        od_provider:get_or_fetch(oneprovider:get_provider_id()),
    lists:foreach(fun cleanup_space/1, Spaces).

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
    FilesToClean = file_popularity_view:get_unpopular_files(
        SpaceId, ?HOURS_SINCE_LAST_OPEN_LIMIT, ?TOTAL_OPEN_LIMIT,
        ?HOUR_AVERAGE_LIMIT, ?DAY_AVERAGE_LIMIT, ?MONTH_AVERAGE_LIMIT
    ),
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
    sync_req:invalidate_file_replica(RootUserCtx, FileCtx, undefined).