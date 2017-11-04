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
-include_lib("ctool/include/logging.hrl").

%% API
-export([maybe_start/1, force_cleanup/1, get_details/1, disable_autocleaning/1,
    status/1, list_reports_since/2, is_autocleaning_enabled/1, configure_autocleaning/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This function starts space_cleanup forcefully (even if storage
%% occupancy hasn't reached configured threshold).
%% @end
%%-------------------------------------------------------------------
-spec force_cleanup(od_space:id()) -> ok | {error, term()}.
force_cleanup(SpaceId) ->
    case space_storage:get(SpaceId) of
        {ok, SpaceStorageDoc} ->
            CleanupConfig = space_storage:get_autocleaning_config(SpaceStorageDoc),
            CurrentSize = space_quota:current_size(SpaceId),
            autocleaning:start(SpaceId, CleanupConfig, CurrentSize);
        Error = {error, _} ->
            ?error_stacktrace("No space_storage mapping for space ~p", [SpaceId]),
            Error
    end.

%%-------------------------------------------------------------------
%% @doc
%% This function is responsible for scheduling autocleaning operations.
%% It schedules autocleaning if cleanup is enabled and current storage
%% occupation has reached threshold defined in cleanup configuration in
%% space_storage record or if flag Force is set to true.
%% @end
%%-------------------------------------------------------------------
-spec maybe_start(od_space:id()) -> ok.
maybe_start(SpaceId) ->
    case is_autocleaning_enabled(SpaceId) of
        true ->
            CleanupConfig = space_storage:get_autocleaning_config(SpaceId),
            CurrentSize = space_quota:current_size(SpaceId),
            case autocleaning_config:should_start_autoclean(CurrentSize, CleanupConfig) of
                true-> autocleaning:start(SpaceId, CleanupConfig, CurrentSize);
                _ -> ok
            end;
        _ -> ok
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks whether automatic cleanup is enabled for storage supporting
%% given space.
%% @end
%%--------------------------------------------------------------------
-spec is_autocleaning_enabled(od_space:id()) -> boolean().
is_autocleaning_enabled(SpaceId) ->
    case space_storage:get(SpaceId) of
        {ok, #document{value = #space_storage{cleanup_enabled = CleanupEnabled}}} ->
            CleanupEnabled;
        {error, _} ->
            ?error_stacktrace("No space_storage mapping for space ~p", [SpaceId]),
            false
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns autocleaning details for given space.
%% @end
%%-------------------------------------------------------------------
-spec get_details(od_space:id()) -> proplists:proplist().
get_details(SpaceId) -> [
    {enabled, is_autocleaning_enabled(SpaceId)},
    {settings, get_autocleaning_settings(SpaceId)}
].

%%-------------------------------------------------------------------
%% @doc
%% This function is responsible for configuration of autocleaning.
%% @end
%%-------------------------------------------------------------------
-spec configure_autocleaning(od_space:id(), maps:map()) ->
    {ok, od_space:id()} | {error,  term()}.
configure_autocleaning(SpaceId, Settings) ->
    space_storage:update(SpaceId, fun(SpaceStorage) ->
        Enabled = maps:get(enabled, Settings, undefined),
        configure_autocleaning(SpaceStorage, Enabled, Settings)
    end).

%%-------------------------------------------------------------------
%% @doc
%% Disables autocleaning.
%% @end
%%-------------------------------------------------------------------
-spec disable_autocleaning(od_space:id()) -> {ok, od_space:id()}.
disable_autocleaning(SpaceId) ->
    configure_autocleaning(SpaceId, #{enabled => false}).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns proplist describing status of autocleaning in given space.
%% @end
%%-------------------------------------------------------------------
-spec status(od_space:id()) -> proplists:proplist().
status(SpaceId) ->
    CurrentSize = space_quota:current_size(SpaceId),
    InProgress = case space_storage:get_cleanup_in_progress(SpaceId) of
        undefined ->
            false;
        _ ->
            true
    end,
    [
        {inProgress, InProgress},
        {spaceOccupancy, CurrentSize}
    ].

%%-------------------------------------------------------------------
%% @doc
%% Returns list of autocleaning reports, that has been scheduled later
%% than Since.
%% @end
%%-------------------------------------------------------------------
-spec list_reports_since(od_space:id(), binary()) -> [maps:map()].
list_reports_since(SpaceId, Since) ->
    SinceEpoch = timestamp_utils:iso8601_to_epoch(Since),
    autocleaning:list_reports_since(SpaceId, SinceEpoch).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Updates autocleaning_config.
%% @end
%%-------------------------------------------------------------------
-spec configure_autocleaning(space_storage:model(), boolean(), maps:map()) ->
    {ok, space_storage:model()} | {error, term()}.
configure_autocleaning(#space_storage{cleanup_enabled = false}, undefined, _) ->
    {error, autocleaning_disabled};
configure_autocleaning(SS = #space_storage{cleanup_enabled = _Enabled}, false, _) ->
    {ok, SS#space_storage{cleanup_enabled = false}};
configure_autocleaning(SS = #space_storage{autocleaning_config = OldConfig}, _, Settings) ->
    case SS#space_storage.file_popularity_enabled of
        true ->
            case autocleaning_config:create_or_update(OldConfig, Settings) of
                {error, Reason} ->
                    {error, Reason};
                NewConfig ->
                    {ok, SS#space_storage{
                        cleanup_enabled = true,
                        autocleaning_config = NewConfig
                    }}
            end;
        _ ->
            {error, file_popularity_disabled}
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns current autocleaning_settings.
%% @end
%%-------------------------------------------------------------------
-spec get_autocleaning_settings(od_space:id()) -> proplists:proplist() | undefined.
get_autocleaning_settings(SpaceId) ->
    case space_storage:get_autocleaning_config(SpaceId) of
        undefined ->
            undefined;
        Config ->
            [
                {lowerFileSizeLimit, autocleaning_config:get_lower_size_limit(Config)},
                {upperFileSizeLimit, autocleaning_config:get_upper_size_limit(Config)},
                {maxFileNotOpenedHours, autocleaning_config:get_max_inactive_limit(Config)},
                {threshold, autocleaning_config:get_threshold(Config)},
                {target, autocleaning_config:get_target(Config)}
            ]
    end.