%%%--------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% API for auto-cleaning mechanism.
%%% Auto-cleaning runs are started basing on #autocleaning{} record.
%%% Each auto-cleaning run is associated with exactly one #autocleaning_run{}
%%% document that contains information about this operation and exactly
%%% one autocleaning_controller gen_server that is responsible for
%%% deletion of unpopular file replicas.
%%% @end
%%%--------------------------------------------------------------------
-module(autocleaning_api).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([force_start/1, maybe_start/2,
    configure/2, disable/1,
    get_configuration/1, status/1,
    list_reports_since/2, list_reports/1, list_reports/4,
    restart_autocleaning_run/1, delete_config/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This function starts autocleaning forcefully (even if storage
%% occupancy hasn't reached the configured threshold).
%% @end
%%-------------------------------------------------------------------
-spec force_start(od_space:id()) -> {ok, autocleaning_run:id()} | {error, term()}.
force_start(SpaceId) ->
    case file_popularity_api:is_enabled(SpaceId) of
        true ->
            case exists_and_is_enabled(SpaceId) of
                {true, AC} ->
                    Config = autocleaning:get_config(AC),
                    CurrentSize = space_quota:current_size(SpaceId),
                    autocleaning_controller:start(SpaceId, Config, CurrentSize);
                false ->
                    {error, autocleaning_disabled}
            end;
        false ->
            {error, file_popularity_disabled}
    end .


%%-------------------------------------------------------------------
%% @doc
%% This function is responsible for scheduling autocleaning operations.
%% It schedules autocleaning if cleanup is enabled and current storage
%% occupation has reached threshold defined in cleanup configuration in
%% space_storage record or if flag Force is set to true.
%% @end
%%-------------------------------------------------------------------
-spec maybe_start(od_space:id(), space_quota:record()) ->
    ok | {ok, autocleaning_run:id()} | {error, term()}.
maybe_start(SpaceId, SpaceQuota) ->
    case file_popularity_api:is_enabled(SpaceId) of
        true ->
            case exists_and_is_enabled(SpaceId) of
                {true, AC} ->
                    Config = autocleaning:get_config(AC),
                    CurrentSize = space_quota:current_size(SpaceQuota),
                    case autocleaning_config:is_threshold_exceeded(CurrentSize, Config) of
                        true -> autocleaning_controller:start(SpaceId, Config, CurrentSize);
                        _ -> ok
                    end;
                _ -> ok
            end;
        false ->
            ok
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns autocleaning details for given space.
%% @end
%%-------------------------------------------------------------------
-spec get_configuration(od_space:id()) -> maps:map().
get_configuration(SpaceId) ->
    case autocleaning:get_config(SpaceId) of
        undefined ->
            ?warning("undefined configuration for auto-cleaning in space ~p", [SpaceId]),
            #{};
        Config ->
            autocleaning_config:to_map(Config)
    end.

%%-------------------------------------------------------------------
%% @doc
%% This function is responsible for updating auto-cleaning configuration.
%% @end
%%-------------------------------------------------------------------
-spec configure(od_space:id(), maps:map()) -> ok | {error, term()}.
configure(SpaceId, Configuration) ->
    case file_popularity_api:is_enabled(SpaceId) of
        true ->
            autocleaning:create_or_update(SpaceId, Configuration);
        false ->
            {error, file_popularity_disabled}
    end.

-spec disable(od_space:id()) -> ok | {error, term()}.
disable(SpaceId) ->
    configure(SpaceId, #{enabled => false}).

%%-------------------------------------------------------------------
%% @doc
%% Returns map describing status of autocleaning in given space,
%% understandable by onepanel.
%% @end
%%-------------------------------------------------------------------
-spec status(od_space:id()) -> maps:map().
status(SpaceId) ->
    CurrentSize = space_quota:current_size(SpaceId),
    InProgress = case autocleaning:get_current_run(SpaceId) of
        undefined ->
            false;
        _ ->
            true
    end,
    #{
        in_progress => InProgress,
        space_occupancy => CurrentSize
    }.

%%-------------------------------------------------------------------
%% @doc
%% List all reports of auto-cleaning runs. The list is decreasingly
%% sorted by start time.
%% @end
%%-------------------------------------------------------------------
-spec list_reports(od_space:id()) -> {ok, [autocleaning_run:id()]}.
list_reports(SpaceId) ->
    list_reports(SpaceId, undefined, 0, all).

%%-------------------------------------------------------------------
%% @doc
%% List up to Limit reports of auto-cleaning runs starting from
%% Offset after StartId. The list is decreasingly sorted by start time.
%% @end
%%-------------------------------------------------------------------
-spec list_reports(od_space:id(), autocleaning_run:id() | undefined,
    autocleaning_run_links:offset(), autocleaning_run_links:list_limit()) ->
    {ok, [autocleaning_run:id()]}.
list_reports(SpaceId, StartId, Offset, Limit) ->
    autocleaning_run_links:list(SpaceId, StartId, Offset, Limit).

%%-------------------------------------------------------------------
%% @doc
%% Returns list of autocleaning reports, that has been scheduled later
%% than Since.
%% @end
%%-------------------------------------------------------------------
-spec list_reports_since(od_space:id(), binary()) -> [maps:map()].
list_reports_since(SpaceId, Since) ->
    SinceEpoch = time_utils:iso8601_to_epoch(Since),
    autocleaning_run:list_reports_since(SpaceId, SinceEpoch).


-spec restart_autocleaning_run(autocleaning:id()) ->
    ok | {ok, autocleaning_run:id()} | {error, term()}.
restart_autocleaning_run(SpaceId) ->
    case autocleaning:get(SpaceId) of
        {ok, #document{value = #autocleaning{current_run = undefined}}} ->
            % there is no autocleaning_run to restart
            ok;
        {ok, #document{value = #autocleaning{current_run = ARId, config = Config}}} ->
            case autocleaning_config:is_enabled(Config) of
                true ->
                    autocleaning_controller:restart(ARId, SpaceId, Config);
                false ->
                    ?warning("Could not restart auto-cleaning run ~p in space "
                    "because auto-cleaning mechanism has been disabled", [ARId])
            end;
        {error, not_found} ->
            ok;
        Error ->
            ?error("Could not restart auto-cleaning run in space ~p due to ~p",
                [SpaceId, Error])
    end.

-spec delete_config(autocleaning:id()) -> ok.
delete_config(SpaceId) ->
    autocleaning:delete(SpaceId).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec exists_and_is_enabled(autocleaning:id()) -> {true, autocleaning:record()} | false.
exists_and_is_enabled(SpaceId) ->
    case autocleaning:get(SpaceId) of
        {ok, #document{value = AC}} ->
            case autocleaning:is_enabled(AC) of
                true -> {true, AC};
                false -> false
            end;
        _ ->
            false
    end.