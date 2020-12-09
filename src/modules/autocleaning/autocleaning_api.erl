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
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([force_run/1, check/1, configure/2, disable/1, get_configuration/1,
    get_status/1, list_reports/1, list_reports/4, restart_autocleaning_run/1,
    delete_config/1, get_run_report/1, get_run_report/2, cancel_run/1, cancel_run/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%-------------------------------------------------------------------
%% @doc
%% This function starts autocleaning forcefully (even if storage
%% occupancy hasn't reached the configured threshold).
%% @end
%%-------------------------------------------------------------------
-spec force_run(od_space:id()) ->
    {ok, autocleaning:run_id()} | errors:error() | {error, term()}.
force_run(SpaceId) ->
    case file_popularity_api:is_enabled(SpaceId) of
        true ->
            case exists_and_is_enabled(SpaceId) of
                {true, AC} ->
                    Config = autocleaning:get_config(AC),
                    CurrentSize = space_quota:current_size(SpaceId),
                    case autocleaning_config:is_target_reached(CurrentSize, Config) of
                        false ->
                            autocleaning_run_controller:start(SpaceId, Config, CurrentSize);
                        true ->
                            {error, nothing_to_clean}
                    end;
                false ->
                    ?ERROR_AUTO_CLEANING_DISABLED
            end;
        false ->
            ?ERROR_FILE_POPULARITY_DISABLED
    end .

-spec cancel_run(od_space:id()) -> ok.
cancel_run(SpaceId) ->
    case autocleaning:get_current_run(SpaceId) of
        undefined ->
            ok;
        ARId ->
            cancel_run(SpaceId, ARId)
    end.

-spec cancel_run(od_space:id(), autocleaning_run:id()) -> ok.
cancel_run(SpaceId, ARId) ->
    autocleaning_run_controller:cancel_cleaning(SpaceId, ARId).

%%-------------------------------------------------------------------
%% @doc
%% This function is responsible for scheduling autocleaning operations.
%% It schedules autocleaning if cleanup is enabled and current storage
%% occupation has reached threshold defined in cleanup configuration.
%% @end
%%-------------------------------------------------------------------
-spec check(od_space:id()) -> ok | {ok, autocleaning:run_id()} | {error, term()}.
check(SpaceId) ->
    case file_popularity_api:is_enabled(SpaceId) of
        true ->
            case exists_and_is_enabled(SpaceId) of
                {true, AC} ->
                    Config = autocleaning:get_config(AC),
                    CurrentSize = space_quota:current_size(SpaceId),
                    case {autocleaning_config:is_threshold_exceeded(CurrentSize, Config),
                        autocleaning_config:is_target_reached(CurrentSize, Config)} of
                        {true, false} ->
                            autocleaning_run_controller:start(SpaceId, Config, CurrentSize);
                        _ ->
                            ok
                    end;
                false ->
                    ok
            end;
        false ->
            ok
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns autocleaning details for given space.
%% @end
%%-------------------------------------------------------------------
-spec get_configuration(od_space:id()) -> map().
get_configuration(SpaceId) ->
    case autocleaning:get_config(SpaceId) of
        undefined ->
            #{};
        Config ->
            autocleaning_config:to_map(Config)
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns info about given auto-cleaning run in the form
%% understandable by onepanel.
%% @end
%%-------------------------------------------------------------------
-spec get_run_report(autocleaning_run:id() | autocleaning_run:doc()) ->
    {ok, map()} | {error, term()}.
get_run_report(#document{key = ARId, value = AR}) ->
    get_run_report(ARId, AR);
get_run_report(ARId) ->
    case autocleaning_run:get(ARId) of
        {ok, ARDoc} -> get_run_report(ARDoc);
        Error -> Error
    end.

%%-------------------------------------------------------------------
%% @doc
%% This function is responsible for updating auto-cleaning configuration.
%% @end
%%-------------------------------------------------------------------
-spec configure(od_space:id(), map()) -> ok | {error, term()}.
configure(SpaceId, Configuration) ->
    case file_popularity_api:is_enabled(SpaceId) of
        true ->
            autocleaning:create_or_update(SpaceId, Configuration);
        false ->
            ?ERROR_FILE_POPULARITY_DISABLED
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
-spec get_status(od_space:id()) -> map().
get_status(SpaceId) ->
    CurrentSize = space_quota:current_size(SpaceId),
    Status = case list_reports(SpaceId, undefined, 0, 1) of
        {ok, []} ->
            null;
        {ok, [ARId]} ->
            case autocleaning_run:get(ARId) of
                {ok, ARDoc} -> autocleaning_run:get_status(ARDoc);
                _ -> null
            end
    end,
    #{
        lastRunStatus => Status,
        spaceOccupancy => CurrentSize
    }.

%%-------------------------------------------------------------------
%% @doc
%% List all reports of auto-cleaning runs. The list is decreasingly
%% sorted by start time.
%% @end
%%-------------------------------------------------------------------
-spec list_reports(od_space:id()) -> {ok, [autocleaning:run_id()]}.
list_reports(SpaceId) ->
    list_reports(SpaceId, undefined, 0, all).

%%-------------------------------------------------------------------
%% @doc
%% List up to Limit reports of auto-cleaning runs starting from
%% Offset after StartId. The list is decreasingly sorted by start time.
%% @end
%%-------------------------------------------------------------------
-spec list_reports(od_space:id(), autocleaning:run_id() | undefined,
    autocleaning_run_links:offset(), autocleaning_run_links:list_limit()) ->
    {ok, [autocleaning:run_id()]}.
list_reports(SpaceId, StartId, Offset, Limit) ->
    autocleaning_run_links:list(SpaceId, StartId, Offset, Limit).


-spec restart_autocleaning_run(autocleaning:id()) ->
    ok | {ok, autocleaning:run_id()} | {error, term()}.
restart_autocleaning_run(SpaceId) ->
    case autocleaning:get(SpaceId) of
        {ok, #document{value = #autocleaning{current_run = undefined}}} ->
            % there is no autocleaning_run to restart
            ok;
        {ok, #document{value = #autocleaning{current_run = ARId, config = Config}}} ->
            case autocleaning_config:is_enabled(Config) of
                true ->
                    autocleaning_run_controller:restart(ARId, SpaceId, Config);
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

%%-------------------------------------------------------------------
%% @doc
%% Returns info about given auto-cleaning run in the form
%% understandable by onepanel.
%% @end
%%-------------------------------------------------------------------
-spec get_run_report(autocleaning:run_id(), autocleaning_run:record()) -> {ok, map()}.
get_run_report(ARId, #autocleaning_run{
    started_at = StartedAt,
    stopped_at = StoppedAt,
    status = Status,
    released_bytes = ReleasedBytes,
    bytes_to_release = BytesToRelease,
    released_files = ReleasedFiles
}) ->
    StoppedAt2 = case StoppedAt of
        undefined -> null;
        StoppedAt -> time:seconds_to_iso8601(StoppedAt)
    end,
    {ok, #{
        id => ARId,
        index => autocleaning_run_links:link_key(ARId, StartedAt),
        started_at => time:seconds_to_iso8601(StartedAt),
        stopped_at => StoppedAt2,
        status => atom_to_binary(Status, utf8),
        released_bytes => ReleasedBytes,
        bytes_to_release => BytesToRelease,
        files_number => ReleasedFiles
    }}.
