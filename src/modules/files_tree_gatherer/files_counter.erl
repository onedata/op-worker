%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for counting files inside directory and
%%% calculating directory sizes. It provides following parameters for each
%%% directory:
%%%    - files count,
%%%    - directories count,
%%%    - directory size.
%%%
%%% All parameters are stored as histogram to allow tracking parameter
%%% value changes.
%%%
%%% TODO VFS-8837 - counter works only for spaces supported after upgrade
%%% @end
%%%-------------------------------------------------------------------
-module(files_counter).
-author("Michal Wrzeszcz").


-behavior(files_tree_gatherer_behaviour).


-include("modules/tree_gatherer/files_tree_gatherer.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("cluster_worker/include/modules/datastore/ts_metric_config.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([get_values_map/1, get_values_map/2, get_values_map_and_histogram/1, update_size/2,
    increment_file_count/1, decrement_file_count/1,
    increment_dir_count/1, decrement_dir_count/1,
    update_parameter/3, delete_histogram/1]).

%% files_tree_gatherer_behaviour callbacks
-export([init_cache/1, merge/3, save/2]).

%% datastore_model callbacks
-export([get_ctx/0]).


-type ctx() :: datastore:ctx().
-type parameter() :: binary(). % see module doc and parameters/0 function
-type parameter_value() :: non_neg_integer().
-type values_map() :: #{
    parameter () := parameter_value()
}.


-define(CTX, #{
    model => ?MODULE
}).


%%%===================================================================
%%% API
%%%===================================================================

-spec get_values_map(file_id:file_guid()) -> {ok, values_map()}.
get_values_map(Guid) ->
    get_values_map(Guid, [<<"file_count">>, <<"dir_count">>, <<"size_sum">>]).


-spec get_values_map(file_id:file_guid(), [parameter()]) -> {ok, values_map()}.
get_values_map(Guid, Parameters) ->
    files_tree_gatherer_pes_executor:call(#ftg_get_request{
        guid = Guid,
        handler_module = ?MODULE,
        parameters = Parameters
    }).


-spec get_values_map_and_histogram(file_id:file_guid()) ->
    {ok, values_map(), time_series_collection:windows_map()} | {error, term()}.
get_values_map_and_histogram(Guid) ->
    files_tree_gatherer_pes_executor:flush(Guid),
    {Uuid, _SpaceId} = file_id:unpack_guid(Guid),
    parse_list_windows_ans(datastore_time_series_collection:list_windows(?CTX, Uuid, #{})).


-spec update_size(file_id:file_guid(), non_neg_integer()) -> ok.
update_size(Guid, SizeDiff) ->
    update_parameter(Guid, <<"size_sum">>, SizeDiff).


-spec increment_file_count(file_id:file_guid()) -> ok.
increment_file_count(Guid) ->
    update_parameter(Guid, <<"file_count">>, 1).


-spec decrement_file_count(file_id:file_guid()) -> ok.
decrement_file_count(Guid) ->
    update_parameter(Guid, <<"file_count">>, -1).


-spec increment_dir_count(file_id:file_guid()) -> ok.
increment_dir_count(Guid) ->
    update_parameter(Guid, <<"dir_count">>, 1).


-spec decrement_dir_count(file_id:file_guid()) -> ok.
decrement_dir_count(Guid) ->
    update_parameter(Guid, <<"dir_count">>, -1).


-spec update_parameter(file_id:file_guid(), parameter(), integer()) -> ok.
update_parameter(Guid, Parameter, Diff) ->
    files_tree_gatherer_pes_executor:call(#ftg_update_request{
        guid = Guid,
        handler_module = ?MODULE,
        diff_map = #{Parameter => Diff}
    }).


-spec delete_histogram(file_id:file_guid()) -> ok.
delete_histogram(Guid) ->
    files_tree_gatherer_pes_executor:flush(Guid),
    {Uuid, _SpaceId} = file_id:unpack_guid(Guid),
    case datastore_time_series_collection:delete(?CTX, Uuid) of
        ok -> ok;
        {error, not_found} -> ok
    end.


%%%===================================================================
%%% files_tree_gatherer_behaviour callbacks
%%%===================================================================

-spec init_cache(file_id:file_guid()) -> {ok, values_map()} | {error, term()}.
init_cache(Guid) ->
    {Uuid, _SpaceId} = file_id:unpack_guid(Guid),
    RequestRange = lists:map(fun(Parameter) -> {Parameter, <<"current">>} end, parameters()),

    case datastore_time_series_collection:list_windows(?CTX, Uuid, RequestRange, #{limit => 1}) of
        {ok, WindowsMap} ->
            WindowToValue = fun
                ({{Parameter, <<"current">>}, [{_Timestamp, Value}]}) -> {Parameter, Value};
                ({{Parameter, <<"current">>}, []}) -> {Parameter, 0}
            end,
            {ok, maps:from_list(lists:map(WindowToValue, maps:to_list(WindowsMap)))};
        {error, not_found} ->
            {ok, maps:from_list(lists:map(fun(Parameter) -> {Parameter, 0} end, parameters()))};
        Error ->
            Error
    end.


-spec merge(parameter(), parameter_value(), parameter_value()) -> parameter_value().
merge(_, Value1, Value2) ->
    Value1 + Value2.


-spec save(file_id:file_guid(), values_map()) -> ok | {error, term()}.
save(Guid, ValuesMap) ->
    {Uuid, _SpaceId} = file_id:unpack_guid(Guid),
    case datastore_time_series_collection:update(?CTX, Uuid,
        global_clock:timestamp_seconds(), maps:to_list(ValuesMap))
    of
        ok ->
            ok;
        ?ERROR_NOT_FOUND ->
            Config = maps:from_list(lists:map(fun(Parameter) -> {Parameter, metrics()} end, parameters())),
            case datastore_time_series_collection:create(?CTX, Uuid, Config) of
                ok -> save(Guid, ValuesMap);
                CreateError -> CreateError % NOTE: single pes process is dedicated for each guid so
                                           % {error, collection_already_exists} is impossible
            end;
        UpdateError -> UpdateError
    end.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec parameters() -> [parameter()].
parameters() ->
    [<<"file_count">>, <<"dir_count">>, <<"size_sum">>].


-spec metrics() -> #{ts_metric:id() => ts_metric:config()}.
metrics() -> #{
    <<"current">> => #metric_config{
        resolution = 1,
        retention = 1,
        aggregator = last
    },
    <<"minute">> => #metric_config{
        resolution = timer:minutes(1),
        retention = 120,
        aggregator = sum
    },
    <<"hour">> => #metric_config{
        resolution = timer:hours(1),
        retention = 48,
        aggregator = sum
    },
    <<"day">> => #metric_config{
        resolution = timer:hours(24),
        retention = 60,
        aggregator = sum
    },
    <<"month">> => #metric_config{
        resolution = timer:hours(24 * 30),
        retention = 12,
        aggregator = sum
    }
}.


-spec parse_list_windows_ans({ok, time_series_collection:windows_map()} | {error, term()}) ->
    {ok, values_map(), time_series_collection:windows_map()} | {error, term()}.
parse_list_windows_ans({ok, WindowsMap}) ->
    MappedWindows = maps:from_list(lists:map(fun
        ({{Parameter, <<"current">>}, [{_Timestamp, Value}]}) ->
            {Parameter, Value};
        ({{Parameter, <<"current">>}, []}) ->
            {Parameter, 0};
        ({Metric, Windows}) ->
            {Metric, lists:map(fun({Timestamp, {Count, Sum}}) -> {Timestamp, round(Sum / Count)} end, Windows)}
    end, maps:to_list(WindowsMap))),
    Parameters = parameters(),
    {ok, maps:with(Parameters, MappedWindows), maps:without(Parameters, MappedWindows)};
parse_list_windows_ans({error, not_found}) ->
    Parameters = parameters(),
    MetricIds = maps:keys(metrics()),
    Windows = maps:from_list(lists:foldl(fun(Parameter, Acc) ->
        Acc ++ lists:map(fun
            (<<"current">>) -> {Parameter, 0};
            (MetricId) -> {{Parameter, MetricId}, []}
        end, MetricIds)
    end, [], Parameters)),
    {ok, maps:with(Parameters, Windows), maps:without(Parameters, Windows)};
parse_list_windows_ans(Error) ->
    Error.