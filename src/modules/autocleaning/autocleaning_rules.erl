%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Helper module for operating on #autocleaning_rules{} record.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_rules).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").


-type rule_name() :: min_file_size | max_file_size |
                     min_hours_since_last_open | max_open_count |
                     max_hourly_moving_average | max_daily_moving_average |
                     max_monthly_moving_average.
-type rule_setting() :: autocleaning_rule_setting:rule_setting().
-type rule_setting_value() :: autocleaning_rule_setting:value().
-type rules() :: #autocleaning_rules{}.

-export_type([rule_setting/0, rules/0]).

%% API
-export([to_map/1, update/2, default/0, are_rules_satisfied/2,
    to_file_popularity_start_key/1, to_file_popularity_end_key/1]).

%%defaults
-define(DEFAULT_LOWER_SIZE_LIMIT, 1).
-define(DEFAULT_UPPER_SIZE_LIMIT, 1125899906842624). % 1 PiB
-define(DEFAULT_MIN_HOURS_SINCE_LAST_OPEN, 0).
-define(DEFAULT_MAX_OPEN_COUNT, 9007199254740991). % 2 ^ 53 - 1
-define(DEFAULT_MAX_HOURLY_MOVING_AVG, 9007199254740991).  % 2 ^ 53 - 1
-define(DEFAULT_MAX_DAILY_MOVING_AVG, 9007199254740991).   % 2 ^ 53 - 1
-define(DEFAULT_MAX_MONTHLY_MOVING_AVG, 9007199254740991). % 2 ^ 53 - 1

-define(DEFAULTS, #{
    min_file_size => ?DEFAULT_LOWER_SIZE_LIMIT,
    max_file_size => ?DEFAULT_UPPER_SIZE_LIMIT,
    min_hours_since_last_open => ?DEFAULT_MIN_HOURS_SINCE_LAST_OPEN,
    max_open_count => ?DEFAULT_MAX_OPEN_COUNT,
    max_hourly_moving_average => ?DEFAULT_MAX_HOURLY_MOVING_AVG,
    max_daily_moving_average => ?DEFAULT_MAX_DAILY_MOVING_AVG,
    max_monthly_moving_average => ?DEFAULT_MAX_MONTHLY_MOVING_AVG
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec to_map(rules()) -> maps:map().
to_map(#autocleaning_rules{
    enabled = Enabled,
    min_file_size = MinFileSize,
    max_file_size = MaxFileSize,
    min_hours_since_last_open = MinHoursSinceLastOpen,
    max_open_count = MaxOpenCount,
    max_hourly_moving_average = MaxHourlyMovingAverage,
    max_daily_moving_average = MaxDailyMovingAverage,
    max_monthly_moving_average = MaxMonthlyMovingAverage
}) ->
    #{
        enabled => Enabled,
        min_file_size => setting_to_map(MinFileSize),
        max_file_size => setting_to_map(MaxFileSize),
        min_hours_since_last_open => setting_to_map(MinHoursSinceLastOpen),
        max_open_count => setting_to_map(MaxOpenCount),
        max_hourly_moving_average => setting_to_map(MaxHourlyMovingAverage),
        max_daily_moving_average => setting_to_map(MaxDailyMovingAverage),
        max_monthly_moving_average => setting_to_map(MaxMonthlyMovingAverage)
    }.

-spec update(rules(), maps:map()) -> rules().
update(undefined, UpdateRulesMap) ->
    update(default(), UpdateRulesMap);
update(#autocleaning_rules{
    enabled = Enabled,
    min_file_size = MinFileSize,
    max_file_size = MaxFileSize,
    min_hours_since_last_open = MinHoursSinceLastOpen,
    max_open_count = MaxOpenCount,
    max_hourly_moving_average = MaxHourlyMovingAverage,
    max_daily_moving_average = MaxDailyMovingAverage,
    max_monthly_moving_average = MaxMonthlyMovingAverage
}, UpdateRulesMap) ->
    NewEnabled = autocleaning_utils:get_defined(enabled, UpdateRulesMap, Enabled),
    #autocleaning_rules{
        enabled = autocleaning_utils:assert_boolean(NewEnabled, enabled),
        min_file_size =
            update_setting(min_file_size, MinFileSize, UpdateRulesMap),
        max_file_size =
            update_setting(max_file_size, MaxFileSize, UpdateRulesMap),
        min_hours_since_last_open =
            update_setting(min_hours_since_last_open, MinHoursSinceLastOpen, UpdateRulesMap),
        max_open_count =
            update_setting(max_open_count, MaxOpenCount, UpdateRulesMap),
        max_hourly_moving_average =
            update_setting(max_hourly_moving_average, MaxHourlyMovingAverage, UpdateRulesMap),
        max_daily_moving_average =
            update_setting(max_daily_moving_average, MaxDailyMovingAverage, UpdateRulesMap),
        max_monthly_moving_average =
            update_setting(max_monthly_moving_average, MaxMonthlyMovingAverage, UpdateRulesMap)
    }.


-spec default() -> rules().
default() ->
    #autocleaning_rules{
        enabled = false,
        min_file_size = default(?DEFAULT_LOWER_SIZE_LIMIT),
        max_file_size = default(?DEFAULT_UPPER_SIZE_LIMIT),
        min_hours_since_last_open = default(?DEFAULT_MIN_HOURS_SINCE_LAST_OPEN),
        max_open_count = default(?DEFAULT_MAX_OPEN_COUNT),
        max_hourly_moving_average = default(?DEFAULT_MAX_HOURLY_MOVING_AVG),
        max_daily_moving_average = default(?DEFAULT_MAX_DAILY_MOVING_AVG),
        max_monthly_moving_average = default(?DEFAULT_MAX_MONTHLY_MOVING_AVG)
    }.

-spec are_rules_satisfied(file_ctx:ctx(), rules()) -> boolean().
are_rules_satisfied(_FileCtx, #autocleaning_rules{enabled = false}) ->
    true;
are_rules_satisfied(FileCtx, #autocleaning_rules{
    enabled = true,
    max_open_count = MaxOpenCountSetting,
    min_file_size = MinFileSizeSetting,
    max_file_size = MaxFileSizeSetting,
    min_hours_since_last_open = MinHoursSinceLastOpenSetting,
    max_hourly_moving_average = MaxHourlyMovingAvgSetting,
    max_daily_moving_average = MaxDailyMovingAvgSetting,
    max_monthly_moving_average = MaxMonthlyMovingAvgSetting
}) ->
    Uuid = file_ctx:get_uuid_const(FileCtx),
    {ok, #document{value=FilePopularity}} = file_popularity:get(Uuid),
    is_max_open_count_rule_satisfied(FilePopularity, MaxOpenCountSetting)
    andalso is_min_file_size_rule_satisfied(FilePopularity, MinFileSizeSetting)
    andalso is_max_file_size_rule_satisfied(FilePopularity, MaxFileSizeSetting)
    andalso is_min_hours_since_last_open_rule_satisfied(FilePopularity, MinHoursSinceLastOpenSetting)
    andalso is_max_hourly_moving_average_rule_satisfied(FilePopularity, MaxHourlyMovingAvgSetting)
    andalso is_max_daily_moving_average_rule_satisfied(FilePopularity, MaxDailyMovingAvgSetting)
    andalso is_max_monthly_moving_average_rule_satisfied(FilePopularity, MaxMonthlyMovingAvgSetting).

%%-------------------------------------------------------------------
%% @doc
%% Returns JSON encoded start_key understandable by the
%% file_popularity_view. The key is constructed basing on
%% the #autocleaning_rules{} record.
%% @end
%%-------------------------------------------------------------------
-spec to_file_popularity_start_key(rules()) -> [non_neg_integer()].
to_file_popularity_start_key(#autocleaning_rules{enabled = false}) ->
    to_file_popularity_start_key(
        maps:get(max_open_count, ?DEFAULTS),
        maps:get(min_hours_since_last_open, ?DEFAULTS),
        maps:get(max_file_size, ?DEFAULTS),
        maps:get(max_hourly_moving_average, ?DEFAULTS),
        maps:get(max_daily_moving_average, ?DEFAULTS),
        maps:get(max_monthly_moving_average, ?DEFAULTS)
    );
to_file_popularity_start_key(#autocleaning_rules{
    enabled = true,
    max_open_count = MaxOpenCountSetting,
    max_file_size = MaxFileSizeSetting,
    min_hours_since_last_open = MinHoursSinceLastOpenSetting,
    max_hourly_moving_average = MaxHourlyMovingAvgSetting,
    max_daily_moving_average = MaxDailyMovingAvgSetting,
    max_monthly_moving_average = MaxMonthlyMovingAvgSetting
}) ->
    to_file_popularity_start_key(
      get_value(max_open_count, MaxOpenCountSetting),
      get_value(max_file_size, MaxFileSizeSetting),
      get_value(min_hours_since_last_open, MinHoursSinceLastOpenSetting),
      get_value(max_hourly_moving_average, MaxHourlyMovingAvgSetting),
      get_value(max_daily_moving_average, MaxDailyMovingAvgSetting),
      get_value(max_monthly_moving_average, MaxMonthlyMovingAvgSetting)
    ).

%%-------------------------------------------------------------------
%% @doc
%% Returns JSON encoded end_key understandable by the
%% file_popularity_view. The key is constructed basing on
%% the #autocleaning_rules{} record.
%% @end
%%-------------------------------------------------------------------
-spec to_file_popularity_end_key(rules()) -> [non_neg_integer()].
to_file_popularity_end_key(#autocleaning_rules{enabled = false}) ->
    [0, 0, 0, 0, 0, 0];
to_file_popularity_end_key(#autocleaning_rules{enabled = true,
    min_file_size = MinFileSizeSetting
}) ->
    MinFileSize = get_value(min_file_size, MinFileSizeSetting),
    [0, 0, MinFileSize, 0, 0, 0].

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec setting_to_map(rule_setting()) -> maps:map().
setting_to_map(RuleSetting) ->
    autocleaning_rule_setting:to_map(RuleSetting).

-spec update_setting(atom(), rule_setting(), maps:map()) -> rule_setting().
update_setting(RuleName, CurrentSetting, UpdateRulesMap) ->
    try
        update_setting(CurrentSetting, get_update_setting_map(RuleName, UpdateRulesMap))
    catch
        throw:Reason ->
            throw({Reason, RuleName})
    end.

-spec update_setting(rule_setting(), maps:map()) -> rule_setting().
update_setting(CurrentSetting, UpdateSettingMap) ->
    autocleaning_rule_setting:update(CurrentSetting, UpdateSettingMap).

-spec get_update_setting_map(atom(), maps:map()) -> maps:map().
get_update_setting_map(RuleName, UpdateRulesMap) ->
    autocleaning_utils:get_defined(RuleName, UpdateRulesMap, #{}).


-spec default(non_neg_integer()) -> rule_setting().
default(Value) ->
    #autocleaning_rule_setting{value = Value}.

-spec get_value(rule_name(), rule_setting()) -> rule_setting_value().
get_value(RuleName, #autocleaning_rule_setting{enabled = false}) ->
    maps:get(RuleName, ?DEFAULTS);
get_value(_RuleName, #autocleaning_rule_setting{value = Value}) ->
    Value.

-spec is_max_open_count_rule_satisfied(file_popularity:record(),
    rule_setting()) -> boolean().
is_max_open_count_rule_satisfied(#file_popularity{open_count = OpenCount},
    RuleSetting
) ->
    autocleaning_rule_setting:is_less_or_equal(OpenCount, RuleSetting).

-spec is_min_file_size_rule_satisfied(file_popularity:record(),
    rule_setting()) -> boolean().
is_min_file_size_rule_satisfied(#file_popularity{size = Size},
    RuleSetting
) ->
    autocleaning_rule_setting:is_greater_or_equal(Size, RuleSetting).

-spec is_max_file_size_rule_satisfied(file_popularity:record(),
    rule_setting()) -> boolean().
is_max_file_size_rule_satisfied(#file_popularity{size = Size},
    RuleSetting
) ->
    autocleaning_rule_setting:is_less_or_equal(Size, RuleSetting).

-spec is_min_hours_since_last_open_rule_satisfied(file_popularity:record(),
    rule_setting()) -> boolean().
is_min_hours_since_last_open_rule_satisfied(#file_popularity{last_open = LastOpen},
    RuleSetting
) ->
    CurrentTimeInHours = time_utils:cluster_time_seconds() div 3600,
    autocleaning_rule_setting:is_greater_or_equal(CurrentTimeInHours - LastOpen,
        RuleSetting).

-spec is_max_hourly_moving_average_rule_satisfied(file_popularity:record(),
    rule_setting()) -> boolean().
is_max_hourly_moving_average_rule_satisfied(#file_popularity{hr_mov_avg = HrMovAvg},
    RuleSetting
) ->
    autocleaning_rule_setting:is_less_or_equal(HrMovAvg, RuleSetting).

-spec is_max_daily_moving_average_rule_satisfied(file_popularity:record(),
    rule_setting()) -> boolean().
is_max_daily_moving_average_rule_satisfied(#file_popularity{dy_mov_avg = DyMovAvg},
    RuleSetting
) ->
    autocleaning_rule_setting:is_less_or_equal(DyMovAvg, RuleSetting).

-spec is_max_monthly_moving_average_rule_satisfied(file_popularity:record(),
    rule_setting()) -> boolean().
is_max_monthly_moving_average_rule_satisfied(#file_popularity{mth_mov_avg = MthMovAvg},
    RuleSetting
) ->
    autocleaning_rule_setting:is_less_or_equal(MthMovAvg, RuleSetting).

-spec to_file_popularity_start_key(non_neg_integer(), non_neg_integer(),
    non_neg_integer(), non_neg_integer(), non_neg_integer(),
    non_neg_integer()) -> binary().
to_file_popularity_start_key(MaxOpenCount, MinHoursSinceLastOpen,
    MaxFileSize, MaxHourlyMovingAvg, MaxDailyMovingAvg, MaxMonthlyMovingAvg
) ->
    CurrentTimeInHours = time_utils:cluster_time_seconds() div 3600,
    [
        MaxOpenCount,
        CurrentTimeInHours - MinHoursSinceLastOpen,
        MaxFileSize,
        MaxHourlyMovingAvg,
        MaxDailyMovingAvg,
        MaxMonthlyMovingAvg
    ].