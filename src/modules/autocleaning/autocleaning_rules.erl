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


-type rule_name() :: lower_file_size_limit | upper_file_size_limit |
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
    lower_file_size_limit => ?DEFAULT_LOWER_SIZE_LIMIT,
    upper_file_size_limit => ?DEFAULT_UPPER_SIZE_LIMIT,
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
    lower_file_size_limit = LowerFileSizeLimit,
    upper_file_size_limit = UpperFileSizeLimit,
    min_hours_since_last_open = MinHoursSinceLastOpen,
    max_open_count = MaxOpenCount,
    max_hourly_moving_average = MaxHourlyMovingAverage,
    max_daily_moving_average = MaxDailyMovingAverage,
    max_monthly_moving_average = MaxMonthlyMovingAverage
}) ->
    #{
        enabled => Enabled,
        lower_file_size_limit => setting_to_map(LowerFileSizeLimit),
        upper_file_size_limit => setting_to_map(UpperFileSizeLimit),
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
    lower_file_size_limit = LowerFileSizeLimit,
    upper_file_size_limit = UpperFileSizeLimit,
    min_hours_since_last_open = MinHoursSinceLastOpen,
    max_open_count = MaxOpenCount,
    max_hourly_moving_average = MaxHourlyMovingAverage,
    max_daily_moving_average = MaxDailyMovingAverage,
    max_monthly_moving_average = MaxMonthlyMovingAverage
}, UpdateRulesMap) ->
    NewEnabled = autocleaning_utils:get_defined(enabled, UpdateRulesMap, Enabled),
    #autocleaning_rules{
        enabled = autocleaning_utils:assert_boolean(NewEnabled, enabled),
        lower_file_size_limit =
            update_setting(lower_file_size_limit, LowerFileSizeLimit, UpdateRulesMap),
        upper_file_size_limit =
            update_setting(upper_file_size_limit, UpperFileSizeLimit, UpdateRulesMap),
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
        lower_file_size_limit = default(?DEFAULT_LOWER_SIZE_LIMIT),
        upper_file_size_limit = default(?DEFAULT_UPPER_SIZE_LIMIT),
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
    lower_file_size_limit = LowerFileSizeLimitSetting,
    upper_file_size_limit = UpperFileSizeLimitSetting,
    min_hours_since_last_open = MinHoursSinceLastOpenSetting,
    max_hourly_moving_average = MaxHourlyMovingAvgSetting,
    max_daily_moving_average = MaxDailyMovingAvgSetting,
    max_monthly_moving_average = MaxMonthlyMovingAvgSetting
}) ->
    ?critical("DUPA0AAAA"),
    Uuid = file_ctx:get_uuid_const(FileCtx),
    {ok, #document{value=FilePopularity}} = file_popularity:get(Uuid),
    begin
    ?critical("DUPA1"),
    is_max_open_count_rule_satisfied(FilePopularity, MaxOpenCountSetting)
    end
    andalso
    begin
    ?critical("DUPA2"),
    is_lower_file_size_limit_rule_satisfied(FilePopularity, LowerFileSizeLimitSetting)
    end
    andalso
    begin
    ?critical("DUPA3"),
    is_upper_file_size_limit_rule_satisfied(FilePopularity, UpperFileSizeLimitSetting)
    end
    andalso
    begin
    ?critical("DUPA4"),
    is_min_hours_since_last_open_rule_satisfied(FilePopularity, MinHoursSinceLastOpenSetting)
    end
    andalso
    begin
    ?critical("DUPA5"),
    is_max_hourly_moving_average_rule_satisfied(FilePopularity, MaxHourlyMovingAvgSetting)
    end
    andalso
    begin
    ?critical("DUPA6"),
    is_max_daily_moving_average_rule_satisfied(FilePopularity, MaxDailyMovingAvgSetting)
    end
    andalso
    begin
    ?critical("DUPA7"),
    is_max_monthly_moving_average_rule_satisfied(FilePopularity, MaxMonthlyMovingAvgSetting)
    end.

%%-------------------------------------------------------------------
%% @doc
%% Returns JSON encoded start_key understandable by the
%% file_popularity_view. The key is constructed basing on
%% the #autocleaning_rules{} record.
%% @end
%%-------------------------------------------------------------------
-spec to_file_popularity_start_key(rules()) -> any().
to_file_popularity_start_key(#autocleaning_rules{enabled = false}) ->
    to_file_popularity_start_key(
        maps:get(max_open_count, ?DEFAULTS),
        maps:get(min_hours_since_last_open, ?DEFAULTS),
        maps:get(upper_file_size_limit, ?DEFAULTS),
        maps:get(max_hourly_moving_average, ?DEFAULTS),
        maps:get(max_daily_moving_average, ?DEFAULTS),
        maps:get(max_monthly_moving_average, ?DEFAULTS)
    );
to_file_popularity_start_key(#autocleaning_rules{
    enabled = true,
    max_open_count = MaxOpenCountSetting,
    upper_file_size_limit = UpperFileSizeLimitSetting,
    min_hours_since_last_open = MinHoursSinceLastOpenSetting,
    max_hourly_moving_average = MaxHourlyMovingAvgSetting,
    max_daily_moving_average = MaxDailyMovingAvgSetting,
    max_monthly_moving_average = MaxMonthlyMovingAvgSetting
}) ->
    to_file_popularity_start_key(
      get_value(max_open_count, MaxOpenCountSetting),
      get_value(upper_file_size_limit, UpperFileSizeLimitSetting),
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
-spec to_file_popularity_end_key(rules()) -> binary().
to_file_popularity_end_key(#autocleaning_rules{enabled = false}) ->
    to_file_popularity_key(0, 0, 0, 0, 0, 0);
to_file_popularity_end_key(#autocleaning_rules{enabled = true,
    lower_file_size_limit = LowerFileSizeLimitSetting
}) ->
    LowerFileSizeLimit = get_value(lower_file_size_limit, LowerFileSizeLimitSetting),
    to_file_popularity_key(0, 0, LowerFileSizeLimit, 0, 0, 0).

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

-spec is_lower_file_size_limit_rule_satisfied(file_popularity:record(),
    rule_setting()) -> boolean().
is_lower_file_size_limit_rule_satisfied(#file_popularity{size = Size},
    RuleSetting
) ->
    ?critical("Size: ~p", [Size]),
    ?critical("RuleSetting: ~p", [RuleSetting]),

    autocleaning_rule_setting:is_greater_or_equal(Size, RuleSetting).

-spec is_upper_file_size_limit_rule_satisfied(file_popularity:record(),
    rule_setting()) -> boolean().
is_upper_file_size_limit_rule_satisfied(#file_popularity{size = Size},
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
    UpperFileSizeLimit, MaxHourlyMovingAvg, MaxDailyMovingAvg, MaxMonthlyMovingAvg
) ->
    CurrentTimeInHours = time_utils:cluster_time_seconds() div 3600,
    to_file_popularity_key(
        MaxOpenCount,
        CurrentTimeInHours - MinHoursSinceLastOpen,
        UpperFileSizeLimit,
        MaxHourlyMovingAvg,
        MaxDailyMovingAvg,
        MaxMonthlyMovingAvg
    ).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns JSON encoded key understandable by the file_popularity_view.
%% @end
%%-------------------------------------------------------------------
-spec to_file_popularity_key(non_neg_integer(), non_neg_integer(),
    non_neg_integer(), non_neg_integer(), non_neg_integer(),
    non_neg_integer()) -> binary().
to_file_popularity_key(OpenCount, LastOpen, Size, HourlyMovingAvg,
    DailyMovingAvg, MonthlyMovingAvg
) ->
    json_utils:encode([OpenCount, LastOpen, Size, HourlyMovingAvg, DailyMovingAvg,
        MonthlyMovingAvg
    ]).