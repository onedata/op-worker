%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% WRITEME
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_config_test).
-author("Jakub Kudzia").

-include_lib("eunit/include/eunit.hrl").
-include("modules/datastore/datastore_models.hrl").

-define(SETTING(Value), ?SETTING(true, Value)).
-define(SETTING(Enabled, Value),
    #autocleaning_rule_setting{enabled = Enabled, value = Value}).

-define(CONFIG_RECORD,
    #autocleaning_config{
        enabled = true,
        target = 0,
        threshold = 1,
        rules = #autocleaning_rules{
            enabled = true,
            lower_file_size_limit = ?SETTING(2),
            upper_file_size_limit = ?SETTING(3),
            min_hours_since_last_open = ?SETTING(4),
            max_open_count = ?SETTING(5),
            max_hourly_moving_average = ?SETTING(6),
            max_daily_moving_average = ?SETTING(7),
            max_monthly_moving_average = ?SETTING(8)
}}).

-define(CONFIG_RECORD2,
    #autocleaning_config{
        enabled = true,
        target = 10,
        threshold = 100,
        rules = #autocleaning_rules{
            enabled = true,
            lower_file_size_limit = ?SETTING(20),
            upper_file_size_limit = ?SETTING(30),
            min_hours_since_last_open = ?SETTING(40),
            max_open_count = ?SETTING(50),
            max_hourly_moving_average = ?SETTING(60),
            max_daily_moving_average = ?SETTING(70),
            max_monthly_moving_average = ?SETTING(80)
}}).

-define(CONFIG_WITH_DISABLED_RULES_RECORD, 
    #autocleaning_config{
        enabled = true,
        target = 0,
        threshold = 1,
        rules = #autocleaning_rules{
            enabled = false
        }
}).

-define(SETTING_MAP(Value), ?SETTING_MAP(true, Value)).
-define(SETTING_MAP(Enabled, Value), #{enabled => Enabled, value => Value}).

-define(CONFIG_MAP,
    #{
        enabled => true,
        target => 0,
        threshold => 1,
        rules => #{
            enabled => true,
            lower_file_size_limit => ?SETTING_MAP(2),
            upper_file_size_limit => ?SETTING_MAP(3),
            min_hours_since_last_open => ?SETTING_MAP(4),
            max_open_count => ?SETTING_MAP(5),
            max_hourly_moving_average => ?SETTING_MAP(6),
            max_daily_moving_average => ?SETTING_MAP(7),
            max_monthly_moving_average => ?SETTING_MAP(8)
    }}).

-define(CONFIG_MAP2,
    #{
        enabled => true,
        target => 10,
        threshold => 100,
        rules => #{
            enabled => true,
            lower_file_size_limit => ?SETTING_MAP(20),
            upper_file_size_limit => ?SETTING_MAP(30),
            min_hours_since_last_open => ?SETTING_MAP(40),
            max_open_count => ?SETTING_MAP(50),
            max_hourly_moving_average => ?SETTING_MAP(60),
            max_daily_moving_average => ?SETTING_MAP(70),
            max_monthly_moving_average => ?SETTING_MAP(80)
        }}).

%%%===================================================================
%%% Test generators
%%%===================================================================

autocleaning_config_to_map_test() ->
    ?assertEqual(#{
        enabled => true,
        target => 0,
        threshold => 1,
        rules => #{
            enabled => true,
            lower_file_size_limit => #{enabled => true, value => 2},
            upper_file_size_limit => #{enabled => true, value => 3},
            min_hours_since_last_open => #{enabled => true, value => 4},
            max_open_count => #{enabled => true, value => 5},
            max_hourly_moving_average => #{enabled => true, value => 6},
            max_daily_moving_average => #{enabled => true, value => 7},
            max_monthly_moving_average => #{enabled => true, value => 8}
    }}, autocleaning_config:to_map(?CONFIG_RECORD)).

autocleaning_configure_undefined_test() ->
    ?assertEqual({ok, ?CONFIG_RECORD},
        autocleaning_config:configure(undefined, ?CONFIG_MAP, 10)).

autocleaning_configure_test() ->
    ?assertEqual({ok, ?CONFIG_RECORD2},
        autocleaning_config:configure(?CONFIG_RECORD, ?CONFIG_MAP2, 1000)).

all_rules_should_be_disabled_by_default_test() ->
    SupportSize = 10,
    ?assertMatch({ok, #autocleaning_config{
        enabled = true,
        target = 0,
        threshold = 1,
        rules = #autocleaning_rules{
            enabled = false,
            lower_file_size_limit = #autocleaning_rule_setting{enabled = false},
            upper_file_size_limit = #autocleaning_rule_setting{enabled = false},
            min_hours_since_last_open = #autocleaning_rule_setting{enabled = false},
            max_open_count = #autocleaning_rule_setting{enabled = false},
            max_hourly_moving_average = #autocleaning_rule_setting{enabled = false},
            max_daily_moving_average = #autocleaning_rule_setting{enabled = false},
            max_monthly_moving_average = #autocleaning_rule_setting{enabled = false}
        }
    }}, autocleaning_config:configure(undefined, #{
        enabled => true,
        target => 0,
        threshold => 1
    }, SupportSize)).

only_explicitly_passed_params_are_changed_test() ->
    SupportSize = 100,
    ?assertEqual({ok, #autocleaning_config{
        enabled = false,
        target = 0,
        threshold = 50,
        rules = #autocleaning_rules{
            enabled = true,
            lower_file_size_limit = #autocleaning_rule_setting{enabled = true, value = 2},
            upper_file_size_limit = #autocleaning_rule_setting{enabled = true, value = 3},
            min_hours_since_last_open = #autocleaning_rule_setting{enabled = true, value = 4},
            max_open_count = #autocleaning_rule_setting{enabled = true, value = 5},
            max_hourly_moving_average = #autocleaning_rule_setting{enabled = true, value = 6},
            max_daily_moving_average = #autocleaning_rule_setting{enabled = true, value = 7},
            max_monthly_moving_average = #autocleaning_rule_setting{enabled = true, value = 8}
        }
    }}, autocleaning_config:configure(?CONFIG_RECORD, #{
        enabled => false,
        threshold => 50
    }, SupportSize)).

configuring_only_enable_param_for_the_first_time_should_set_target_and_threshold_to_support_size_test() ->
    SupportSize = 10,
    ?assertMatch({ok, #autocleaning_config{
        enabled = true,
        target = SupportSize,
        threshold = SupportSize
    }}, autocleaning_config:configure(undefined, #{enabled => true}, SupportSize)).

configuring_only_enable_param_should_leave_target_and_threshold_values_unchanged_test() ->
    ?assertEqual({ok, ?CONFIG_RECORD#autocleaning_config{
        enabled = false,
        target = 0,
        threshold = 1
    }}, autocleaning_config:configure(?CONFIG_RECORD, #{enabled => false}, 10)).

setting_enabled_to_not_boolean_value_should_throw_illegal_type_exception_test() ->
    ?assertEqual({error, {illegal_type, enabled}},
        autocleaning_config:configure(?CONFIG_RECORD, #{enabled => not_boolean}, 10)).

setting_target_to_not_integer_value_should_throw_illegal_type_exception_test() ->
    ?assertEqual({error, {illegal_type, target}},
        autocleaning_config:configure(?CONFIG_RECORD, #{target => not_integer}, 10)).

setting_threshold_to_not_integer_value_should_throw_illegal_type_exception_test() ->
    ?assertEqual({error, {illegal_type, threshold}},
        autocleaning_config:configure(?CONFIG_RECORD, #{threshold => not_integer}, 10)).

setting_target_negative_integer_value_should_throw_negative_value_exception_test() ->
    ?assertEqual({error, {negative_value, target}},
        autocleaning_config:configure(?CONFIG_RECORD, #{target => -1}, 10)).

setting_threshold_negative_integer_value_should_throw_negative_value_exception_test() ->
    ?assertEqual({error, {negative_value, threshold}},
        autocleaning_config:configure(?CONFIG_RECORD, #{threshold => -1}, 10)).

setting_target_greater_than_threshold_should_throw_value_greater_than_exception_test() ->
    ?assertEqual({error, {value_grater_than, target, threshold}},
        autocleaning_config:configure(?CONFIG_RECORD, #{target => 1, threshold => 0}, 10)).

setting_threshold_greater_than_support_size_should_throw_value_greater_than_exception_test() ->
    ?assertEqual({error, {value_grater_than, threshold, support_size}},
        autocleaning_config:configure(?CONFIG_RECORD, #{threshold => 11}, 10)).

setting_rule_to_negative_value_should_return_negative_value_error_test() ->
    ?assertEqual({error, {negative_value, lower_file_size_limit}},
        autocleaning_config:configure(?CONFIG_RECORD, #{
            rules => #{
                lower_file_size_limit => #{value => -1}
        }}, 10)
    ).

