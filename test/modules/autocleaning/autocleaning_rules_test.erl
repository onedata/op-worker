%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Tests of functions from autocleaning_rules module.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_rules_test).
-author("Jakub Kudzia").

-include_lib("eunit/include/eunit.hrl").
-include("modules/datastore/datastore_models.hrl").

-define(SETTING(Value), ?SETTING(true, Value)).
-define(SETTING(Enabled, Value),
    #autocleaning_rule_setting{enabled = Enabled, value = Value}).

-define(RULES_RECORD, #autocleaning_rules{
    enabled = true,
    min_file_size = ?SETTING(1),
    max_file_size = ?SETTING(2),
    min_hours_since_last_open = ?SETTING(3),
    max_open_count = ?SETTING(4),
    max_hourly_moving_average = ?SETTING(5),
    max_daily_moving_average = ?SETTING(6),
    max_monthly_moving_average = ?SETTING(7)
}).

-define(RULES_RECORD2, #autocleaning_rules{
    enabled = true,
    min_file_size = ?SETTING(10),
    max_file_size = ?SETTING(20),
    min_hours_since_last_open = ?SETTING(30),
    max_open_count = ?SETTING(40),
    max_hourly_moving_average = ?SETTING(50),
    max_daily_moving_average = ?SETTING(60),
    max_monthly_moving_average = ?SETTING(70)
}).


-define(SETTING_MAP(Value), ?SETTING_MAP(true, Value)).
-define(SETTING_MAP(Enabled, Value), #{enabled => Enabled, value => Value}).

-define(RULES_MAP,
    #{
        enabled => true,
        min_file_size => ?SETTING_MAP(1),
        max_file_size => ?SETTING_MAP(2),
        min_hours_since_last_open => ?SETTING_MAP(3),
        max_open_count => ?SETTING_MAP(4),
        max_hourly_moving_average => ?SETTING_MAP(5),
        max_daily_moving_average => ?SETTING_MAP(6),
        max_monthly_moving_average => ?SETTING_MAP(7)
    }
).

-define(RULES_MAP2,
    #{
        enabled => true,
        min_file_size => ?SETTING_MAP(10),
        max_file_size => ?SETTING_MAP(20),
        min_hours_since_last_open => ?SETTING_MAP(30),
        max_open_count => ?SETTING_MAP(40),
        max_hourly_moving_average => ?SETTING_MAP(50),
        max_daily_moving_average => ?SETTING_MAP(60),
        max_monthly_moving_average => ?SETTING_MAP(70)
    }
).

%%%===================================================================
%%% Tests
%%%===================================================================

autocleaning_rules_to_map_test() ->
    ?assertEqual(#{
        enabled => true,
        min_file_size => #{enabled => true, value => 1},
        max_file_size => #{enabled => true, value => 2},
        min_hours_since_last_open => #{enabled => true, value => 3},
        max_open_count => #{enabled => true, value => 4},
        max_hourly_moving_average => #{enabled => true, value => 5},
        max_daily_moving_average => #{enabled => true, value => 6},
        max_monthly_moving_average => #{enabled => true, value => 7}
    }, autocleaning_rules:to_map(?RULES_RECORD)).

autocleaning_rules_update_undefined_test() ->
    ?assertEqual(?RULES_RECORD, autocleaning_rules:update(undefined, ?RULES_MAP)).

autocleaning_rules_update_test() ->
    ?assertEqual(?RULES_RECORD2, autocleaning_rules:update(?RULES_RECORD, ?RULES_MAP2)).

enable_rules_test() ->
    ?assertMatch(#autocleaning_rules{enabled = true}, autocleaning_rules:update(undefined, #{enabled => true})).

disable_rules_test() ->
    ?assertMatch(#autocleaning_rules{enabled = false}, autocleaning_rules:update(undefined, #{disabled => true})).

enable_just_one_rule_test() ->
    lists:foreach(fun(RuleName) ->
        enable_just_one_rule_test_helper(RuleName)
    end, list_rules()).

disable_just_one_setting_test() ->
    lists:foreach(fun(RuleName) ->
        disable_just_one_rule_test_helper(RuleName)
    end, list_rules()).

setting_enabled_field_to_not_boolean_should_throw_illegal_type_exception_test() ->
    lists:foreach(fun(FieldName) ->
        setting_rule_enabled_field_to_not_boolean_should_throw_illegal_type_exception_test_helper(FieldName)
    end, list_rules()).

setting_field_to_not_integer_should_throw_illegal_type_exception_test() ->
    lists:foreach(fun(FieldName) ->
        setting_rule_value_to_not_integer_should_throw_illegal_type_exception_test_helper(FieldName)
    end, list_rules()).

setting_rule_value_to_negative_integer_should_throw_negative_rule_setting_exception_test() ->
    lists:foreach(fun(FieldName) ->
        setting_rule_value_to_negative_integer_should_throw_negative_rule_setting_exception_test_helper(FieldName)
    end, list_rules()).

%%%===================================================================
%%% Internal functions
%%%===================================================================

disable_just_one_rule_test_helper(FieldToDisable) ->
    UpdatedRules = autocleaning_rules:update(?RULES_RECORD, #{
        FieldToDisable => #{enabled => false}
    }),

    lists:foreach(fun(FieldName) ->
        case FieldName =:= FieldToDisable of
            true ->
                ?assertMatch(#autocleaning_rule_setting{enabled = false},
                    get_field(UpdatedRules, FieldToDisable));
            _ ->
                ?assertMatch(#autocleaning_rule_setting{enabled = true},
                    get_field(UpdatedRules, FieldName))
        end
    end, list_rules()).

enable_just_one_rule_test_helper(FieldToEnable) ->
    UpdatedRules = autocleaning_rules:update(undefined, #{
        FieldToEnable => #{enabled => true}
    }),

    lists:foreach(fun(FieldName) ->
        case FieldName =:= FieldToEnable of
            true ->
                ?assertMatch(#autocleaning_rule_setting{enabled = true},
                    get_field(UpdatedRules, FieldToEnable));
            _ ->
                ?assertMatch(#autocleaning_rule_setting{enabled = false},
                    get_field(UpdatedRules, FieldName))
        end
    end, list_rules()).

setting_rule_enabled_field_to_not_boolean_should_throw_illegal_type_exception_test_helper(FieldToUpdate) ->
    ?assertException(throw, {illegal_type, FieldToUpdate},
        autocleaning_rules:update(undefined, #{FieldToUpdate => #{enabled => not_boolean}})).

setting_rule_value_to_not_integer_should_throw_illegal_type_exception_test_helper(FieldToUpdate) ->
    ?assertException(throw, {illegal_type, FieldToUpdate},
        autocleaning_rules:update(undefined, #{FieldToUpdate => #{value => not_integer}})).

setting_rule_value_to_negative_integer_should_throw_negative_rule_setting_exception_test_helper(FieldToUpdate) ->
    ?assertException(throw, {negative_value, FieldToUpdate},
        autocleaning_rules:update(undefined, #{FieldToUpdate => #{value => -1}})).


list_rules() ->
     list_fields() -- [enabled].

list_fields() ->
    record_info(fields, autocleaning_rules).

get_field(Record, FieldName) ->
    FieldsList = list_fields(),
    Index = index(FieldName, FieldsList),
    element(Index + 1, Record).

index(Key, List) ->
    {Index, _} = lists:keyfind(Key, 2, lists:zip(lists:seq(1, length(List)), List)),
    Index.
