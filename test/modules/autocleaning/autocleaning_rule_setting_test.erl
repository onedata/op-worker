%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Tests of functions from autocleaning_rule_setting module.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_rule_setting_test).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT, #autocleaning_rule_setting{enabled =  false, value = 0}).
-define(DEFAULT2, #autocleaning_rule_setting{enabled =  true, value = 0}).

%%%===================================================================
%%% Tests
%%%===================================================================

autocleaning_rule_setting_to_map_test() ->
    ?assertMatch(#{
        enabled := true,
        value := 1
    }, autocleaning_rule_setting:to_map(#autocleaning_rule_setting{
        enabled = true,
        value = 1
    })).

update_auto_cleaning_rule_setting_test() ->
    ?assertEqual(#autocleaning_rule_setting{
        enabled = true,
        value = 1
    }, autocleaning_rule_setting:update(?DEFAULT, #{enabled => true, value => 1})).

enable_auto_cleaning_rule_setting_test() ->
    ?assertEqual(#autocleaning_rule_setting{
        enabled = true,
        value = 0
    }, autocleaning_rule_setting:update(?DEFAULT, #{enabled => true})).

disable_auto_cleaning_rule_setting_test() ->
    ?assertEqual(#autocleaning_rule_setting{
        enabled = false,
        value = 0
    }, autocleaning_rule_setting:update(?DEFAULT2, #{enabled => false})).

change_auto_cleaning_rule_setting_value_test() ->
    ?assertEqual(#autocleaning_rule_setting{
        enabled = false,
        value = 123
    }, autocleaning_rule_setting:update(?DEFAULT, #{value => 123})).

setting_enabled_field_to_not_boolean_should_fail_test() ->
    ?assertException(throw, ?ERROR_BAD_VALUE_BOOLEAN(<<"enabled">>),
        autocleaning_rule_setting:update(?DEFAULT, #{enabled => not_boolean})).

setting_value_field_to_not_integer_should_fail_test() ->
    ?assertException(throw, ?ERROR_BAD_VALUE_INTEGER(<<"value">>),
        autocleaning_rule_setting:update(?DEFAULT, #{value => not_integer})).

setting_value_field_to_negative_integer_should_fail_test() ->
    ?assertException(throw, ?ERROR_BAD_VALUE_TOO_LOW(<<"value">>, 0),
        autocleaning_rule_setting:update(?DEFAULT, #{value => -1})).