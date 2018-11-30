%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Helper module for operating on #autocleaning_rule_setting{} record.
%%% @end
%%%-------------------------------------------------------------------
-module(autocleaning_rule_setting).
-author("Jakub Kudzia").

-include("modules/datastore/datastore_models.hrl").


-type rule_setting() :: #autocleaning_rule_setting{}.
-type key() :: enabled | value.
-type value() :: boolean() | non_neg_integer().
-type rule_setting_map() :: maps:map(key(), value()).

-export_type([rule_setting/0, rule_setting_map/0, value/0]).

%% API
-export([is_enabled/1, get_value/1, to_map/1, update/2,
    is_greater_or_equal/2, is_less_or_equal/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec is_enabled(rule_setting() | rule_setting_map()) -> boolean().
is_enabled(#autocleaning_rule_setting{enabled = Enabled}) -> Enabled;
is_enabled(#{enabled := Enabled}) -> Enabled.

-spec get_value(rule_setting() | rule_setting_map()) -> non_neg_integer().
get_value(#autocleaning_rule_setting{value = Value}) -> Value;
get_value(#{value := Value}) -> Value.

-spec to_map(rule_setting()) -> maps:map().
to_map(#autocleaning_rule_setting{
    enabled = Enabled,
    value = Value
}) ->
    #{
        enabled => Enabled,
        value => Value
    }.

-spec update(rule_setting(), maps:map()) -> rule_setting().
update(#autocleaning_rule_setting{
    enabled = CurrentEnabled,
    value = CurrentValue
}, UpdateSettingMap) ->
    Enabled = autocleaning_utils:get_defined(enabled, UpdateSettingMap, CurrentEnabled),
    Value = autocleaning_utils:get_defined(value, UpdateSettingMap, CurrentValue),
    #autocleaning_rule_setting{
        enabled = autocleaning_utils:assert_boolean(Enabled),
        value = autocleaning_utils:assert_non_negative_integer(Value)
    }.

%%-------------------------------------------------------------------
%% @doc
%% Checks whether given value is greater or equal to the value set in
%% the rule_setting record. The check is performed if and only if the
%% rule_setting is enabled.
%% If the rule_setting is disabled always returns true.
%% @end
%%-------------------------------------------------------------------
-spec is_greater_or_equal(non_neg_integer(), rule_setting()) -> boolean().
is_greater_or_equal(_Value, #autocleaning_rule_setting{enabled = false}) ->
    true;
is_greater_or_equal(Value, #autocleaning_rule_setting{value = RuleValue}) ->
    Value >= RuleValue.

%%-------------------------------------------------------------------
%% @doc
%% Checks whether given value is less or equal to the value set in
%% the rule_setting record. The check is performed if and only if the
%% rule_setting is enabled.
%% If the rule_setting is disabled always returns true.
%% @end
%%-------------------------------------------------------------------
-spec is_less_or_equal(non_neg_integer(), rule_setting()) -> boolean().
is_less_or_equal(_Value, #autocleaning_rule_setting{enabled = false}) ->
    true;
is_less_or_equal(Value, #autocleaning_rule_setting{value = RuleValue}) ->
    Value =< RuleValue.
