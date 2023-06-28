%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing "polymorphic" store content browse options used
%%% in automation machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_content_browse_options).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([sanitize/2]).


-type type() ::
    atm_audit_log_store_content_browse_options |
    atm_exception_store_content_browse_options |
    atm_list_store_content_browse_options |
    atm_range_store_content_browse_options |
    atm_single_value_store_content_browse_options |
    atm_time_series_store_content_browse_options |
    atm_tree_forest_store_content_browse_options.

-type record() ::
    atm_audit_log_store_content_browse_options:record() |
    atm_exception_store_content_browse_options:record() |
    atm_list_store_content_browse_options:record() |
    atm_range_store_content_browse_options:record() |
    atm_single_value_store_content_browse_options:record() |
    atm_time_series_store_content_browse_options:record() |
    atm_tree_forest_store_content_browse_options:record().

-export_type([type/0, record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback sanitize(json_utils:json_map()) -> record() | no_return().


%%%===================================================================
%%% API
%%%===================================================================


-spec sanitize(atm_store:type(), json_utils:json_map()) ->
    record() | no_return().
sanitize(AtmStoreType, EmptyOptions) when is_map(EmptyOptions), map_size(EmptyOptions) == 0 ->
    % options are optional and if not specified should resolve to reasonable defaults
    sanitize(AtmStoreType, #{<<"type">> => store_type_to_type_json(AtmStoreType)});

sanitize(audit_log, #{<<"type">> := <<"auditLogStoreContentBrowseOptions">>} = Data) ->
    atm_audit_log_store_content_browse_options:sanitize(Data);

sanitize(exception, #{<<"type">> := <<"exceptionStoreContentBrowseOptions">>} = Data) ->
    atm_exception_store_content_browse_options:sanitize(Data);

sanitize(list, #{<<"type">> := <<"listStoreContentBrowseOptions">>} = Data) ->
    atm_list_store_content_browse_options:sanitize(Data);

sanitize(range, #{<<"type">> := <<"rangeStoreContentBrowseOptions">>} = Data) ->
    atm_range_store_content_browse_options:sanitize(Data);

sanitize(single_value, #{<<"type">> := <<"singleValueStoreContentBrowseOptions">>} = Data) ->
    atm_single_value_store_content_browse_options:sanitize(Data);

sanitize(time_series, #{<<"type">> := <<"timeSeriesStoreContentBrowseOptions">>} = Data) ->
    atm_time_series_store_content_browse_options:sanitize(Data);

sanitize(tree_forest, #{<<"type">> := <<"treeForestStoreContentBrowseOptions">>} = Data) ->
    atm_tree_forest_store_content_browse_options:sanitize(Data);

sanitize(AtmStoreType, #{<<"type">> := _}) ->
    throw(?ERROR_BAD_VALUE_NOT_ALLOWED(<<"type">>, [store_type_to_type_json(AtmStoreType)]));

sanitize(_, _) ->
    throw(?ERROR_MISSING_REQUIRED_VALUE(<<"type">>)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec store_type_to_type_json(atm_store:type()) -> binary().
store_type_to_type_json(audit_log) -> <<"auditLogStoreContentBrowseOptions">>;
store_type_to_type_json(exception) -> <<"exceptionStoreContentBrowseOptions">>;
store_type_to_type_json(list) -> <<"listStoreContentBrowseOptions">>;
store_type_to_type_json(range) -> <<"rangeStoreContentBrowseOptions">>;
store_type_to_type_json(single_value) -> <<"singleValueStoreContentBrowseOptions">>;
store_type_to_type_json(time_series) -> <<"timeSeriesStoreContentBrowseOptions">>;
store_type_to_type_json(tree_forest) -> <<"treeForestStoreContentBrowseOptions">>.
