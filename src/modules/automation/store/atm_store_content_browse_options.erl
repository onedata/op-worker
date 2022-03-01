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
    atm_list_store_content_browse_options |
    atm_range_store_content_browse_options |
    atm_single_value_store_content_browse_options |
    atm_tree_forest_store_content_browse_options.

-type record() ::
    atm_audit_log_store_content_browse_options:record() |
    atm_list_store_content_browse_options:record() |
    atm_range_store_content_browse_options:record() |
    atm_single_value_store_content_browse_options:record() |
    atm_tree_forest_store_content_browse_options:record().

-export_type([type/0, record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback sanitize(json_utils:json_map()) -> record() | no_return().


%%%===================================================================
%%% API
%%%===================================================================


-spec sanitize(automation:store_type(), json_utils:json_map()) ->
    record() | no_return().
sanitize(AtmStoreType, EmptyOptions) when is_map(EmptyOptions), map_size(EmptyOptions) == 0 ->
    % options are optional and if not specified should resolve to reasonable defaults
    sanitize(AtmStoreType, #{<<"type">> => encode_type(store_type_to_type(AtmStoreType))});

sanitize(audit_log, #{<<"type">> := <<"auditLogStoreContentBrowseOptions">>} = Data) ->
    atm_audit_log_store_content_browse_options:sanitize(Data);

sanitize(list, #{<<"type">> := <<"listStoreContentBrowseOptions">>} = Data) ->
    atm_list_store_content_browse_options:sanitize(Data);

sanitize(range, #{<<"type">> := <<"rangeStoreContentBrowseOptions">>} = Data) ->
    atm_range_store_content_browse_options:sanitize(Data);

sanitize(single_value, #{<<"type">> := <<"singleValueStoreContentBrowseOptions">>} = Data) ->
    atm_single_value_store_content_browse_options:sanitize(Data);

sanitize(tree_forest, #{<<"type">> := <<"treeForestStoreContentBrowseOptions">>} = Data) ->
    atm_tree_forest_store_content_browse_options:sanitize(Data);

sanitize(AtmStoreType, #{<<"type">> := _}) ->
    throw(?ERROR_BAD_VALUE_NOT_ALLOWED(
        <<"type">>,
        [encode_type(store_type_to_type(AtmStoreType))]
    ));

sanitize(_, _) ->
    throw(?ERROR_MISSING_REQUIRED_VALUE(<<"type">>)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec store_type_to_type(automation:store_type()) -> type().
store_type_to_type(audit_log) -> atm_audit_log_store_content_browse_options;
store_type_to_type(list) -> atm_list_store_content_browse_options;
store_type_to_type(range) -> atm_range_store_content_browse_options;
store_type_to_type(single_value) -> atm_single_value_store_content_browse_options;
store_type_to_type(tree_forest) -> atm_tree_forest_store_content_browse_options.


%% @private
-spec encode_type(type()) -> binary().
encode_type(atm_audit_log_store_content_browse_options) ->
    <<"auditLogStoreContentBrowseOptions">>;
encode_type(atm_list_store_content_browse_options) ->
    <<"listStoreContentBrowseOptions">>;
encode_type(atm_range_store_content_browse_options) ->
    <<"rangeStoreContentBrowseOptions">>;
encode_type(atm_single_value_store_content_browse_options) ->
    <<"singleValueStoreContentBrowseOptions">>;
encode_type(atm_tree_forest_store_content_browse_options) ->
    <<"treeForestStoreContentBrowseOptions">>.
