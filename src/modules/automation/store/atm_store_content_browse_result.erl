%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing "polymorphic" store content browse result used
%%% in automation machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_store_content_browse_result).
-author("Bartosz Walkowicz").

-include("modules/automation/atm_execution.hrl").

%% API
-export([to_json/1]).


-type record() ::
    atm_audit_log_store_content_browse_result:record() |
    atm_list_store_content_browse_result:record() |
    atm_range_store_content_browse_result:record() |
    atm_single_value_store_content_browse_result:record() |
    atm_tree_forest_store_content_browse_result:record().

-export_type([record/0]).


%%%===================================================================
%%% Callbacks
%%%===================================================================


-callback to_json(record()) -> json_utils:json_term().


%%%===================================================================
%%% API
%%%===================================================================


-spec to_json(record()) -> json_utils:json_map().
to_json(AtmStoreContentBrowseResult) ->
    RecordType = utils:record_type(AtmStoreContentBrowseResult),
    RecordType:to_json(AtmStoreContentBrowseResult).
