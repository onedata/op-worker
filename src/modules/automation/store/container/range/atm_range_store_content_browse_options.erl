%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing store content browse options specialization for
%%% range store used in automation machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_range_store_content_browse_options).
-author("Bartosz Walkowicz").

-behaviour(atm_store_content_browse_options).

-include("modules/automation/atm_execution.hrl").

%% API
-export([sanitize/1]).

-type record() :: #atm_range_store_content_browse_options{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec sanitize(json_utils:json_map()) -> record().
sanitize(#{<<"type">> := <<"rangeStoreContentBrowseOptions">>}) ->
    #atm_range_store_content_browse_options{}.
