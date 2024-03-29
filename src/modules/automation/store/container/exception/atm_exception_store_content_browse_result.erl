%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing store content browse result specialization for
%%% exception store used in automation machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_exception_store_content_browse_result).
-author("Bartosz Walkowicz").

-behaviour(atm_store_content_browse_result).

-include("modules/automation/atm_execution.hrl").

%% API
-export([to_json/1]).

-type record() :: #atm_exception_store_content_browse_result{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec to_json(record()) -> json_utils:json_map().
to_json(#atm_exception_store_content_browse_result{
    items = Items,
    is_last = IsLast
}) ->
    #{
        <<"items">> => lists:map(
            fun atm_store_container_infinite_log_backend:entry_to_json/1,
            Items
        ),
        <<"isLast">> => IsLast
    }.
