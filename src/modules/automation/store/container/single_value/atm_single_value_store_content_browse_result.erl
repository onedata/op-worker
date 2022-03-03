%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing store content browse result specialization for
%%% single_value store used in automation machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_single_value_store_content_browse_result).
-author("Bartosz Walkowicz").

-behaviour(atm_store_content_browse_result).

-include("modules/automation/atm_execution.hrl").

%% API
-export([to_json/1]).

-type record() :: #atm_single_value_store_content_browse_result{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec to_json(record()) -> json_utils:json_term().
to_json(#atm_single_value_store_content_browse_result{item = undefined}) ->
    <<>>;

to_json(#atm_single_value_store_content_browse_result{item = {ok, Item}}) ->
    #{
        <<"success">> => true,
        <<"value">> => Item
    };

to_json(#atm_single_value_store_content_browse_result{item = Error = {error, _}}) ->
    #{
        <<"success">> => false,
        <<"value">> => errors:to_json(Error)
    }.
