%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Record expressing store content browse result specialization for
%%% tree_forest store used in automation machinery.
%%% @end
%%%-------------------------------------------------------------------
-module(atm_tree_forest_store_content_browse_result).
-author("Bartosz Walkowicz").

-behaviour(atm_store_content_browse_result).

-include("modules/automation/atm_execution.hrl").

%% API
-export([to_json/1]).

-type record() :: #atm_tree_forest_store_content_browse_result{}.

-export_type([record/0]).


%%%===================================================================
%%% API
%%%===================================================================


-spec to_json(record()) -> json_utils:json_map().
to_json(#atm_tree_forest_store_content_browse_result{
    tree_roots = TreeRoots,
    is_last = IsLast
}) ->
    #{
        <<"treeRoots">> => lists:map(fun
            ({Index, {ok, Value}}) ->
                #{
                    <<"index">> => Index,
                    <<"success">> => true,
                    <<"value">> => Value
                };
            ({Index, {error, _} = Error}) ->
                #{
                    <<"index">> => Index,
                    <<"success">> => false,
                    <<"error">> => errors:to_json(Error)
                }
        end, TreeRoots),
        <<"isLast">> => IsLast
    }.
