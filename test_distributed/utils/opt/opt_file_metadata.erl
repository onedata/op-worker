%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for manipulating archives in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_file_metadata).
-author("Michal Stanisz").

-export([
    get_distribution_deprecated/3
]).

-define(CALL(NodeSelector, Args), ?CALL(NodeSelector, ?FUNCTION_NAME, Args)).
-define(CALL(NodeSelector, FunctionName, Args),
    try opw_test_rpc:insecure_call(NodeSelector, mi_file_metadata, FunctionName, Args, timer:minutes(3)) of
        ok -> ok;
        __RESULT -> {ok, __RESULT}
    catch throw:__ERROR ->
        __ERROR
    end
).


%%%===================================================================
%%% API
%%%===================================================================

-spec get_distribution_deprecated(oct_background:node_selector(), session:id(), lfm:file_ref()) -> 
    json_utils:json_term().
get_distribution_deprecated(NodeSelector, SessionId, FileRef) ->
    case ?CALL(NodeSelector, get_distribution, [SessionId, FileRef]) of
        {ok, Distribution} ->
            {ok, file_distribution_get_result:to_json_deprecated(Distribution)};
        Other ->
            Other
    end.
