%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for manipulating file tree in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_file_tree).
-author("Bartosz Walkowicz").

-export([
    get_path/3,
    get_parent/3
]).

-define(CALL(NodeSelector, Args), ?CALL(NodeSelector, ?FUNCTION_NAME, Args)).
-define(CALL(NodeSelector, FunctionName, Args),
    try opw_test_rpc:insecure_call(NodeSelector, mi_file_tree, FunctionName, Args, timer:minutes(3)) of
        ok -> ok;
        __RESULT -> {ok, __RESULT}
    catch throw:__ERROR ->
        __ERROR
    end
).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_path(oct_background:node_selector(), session:id(), file_id:file_guid()) ->
    {ok, file_meta:path()} | errors:error().
get_path(NodeSelector, SessionId, FileGuid) ->
    ?CALL(NodeSelector, [SessionId, FileGuid]).


-spec get_parent(oct_background:node_selector(), session:id(), lfm:file_key()) ->
    {ok, file_id:file_guid()} | errors:error().
get_parent(NodeSelector, SessionId, FileKey) ->
    ?CALL(NodeSelector, [SessionId, FileKey]).
