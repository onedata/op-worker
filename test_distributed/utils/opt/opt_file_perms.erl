%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for manipulating file perms in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_file_perms).
-author("Bartosz Walkowicz").

-export([
    get_acl/3,
    set_acl/4,
    remove_acl/3,

    check_file_access/4
]).

-define(CALL(NodeSelector, Args), ?CALL(NodeSelector, ?FUNCTION_NAME, Args)).
-define(CALL(NodeSelector, FunctionName, Args),
    try opw_test_rpc:insecure_call(NodeSelector, mi_file_perms, FunctionName, Args, timer:minutes(3)) of
        ok -> ok;
        __RESULT -> {ok, __RESULT}
    catch throw:__ERROR ->
        __ERROR
    end
).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_acl(oct_background:node_selector(), session:id(), lfm:file_key()) ->
 {ok, acl:acl()} | no_return().
get_acl(NodeSelector, SessionId, FileKey) ->
    ?CALL(NodeSelector, [SessionId, FileKey]).


-spec set_acl(oct_background:node_selector(), session:id(), lfm:file_key(), acl:acl()) ->
    ok | no_return().
set_acl(NodeSelector, SessionId, FileKey, Acl) ->
    ?CALL(NodeSelector, [SessionId, FileKey, Acl]).


-spec remove_acl(oct_background:node_selector(), session:id(), lfm:file_key()) ->
    ok | no_return().
remove_acl(NodeSelector, SessionId, FileKey) ->
    ?CALL(NodeSelector, [SessionId, FileKey]).


-spec check_file_access(oct_background:node_selector(), session:id(), lfm:file_key(), fslogic_worker:open_flag()) ->
    ok | no_return().
check_file_access(NodeSelector, SessionId, FileKey, Flag) ->
    ?CALL(NodeSelector, [SessionId, FileKey, Flag]).
