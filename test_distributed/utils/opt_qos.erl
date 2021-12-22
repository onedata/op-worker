%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for manipulating qos in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_qos).
-author("Bartosz Walkowicz").

%% API
-export([
    add_qos_entry/5,
    get_effective_file_qos/3,
    get_qos_entry/3,
    remove_qos_entry/3,
    check_qos_status/3, check_qos_status/4
]).

-define(CALL(NodeSelector, Args),
    try opw_test_rpc:call(NodeSelector, mi_qos, ?FUNCTION_NAME, Args) of
        ok -> ok;
        __RESULT -> {ok, __RESULT}
    catch throw:__ERROR ->
        __ERROR
    end
).


%%%===================================================================
%%% API
%%%===================================================================


-spec add_qos_entry(
    oct_background:node_selector(), 
    session:id(), 
    lfm:file_key(),
    qos_expression:infix() | qos_expression:expression(),
    qos_entry:replicas_num()
) -> 
    {ok, qos_entry:id()} | errors:error().
add_qos_entry(NodeSelector, SessionId, FileKey, Expression, ReplicasNum) ->
    ?CALL(NodeSelector, [SessionId, FileKey, Expression, ReplicasNum]).


-spec get_effective_file_qos(oct_background:node_selector(), session:id(), lfm:file_key()) ->
    {ok, qos_req:eff_file_qos()} | errors:error().
get_effective_file_qos(NodeSelector, SessionId, FileKey) ->
    ?CALL(NodeSelector, [SessionId, FileKey]).


-spec get_qos_entry(oct_background:node_selector(), session:id(), qos_entry:id()) ->
    {ok, qos_entry:record()} | errors:error().
get_qos_entry(NodeSelector, SessionId, QosEntryId) ->
    ?CALL(NodeSelector, [SessionId, QosEntryId]).


-spec remove_qos_entry(oct_background:node_selector(), session:id(), qos_entry:id()) ->
    ok | errors:error().
remove_qos_entry(NodeSelector, SessionId, QosEntryId) ->
    ?CALL(NodeSelector, [SessionId, QosEntryId]).


-spec check_qos_status(oct_background:node_selector(), session:id(), qos_entry:id()) ->
    {ok, qos_status:summary()} | errors:error().
check_qos_status(NodeSelector, SessionId, QosEntryId) ->
    ?CALL(NodeSelector, [SessionId, QosEntryId]).


-spec check_qos_status(oct_background:node_selector(), session:id(), qos_entry:id(), lfm:file_key()) ->
    {ok, qos_status:summary()} | errors:error().
check_qos_status(NodeSelector, SessionId, QosEntryId, FileKey) ->
    ?CALL(NodeSelector, [SessionId, QosEntryId, FileKey]).
