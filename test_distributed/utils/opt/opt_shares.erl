%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for manipulating shares in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_shares).
-author("Bartosz Walkowicz").

-include_lib("ctool/include/test/assertions.hrl").

%% API
-export([
    create/4, create/5,
    get/3,
    get_handle/3,
    remove/3
]).

-define(CALL(NodeSelector, Args),
    try opw_test_rpc:insecure_call(NodeSelector, mi_shares, ?FUNCTION_NAME, Args) of
        ok -> ok;
        __RESULT -> {ok, __RESULT}
    catch throw:__ERROR ->
        __ERROR
    end
).


%%%===================================================================
%%% API
%%%===================================================================


-spec create(
    oct_background:node_selector(),
    session:id(),
    lfm:file_key(),
    od_share:name()
) ->
    {ok, od_share:id()} | errors:error().
create(NodeSelector, SessionId, FileKey, Name) ->
    RandomDescription = str_utils:rand_hex(100),
    create(NodeSelector, SessionId, FileKey, Name, RandomDescription).


-spec create(
    oct_background:node_selector(),
    session:id(),
    lfm:file_key(),
    od_share:name(),
    od_share:description()
) ->
    {ok, od_share:id()} | errors:error().
create(NodeSelector, SessionId, FileKey, Name, Description) ->
    ?CALL(NodeSelector, [SessionId, FileKey, Name, Description]).


-spec get(oct_background:node_selector(), session:id(), od_share:id()) ->
    {ok, od_share:doc()} | errors:error().
get(NodeSelector, SessionId, ShareId) ->
    opw_test_rpc:call(NodeSelector, share_logic, get, [SessionId, ShareId]).


-spec get_handle(oct_background:node_selector(), session:id(), od_share:id()) ->
    od_handle:id() | undefined.
get_handle(NodeSelector, SessionId, ShareId) ->
    {ok, HandleId} = ?assertMatch({ok, _}, opw_test_rpc:call(
        NodeSelector, share_logic, get_handle, [SessionId, ShareId])),
    HandleId.


-spec remove(oct_background:node_selector(), session:id(), od_share:id()) ->
    ok | errors:error().
remove(NodeSelector, SessionId, ShareId) ->
    ?CALL(NodeSelector, [SessionId, ShareId]).
