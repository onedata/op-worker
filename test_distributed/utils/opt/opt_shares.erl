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

%% API
-export([
    create/4, create/5,
    remove/3
]).

-define(CALL(NodeSelector, Args),
    try opw_test_rpc:call(NodeSelector, mi_shares, ?FUNCTION_NAME, Args) of
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


-spec remove(oct_background:node_selector(), session:id(), od_share:id()) ->
    ok | errors:error().
remove(NodeSelector, SessionId, ShareId) ->
    ?CALL(NodeSelector, [SessionId, ShareId]).
