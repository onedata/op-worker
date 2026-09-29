%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022-2026 Onedata (onedata.org)
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% File operations for tests that would otherwise repeat the same node and
%%% session id in every call: the pair is stored once (set_default_context/2)
%%% and every later call takes just the file path. A test working with several
%%% providers or users saves them under names (save_named_context/3) and picks
%%% one per call with the *_with_ctx functions.
%%%
%%% The contexts live in the node-wide cache of the node running the test code,
%%% so they are shared by everything running there and outlive the test case that
%%% set them - hence clear_context/0, and hence a test must set its own context
%%% rather than count on the one left by whatever ran before.
%%%
%%% Unlike the rest of this domain (see lfm_test_utils, file_ops_test_utils,
%%% file_tree_test_utils), which name the node and session at every call.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_ct).
-author("Michal Stanisz").

-include("modules/logical_file_manager/lfm.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([set_default_context/2, save_named_context/3, clear_context/0]).

-export([
    mkdir/1, 
    mkdir/2,
    mkdir_with_ctx/2,
    create/1,
    create_with_ctx/2,
    unlink/1
]).

-type ctx_name() :: any().

-define(CACHE_KEY, ?MODULE).
-define(DEFAULT_CTX, default).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec set_default_context(node(), session:id()) -> ok.
set_default_context(Node, SessionId) ->
    save_context(?DEFAULT_CTX, #{node => Node, session_id => SessionId}).


-spec save_named_context(ctx_name(), node(), session:id()) -> ok.
save_named_context(CtxName, Node, SessionId) ->
    save_context(CtxName, #{node => Node, session_id => SessionId}).


-spec clear_context() -> ok.
clear_context() ->
    node_cache:clear(?CACHE_KEY).


-spec mkdir(file_meta:path()) -> file_id:file_guid() | no_return().
mkdir(Path) ->
    mkdir_with_ctx(?DEFAULT_CTX, [Path]).

-spec mkdir(file_meta:path(), file_meta:posix_permissions()) -> file_id:file_guid() | no_return().
mkdir(Path, Mode) ->
    mkdir_with_ctx(?DEFAULT_CTX, [Path, Mode]).

-spec mkdir_with_ctx(ctx_name(), [any()]) -> file_id:file_guid() | no_return().
mkdir_with_ctx(CtxName, Args) ->
    {ok, Guid} = ?assertMatch({ok, _}, execute_in_context(CtxName, mkdir, Args)),
    Guid.


-spec create(file_meta:path()) -> file_id:file_guid() | no_return().
create(Path) ->
    create_with_ctx(?DEFAULT_CTX, [Path]).

-spec create_with_ctx(ctx_name(), [any()]) -> file_id:file_guid() | no_return().
create_with_ctx(CtxName, Args) ->
    {ok, Guid} = ?assertMatch({ok, _}, execute_in_context(CtxName, create, Args)),
    Guid.


-spec unlink(file_id:file_guid()) -> ok | no_return().
unlink(Guid) ->
    unlink_with_ctx(?DEFAULT_CTX, [?FILE_REF(Guid)]).

%% @private
-spec unlink_with_ctx(ctx_name(), [any()]) -> ok | no_return().
unlink_with_ctx(CtxName, Args) ->
    ?assertEqual(ok, execute_in_context(CtxName, unlink, Args)).


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec save_context(ctx_name(), map()) -> ok.
save_context(CtxName, Values) ->
    CurrentCtx = node_cache:get(?CACHE_KEY, #{}),
    node_cache:put(?CACHE_KEY, CurrentCtx#{CtxName => Values}).


-spec execute_in_context(ctx_name(), atom(), [term()]) -> any().
execute_in_context(CtxName, FunctionName, Args) ->
    Ctxs = node_cache:get(?CACHE_KEY),
    #{node := Node, session_id := SessionId} = maps:get(CtxName, Ctxs),
    erlang:apply(lfm_proxy, FunctionName, [Node, SessionId | Args]).

