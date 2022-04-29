%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for tests operations on files.
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
    unlink/1,
    unlink_with_ctx/2
]).

% TODO VFS-7215 - merge this module with file_ops_test_utils

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

