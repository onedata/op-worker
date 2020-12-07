%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions operating on files used in ct tests.
%%% @end
%%%-------------------------------------------------------------------
-module(file_test_utils).
-author("Bartosz Walkowicz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([
    get_content/2, get_content/3,
    get_attrs/2
]).
-export([wait_for_sync/2]).
-export([
    assert_size/3,
    assert_content/3, assert_content/4
]).

-type offset() :: non_neg_integer().

-type error() :: {error, term()}.

-export_type([offset/0]).


-define(DEFAULT_ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


-spec get_content(node(), file_id:file_guid()) -> {ok, binary()} | error().
get_content(Node, FileGuid) ->
    get_content(Node, FileGuid, 0).


-spec get_content(node(), file_id:file_guid(), offset()) -> {ok, binary()} | error().
get_content(Node, FileGuid, Offset) ->
    case get_attrs(Node, FileGuid) of
        {ok, #file_attr{size = FileSize}} ->
            {ok, FileHandle} = lfm_proxy:open(Node, ?ROOT_SESS_ID, {guid, FileGuid}, read),
            Result = lfm_proxy:read(Node, FileHandle, Offset, FileSize),
            lfm_proxy:close(Node, FileHandle),
            Result;
        {error, _} = Error ->
            Error
    end.


-spec get_attrs(node(), file_id:file_guid()) ->
    {ok, lfm_attrs:file_attributes()} | error().
get_attrs(Node, FileGuid) ->
    case lfm_proxy:stat(Node, ?ROOT_SESS_ID, {guid, FileGuid}) of
        % File attrs are constructed from several records so it is possible that
        % even if 'file_meta' (the main doc) was synchronized 'times' doc wasn't
        {ok, #file_attr{mtime = 0}} ->
            {error, times_not_synchronized};
        Result ->
            Result
    end.


-spec wait_for_sync(node() | [node()], file_id:file_guid()) -> ok.
wait_for_sync(Nodes, FileGuid) ->
    Attempts = get_attempts(),

    lists:foreach(fun(Node) ->
        ?assertMatch({ok, _}, get_attrs(Node, FileGuid), Attempts)
    end, utils:ensure_list(Nodes)).


-spec assert_size(node() | [node()], file_id:file_guid(), file_meta:size()) ->
    ok | no_return().
assert_size(NodesToVerify, FileGuid, ExpFileSize) ->
    Attempts = get_attempts(),

    lists:foreach(fun(Provider) ->
        ?assertMatch({ok, #file_attr{size = ExpFileSize}}, get_attrs(Provider, FileGuid), Attempts)
    end, utils:ensure_list(NodesToVerify)).


-spec assert_content(node() | [node()], file_id:file_guid(), ExpContent :: binary()) ->
    ok | no_return().
assert_content(Nodes, FileGuid, ExpContent) ->
    assert_content(Nodes, FileGuid, ExpContent, 0).


-spec assert_content(node() | [node()], file_id:file_guid(), ExpContent :: binary(), offset()) ->
    ok | no_return().
assert_content(Nodes, FileGuid, ExpContent, Offset) ->
    Attempts = get_attempts(),

    lists:foreach(fun(Node) ->
        ?assertEqual({ok, ExpContent}, get_content(Node, FileGuid, Offset), Attempts)
    end, utils:ensure_list(Nodes)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_attempts() -> non_neg_integer().
get_attempts() ->
    node_cache:get(attempts, ?DEFAULT_ATTEMPTS).
