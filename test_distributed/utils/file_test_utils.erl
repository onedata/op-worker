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
-include("proto/oneclient/common_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([
    get_content/2, get_content/3,
    get_attrs/2
]).
-export([wait_for_sync/2]).
-export([
    await_size/3,
    await_content/3, await_content/4,
    await_distribution/3
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


-spec wait_for_sync(node() | [node()], file_id:file_guid() | [file_id:file_guid()]) ->
    ok | no_return().
wait_for_sync(Nodes, Files) ->
    Attempts = get_attempts(),

    lists:foreach(fun(Node) ->
        lists:foreach(fun(FileGuid) ->
            ?assertMatch({ok, _}, get_attrs(Node, FileGuid), Attempts)
        end, utils:ensure_list(Files))
    end, utils:ensure_list(Nodes)).


-spec await_size(node() | [node()], file_id:file_guid(), file_meta:size()) ->
    ok | no_return().
await_size(Nodes, FileGuid, ExpFileSize) ->
    Attempts = get_attempts(),

    lists:foreach(fun(Provider) ->
        ?assertMatch({ok, #file_attr{size = ExpFileSize}}, get_attrs(Provider, FileGuid), Attempts)
    end, utils:ensure_list(Nodes)).


-spec await_content(node() | [node()], file_id:file_guid(), ExpContent :: binary()) ->
    ok | no_return().
await_content(Nodes, FileGuid, ExpContent) ->
    await_content(Nodes, FileGuid, ExpContent, 0).


-spec await_content(node() | [node()], file_id:file_guid(), ExpContent :: binary(), offset()) ->
    ok | no_return().
await_content(Nodes, FileGuid, ExpContent, Offset) ->
    Attempts = get_attempts(),

    lists:foreach(fun(Node) ->
        ?assertEqual({ok, ExpContent}, get_content(Node, FileGuid, Offset), Attempts)
    end, utils:ensure_list(Nodes)).


-spec await_distribution(
    node() | [node()],
    file_id:file_guid() | [file_id:file_guid()],
    [{node(), file_meta:size() | [fslogic_blocks:blocks()]}]
) ->
    ok | no_return().
await_distribution(Nodes, Files, ExpSizeOrBlocksPerProvider) ->
    Attempts = get_attempts(),

    ExpDistribution = lists:sort(lists:map(fun
        ({Node, ExpSize}) when is_integer(ExpSize) ->
            #{
                <<"blocks">> => case ExpSize of
                    0 -> [];
                    _ -> [[0, ExpSize]]
                end,
                <<"providerId">> => op_test_rpc:get_provider_id(Node),
                <<"totalBlocksSize">> => ExpSize
            };
        ({Node, Blocks}) when is_list(Blocks) ->
            #{
                <<"blocks">> => lists:foldr(fun
                    (#file_block{offset = _Offset, size = 0}, Acc) ->
                        Acc;
                    (#file_block{offset = Offset, size = Size}, Acc) ->
                        [[Offset, Size] | Acc]
                end, [], Blocks),
                <<"providerId">> => op_test_rpc:get_provider_id(Node),
                <<"totalBlocksSize">> => lists:sum(lists:map(fun(#file_block{size = Size}) ->
                    Size
                end, Blocks))
            }
    end, ExpSizeOrBlocksPerProvider)),

    FetchDistributionFun = fun(Node, FileGuid) ->
        {ok, Distribution} = lfm_proxy:get_file_distribution(Node, ?ROOT_SESS_ID, {guid, FileGuid}),
        lists:sort(Distribution)
    end,

    lists:foreach(fun(FileGuid) ->
        lists:foreach(fun(Node) ->
            ?assertEqual(ExpDistribution, FetchDistributionFun(Node, FileGuid), Attempts)
        end, utils:ensure_list(Nodes))
    end, utils:ensure_list(Files)).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_attempts() -> non_neg_integer().
get_attempts() ->
    node_cache:get(attempts, ?DEFAULT_ATTEMPTS).
