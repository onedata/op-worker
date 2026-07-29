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
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([
    rand_xattr_name/1,
    get_content/2, get_content/3,
    get_attrs/2, get_attrs/3, get_attrs/4,
    set_xattr/4
]).
-export([await_sync/2]).
-export([
    await_size/3,
    await_content/3, await_content/4,
    await_distribution/3,
    await_xattr/4,
    await_attrs/4
]).
-export([replicate_by_read/4]).

-type offset() :: non_neg_integer().

-type error() :: {error, term()}.

-export_type([offset/0]).


-define(DEFAULT_ATTEMPTS, 60).
-define(READ_CHUNK_SIZE, 33554432).  % 32 MiB
% cap on concurrently replicated files, so that a call for thousands of them
% does not flood the provider with fetch requests
-define(MAX_PARALLEL_REPLICATIONS, 100).


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
            {ok, FileHandle} = lfm_proxy:open(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), read),
            Result = lfm_proxy:read(Node, FileHandle, Offset, FileSize),
            lfm_proxy:close(Node, FileHandle),
            Result;
        {error, _} = Error ->
            Error
    end.


-spec get_attrs(node(), file_id:file_guid()) ->
    {ok, lfm_attrs:file_attributes()} | error().
get_attrs(Node, FileGuid) ->
    get_attrs(Node, ?ROOT_SESS_ID, FileGuid).

-spec get_attrs(node(), session:id(), file_id:file_guid()) ->
    {ok, lfm_attrs:file_attributes()} | error().
get_attrs(Node, SessId, FileGuid) ->
    get_attrs(Node, SessId, FileGuid, []).


-spec get_attrs(node(), session:id(), file_id:file_guid(), [onedata_file:xattr_name()]) ->
    {ok, lfm_attrs:file_attributes()} | error().
get_attrs(Node, SessId, FileGuid, RequestedXattrs) ->
    XattrsOpt = case RequestedXattrs of
        [] -> [];
        _ -> [{xattrs, RequestedXattrs}]
    end,
    case lfm_proxy:stat(Node, SessId, ?FILE_REF(FileGuid), [XattrsOpt | ?API_FILE_ATTRS]) of
        % File attrs are constructed from several records so it is possible that
        % even if 'file_meta' (the main doc) was synchronized 'times' doc wasn't
        {ok, #file_attr{mtime = 0}} ->
            {error, times_not_synchronized};
        Result ->
            Result
    end.


%%--------------------------------------------------------------------
%% @doc
%% Builds an xattr name unique to the calling test case, so that the
%% files it sets the xattr on are not confused with the ones left in the
%% same space by other cases (or by earlier runs against a long-lived
%% deployment).
%% @end
%%--------------------------------------------------------------------
-spec rand_xattr_name(atom()) -> onedata_file:xattr_name().
rand_xattr_name(CaseName) ->
    str_utils:format_bin("xattr_~ts_~ts", [CaseName, str_utils:rand_hex(6)]).


-spec set_xattr(node(), file_id:file_guid(), onedata_file:xattr_name(), custom_metadata:value()) ->
    ok | error().
set_xattr(Node, FileGuid, Name, Value) ->
    lfm_proxy:set_xattr(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), #xattr{
        name = Name,
        value = Value
    }).


-spec await_sync(node() | [node()], file_id:file_guid() | [file_id:file_guid()]) ->
    ok | no_return().
await_sync(Nodes, Files) ->
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
                <<"providerId">> => opw_test_rpc:get_provider_id(Node),
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
                <<"providerId">> => opw_test_rpc:get_provider_id(Node),
                <<"totalBlocksSize">> => lists:sum(lists:map(fun(#file_block{size = Size}) ->
                    Size
                end, Blocks))
            }
    end, ExpSizeOrBlocksPerProvider)),

    FetchDistributionFun = fun(Node, FileGuid) ->
        {ok, Distribution} = opt_file_metadata:get_distribution_deprecated(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid)),
        lists:sort(Distribution)
    end,

    lists:foreach(fun(FileGuid) ->
        lists:foreach(fun(Node) ->
            ?assertEqual(ExpDistribution, FetchDistributionFun(Node, FileGuid), Attempts)
        end, utils:ensure_list(Nodes))
    end, utils:ensure_list(Files)).


-spec await_xattr(node(), file_id:file_guid(), [onedata_file:xattr_name()], non_neg_integer()) -> ok.
await_xattr(Node, FileGuid, ExpectedXattrs, Attempts) ->
    CheckFun = fun() ->
        try
            {ok, CurrentXattrs} = lfm_proxy:list_xattr(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), false, false),
            lists:foreach(fun(Xattr) ->
                true = lists:member(Xattr, CurrentXattrs)
            end, ExpectedXattrs)
        catch _:E ->
            E
        end
    end,
    ?assertEqual(ok, CheckFun(), Attempts).


-spec await_attrs(node(), file_id:file_guid(), #{onedata_file:attr_name() => json_utils:json_term()},
    non_neg_integer()) -> ok.
await_attrs(Node, FileGuid, ExpectedAttrsMap, Attempts) ->
    GetAttrFun = fun() ->
        try
            {ok, CurrentAttrs} = lfm_proxy:stat(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), maps:keys(ExpectedAttrsMap)),
            CurrentAttrsJson = file_attr_translator:to_json(CurrentAttrs, default, maps:keys(ExpectedAttrsMap)),
            maps:fold(fun(Key, Value, Acc) ->
                Acc#{onedata_file:attr_name_from_json(Key) => Value}
            end, #{}, CurrentAttrsJson)
        catch _:E ->
            E
        end
    end,
    ?assertEqual(ExpectedAttrsMap, GetAttrFun(), Attempts).
    

%%--------------------------------------------------------------------
%% @doc
%% Forces replication of the given files to TargetNode by reading their whole
%% content there (the "on the fly" replication), which is way cheaper than
%% scheduling a transfer per file - those are dispatched through a small pool
%% of controllers, each blocked for the entire lifetime of its transfer.
%% The files are read in parallel and in chunks, so that big ones do not
%% materialize as single huge binaries.
%%
%% The size of each file is taken from SourceNode - a provider that holds its
%% replica - and only then awaited on TargetNode. A file syncs to a provider
%% before its size does, so a size read on TargetNode too early would turn this
%% function into a no-op: nothing to read, nothing fetched, no replica registered.
%% @end
%%--------------------------------------------------------------------
-spec replicate_by_read(
    SourceNode :: node(),
    TargetNode :: node(),
    TargetSessionId :: session:id(),
    file_id:file_guid() | [file_id:file_guid()]
) ->
    ok | no_return().
replicate_by_read(SourceNode, TargetNode, TargetSessionId, Files) ->
    lists_utils:pforeach(fun(FileGuid) ->
        {ok, #file_attr{size = FileSize}} = ?assertMatch(
            {ok, _}, get_attrs(SourceNode, FileGuid), get_attempts()
        ),
        await_size(TargetNode, FileGuid, FileSize),

        {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(
            TargetNode, TargetSessionId, ?FILE_REF(FileGuid), read
        )),
        ok = read_in_chunks(TargetNode, Handle, 0, FileSize),
        ok = lfm_proxy:close(TargetNode, Handle)
    end, utils:ensure_list(Files), ?MAX_PARALLEL_REPLICATIONS).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec get_attempts() -> non_neg_integer().
get_attempts() ->
    node_cache:get(attempts, ?DEFAULT_ATTEMPTS).


%% @private
-spec read_in_chunks(node(), lfm:handle(), offset(), file_meta:size()) -> ok.
read_in_chunks(_Node, _Handle, Offset, FileSize) when Offset >= FileSize ->
    ok;
read_in_chunks(Node, Handle, Offset, FileSize) ->
    ChunkSize = min(?READ_CHUNK_SIZE, FileSize - Offset),
    {ok, Data} = ?assertMatch({ok, _}, lfm_proxy:read(Node, Handle, Offset, ChunkSize)),
    ?assertEqual(ChunkSize, byte_size(Data)),
    read_in_chunks(Node, Handle, Offset + ChunkSize, FileSize).
