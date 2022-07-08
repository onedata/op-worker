%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions for manipulating file metadata in CT tests.
%%% @end
%%%-------------------------------------------------------------------
-module(opt_file_metadata).
-author("Michal Stanisz").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/file_distribution.hrl").
-include("proto/oneprovider/provider_messages.hrl").

-export([
    get_distribution_deprecated/3
]).

-export([get_local_knowledge_of_remote_provider_blocks/3]).

-define(CALL(NodeSelector, Args), ?CALL(NodeSelector, ?FUNCTION_NAME, Args)).
-define(CALL(NodeSelector, FunctionName, Args),
    try opw_test_rpc:insecure_call(NodeSelector, mi_file_metadata, FunctionName, Args, timer:minutes(3)) of
        ok -> ok;
        __RESULT -> {ok, __RESULT}
    catch throw:__ERROR ->
        __ERROR
    end
).


%%%===================================================================
%%% API
%%%===================================================================

-spec get_distribution_deprecated(oct_background:node_selector(), session:id(), lfm:file_ref()) -> 
    json_utils:json_term().
get_distribution_deprecated(NodeSelector, SessionId, FileRef) ->
    case ?CALL(NodeSelector, gather_distribution, [SessionId, FileRef]) of
        {ok, Distribution} ->
            {ok, to_json_deprecated(Distribution)};
        Other ->
            Other
    end.


%% @TODO VFS-VFS-9498 not needed after replica_deletion uses fetched file location instead of dbsynced
%% Required to call before eviction to ensure that evicting provider has knowledge of remote provider 
%% blocks (through dbsync), as otherwise it can skip eviction.
-spec get_local_knowledge_of_remote_provider_blocks(oct_background:node_selector(), file_id:file_guid(), oneprovider:id()) ->
    {ok, [[integer()]]} | {error, term()}.
get_local_knowledge_of_remote_provider_blocks(NodeSelector, FileGuid, RemoteProviderId) ->
    FileUuid = file_id:guid_to_uuid(FileGuid),
    FileLocationId = file_location:id(FileUuid, RemoteProviderId),
    Result = opw_test_rpc:insecure_call(
        NodeSelector, fslogic_location_cache, get_location, [FileLocationId, FileUuid, true], timer:minutes(3)),
    case Result of
        {ok, #document{value = #file_location{blocks = Blocks}}} ->
            {BlockList, _} = get_blocks_summary(Blocks),
            {ok, BlockList};
        {error, not_found} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.
    

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec to_json_deprecated(file_distribution:get_result()) -> json_utils:json_term().
to_json_deprecated(#file_distribution_gather_result{distribution = #reg_distribution_gather_result{distribution_per_provider = FileBlocksPerProvider}}) ->
    DistributionUnsorted = maps:fold(fun(ProviderId, #provider_reg_distribution_get_result{blocks_per_storage = BlocksPerStorage}, Acc) ->
        [Blocks] = maps:values(BlocksPerStorage),
        {BlockList, TotalBlocksSize} = get_blocks_summary(Blocks),
        [#{
            <<"providerId">> => ProviderId,
            <<"blocks">> => BlockList,
            <<"totalBlocksSize">> => TotalBlocksSize
        } | Acc]
    end, [], FileBlocksPerProvider),
    lists:sort(fun(#{<<"providerId">> := ProviderIdA}, #{<<"providerId">> := ProviderIdB}) ->
        ProviderIdA =< ProviderIdB
    end, DistributionUnsorted);
to_json_deprecated(_) ->
    [].


-spec get_blocks_summary(fslogic_blocks:blocks()) -> 
    {[[integer()]], TotalBlockSize :: integer()}.
get_blocks_summary(FileBlocks) ->
    lists:mapfoldl(
        fun(#file_block{offset = O, size = S}, SizeAcc) -> {[O, S], SizeAcc + S} end,
        0,
        FileBlocks
    ).
