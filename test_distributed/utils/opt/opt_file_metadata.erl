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

-include("modules/fslogic/file_distribution.hrl").
-include("proto/oneprovider/provider_messages.hrl").

-export([
    get_distribution_deprecated/3
]).

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


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec to_json_deprecated(file_distribution:get_result()) -> json_utils:json_term().
to_json_deprecated(#file_distribution_gather_result{distribution = #reg_distribution_gather_result{distribution_per_provider = FileBlocksPerProvider}}) ->
    maps:fold(fun(ProviderId, #provider_reg_distribution_get_result{blocks_per_storage = BlocksPerStorage}, Acc) ->
        [Blocks] = maps:values(BlocksPerStorage),
        {BlockList, TotalBlocksSize} = get_blocks_summary(Blocks),
        [#{
            <<"providerId">> => ProviderId,
            <<"blocks">> => BlockList,
            <<"totalBlocksSize">> => TotalBlocksSize
        } | Acc]
    end, [], FileBlocksPerProvider);
to_json_deprecated(_) ->
    [].


-spec get_blocks_summary(fslogic_blocks:blocks()) -> 
    {[{non_neg_integer(), integer()}], TotalBlockSize :: integer()}.
get_blocks_summary(FileBlocks) ->
    lists:mapfoldl(
        fun(#file_block{offset = O, size = S}, SizeAcc) -> {[O, S], SizeAcc + S} end,
        0,
        FileBlocks
    ).
