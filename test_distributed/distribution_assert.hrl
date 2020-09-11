%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains macros with asserts to be used
%%% for checking distribution of files' blocks in tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DISTRIBUTION_ASSERT_HRL).
-define(DISTRIBUTION_ASSERT_HRL, 1).

-include_lib("ctool/include/test/assertions.hrl").

-define(DIST(__ProviderId, __Size),
    (fun
        (P, 0) -> [#{<<"providerId">> => P, <<"blocks">> => []}];
        (P, Blocks) when is_list(Blocks) -> [#{<<"providerId">> => P, <<"blocks">> => Blocks}];
        (P, S) -> [#{<<"providerId">> => P, <<"blocks">> => [[0, S]]}]
    end)(__ProviderId, __Size)).

-define(DISTS(ProviderIds, Sizes), lists:flatmap(fun({PId, __Size}) ->
    ?DIST(PId, __Size)
end, lists:zip(ProviderIds, Sizes))).

-define(normalizeDistribution(__Distributions), lists:sort(lists:map(fun(__Distribution) ->
    __Distribution#{
        <<"totalBlocksSize">> => lists:foldl(fun([_Offset, __Size], __SizeAcc) ->
            __SizeAcc + __Size
        end, 0, maps:get(<<"blocks">>, __Distribution))
    }
end, __Distributions))).

-define(assertDistribution(Worker, SessionId, ExpectedDistribution, FileGuid, Attempts),
    ?assertEqual(?normalizeDistribution(ExpectedDistribution), try
        {ok, __FileBlocks} = lfm_proxy:get_file_distribution(Worker, SessionId, {guid, FileGuid}),
        lists:sort(__FileBlocks)
    catch
        _:_ ->
            error
    end, Attempts)).


-endif.