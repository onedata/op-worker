%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements data_backend_behaviour and is used to synchronize
%%% the file-distribution model used in Ember application.
%%% @end
%%%-------------------------------------------------------------------
-module(file_distribution_data_backend).
-behavior(data_backend_behaviour).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/file_attr.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% data_backend_behaviour callbacks
-export([init/0, terminate/0]).
-export([find_record/2, find_all/1, query/2, query_record/2]).
-export([create_record/2, update_record/3, delete_record/2]).

-type chunks_bar_entry() :: {BarNum :: non_neg_integer(), Fill :: non_neg_integer()}.
-type chunks_bar_data() :: [chunks_bar_entry()].

-define(CHUNKS_BAR_WIDTH, 320).

-ifdef(TEST).
-export([interpolate_chunks/2]).
-endif.

%%%===================================================================
%%% data_backend_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback init/0.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback terminate/0.
%% @end
%%--------------------------------------------------------------------
-spec terminate() -> ok.
terminate() ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_record/2.
%% @end
%%--------------------------------------------------------------------
-spec find_record(ResourceType :: binary(), Id :: binary()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
find_record(<<"file-distribution">>, _Id) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback find_all/1.
%% @end
%%--------------------------------------------------------------------
-spec find_all(ResourceType :: binary()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
find_all(<<"file-distribution">>) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query/2.
%% @end
%%--------------------------------------------------------------------
-spec query(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, [proplists:proplist()]} | op_gui_error:error_result().
query(<<"file-distribution">>, [{<<"file">>, FileId}]) ->
    SessionId = op_gui_session:get_session_id(),
    {ok, Distributions} = lfm:get_file_distribution(
        SessionId, {guid, FileId}
    ),
    SpaceId = file_id:guid_to_space_id(FileId),
    % Distributions contain only the providers which has synchronized file
    % blocks, we need to mark other providers with "neverSynchronized" flag
    % to present that in GUI.
    {ok, Providers} = space_logic:get_provider_ids(SessionId, SpaceId),
    SynchronizedProviders = lists:map(
        fun(#{<<"providerId">> := ProviderId}) ->
            ProviderId
        end, Distributions),
    NeverSynchronizedProviders = Providers -- SynchronizedProviders,

    FileSize = lists:foldl(fun
        (#{<<"blocks">> := []}, Acc) ->
            Acc;
        (#{<<"blocks">> := Blocks}, Acc) ->
            [Offset, Size] = lists:last(Blocks),
            max(Acc, Offset + Size)
    end, 0, Distributions),

    BlocksOfSyncedProviders = lists:map(
        fun(#{<<"providerId">> := ProviderId, <<"blocks">> := Blocks, <<"totalBlocksSize">> := TotalBlocksSize}) ->
            % JSON keys must be binaries
            Data = [{integer_to_binary(BarNum), Fill} || {BarNum, Fill} <- interpolate_chunks(Blocks, FileSize)],
            BlocksPercentage = case FileSize of
                0 -> 0;
                _ -> TotalBlocksSize * 100.0 / FileSize
            end,
            [
                {<<"id">>, op_gui_utils:ids_to_association(FileId, ProviderId)},
                {<<"file">>, FileId},
                {<<"provider">>, ProviderId},
                {<<"chunksBarData">>, Data},
                {<<"blocksPercentage">>, BlocksPercentage},
                {<<"neverSynchronized">>, false}
            ]
        end, Distributions),

    BlocksOfNeverSyncedProviders = lists:map(
        fun(ProviderId) ->
            [
                {<<"id">>, op_gui_utils:ids_to_association(FileId, ProviderId)},
                {<<"provider">>, ProviderId},
                {<<"neverSynchronized">>, true}
            ]
        end, NeverSynchronizedProviders),

    {ok, BlocksOfSyncedProviders ++ BlocksOfNeverSyncedProviders}.


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback query_record/2.
%% @end
%%--------------------------------------------------------------------
-spec query_record(ResourceType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
query_record(<<"file-distribution">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback create_record/2.
%% @end
%%--------------------------------------------------------------------
-spec create_record(RsrcType :: binary(), Data :: proplists:proplist()) ->
    {ok, proplists:proplist()} | op_gui_error:error_result().
create_record(<<"file-distribution">>, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback update_record/3.
%% @end
%%--------------------------------------------------------------------
-spec update_record(RsrcType :: binary(), Id :: binary(),
    Data :: proplists:proplist()) ->
    ok | op_gui_error:error_result().
update_record(<<"file-distribution">>, _Id, _Data) ->
    op_gui_error:report_error(<<"Not implemented">>).


%%--------------------------------------------------------------------
%% @doc
%% {@link data_backend_behaviour} callback delete_record/2.
%% @end
%%--------------------------------------------------------------------
-spec delete_record(RsrcType :: binary(), Id :: binary()) ->
    ok | op_gui_error:error_result().
delete_record(<<"file-distribution">>, _Id) ->
    op_gui_error:report_error(<<"Not implemented">>).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Interpolates file chunks to ?CHUNKS_BAR_WIDTH values between 0 and 100,
%% meaning how many percent of data in given block is held by a provider.
%% If the FileSize is below ?CHUNKS_BAR_WIDTH, it is scaled to a bigger
%% one before applying interpolation logic, together with the blocks.
%% Output is a proplist, where Key means the number of the bar and Value
%% means percentage of data in the segment held by the provider. Adjacent
%% bars with the same percentage are merged into one, i.e.
%%         instead of: [{0,33}, {1,33}, {2,33}, {3,33}, {4,50}, {5,0}, ...]
%% the output will be: [{0,33}, {4,50}, {5,0}, ...]
%% @end
%%--------------------------------------------------------------------
-spec interpolate_chunks(Blocks :: [[non_neg_integer()]], FileSize :: non_neg_integer()) -> chunks_bar_data().
interpolate_chunks([], _) ->
    [{0, 0}];
interpolate_chunks(_, 0) ->
    [{0, 0}];
interpolate_chunks(Blocks, FileSize) when FileSize < ?CHUNKS_BAR_WIDTH ->
    interpolate_chunks([[O * ?CHUNKS_BAR_WIDTH, S * ?CHUNKS_BAR_WIDTH] || [O, S] <- Blocks], FileSize * ?CHUNKS_BAR_WIDTH);
interpolate_chunks(Blocks, FileSize) ->
    interpolate_chunks(lists:reverse(Blocks), FileSize, ?CHUNKS_BAR_WIDTH - 1, 0, []).

% Macros for more concise code, depending on variables in below functions.
-define(bar_start, floor(FileSize / ?CHUNKS_BAR_WIDTH * BarNum)).
-define(bar_end, floor(FileSize / ?CHUNKS_BAR_WIDTH * (BarNum + 1))).
-define(bar_size, (?bar_end - ?bar_start)).

-spec interpolate_chunks(Blocks :: [[non_neg_integer()]], FileSize :: non_neg_integer(), BarNum :: non_neg_integer(),
    BytesAcc :: non_neg_integer(), chunks_bar_data()) -> chunks_bar_data().
interpolate_chunks([], _FileSize, -1, _BytesAcc, ResChunks) ->
    ResChunks;
interpolate_chunks([], _FileSize, _BarNum, 0, ResChunks) ->
    merge_chunks({0, 0}, ResChunks);
interpolate_chunks([], FileSize, BarNum, BytesAcc, ResChunks) ->
    Fill = round(BytesAcc * 100 / ?bar_size),
    interpolate_chunks([], ?bar_start, BarNum - 1, 0, merge_chunks({BarNum, Fill}, ResChunks));
interpolate_chunks([[_Offset, 0] | PrevBlocks], FileSize, BarNum, BytesAcc, ResChunks) ->
    interpolate_chunks(PrevBlocks, FileSize, BarNum, BytesAcc, ResChunks);
interpolate_chunks([[Offset, Size] | PrevBlocks], FileSize, BarNum, BytesAcc, ResChunks) when ?bar_start < Offset ->
    interpolate_chunks(PrevBlocks, FileSize, BarNum, BytesAcc + Size, ResChunks);
interpolate_chunks([[Offset, Size] | PrevBlocks], FileSize, BarNum, BytesAcc, ResChunks) when Offset + Size > ?bar_start ->
    SizeInBar = Offset + Size - ?bar_start,
    Fill = round((BytesAcc + SizeInBar) * 100 / ?bar_size),
    interpolate_chunks(
        [[Offset, Size - SizeInBar] | PrevBlocks], FileSize, BarNum - 1, 0, merge_chunks({BarNum, Fill}, ResChunks)
    );
interpolate_chunks([[Offset, Size] | PrevBlocks], FileSize, BarNum, BytesAcc, ResChunks) -> % Offset + Size =< ?bar_start
    Fill = round(BytesAcc * 100 / ?bar_size),
    interpolate_chunks(
        [[Offset, Size] | PrevBlocks], FileSize, BarNum - 1, 0, merge_chunks({BarNum, Fill}, ResChunks)
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Merges adjacent chunks with the same fill value, otherwise prepends the new chunk.
%% @end
%%--------------------------------------------------------------------
-spec merge_chunks(chunks_bar_entry(), chunks_bar_data()) -> chunks_bar_data().
merge_chunks({BarNum, Fill}, [{_, Fill} | Tail]) ->
    [{BarNum, Fill} | Tail];
merge_chunks({BarNum, Fill}, Result) ->
    [{BarNum, Fill} | Result].
