%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% file entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(file_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("modules/fslogic/file_details.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneprovider/provider_messages.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([
    translate_value/2,
    translate_resource/2,

    translate_dataset_summary/1,
    translate_distribution/2
]).

% For below types description see interpolate_chunks fun doc
-type chunks_bar_entry() :: {BarNum :: non_neg_integer(), Fill :: non_neg_integer()}.
-type chunks_bar_data() :: [chunks_bar_entry()].
-type file_size() :: non_neg_integer().

-type file_distribution() :: #{binary() => term()}.
%% Full file_distribution format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying concrete binaries. Instead it is shown
%% below:
%%
%% %{
%%      <<"providerId">> := od_provider:id(),
%%      <<"blocks">> := fslogic_blocks:blocks(),
%%      <<"totalBlocksSize">> := non_neg_integer()
%% }
-type distribution_per_provider() :: #{binary() => term()}.
%% Full file_distribution format can't be expressed directly in type spec due to
%% dialyzer limitations in specifying concrete binaries. Instead it is shown
%% below:
%%
%% %{
%%      <<"distributionPerProvider">> := #{
%%          od_provider:id() => #{
%%              <<"chunksBarData">> := #{
%%                  non_neg_integer() := non_neg_integer()   % see chunks_bar_data()
%%              },
%%              <<"blocksPercentage">> := number(),
%%          }
%%      }
%% }

-define(CHUNKS_BAR_WIDTH, 320).

-ifdef(TEST).
-export([interpolate_chunks/2]).
-endif.


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
translate_value(#gri{aspect = children_details, scope = Scope}, {ChildrenDetails, IsLast}) ->
    #{
        <<"children">> => lists:map(fun(ChildDetails) ->
            translate_file_details(ChildDetails, Scope)
        end, ChildrenDetails),
        <<"isLast">> => IsLast
    };

translate_value(#gri{aspect = attrs}, Attrs) ->
    #{<<"attributes">> => Attrs};

translate_value(#gri{aspect = As}, Metadata) when
    As =:= xattrs;
    As =:= json_metadata;
    As =:= rdf_metadata
->
    #{<<"metadata">> => Metadata};

translate_value(#gri{aspect = transfers}, TransfersForFile) ->
    TransfersForFile;

translate_value(#gri{aspect = download_url}, URL) ->
    #{<<"fileUrl">> => URL}.


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{aspect = instance, scope = Scope}, FileDetails) ->
    translate_file_details(FileDetails, Scope);

translate_resource(#gri{aspect = distribution, id = FileGuid, scope = private}, Distribution) ->
    translate_distribution(FileGuid, Distribution);

translate_resource(#gri{aspect = acl, scope = private}, Acl) ->
    try
        #{
            <<"list">> => acl:to_json(Acl, gui)
        }
    catch throw:{error, Errno} ->
        throw(?ERROR_POSIX(Errno))
    end;

translate_resource(#gri{aspect = hardlinks, scope = private}, References) ->
    #{
        <<"hardlinks">> => lists:map(fun(FileGuid) ->
            gri:serialize(#gri{
                type = op_file, id = FileGuid,
                aspect = instance, scope = private
            })
        end, References)
    };

translate_resource(#gri{aspect = {hardlinks, _}, scope = private}, Result) ->
    Result;

translate_resource(#gri{aspect = symlink_target, scope = Scope}, FileDetails) ->
    translate_file_details(FileDetails, Scope);

translate_resource(#gri{aspect = shares, scope = private}, ShareIds) ->
    #{
        <<"list">> => lists:map(fun(ShareId) ->
            gri:serialize(#gri{
                type = op_share,
                id = ShareId,
                aspect = instance,
                scope = private
            })
        end, ShareIds)
    };

translate_resource(#gri{aspect = qos_summary, scope = private}, QosSummaryResponse) ->
    maps:without([<<"status">>], QosSummaryResponse);

translate_resource(#gri{aspect = dataset_summary, scope = private}, DatasetSummary) ->
    translate_dataset_summary(DatasetSummary);

translate_resource(#gri{aspect = archive_recall_details, scope = private}, ArchiveRecallDetails) ->
    translate_archive_recall_details(ArchiveRecallDetails);

translate_resource(#gri{aspect = archive_recall_progress, scope = private}, ArchiveRecallProgress) ->
    #{<<"lastError">> := LastError} = ArchiveRecallProgress,
    ArchiveRecallProgress#{
        <<"lastError">> => utils:undefined_to_null(LastError)
    }.


-spec translate_dataset_summary(dataset_api:file_eff_summary()) -> map().
translate_dataset_summary(#file_eff_dataset_summary{
    direct_dataset = DatasetId,
    eff_ancestor_datasets = EffAncestorDatasets,
    eff_protection_flags = EffProtectionFlags
}) ->
    #{
        <<"directDataset">> => case DatasetId of
            undefined ->
                null;
            _ ->
                gri:serialize(#gri{
                    type = op_dataset, id = DatasetId,
                    aspect = instance, scope = private
                })
        end,
        <<"effAncestorDatasets">> => lists:map(fun(AncestorId) ->
            gri:serialize(#gri{
                type = op_dataset, id = AncestorId, aspect = instance, scope = private
            })
        end, EffAncestorDatasets),
        <<"effProtectionFlags">> => file_meta:protection_flags_to_json(EffProtectionFlags)
    }.


-spec translate_distribution(file_id:file_guid(), [file_distribution()]) ->
    distribution_per_provider().
translate_distribution(FileGuid, PossiblyIncompleteDistribution) ->
    {ok, #file_attr{size = FileSize}} = lfm:stat(?ROOT_SESS_ID, ?FILE_REF(FileGuid)),

    %% @TODO VFS-8935 ultimately, location for each file should be created in each provider
    %% and the list of providers in the distribution should always be complete -
    %% for now, add placeholders with zero blocks for missing providers
    SpaceId = file_id:guid_to_space_id(FileGuid),
    {ok, AllProviders} = space_logic:get_provider_ids(SpaceId),
    IncludedProviders = [P || #{<<"providerId">> := P} <- PossiblyIncompleteDistribution],
    MissingProviders = lists_utils:subtract(AllProviders, IncludedProviders),
    ComplementedDistribution = lists:foldl(fun(ProviderId, Acc) ->
        NoBlocksEntryForProvider = #{
            <<"providerId">> => ProviderId,
            <<"blocks">> => [],
            <<"totalBlocksSize">> => 0
        },
        [NoBlocksEntryForProvider | Acc]
    end, PossiblyIncompleteDistribution, MissingProviders),

    DistributionMap = lists:foldl(fun(#{
        <<"providerId">> := ProviderId,
        <<"blocks">> := Blocks,
        <<"totalBlocksSize">> := TotalBlocksSize
    }, Acc) ->
        Data = lists:foldl(fun({BarNum, Fill}, DataAcc) ->
            DataAcc#{integer_to_binary(BarNum) => Fill}
        end, #{}, interpolate_chunks(Blocks, FileSize)),

        Acc#{ProviderId => #{
            <<"chunksBarData">> => Data,
            <<"blocksPercentage">> => case FileSize of
                0 -> 0;
                _ -> TotalBlocksSize * 100.0 / FileSize
            end
        }}
    end, #{}, ComplementedDistribution),

    #{<<"distributionPerProvider">> => DistributionMap}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec translate_file_details(#file_details{}, gri:scope()) -> map().
translate_file_details(#file_details{
    has_metadata = HasMetadata,
    eff_qos_membership = EffQosMembership,
    active_permissions_type = ActivePermissionsType,
    index_startid = StartId,
    eff_dataset_membership = EffDatasetMembership,
    eff_protection_flags = EffFileProtectionFlags,
    recall_root_id = RecallRootId,
    symlink_value = SymlinkValue,
    file_attr = #file_attr{
        guid = FileGuid,
        name = FileName,
        mode = Mode,
        parent_guid = ParentGuid,
        mtime = MTime,
        type = TypeAttr,
        size = SizeAttr,
        shares = Shares,
        provider_id = ProviderId,
        owner_id = OwnerId,
        nlink = NLink
    }
}, Scope) ->
    PosixPerms = list_to_binary(string:right(integer_to_list(Mode, 8), 3, $0)),
    {Type, Size} = case TypeAttr of
        ?DIRECTORY_TYPE -> {<<"DIR">>, null};
        ?REGULAR_FILE_TYPE -> {<<"REG">>, SizeAttr};
        ?SYMLINK_TYPE -> {<<"SYMLNK">>, SizeAttr}
    end,
    IsRootDir = case file_id:guid_to_share_id(FileGuid) of
        undefined -> fslogic_uuid:is_space_dir_guid(FileGuid);
        ShareId -> lists:member(ShareId, Shares)
    end,
    ParentId = case IsRootDir of
        true -> null;
        false -> ParentGuid
    end,
    BasicPublicFields = #{
        <<"hasMetadata">> => HasMetadata,
        <<"guid">> => FileGuid,
        <<"name">> => FileName,
        <<"index">> => StartId,
        <<"posixPermissions">> => PosixPerms,
        <<"parentId">> => ParentId,
        <<"mtime">> => MTime,
        <<"type">> => Type,
        <<"size">> => Size,
        <<"shares">> => Shares,
        <<"activePermissionsType">> => ActivePermissionsType
    },
    PublicFields = case TypeAttr of
        ?SYMLINK_TYPE ->
            BasicPublicFields#{<<"targetPath">> => SymlinkValue};
        _ ->
            BasicPublicFields
    end,
    PublicFields2 = case archivisation_tree:uuid_to_archive_id(file_id:guid_to_uuid(FileGuid)) of
        undefined -> PublicFields;
        ArchiveId -> PublicFields#{<<"archiveId">> => ArchiveId}
    end,
    case {Scope, EffQosMembership} of
        {public, _} ->
            PublicFields2;
        {private, undefined} -> % all or none effective fields are undefined
            PublicFields2#{
                <<"hardlinksCount">> => utils:undefined_to_null(NLink),
                <<"effProtectionFlags">> => [],
                <<"providerId">> => ProviderId,
                <<"ownerId">> => OwnerId
            };
        {private, _} ->
            PublicFields2#{
                <<"hardlinksCount">> => utils:undefined_to_null(NLink),
                <<"effProtectionFlags">> => file_meta:protection_flags_to_json(
                    EffFileProtectionFlags
                ),
                <<"providerId">> => ProviderId,
                <<"ownerId">> => OwnerId,
                <<"effQosMembership">> => translate_membership(EffQosMembership),
                <<"effDatasetMembership">> => translate_membership(EffDatasetMembership),
                <<"recallRootId">> => utils:undefined_to_null(RecallRootId)
            }
    end.


%% @private
-spec translate_archive_recall_details(archive_recall:record()) -> map().
translate_archive_recall_details(#archive_recall_details{
    archive_id = ArchiveId,
    dataset_id = DatasetId,
    start_timestamp = StartTimestamp,
    finish_timestamp = FinishTimestamp,
    cancel_timestamp = CancelTimestamp,
    total_file_count = TargetFileCount,
    total_byte_size = TargetByteSize
}) ->
    #{
        <<"archive">> => gri:serialize(#gri{
            type = op_archive, id = ArchiveId, aspect = instance, scope = private}),
        <<"dataset">> => gri:serialize(#gri{
            type = op_dataset, id = DatasetId, aspect = instance, scope = private}),
        <<"startTime">> => utils:undefined_to_null(StartTimestamp),
        <<"finishTime">> => utils:undefined_to_null(FinishTimestamp),
        <<"cancelTime">> => utils:undefined_to_null(CancelTimestamp),
        <<"totalFileCount">> => TargetFileCount,
        <<"totalByteSize">> => TargetByteSize
    }.


%% @private
-spec translate_membership(file_qos:membership() | dataset:membership()) -> binary().
translate_membership(?NONE_MEMBERSHIP) -> <<"none">>;
translate_membership(?DIRECT_MEMBERSHIP) -> <<"direct">>;
translate_membership(?ANCESTOR_MEMBERSHIP) -> <<"ancestor">>;
translate_membership(?DIRECT_AND_ANCESTOR_MEMBERSHIP) -> <<"directAndAncestor">>.


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
-spec interpolate_chunks(Blocks :: [[non_neg_integer()]], file_size()) ->
    chunks_bar_data().
interpolate_chunks([], _) ->
    [{0, 0}];
interpolate_chunks(_, 0) ->
    [{0, 0}];
interpolate_chunks(Blocks, FileSize) when FileSize < ?CHUNKS_BAR_WIDTH ->
    interpolate_chunks(
        [[O * ?CHUNKS_BAR_WIDTH, S * ?CHUNKS_BAR_WIDTH] || [O, S] <- Blocks],
        FileSize * ?CHUNKS_BAR_WIDTH
    );
interpolate_chunks(Blocks, FileSize) ->
    interpolate_chunks(
        lists:reverse(Blocks),
        FileSize,
        ?CHUNKS_BAR_WIDTH - 1,
        0,
        []
    ).


% Macros for more concise code, depending on variables in below functions.
-define(bar_start, floor(FileSize / ?CHUNKS_BAR_WIDTH * BarNum)).
-define(bar_end, floor(FileSize / ?CHUNKS_BAR_WIDTH * (BarNum + 1))).
-define(bar_size, (?bar_end - ?bar_start)).

%% @private
-spec interpolate_chunks(
    ReversedBlocks :: [[non_neg_integer()]], % File blocks passed to this fun should be in reverse order
    file_size(),
    BarNum :: non_neg_integer(),
    BytesAcc :: non_neg_integer(),
    chunks_bar_data()
) ->
    chunks_bar_data().
interpolate_chunks([], _FileSize, -1, _BytesAcc, ResChunks) ->
    ResChunks;
interpolate_chunks([], _FileSize, _BarNum, 0, ResChunks) ->
    merge_chunks({0, 0}, ResChunks);
interpolate_chunks([], FileSize, BarNum, BytesAcc, ResChunks) ->
    Fill = round(BytesAcc * 100 / ?bar_size),
    interpolate_chunks(
        [],
        ?bar_start,
        BarNum - 1,
        0,
        merge_chunks({BarNum, Fill}, ResChunks)
    );
interpolate_chunks([[Offset, Size] | PrevBlocks], FileSize, BarNum, BytesAcc, ResChunks) when ?bar_start < Offset ->
    interpolate_chunks(PrevBlocks, FileSize, BarNum, BytesAcc + Size, ResChunks);
interpolate_chunks([[Offset, Size] | PrevBlocks], FileSize, BarNum, BytesAcc, ResChunks) when Offset + Size > ?bar_start ->
    SizeInBar = Offset + Size - ?bar_start,
    Fill = round((BytesAcc + SizeInBar) * 100 / ?bar_size),
    interpolate_chunks(
        [[Offset, Size - SizeInBar] | PrevBlocks],
        FileSize,
        BarNum - 1,
        0,
        merge_chunks({BarNum, Fill}, ResChunks)
    );
interpolate_chunks([[Offset, Size] | PrevBlocks], FileSize, BarNum, BytesAcc, ResChunks) -> % Offset + Size =< ?bar_start
    Fill = round(BytesAcc * 100 / ?bar_size),
    interpolate_chunks(
        [[Offset, Size] | PrevBlocks],
        FileSize,
        BarNum - 1,
        0,
        merge_chunks({BarNum, Fill}, ResChunks)
    ).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Merges adjacent chunks with the same fill value,
%% otherwise prepends the new chunk.
%% @end
%%--------------------------------------------------------------------
-spec merge_chunks(chunks_bar_entry(), chunks_bar_data()) ->
    chunks_bar_data().
merge_chunks({BarNum, Fill}, [{_, Fill} | Tail]) ->
    [{BarNum, Fill} | Tail];
merge_chunks({BarNum, Fill}, Result) ->
    [{BarNum, Fill} | Result].
