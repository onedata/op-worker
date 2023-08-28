%%%--------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for calculating file_attrs (see file_attr.hrl).
%%% Attrs are resolved in independent stages that require different documents to be
%%% fetched in order to optimize attrs calculation. Only stages that resolve attrs
%%% that where requested are calculated.
%%% @end
%%%--------------------------------------------------------------------
-module(file_attr).
-author("Michal Stanisz").

-include("modules/dir_stats_collector/dir_size_stats.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").
-include_lib("ctool/include/privileges.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([
    resolve/3,
    should_fetch_xattrs/1
]).

-type attribute() :: guid | active_permissions_type | index | mode | owner_id | parent_guid | provider_id | shares |
    symlink_value | type | link_count | is_deleted | name | conflicting_name | conflicting_files | atime | mtime |
    ctime | local_replication_rate | size | is_fully_replicated | gid | uid | recall_root_id | eff_dataset_membership |
    eff_dataset_protection_flags | eff_protection_flags | archive_id | eff_qos_membership | qos_status | has_metadata |
    {xattrs, [custom_metadata:name()]}.

-type name_conflicts_resolution_policy() ::
    resolve_name_conflicts |
    allow_name_conflicts.

-type resolve_opts() :: #{
    % Tells which attributes are to be calculated.
    attributes := [attribute()],
    % Tells whether to calculate attr even if file was recently removed (default: false).
    allow_deleted_files => boolean(),
    % Tells whether to perform a check if file name collide with other files in
    % directory. If it does suffix will be glued to name to differentiate it
    % and conflicting files will be returned (default: resolve_name_conflicts).
    name_conflicts_resolution_policy => name_conflicts_resolution_policy()
}.

-type file_attr() :: #file_attr{}.

-export_type([file_attr/0, resolve_opts/0, attribute/0]).

-record(state, {
    file_ctx :: file_ctx:ctx(),
    user_ctx :: user_ctx:ctx(),
    options :: resolve_opts(),
    current_stage_attrs :: [attribute()] | [custom_metadata:key()]
}).

-type state() :: #state{}.
-type stage_type() :: direct | effective.

% xattrs are handled specially (see resolve_state/3)
-define(XATTRS_STAGE, xattrs).

-define(STAGES, [
    {?FILE_META_ATTRS, direct, fun resolve_file_meta_attrs/1},
    {?LINKS_ATTRS, direct, fun resolve_name_attrs/1},
    {?TIMES_ATTRS, direct, fun resolve_times_attrs/1},
    {?LOCATION_ATTRS, direct, fun resolve_location_attrs/1},
    {?LUMA_ATTRS, direct, fun resolve_luma_attrs/1},
    {?DATASET_ATTRS, effective, fun resolve_dataset_attrs/1},
    {?ARCHIVE_RECALL_ATTRS, direct, fun resolve_archive_recall_attrs/1},
    {?QOS_STATUS_ATTRS, effective, fun resolve_qos_status_attrs/1},
    {?QOS_EFF_VALUE_ATTRS, effective, fun resolve_qos_eff_value_attrs/1},
    {?METADATA_ATTRS, direct, fun resolve_metadata_attrs/1},
    {?XATTRS_STAGE, direct, fun resolve_xattrs/1}
]).

% do not calculate effective values attrs when references limit is exceeded
-define(REFERENCES_LIMIT, 100).
-define(EFFECTIVE_VALUE_ATTRS, ?DATASET_ATTRS ++ ?QOS_STATUS_ATTRS ++ ?QOS_EFF_VALUE_ATTRS).


%%%===================================================================
%%% API
%%%===================================================================

-spec resolve(user_ctx:ctx(), file_ctx:ctx(), resolve_opts()) -> {file_attr(), file_ctx:ctx()}.
resolve(UserCtx, FileCtx, #{attributes := RequestedAttributes} = Opts) ->
    ExpandedRequestedAttrs = case lists_utils:intersect(?EFFECTIVE_VALUE_ATTRS, RequestedAttributes) of
        [] -> RequestedAttributes;
        _ -> lists:usort([link_count | RequestedAttributes])
    end,
    InitialState = #state{
        file_ctx = FileCtx,
        user_ctx = UserCtx,
        options = Opts#{attributes => ExpandedRequestedAttrs}
    },
    {FinalState, FinalFileAttr} = lists:foldl(fun({AttrsSubset, Type, StageFun}, {AccState, AccFileAttr}) ->
        {StageState, StageFileAtrr} = resolve_stage(Type, AccState, AccFileAttr, AttrsSubset, StageFun),
        {StageState, merge_records(AccFileAttr, StageFileAtrr)}
    end, {InitialState, #file_attr{guid = file_ctx:get_logical_guid_const(FileCtx)}}, ?STAGES),
    {FinalFileAttr, FinalState#state.file_ctx}.


-spec should_fetch_xattrs([attribute()] | resolve_opts()) -> {true, [custom_metadata:name()]} | false.
should_fetch_xattrs(#{attributes := AttributesList}) ->
    should_fetch_xattrs(AttributesList);
should_fetch_xattrs(AttributesList) ->
    case lists:keyfind(xattrs, 1, AttributesList) of
        {xattrs, XattrNames} -> {true, XattrNames};
        false -> false
    end.


%%%===================================================================
%%% Stage functions
%%%===================================================================

%% @private
-spec resolve_file_meta_attrs(state()) -> {state(), file_attr()}.
resolve_file_meta_attrs(#state{user_ctx = UserCtx, current_stage_attrs = Attrs} = State) ->
    {FileDoc, #state{file_ctx = FileCtx} = UpdatedState} = get_file_doc(State),
    {ok, ActivePermissionsType} = file_meta:get_active_perms_type(FileDoc),
    ShareId = file_ctx:get_share_id_const(FileCtx),
    BaseAttrs = case ShareId of
        undefined -> resolve_private_base_attrs(UserCtx, file_ctx:get_space_id_const(FileCtx), FileDoc);
        _ -> get_masked_private_base_attrs(ShareId, FileDoc)
    end,
    {ParentGuid, FileCtx2} = file_tree:get_parent_guid_if_not_root_dir(FileCtx, UserCtx),
    
    {UpdatedState#state{file_ctx = FileCtx2}, BaseAttrs#file_attr{
        active_permissions_type = ActivePermissionsType,
        index = build_index(FileCtx, FileDoc),
        %% @TODO do reviewerów - czy nie chcielibyśmy zrobić haka, że jak plik jest rootem archiwum (mamy to za darmo, vide archive_id),
        %% to wtedy nie dajemy parenta (czyli logicznie byśmy odpieli archiwa od filestytemu spejsa)
        %% Pytanie czy to może mieć niepożądane konsekwencje?
        parent_guid = ParentGuid,
        symlink_value = resolve_symlink_value(FileDoc),
        type = file_meta:get_effective_type(FileDoc),
        link_count = resolve_link_count(FileCtx2, ShareId, Attrs),
        is_deleted = file_meta:is_deleted(FileDoc)
    }}.


%% @private
-spec resolve_name_attrs(state()) -> {state(), file_attr()}.
resolve_name_attrs(#state{file_ctx = FileCtx, user_ctx = UserCtx} = State) ->
    case file_ctx:is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            {FileName, FileCtx2} = file_ctx:get_aliased_name(FileCtx, UserCtx),
            {State#state{file_ctx = FileCtx2}, #file_attr{
                name = FileName
            }};
        false ->
            resolve_name_attrs_internal(State)
    end.


%% @private
-spec resolve_times_attrs(state()) -> {state(), file_attr()}.
resolve_times_attrs(#state{file_ctx = FileCtx} = State) ->
    {{ATime, CTime, MTime}, FileCtx2} = file_ctx:get_times(FileCtx),
    {State#state{file_ctx = FileCtx2}, #file_attr{
        atime = ATime,
        ctime = CTime,
        mtime = MTime
    }}.


%% @private
-spec resolve_location_attrs(state()) -> {state(), file_attr()}.
resolve_location_attrs(#state{file_ctx = FileCtx} = State) ->
    {Type, FileCtx2} = file_ctx:get_effective_type(FileCtx),
    UpdatedState = State#state{file_ctx = FileCtx2},
    case Type of
        ?REGULAR_FILE_TYPE -> resolve_location_attrs_reg_file(UpdatedState);
        ?DIRECTORY_TYPE -> resolve_location_attrs_dir(UpdatedState);
        ?SYMLINK_TYPE -> resolve_location_attrs_symlink(UpdatedState)
    end.


%% @private
-spec resolve_luma_attrs(state()) -> {state(), file_attr()}.
resolve_luma_attrs(#state{file_ctx = FileCtx} = State) ->
    case file_id:guid_to_share_id(file_ctx:get_logical_guid_const(FileCtx)) of
        undefined ->
            {{Uid, Gid}, FileCtx2} = file_ctx:get_display_credentials(FileCtx),
            {State#state{file_ctx = FileCtx2}, #file_attr{
                uid = Uid,
                gid = Gid
            }};
        _ ->
            {State, #file_attr{
                uid = ?SHARE_UID,
                gid = ?SHARE_GID
            }}
    end.


%% @private
-spec resolve_dataset_attrs(state()) -> {state(), file_attr()}.
resolve_dataset_attrs(#state{file_ctx = FileCtx} = State) ->
    {ok, EffectiveDatasetMembership, EffectiveProtectionFlags, EffDatasetProtectionFlags, FileCtx2} =
        dataset_api:get_effective_membership_and_protection_flags(FileCtx),
    {State#state{file_ctx = FileCtx2}, #file_attr{
        eff_dataset_membership = EffectiveDatasetMembership,
        eff_protection_flags = EffectiveProtectionFlags,
        eff_dataset_protection_flags = EffDatasetProtectionFlags,
        archive_id = archivisation_tree:uuid_to_archive_id(file_ctx:get_logical_uuid_const(FileCtx2))
    }}.


%% @private
-spec resolve_archive_recall_attrs(state()) -> {state(), file_attr()}.
resolve_archive_recall_attrs(State) ->
    {FileMetaDoc, #state{file_ctx = FileCtx} = UpdatedState} = get_file_doc(State),
    EffectiveRecallRootGuid = case archive_recall:get_effective_recall(FileMetaDoc) of
        {ok, undefined} -> undefined;
        {ok, Uuid} -> file_id:pack_guid(Uuid, file_ctx:get_space_id_const(FileCtx))
    end,
    {UpdatedState, #file_attr{
        recall_root_id = EffectiveRecallRootGuid
    }}.


%% @private
-spec resolve_qos_status_attrs(state()) -> {state(), file_attr()}.
resolve_qos_status_attrs(#state{file_ctx = FileCtx} = State) ->
    QosStatus = case qos_req:get_effective_file_qos_insecure(FileCtx) of
        {ok, {QosEntriesWithStatus, _}} when map_size(QosEntriesWithStatus) == 0 ->
            undefined;
        {ok, {QosEntriesWithStatus, _}} ->
            qos_status:aggregate(maps:values(QosEntriesWithStatus));
        {error, _} ->
            unknown
    end,
    {State, #file_attr{qos_status = QosStatus}}.


%% @private
-spec resolve_qos_eff_value_attrs(state()) -> {state(), file_attr()}.
resolve_qos_eff_value_attrs(State) ->
    {FileDoc, UpdatedState} = get_file_doc(State),
    EffectiveQoSMembership = file_qos:qos_membership(FileDoc),
    {UpdatedState, #file_attr{eff_qos_membership = EffectiveQoSMembership}}.


%% @private
-spec resolve_metadata_attrs(state()) -> {state(), file_attr()}.
resolve_metadata_attrs(#state{file_ctx = FileCtx} = State) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    {ok, XattrList} = xattr:list_insecure(RootUserCtx, FileCtx, false, true),
    {State, #file_attr{
        has_metadata = lists:any(fun
            (<<?CDMI_PREFIX_STR, _/binary>>) -> false;
            (?JSON_METADATA_KEY) -> true;
            (?RDF_METADATA_KEY) -> true;
            (<<?ONEDATA_PREFIX_STR, _/binary>>) -> false;
            (_) -> true
        end, XattrList)
    }}.


%% @private
-spec resolve_xattrs(state()) -> {state(), file_attr()}.
resolve_xattrs(#state{file_ctx = FileCtx, current_stage_attrs = XattrNames} = State) ->
    {ok, AllDirectXattrs} = xattr:get_all_direct_insecure(FileCtx),
    {State, #file_attr{xattrs = lists:foldl(fun(Xattr, Acc) ->
        Acc#{Xattr => maps:get(Xattr, AllDirectXattrs, undefined)}
    end, #{}, XattrNames)}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec resolve_stage(stage_type(), state(), file_attr(), [attribute()], fun((state()) -> {state(), file_attr()})) ->
    {state(), file_attr()}.
resolve_stage(effective, State, #file_attr{link_count = LinkCount}, _StageAttrs, _StageFun)
    when LinkCount > ?REFERENCES_LIMIT
->
    {State, #file_attr{}};
resolve_stage(direct, #state{options = #{attributes := RequestedAttrs}} = State, _FileAttr, ?XATTRS_STAGE, StageFun) ->
    case should_fetch_xattrs(RequestedAttrs) of
        {true, XattrsNames} ->
            StageFun(State#state{current_stage_attrs = XattrsNames});
        false ->
            {State, #file_attr{}}
    end;
resolve_stage(_, #state{options = #{attributes := RequestedAttrs}} = State, _FileAttr, StageAttrs, StageFun) ->
    case lists_utils:intersect(RequestedAttrs, StageAttrs) of
        [] ->
            {State, #file_attr{}};
        AttrsToCalculate ->
            StageFun(State#state{current_stage_attrs = AttrsToCalculate})
    end.


%% @private
-spec resolve_symlink_value(file_meta:doc()) -> file_meta_symlinks:symlink() | undefined.
resolve_symlink_value(#document{key = Uuid} = FileDoc) ->
    case fslogic_file_id:is_symlink_uuid(Uuid) of
        true ->
            {ok, SymlinkValue} = file_meta_symlinks:readlink(FileDoc),
            SymlinkValue;
        false ->
            undefined
    end.


%% @private
-spec build_index(file_ctx:ctx(), file_meta:doc()) -> file_listing:index().
build_index(FileCtx, FileDoc) ->
    case file_ctx:is_space_dir_const(FileCtx) of
        true ->
            % As provider id in space doc is random (depends on which provider called `file_meta:make_space_exist/0`)
            % use only space id in index (there are no conflicts on spaces between providers, so it is not a problem).
            file_listing:build_index(file_meta:get_name(FileDoc));
        false ->
            file_listing:build_index(
                file_meta:get_name(FileDoc), file_meta:get_provider_id(FileDoc))
    end.


%% @private
-spec resolve_private_base_attrs(user_ctx:ctx(), od_space:id(), file_meta:doc()) ->
    file_attr().
resolve_private_base_attrs(UserCtx, SpaceId, #document{value = #file_meta{shares = Shares} = FM}) ->
    VisibleShares = case user_ctx:is_root(UserCtx) of
        true ->
            Shares;
        false ->
            UserId = user_ctx:get_user_id(UserCtx),
            case space_logic:has_eff_privilege(SpaceId, UserId, ?SPACE_VIEW) of
                true -> Shares;
                false -> []
            end
    end,
    #file_attr{
        mode = FM#file_meta.mode,
        provider_id = FM#file_meta.provider_id,
        owner_id = FM#file_meta.owner,
        shares = VisibleShares
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns masked private attrs values when accessing file via share guid (e.g.
%% in share mode only last 3 bits of mode - 'other' bits - should be visible).
%%
%% NOTE !!!
%% ShareId is added to file_meta.shares only for directly shared
%% files/directories and not their children, so not every file
%% accessed via share guid will have ShareId in `file_attrs.shares`
%% @end
%%--------------------------------------------------------------------
-spec get_masked_private_base_attrs(od_share:id(), file_meta:doc()) ->
    file_attr().
get_masked_private_base_attrs(ShareId, #document{value = #file_meta{
    mode = RealMode,
    shares = AllShares
}}) ->
    #file_attr{
        mode = RealMode band 2#111,
        provider_id = <<"unknown">>,
        owner_id = <<"unknown">>,
        shares = case lists:member(ShareId, AllShares) of
            true -> [ShareId];
            false -> []
        end
    }.


%% @private
-spec resolve_link_count(file_ctx:ctx(), od_share:id(), [attribute()]) ->
    non_neg_integer() | undefined.
resolve_link_count(FileCtx, ShareId, Attrs) ->
    case {ShareId, lists:member(link_count, Attrs)} of
        {undefined, true} ->
            {ok, LinkCount} = file_ctx:count_references_const(FileCtx),
            LinkCount;
        _ ->
            undefined
    end.


%% @private
-spec resolve_location_attrs_reg_file(state()) -> {state(), file_attr()}.
resolve_location_attrs_reg_file(#state{file_ctx = FileCtx, current_stage_attrs = RequestedAttrs} = State) ->
    {Size, FileCtx2} = file_ctx:get_file_size(FileCtx),
    {OptionalAttr, FileCtx3} = case {are_any_attrs_requested([local_replication_rate, is_fully_replicated], State), Size} of
        {true, 0} ->
            {#file_attr{
                is_fully_replicated = true,
                local_replication_rate = 1.0
            }, FileCtx2};
        {true, _} ->
            {FLDoc, FC3} = file_ctx:get_local_file_location_doc(FileCtx2),
            ResAttr = lists:foldl(fun
                %% @TODO do MW - czy zliczanie wszystkich bloków zamiast tylko patrzenie czy są 2 i potem porównianie
                %% do size'a byłoby dopuszczalne z punktu widzienia performancu? I czy nie ma tam jakichś możliwych rejsów?
                %% (chodzi o to, żeby liczyć tylko local replication rate i potem na translacji dla clienta porównywać, czy jest 1)
                (local_replication_rate, AccAttr) ->
                    AccAttr#file_attr{local_replication_rate = file_location:count_bytes(FLDoc)/Size};
                (is_fully_replicated, AccAttr) ->
                    AccAttr#file_attr{is_fully_replicated = fslogic_location_cache:is_fully_replicated(FLDoc, Size)};
                (_, AccAttr) ->
                    AccAttr
            end, #file_attr{}, RequestedAttrs),
            {ResAttr, FC3};
        {false, _} ->
            {#file_attr{}, FileCtx2}
    end,
    {State#state{file_ctx = FileCtx3}, OptionalAttr#file_attr{size = Size}}.


%% @private
-spec resolve_location_attrs_dir(state()) -> {state(), file_attr()}.
resolve_location_attrs_dir(#state{file_ctx = FileCtx, user_ctx = UserCtx} = State) ->
    case file_ctx:is_user_root_dir_const(FileCtx, UserCtx) of
        true ->
            {0, 0};
        _ ->
            Guid = file_ctx:get_logical_guid_const(FileCtx),
            ShouldCalculateRatio = are_any_attrs_requested([local_replication_rate], State),
            {StatsToGet, FileCtx2} = case ShouldCalculateRatio of
                true ->
                    {StorageId, FC2} = file_ctx:get_storage_id(FileCtx),
                    {[?TOTAL_SIZE, ?SIZE_ON_STORAGE(StorageId)], FC2};
                false ->
                    {[?TOTAL_SIZE], FileCtx}
            end,
            StatsResult = case dir_size_stats:get_stats(Guid, StatsToGet) of
                {ok, StatsMap} -> StatsMap;
                ?ERROR_NOT_FOUND -> #{};
                ?ERROR_DIR_STATS_DISABLED_FOR_SPACE -> error;
                ?ERROR_DIR_STATS_NOT_READY -> error
            end,
            case StatsResult of
                error ->
                    {State#state{file_ctx = FileCtx2}, #file_attr{}};
                _ ->
                    {State#state{file_ctx = FileCtx2}, build_dir_size_attr(StatsResult, ShouldCalculateRatio, FileCtx2)}
            end
    end.


%% @private
-spec build_dir_size_attr(map(), boolean(), file_ctx:ctx()) -> file_attr().
build_dir_size_attr(StatsResult, ShouldCalculateRatio, FileCtx) ->
    Size = maps:get(?TOTAL_SIZE, StatsResult, 0),
    OptionalAttr = case ShouldCalculateRatio of
        true ->
            % storage id is already cached in file_ctx
            {StorageId, _} = file_ctx:get_storage_id(FileCtx),
            StorageSize = maps:get(?SIZE_ON_STORAGE(StorageId), StatsResult, 0),
            #file_attr{local_replication_rate = case Size of
                0 -> 1.0;
                _ -> StorageSize/Size
            end};
        false ->
            #file_attr{}
    end,
    OptionalAttr#file_attr{size = Size}.


%% @private
-spec resolve_location_attrs_symlink(state()) -> {state(), file_attr()}.
resolve_location_attrs_symlink(State) ->
    {FileDoc, UpdatedState} = get_file_doc(State),
    {ok, Symlink} = file_meta_symlinks:readlink(FileDoc),
    {UpdatedState, #file_attr{
        size = byte_size(Symlink)
    }}.


%% @private
-spec resolve_name_attrs_internal(state()) -> {state(), file_attr()}.
resolve_name_attrs_internal(#state{file_ctx = FileCtx, user_ctx = UserCtx} = State) ->
    FileName = file_ctx:get_aliased_name(FileCtx, UserCtx),
    
    ShouldCalculateConflicts =
        are_any_attrs_requested([conflicting_name, conflicting_files], State) orelse
            read_option(name_conflicts_resolution_policy, State, resolve_name_conflicts) == resolve_name_conflicts,

    case ShouldCalculateConflicts of
        false ->
            {State, #file_attr{name = FileName}};
        _ ->
            resolve_name_attrs_conflicts(State)
    end.


%% @private
-spec resolve_name_attrs_conflicts(state()) -> {state(), file_attr()}.
resolve_name_attrs_conflicts(State) ->
    {FileDoc, #state{file_ctx = FileCtx} = UpdatedState} = get_file_doc(State),
    {ok, ParentUuid} = file_meta:get_parent_uuid(FileDoc),
    FileName = file_meta:get_name(FileDoc),
    ProviderId = file_meta:get_provider_id(FileDoc),
    Scope = file_meta:get_scope(FileDoc),
    {ok, FileUuid} = file_meta:get_uuid(FileDoc),
    case file_meta:check_name_and_get_conflicting_files(ParentUuid, FileName, FileUuid, ProviderId, Scope) of
        {conflicting, ExtendedName, ConflictingFiles} ->
            {UpdatedState#state{file_ctx = file_ctx:cache_name(ExtendedName, FileCtx)}, #file_attr{
                name = ExtendedName,
                conflicting_name = FileName,
                conflicting_files = ConflictingFiles
            }};
        _ ->
            {UpdatedState, #file_attr{name = FileName}}
    end.


%% @private
-spec get_file_doc(state()) -> {file_meta:doc(), state()}.
get_file_doc(#state{file_ctx = FileCtx} = State) ->
    {FileDoc, FileCtx2} = case read_option(allow_deleted_files, State, false) of
        true ->
            file_ctx:get_and_cache_file_doc_including_deleted(FileCtx);
        false ->
            file_ctx:get_file_doc(FileCtx)
    end,
    {FileDoc, State#state{file_ctx = FileCtx2}}.


%% @private
-spec merge_records(tuple(), tuple()) -> tuple().
merge_records(RecordA, RecordB) ->
    list_to_tuple(lists:map(fun
        ({undefined, ValB}) -> ValB;
        ({[], ValB}) when is_list(ValB) -> ValB;
        ({ValA, _}) -> ValA
    end, lists:zip(tuple_to_list(RecordA), tuple_to_list(RecordB)))).


%% @private
-spec are_any_attrs_requested([attribute()], state()) -> boolean().
are_any_attrs_requested(Attrs, #state{current_stage_attrs = RequestedAttrs}) ->
    lists_utils:intersect(Attrs, RequestedAttrs) =/= [].


%% @private
-spec read_option(atom(), state(), any()) -> any().
read_option(Option, #state{options = Options}, Default) ->
    maps:get(Option, Options, Default).