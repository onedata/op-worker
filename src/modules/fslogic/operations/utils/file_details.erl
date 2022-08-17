%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon, Michal Stanisz
%%% @copyright (C) 2016-2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for calculating file_details (see file_details.hrl).
%%% @end
%%%--------------------------------------------------------------------
-module(file_details).
-author("Tomasz Lichon").
-author("Michal Stanisz").

-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/fslogic/metadata.hrl").

%% API
-export([
    resolve/2
]).

-type record() :: #file_details{}.
-type effective_values() :: #{
    effective_qos_membership => file_qos:membership(),
    effective_dataset_membership => dataset:membership(), 
    effective_protection_flags => data_access_control:bitmask(),
    effective_recall => file_id:file_guid()
}.

-define(DEFAULT_REFERENCES_LIMIT, 100).

%%%===================================================================
%%% API
%%%===================================================================

-spec resolve(user_ctx:ctx(), file_ctx:ctx()) ->
    record().
resolve(UserCtx, FileCtx) ->
    ResolveAttrOpts = #{
        allow_deleted_files => false,
        name_conflicts_resolution_policy => resolve_name_conflicts,
        include_optional_attrs => [size, link_count]
    },
    {#file_attr{name = FileAttrName} = FileAttr, FileDoc, _, FileCtx2} =
        file_attr:resolve(UserCtx, FileCtx, ResolveAttrOpts),
    {ok, ActivePermissionsType} = file_meta:get_active_perms_type(FileDoc),
    
    {EffectiveValues, FileCtx3} = resolve_effective_values(
        FileCtx2, FileDoc, ?DEFAULT_REFERENCES_LIMIT),
    
    Uuid = file_ctx:get_logical_uuid_const(FileCtx3),
    #file_details{
        file_attr = FileAttr,
        symlink_value = resolve_symlink_value(FileDoc, Uuid),
        active_permissions_type = ActivePermissionsType,
        has_metadata = has_metadata(FileCtx3),
        eff_qos_membership = maps:get(effective_qos_membership, EffectiveValues, undefined),
        eff_dataset_membership = maps:get(effective_dataset_membership, EffectiveValues, undefined),
        eff_protection_flags = maps:get(effective_protection_flags, EffectiveValues, undefined),
        recall_root_id = maps:get(effective_recall, EffectiveValues, undefined),
        conflicting_name = resolve_conflicting_name(FileDoc, Uuid, FileAttrName)
    }.


%% @private
-spec has_metadata(file_ctx:ctx()) -> boolean().
has_metadata(FileCtx) ->
    RootUserCtx = user_ctx:new(?ROOT_SESS_ID),
    {ok, XattrList} = xattr:list_insecure(RootUserCtx, FileCtx, false, true),
    lists:any(fun
        (<<?CDMI_PREFIX_STR, _/binary>>) -> false;
        (?JSON_METADATA_KEY) -> true;
        (?RDF_METADATA_KEY) -> true;
        (<<?ONEDATA_PREFIX_STR, _/binary>>) -> false;
        (_) -> true
    end, XattrList).


%% @private
-spec resolve_effective_values(file_ctx:ctx(), file_meta:doc(), non_neg_integer()) ->
    {effective_values(), file_ctx:ctx()}.
resolve_effective_values(FileCtx, FileDoc, ReferencesLimit) ->
    ShouldCalculateEffectiveValues = case ReferencesLimit of
        infinity -> 
            true;
        _ ->
            case file_meta_hardlinks:count_references(FileDoc) of
                {ok, LinksCount} -> LinksCount =< ReferencesLimit;
                _ -> false
            end
    end,
    case ShouldCalculateEffectiveValues of
        true -> calculate_effective_values(FileCtx);
        false -> {#{}, FileCtx}
    end.


%% @private
-spec calculate_effective_values(file_ctx:ctx()) -> {effective_values(), file_ctx:ctx()}.
calculate_effective_values(FileCtx) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    EffectiveQoSMembership = file_qos:qos_membership(FileDoc),
    {ok, EffectiveDatasetMembership, EffectiveProtectionFlags, FileCtx3} =
        dataset_api:get_effective_membership_and_protection_flags(FileCtx2),
    EffectiveRecallRootGuid = case archive_recall:get_effective_recall(FileDoc) of
        {ok, undefined} -> undefined;
        {ok, Uuid} -> file_id:pack_guid(Uuid, file_ctx:get_space_id_const(FileCtx))
    end,
    {#{
        effective_qos_membership => EffectiveQoSMembership,
        effective_dataset_membership => EffectiveDatasetMembership,
        effective_protection_flags => EffectiveProtectionFlags,
        effective_recall => EffectiveRecallRootGuid
    }, FileCtx3}.


%% @private
-spec resolve_symlink_value(file_meta:doc(), file_meta:uuid()) -> 
    file_meta_symlinks:symlink() | undefined.
resolve_symlink_value(FileDoc, Uuid) ->
    case fslogic_file_id:is_symlink_uuid(Uuid) of
        true ->
            {ok, SymlinkValue} = file_meta_symlinks:readlink(FileDoc),
            SymlinkValue;
        false ->
            undefined
    end.


%% @private
-spec resolve_conflicting_name(file_meta:doc(), file_meta:uuid(), file_meta:name()) -> 
    file_meta:name().
resolve_conflicting_name(FileDoc, Uuid, FileAttrName) ->
    case {fslogic_file_id:is_space_dir_uuid(Uuid), file_meta:get_name(FileDoc)} of
        {true, _} -> undefined;
        {false, FileAttrName} -> undefined;
        {false, ConflictingName} -> ConflictingName
    end.