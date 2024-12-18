%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions operating on files used in ct tests.
%%%
%%% FUTURE IMPROVEMENTS:
%%% * flag to ensure files are created on storage
%%% @end
%%%-------------------------------------------------------------------
-module(onenv_file_test_utils).
-author("Bartosz Walkowicz").

-include("onenv_test_utils.hrl").
-include("distribution_assert.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/errors.hrl").


-export([
    resolve_file/1,
    ls/4,
    create_file_tree/4,
    create_and_sync_file_tree/3, create_and_sync_file_tree/4,
    mv_and_sync_file/3, rm_and_sync_file/2, await_file_metadata_sync/3, prepare_symlink_value/3
]).
-export([get_object_attributes/3]).
-type share_spec() :: #share_spec{}.

-type space_selector() :: oct_background:entity_selector().
-type object_selector() :: file_id:file_guid() | space_selector().
-type object_spec() :: #file_spec{} | #dir_spec{} | #symlink_spec{} | metadata_spec().

-type object() :: #object{}.
-type metadata_object() :: #metadata_object{}.
-type metadata_spec() :: #metadata_spec{}.

-type custom_labels_map() :: #{any() => file_id:file_guid()}.
-type custom_label() :: any().

-export_type([share_spec/0, space_selector/0, object_selector/0, object_spec/0, object/0,
    metadata_spec/0, metadata_object/0, custom_label/0
]).

-define(LS_SIZE, 1000).

-define(ATTEMPTS, 60).


%%%===================================================================
%%% API
%%%===================================================================


-spec resolve_file(object_selector()) -> {file_id:file_guid(), od_space:id()}.
resolve_file(FileSelector) ->
    try
        SpaceId = oct_background:get_space_id(FileSelector),
        {fslogic_file_id:spaceid_to_space_dir_guid(SpaceId), SpaceId}
    catch error:{badkeys, _} ->
        {FileSelector, file_id:guid_to_space_id(FileSelector)}
    end.


-spec ls(oct_background:entity_selector(), object_selector(), file_listing:offset(), file_listing:limit()) ->
    {ok, [{fslogic_worker:file_guid(), file_meta:name()}]} | lfm:error_reply().
ls(UserSelector, FileSelector, Offset, Limit) ->
    {FileGuid, SpaceId} = resolve_file(FileSelector),
    ProviderId = ?RAND_ELEMENT(oct_background:get_space_supporting_providers(
        SpaceId
    )),
    Node = oct_background:get_random_provider_node(ProviderId),
    UserSessionId = oct_background:get_user_session_id(UserSelector, ProviderId),

    lfm_proxy:get_children(Node, UserSessionId, ?FILE_REF(FileGuid), Offset, Limit).


-spec create_file_tree(od_user:id(), file_id:file_guid(), oct_background:entity_selector(), object_spec()) ->
    object().
create_file_tree(UserId, ParentGuid, CreationProvider, FileDesc) ->
    {Object, _} = create_file_tree(UserId, ParentGuid, CreationProvider, FileDesc, parallel, #{}),
    Object.


-spec create_and_sync_file_tree(
    oct_background:entity_selector(),
    object_selector(),
    object_spec() | [object_spec()]
) ->
    object() | [object()].
create_and_sync_file_tree(UserSelector, ParentSelector, FileDesc) ->
    {_ParentGuid, SpaceId} = resolve_file(ParentSelector),
    [CreationProvider | _] = oct_background:get_space_supporting_providers(
        SpaceId
    ),
    create_and_sync_file_tree(UserSelector, ParentSelector, FileDesc, CreationProvider).


-spec create_and_sync_file_tree(
    oct_background:entity_selector(),
    object_selector(),
    object_spec() | [object_spec()],
    oct_background:entity_selector()
) ->
    object() | [object()].
create_and_sync_file_tree(UserSelector, ParentSelector, FilesDesc, CreationProviderSelector) ->
    % if there are any custom labels files must be created sequentially, as labelled files must be created before their specification
    Mode = case contains_any_custom_labels(FilesDesc) of
        true -> sequential;
        false -> parallel
    end,
    create_and_sync_file_tree(UserSelector, ParentSelector, FilesDesc, CreationProviderSelector, Mode).


-spec mv_and_sync_file(oct_background:entity_selector(), object_selector(), file_meta:path()) ->
    ok.
mv_and_sync_file(UserSelector, FileSelector, DstPath) ->
    UserId = oct_background:get_user_id(UserSelector),
    {FileGuid, SpaceId} = resolve_file(FileSelector),
    [MvProvider | RestProviders] = lists_utils:shuffle(oct_background:get_space_supporting_providers(
        SpaceId
    )),

    mv_file(UserId, FileGuid, DstPath, MvProvider),

    lists:foreach(fun(Provider) ->
        Node = ?OCT_RAND_OP_NODE(Provider),
        UserSessId = oct_background:get_user_session_id(UserId, Provider),
        ?assertEqual({ok, DstPath}, lfm_proxy:get_file_path(Node, UserSessId, FileGuid), ?ATTEMPTS)
    end, RestProviders).


-spec rm_and_sync_file(oct_background:entity_selector(), object_selector()) ->
    ok.
rm_and_sync_file(UserSelector, FileSelector) ->
    UserId = oct_background:get_user_id(UserSelector),
    {FileGuid, SpaceId} = resolve_file(FileSelector),
    [RmProvider | RestProviders] = lists_utils:shuffle(oct_background:get_space_supporting_providers(
        SpaceId
    )),

    rm_file(UserId, FileGuid, RmProvider),

    lists:foreach(fun(Provider) ->
        Node = ?OCT_RAND_OP_NODE(Provider),
        UserSessId = oct_background:get_user_session_id(UserId, Provider),
        ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, UserSessId, ?FILE_REF(FileGuid)), ?ATTEMPTS)
    end, RestProviders).


-spec get_object_attributes(oct_background:entity_selector(), session:id(), file_id:file_guid()) ->
    {ok, object()} | {error, term()}.
get_object_attributes(Node, SessId, Guid) ->
    case file_test_utils:get_attrs(Node, SessId, Guid) of
        {ok, #file_attr{guid = Guid, name = Name, type = Type, mode = Mode, shares = Shares}} ->
            {ok, #object{
                guid = Guid, name = Name,
                type = Type, mode = Mode,
                shares = lists:sort(Shares)
            }};
        {error, _} = Error ->
            Error
    end.


-spec prepare_symlink_value(node(), session:id(), file_id:file_guid()) ->
    binary().
prepare_symlink_value(Node, SessId, FileGuid) ->
    SpaceId = file_id:guid_to_space_id(FileGuid),
    {ok, CanonicalPath} = lfm_proxy:get_file_path(Node, SessId, FileGuid),
    SpaceIdPrefix = ?SYMLINK_SPACE_ID_ABS_PATH_PREFIX(SpaceId),
    [_Sep, _SpaceId | Rest] = filename:split(CanonicalPath),
    filename:join([SpaceIdPrefix | Rest]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec contains_any_custom_labels(object_spec() | [object_spec()]) -> boolean().
contains_any_custom_labels(SpecAsList) when is_list(SpecAsList) ->
    lists:any(fun(SubSpec) ->
        contains_any_custom_labels(SubSpec)
    end, SpecAsList);
contains_any_custom_labels(#file_spec{custom_label = undefined}) ->
    false;
contains_any_custom_labels(#symlink_spec{custom_label = undefined}) ->
    false;
contains_any_custom_labels(#dir_spec{custom_label = undefined, children = ChildrenSpec}) ->
    contains_any_custom_labels(ChildrenSpec);
contains_any_custom_labels(_) ->
    true.


%% @private
-spec create_and_sync_file_tree(
    oct_background:entity_selector(),
    object_selector(),
    object_spec() | [object_spec()],
    oct_background:entity_selector(),
    parallel | sequential
) ->
    object() | [object()].
create_and_sync_file_tree(UserSelector, ParentSelector, FilesDesc, CreationProviderSelector, parallel) when is_list(FilesDesc) ->
    lists_utils:pmap(fun(FileDesc) ->
        {Res, _} = create_and_sync_file_tree(UserSelector, ParentSelector, FileDesc, CreationProviderSelector, parallel, #{}),
        Res
    end, FilesDesc);

create_and_sync_file_tree(UserSelector, ParentSelector, FilesDesc, CreationProviderSelector, sequential) when is_list(FilesDesc) ->
    {Res, _} = lists:foldl(fun(FileDesc, {Acc, TmpCustomLabelsMap}) ->
        {Object, NewIdsMap} = create_and_sync_file_tree(UserSelector, ParentSelector, FileDesc, CreationProviderSelector, sequential, TmpCustomLabelsMap),
        {[Object | Acc], NewIdsMap}
    end, {[], #{}}, FilesDesc),
    lists:reverse(Res);

create_and_sync_file_tree(UserSelector, ParentSelector, FilesDesc, CreationProviderSelector, Mode) ->
    {Res, _} = create_and_sync_file_tree(UserSelector, ParentSelector, FilesDesc, CreationProviderSelector, Mode, #{}),
    Res.


%% @private
-spec create_and_sync_file_tree(
    oct_background:entity_selector(),
    object_selector(),
    object_spec() | [object_spec()],
    oct_background:entity_selector(),
    parallel | sequential,
    custom_labels_map()
) ->
    {object() | [object()], custom_labels_map()}.
create_and_sync_file_tree(UserSelector, ParentSelector, FileDesc, CreationProviderSelector, Mode, CustomLabelsMap) ->
    UserId = oct_background:get_user_id(UserSelector),
    {ParentGuid, SpaceId} = resolve_file(ParentSelector),
    SupportingProviders = oct_background:get_space_supporting_providers(SpaceId),
    CreationProvider = oct_background:get_provider_id(CreationProviderSelector),
    SyncProviders = SupportingProviders -- [CreationProvider],
    
    {FileTree, FinalCustomLabelsMaps} = create_file_tree(UserId, ParentGuid, CreationProvider, FileDesc, Mode, CustomLabelsMap),
    await_sync(CreationProvider, SyncProviders, UserId, FileTree),
    await_parent_links_sync(SyncProviders, UserId, ParentGuid, FileTree),
    
    {FileTree, FinalCustomLabelsMaps}.


%% @private
-spec create_file_tree(
    od_user:id(),
    file_id:file_guid(),
    oct_background:entity_selector(),
    object_spec(),
    parallel | sequential,
    custom_labels_map()
) ->
    {object(), custom_labels_map()}.
create_file_tree(UserId, ParentGuid, CreationProvider, #file_spec{
    name = NameOrUndefined,
    mode = FileMode,
    shares = ShareSpecs,
    dataset = DatasetSpec,
    content = Content,
    metadata = MetadataSpec,
    custom_label = CustomLabel
}, _Mode, CustomLabelsMap) ->
    FileName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = ?OCT_RAND_OP_NODE(CreationProvider),

    {ok, FileGuid} = create_file(CreationNode, UserSessId, ParentGuid, FileName, FileMode),
    Content /= <<>> andalso write_file(CreationNode, UserSessId, FileGuid, Content),
    MetadataObj = create_metadata(CreationNode, UserSessId, FileGuid, MetadataSpec),

    DatasetObj = onenv_dataset_test_utils:set_up_dataset(
        CreationProvider, UserId, FileGuid, DatasetSpec
    ),

    {#object{
        guid = FileGuid,
        name = FileName,
        type = ?REGULAR_FILE_TYPE,
        mode = FileMode,
        shares = create_shares(CreationProvider, UserSessId, FileGuid, ShareSpecs),
        dataset = DatasetObj,
        content = Content,
        children = undefined,
        metadata = MetadataObj
    }, insert_custom_label(CustomLabel, FileGuid, CustomLabelsMap)};

create_file_tree(UserId, ParentGuid, CreationProvider, #symlink_spec{
    name = NameOrUndefined,
    symlink_value = SymlinkValue,
    dataset = DatasetSpec,
    custom_label = CustomLabel
}, _Mode, CustomLabelsMap) ->
    FileName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = lists_utils:random_element(oct_background:get_provider_nodes(CreationProvider)),

    FinalSymlinkValue = case SymlinkValue of
        {custom_label, TargetCustomLabel} ->
            prepare_symlink_value(CreationNode, UserSessId, maps:get(TargetCustomLabel, CustomLabelsMap));
        _ ->
            SymlinkValue
    end,
    {ok, #file_attr{guid = SymlinkGuid}} = create_symlink(CreationNode, UserSessId, ParentGuid, FileName, FinalSymlinkValue),

    DatasetObj = onenv_dataset_test_utils:set_up_dataset(
        CreationProvider, UserId, SymlinkGuid, DatasetSpec
    ),

    {#object{
        guid = SymlinkGuid,
        name = FileName,
        type = ?SYMLINK_TYPE,
        shares = [],
        children = undefined,
        content = undefined,
        dataset = DatasetObj,
        mode = ?DEFAULT_SYMLINK_PERMS,
        symlink_value = SymlinkValue
    }, insert_custom_label(CustomLabel, SymlinkGuid, CustomLabelsMap)};

create_file_tree(UserId, ParentGuid, CreationProvider, #hardlink_spec{
    name = NameOrUndefined,
    shares = ShareSpecs,
    target = Target,
    dataset = DatasetSpec,
    custom_label = CustomLabel
}, _Mode, CustomLabelsMap) ->
    FileName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = lists_utils:random_element(oct_background:get_provider_nodes(CreationProvider)),
    
    FinalTarget = case Target of
        {custom_label, TargetCustomLabel} ->
            maps:get(TargetCustomLabel, CustomLabelsMap);
        _ ->
            Target
    end,
    {ok, #file_attr{guid = HardlinkGuid}} = create_hardlink(CreationNode, UserSessId, ParentGuid, FileName, FinalTarget),
    
    DatasetObj = onenv_dataset_test_utils:set_up_dataset(
        CreationProvider, UserId, HardlinkGuid, DatasetSpec
    ),
    
    {#object{
        guid = HardlinkGuid,
        name = FileName,
        type = ?REGULAR_FILE_TYPE,
        shares = create_shares(CreationProvider, UserSessId, HardlinkGuid, ShareSpecs),
        children = undefined,
        content = undefined,
        dataset = DatasetObj,
        mode = ?DEFAULT_FILE_MODE
    }, insert_custom_label(CustomLabel, HardlinkGuid, CustomLabelsMap)};

create_file_tree(UserId, ParentGuid, CreationProvider, #dir_spec{
    name = NameOrUndefined,
    mode = DirMode,
    shares = ShareSpecs,
    dataset = DatasetSpec,
    children = ChildrenSpec,
    metadata = MetadataSpec,
    custom_label = CustomLabel
}, Mode, CustomLabelsMap) ->
    DirName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = ?OCT_RAND_OP_NODE(CreationProvider),

    {ok, DirGuid} = create_dir(CreationNode, UserSessId, ParentGuid, DirName, DirMode),

    {Children, UpdatedCustomLabelsMap} = case Mode of
        parallel ->
            {lists_utils:pmap(fun(File) ->
                {Res, _} = create_file_tree(UserId, DirGuid, CreationProvider, File, Mode, CustomLabelsMap),
                Res
            end, ChildrenSpec), CustomLabelsMap};
        sequential ->
            {ResList, LabelsMap} = lists:foldl(fun(File, {Acc, TmpCustomLabelsMap}) ->
                {Object, NewIdsMap} = create_file_tree(UserId, DirGuid, CreationProvider, File, Mode, TmpCustomLabelsMap),
                {[Object | Acc], NewIdsMap}
            end, {[], CustomLabelsMap}, ChildrenSpec),
            {lists:reverse(ResList), LabelsMap}
    end,
    MetadataObj = create_metadata(CreationNode, UserSessId, DirGuid, MetadataSpec),
    
    DatasetObj = onenv_dataset_test_utils:set_up_dataset(
        CreationProvider, UserId, DirGuid, DatasetSpec
    ),
    
    {#object{
        guid = DirGuid,
        name = DirName,
        type = ?DIRECTORY_TYPE,
        mode = DirMode,
        shares = create_shares(CreationProvider, UserSessId, DirGuid, ShareSpecs),
        dataset = DatasetObj,
        children = Children,
        metadata = MetadataObj
    }, insert_custom_label(CustomLabel, DirGuid, UpdatedCustomLabelsMap)}.



%% @private
-spec insert_custom_label(custom_label(), file_id:file_guid(), custom_labels_map()) -> 
    custom_labels_map().
insert_custom_label(undefined, _, CustomLabelsMap) ->
    CustomLabelsMap;
insert_custom_label(CustomLabel, Guid, CustomLabelsMap) ->
    CustomLabelsMap#{CustomLabel => Guid}.


%% @private
-spec create_shares(oct_background:entity_selector(), session:id(), file_id:file_guid(), [share_spec()]) ->
    [od_share:id()] | no_return().
create_shares(CreationProvider, SessId, FileGuid, ShareSpecs) ->
    CreationNode = ?OCT_RAND_OP_NODE(CreationProvider),
    lists:sort(lists:map(fun(#share_spec{name = Name, description = Description}) ->
        {ok, ShareId} = ?assertMatch(
            {ok, _},
            opt_shares:create(CreationNode, SessId, ?FILE_REF(FileGuid), Name, Description),
            ?ATTEMPTS
        ),
        ShareId
    end, ShareSpecs)).


%% @private
-spec await_sync(oct_background:entity_selector(), [oct_background:entity_selector()], od_user:id(), object()) ->
    ok | no_return().
await_sync(CreationProvider, SyncProviders, UserId, #object{
    type = ?REGULAR_FILE_TYPE,
    dataset = DatasetObj
} = Object) ->
    await_file_attr_sync(SyncProviders, UserId, Object),
    await_file_metadata_sync(SyncProviders, UserId, Object),
    onenv_dataset_test_utils:await_dataset_sync(CreationProvider, SyncProviders, UserId, DatasetObj),
    await_file_distribution_sync(CreationProvider, SyncProviders, UserId, Object);

await_sync(CreationProvider, SyncProviders, UserId, #object{
    type = ?SYMLINK_TYPE,
    dataset = DatasetObj
} = Object) ->
    % file_attr construction uses file_meta document, so this checks symlink value synchronization
    await_file_attr_sync(SyncProviders, UserId, Object),
    onenv_dataset_test_utils:await_dataset_sync(CreationProvider, SyncProviders, UserId, DatasetObj);

await_sync(CreationProvider, SyncProviders, UserId, #object{
    guid = DirGuid,
    type = ?DIRECTORY_TYPE,
    dataset = DatasetObj,
    children = Children
} = Object) ->
    await_file_attr_sync(SyncProviders, UserId, Object#object{children = undefined}),
    onenv_dataset_test_utils:await_dataset_sync(CreationProvider, SyncProviders, UserId, DatasetObj),

    ExpChildrenList = lists:keysort(2, lists_utils:pmap(fun(#object{guid = ChildGuid, name = ChildName} = Child) ->
        await_sync(CreationProvider, SyncProviders, UserId, Child),
        {ChildGuid, ChildName}
    end, Children)),

    await_dir_links_sync(SyncProviders, UserId, DirGuid, ExpChildrenList).


%% @private
-spec await_file_attr_sync([oct_background:entity_selector()], od_user:id(), object()) ->
    ok | no_return().
await_file_attr_sync(SyncProviders, UserId, #object{guid = Guid} = Object) ->
    lists:foreach(fun(SyncProvider) ->
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),
        SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),
        ExpObjectAttrs = Object#object{
            dataset = undefined,
            content = undefined,
            children = undefined,
            symlink_value = undefined,
            metadata = undefined
        },
        ?assertEqual({ok, ExpObjectAttrs}, get_object_attributes(SyncNode, SessId, Guid), ?ATTEMPTS)
    end, SyncProviders).


-spec await_file_metadata_sync([oct_background:entity_selector()], od_user:id(), object()) ->
    ok | no_return().
await_file_metadata_sync(_SyncProviders, _UserId, #object{metadata = undefined}) ->
    ok;
await_file_metadata_sync(SyncProviders, UserId, #object{guid = Guid, metadata = #metadata_object{
    json = ExpJson,
    rdf = ExpRdf,
    xattrs = ExpXattrs
}}) ->
    lists:foreach(fun(SyncProvider) ->
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),
        SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),

        await_json_metadata_sync(SyncNode, SessId, Guid, ExpJson),
        await_rdf_metadata_sync(SyncNode, SessId, Guid, ExpRdf),
        await_xattrs_sync(SyncNode, SessId, Guid, ExpXattrs)

    end, SyncProviders).


%% @private
-spec await_json_metadata_sync(node(), session:id(), file_id:file_guid(), undefined | json_utils:json_term()) -> ok.
await_json_metadata_sync(_SyncNode, _SessId, _FileGuid, undefined) ->
    ok;
await_json_metadata_sync(SyncNode, SessId, FileGuid, ExpJson) ->
    ?assertEqual({ok, ExpJson}, opt_file_metadata:get_custom_metadata(SyncNode, SessId, ?FILE_REF(FileGuid), json, [], false), ?ATTEMPTS),
    ok.


%% @private
-spec await_rdf_metadata_sync(node(), session:id(), file_id:file_guid(), undefined | binary()) -> ok.
await_rdf_metadata_sync(_SyncNode, _SessId, _FileGuid, undefined) ->
    ok;
await_rdf_metadata_sync(SyncNode, SessId, FileGuid, ExpRdf) ->
    ?assertEqual({ok, ExpRdf}, opt_file_metadata:get_custom_metadata(SyncNode, SessId, ?FILE_REF(FileGuid), rdf, [], false), ?ATTEMPTS),
    ok.


%% @private
-spec await_xattrs_sync(node(), session:id(), file_id:file_guid(), undefined | json_utils:json_term()) -> ok.
await_xattrs_sync(_SyncNode, _SessId, _FileGuid, undefined) ->
    ok;
await_xattrs_sync(SyncNode, SessId, FileGuid, ExpXattrs) ->
    ExpXattrsNamesSorted = lists:sort(maps:keys(ExpXattrs)),
    ListXattr = fun() ->
        case lfm_proxy:list_xattr(SyncNode, SessId, ?FILE_REF(FileGuid), false, false) of
            {ok, XattrNames} -> {ok, lists:sort(XattrNames)};
            Other -> Other
        end
    end,
    ?assertEqual({ok, ExpXattrsNamesSorted}, ListXattr(), ?ATTEMPTS),

    maps:fold(fun(XattrName, XattrValue, _) ->
        ?assertMatch({ok, #xattr{name = XattrName, value = XattrValue}},
            lfm_proxy:get_xattr(SyncNode, SessId, ?FILE_REF(FileGuid), XattrName), ?ATTEMPTS)
    end, undefined, ExpXattrs),
    ok.


%% @private
-spec await_file_distribution_sync(
    oct_background:entity_selector(),
    [oct_background:entity_selector()],
    od_user:id(),
    object()
) ->
    ok | no_return().
await_file_distribution_sync(_, _, _, #object{type = ?DIRECTORY_TYPE}) ->
    ok;
await_file_distribution_sync(CreationProvider, SyncProviders, UserId, #object{
    type = ?REGULAR_FILE_TYPE, guid = Guid, content = Content
}) ->
    case fslogic_file_id:is_link_uuid(file_id:guid_to_uuid(Guid)) of
        true ->
            ok;
        false ->
            lists:foreach(fun(SyncProvider) ->
                SessId = oct_background:get_user_session_id(UserId, SyncProvider),
                SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),
                ?assertDistribution(SyncNode, SessId,
                    ?DISTS(
                        [CreationProvider | SyncProviders],
                        [byte_size(Content) | lists:duplicate(length(SyncProviders), 0)]
                    ), Guid, ?ATTEMPTS)
            end, SyncProviders)
    end.


%% @private
-spec await_dir_links_sync(
    [oct_background:entity_selector()],
    od_user:id(),
    file_id:file_guid(),
    [{file_meta:name(), file_id:file_guid()}]
) ->
    ok | no_return().
await_dir_links_sync(SyncProviders, UserId, DirGuid, ExpChildrenList) ->
    lists:foreach(fun(SyncProvider) ->
        SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),
        ?assertEqual({ok, ExpChildrenList}, ls(SyncNode, SessId, DirGuid), ?ATTEMPTS)
    end, SyncProviders).


%% @private
-spec ls(oct_background:entity_selector(), session:id(), file_id:file_guid()) ->
    {ok, [{file_id:file_guid(), file_meta:name()}]} | {error, term()}.
ls(Node, SessId, Guid) ->
    ls(Node, SessId, Guid, undefined, []).


%% @private
-spec ls(
    node(),
    session:id(),
    file_id:file_guid(),
    file_listing:pagination_token() | undefined,
    [{file_id:file_guid(), file_meta:name()}]
) ->
    {ok, [{file_meta:name(), file_id:file_guid()}]} | {error, term()}.
ls(Node, SessId, Guid, NextPageToken, ChildEntriesAcc) ->
    ListOpts = case NextPageToken of
        undefined -> #{tune_for_large_continuous_listing => true};
        _ -> #{pagination_token => NextPageToken}
    end,
    case lfm_proxy:get_children(Node, SessId, ?FILE_REF(Guid), ListOpts#{limit => ?LS_SIZE}) of
        {ok, Children, ListingPaginationToken} ->
            AllChildEntries = ChildEntriesAcc ++ Children,

            case file_listing:is_finished(ListingPaginationToken) of
                true -> {ok, AllChildEntries};
                false -> ls(Node, SessId, Guid, ListingPaginationToken, AllChildEntries)
            end;
        Error ->
            Error
    end.


%% @private
-spec await_parent_links_sync(
    [oct_background:entity_selector()],
    od_user:id(),
    file_id:file_guid(),
    object()
) ->
    ok | no_return().
await_parent_links_sync(SyncProviders, UserId, ParentGuid, #object{
    guid = ChildGuid,
    name = ChildName
}) ->
    lists:foreach(fun(SyncProvider) ->
        SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),

        ?assertMatch(
            {ok, #file_attr{guid = ChildGuid}},
            lfm_proxy:get_child_attr(SyncNode, SessId, ParentGuid, ChildName),
            ?ATTEMPTS
        )
    end, SyncProviders).


%% @private
-spec create_file(node(), session:id(), file_id:file_guid(), file_meta:name(), file_meta:mode()) ->
    {ok, file_id:file_guid()} | no_return().
create_file(Node, SessId, ParentGuid, FileName, FileMode) ->
    ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, ParentGuid, FileName, FileMode)).


%% @private
-spec create_dir(node(), session:id(), file_id:file_guid(), file_meta:name(), file_meta:mode()) ->
    {ok, file_id:file_guid()} | no_return().
create_dir(Node, SessId, ParentGuid, FileName, FileMode) ->
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, ParentGuid, FileName, FileMode)).


%% @private
-spec create_symlink(node(), session:id(), file_id:file_guid(), file_meta:name(), file_meta_symlinks:symlink()) ->
    {ok, #file_attr{}} | no_return().
create_symlink(Node, SessId, ParentGuid, FileName, SymlinkValue) ->
    ?assertMatch({ok, _}, lfm_proxy:make_symlink(Node, SessId, ?FILE_REF(ParentGuid), FileName, SymlinkValue)).


%% @private
-spec create_hardlink(node(), session:id(), file_id:file_guid(), file_meta:name(), file_id:file_guid()) ->
    {ok, #file_attr{}} | no_return().
create_hardlink(Node, SessId, ParentGuid, FileName, TargetGuid) ->
    {ok, ParentPath} = lfm_proxy:get_file_path(Node, SessId, ParentGuid),
    ?assertMatch({ok, _}, lfm_proxy:make_link(Node, SessId, filename:join(ParentPath, FileName), TargetGuid)).


%% @private
-spec write_file(node(), session:id(), file_id:file_guid(), binary()) ->
    ok | no_return().
write_file(Node, SessId, FileGuid, Content) ->
    {ok, FileHandle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Node, FileHandle, 0, Content)),
    ?assertMatch(ok, lfm_proxy:close(Node, FileHandle)).


%% @private
-spec create_metadata(node(), session:id(), file_id:file_guid(), #metadata_spec{}) ->
    ok | no_return().
create_metadata(Node, SessId, FileGuid, #metadata_spec{
    json = Json,
    rdf = Rdf,
    xattrs = Xattrs
}) ->
    Json /= undefined andalso create_json_metadata(Node, SessId, FileGuid, Json),
    Rdf /= undefined andalso create_rdf_metadata(Node, SessId, FileGuid, Rdf),
    Xattrs /= undefined andalso create_xattrs(Node, SessId, FileGuid, Xattrs),
    
    #metadata_object{
        json = Json,
        rdf = Rdf,
        xattrs = Xattrs
    }.


%% @private
-spec create_json_metadata(node(), session:id(), file_id:file_guid(), json_utils:json_term()) -> ok.
create_json_metadata(Node, SessionId, FileGuid, Json) ->
    ?assertEqual(ok, opt_file_metadata:set_custom_metadata(Node, SessionId, ?FILE_REF(FileGuid), json, Json, [])).


%% @private
-spec create_rdf_metadata(node(), session:id(), file_id:file_guid(), binary()) -> ok.
create_rdf_metadata(Node, SessionId, FileGuid, Rdf) ->
    ?assertEqual(ok, opt_file_metadata:set_custom_metadata(Node, SessionId, ?FILE_REF(FileGuid), rdf, Rdf, [])).


%% @private
-spec create_xattrs(node(), session:id(), file_id:file_guid(), json_utils:json_term()) -> ok.
create_xattrs(Node, SessionId, FileGuid, Xattrs) ->
    maps:fold(fun(XattrKey, XattrValue, _) ->
        ?assertEqual(ok, lfm_proxy:set_xattr(Node, SessionId, ?FILE_REF(FileGuid), #xattr{
            name = XattrKey, value = XattrValue
        }))
    end, undefined, Xattrs),
    ok.


    %% @private
-spec mv_file(od_user:id(), file_id:file_guid(), file_meta:path(), oct_background:entity_selector()) ->
    ok.
mv_file(UserId, FileGuid, DstPath, MvProvider) ->
    MvNode = ?OCT_RAND_OP_NODE(MvProvider),
    UserSessId = oct_background:get_user_session_id(UserId, MvProvider),

    ?assertMatch({ok, _}, lfm_proxy:mv(MvNode, UserSessId, ?FILE_REF(FileGuid), DstPath)),
    ok.


%% @private
-spec rm_file(od_user:id(), file_id:file_guid(), oct_background:entity_selector()) ->
    ok.
rm_file(UserId, FileGuid, RmProvider) ->
    RmNode = ?OCT_RAND_OP_NODE(RmProvider),
    UserSessId = oct_background:get_user_session_id(UserId, RmProvider),

    ?assertMatch(ok, lfm_proxy:rm_recursive(RmNode, UserSessId, ?FILE_REF(FileGuid))).